/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package ttrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/containerd/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Client for a ttrpc server
type Client struct {
	codec   codec
	conn    net.Conn
	channel *channel

	streamLock   sync.RWMutex
	streams      map[streamID]*stream
	nextStreamID streamID
	sendLock     chan struct{}

	streamPolicy StreamBufferPolicy

	ctx    context.Context
	closed func()

	closeOnce       sync.Once
	userCloseFunc   func()
	userCloseWaitCh chan struct{}

	interceptor UnaryClientInterceptor
}

// ClientOpts configures a client
type ClientOpts func(c *Client)

// WithOnClose sets the close func whenever the client's Close() method is called
func WithOnClose(onClose func()) ClientOpts {
	return func(c *Client) {
		c.userCloseFunc = onClose
	}
}

// WithUnaryClientInterceptor sets the provided client interceptor
func WithUnaryClientInterceptor(i UnaryClientInterceptor) ClientOpts {
	return func(c *Client) {
		c.interceptor = i
	}
}

// WithClientStreamBufferPolicy configures per-stream receive buffering for
// streams created by this client. The zero value keeps the default policy.
//
// The policy bounds how many messages are buffered for each stream and how
// long the connection's receive loop waits for a slow consumer before failing
// only that stream with ErrStreamFull. It changes local behavior only and is
// compatible with any server.
func WithClientStreamBufferPolicy(policy StreamBufferPolicy) ClientOpts {
	return func(c *Client) {
		c.streamPolicy = policy
	}
}

// WithChainUnaryClientInterceptor sets the provided chain of client interceptors
func WithChainUnaryClientInterceptor(interceptors ...UnaryClientInterceptor) ClientOpts {
	return func(c *Client) {
		if len(interceptors) == 0 {
			return
		}
		if c.interceptor != nil {
			interceptors = append([]UnaryClientInterceptor{c.interceptor}, interceptors...)
		}
		c.interceptor = func(
			ctx context.Context,
			req *Request,
			reply *Response,
			info *UnaryClientInfo,
			final Invoker,
		) error {
			return interceptors[0](ctx, req, reply, info,
				chainUnaryInterceptors(interceptors[1:], final, info))
		}
	}
}

func chainUnaryInterceptors(interceptors []UnaryClientInterceptor, final Invoker, info *UnaryClientInfo) Invoker {
	if len(interceptors) == 0 {
		return final
	}
	return func(
		ctx context.Context,
		req *Request,
		reply *Response,
	) error {
		return interceptors[0](ctx, req, reply, info,
			chainUnaryInterceptors(interceptors[1:], final, info))
	}
}

// NewClient creates a new ttrpc client using the given connection
func NewClient(conn net.Conn, opts ...ClientOpts) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	channel := newChannel(conn)
	c := &Client{
		codec:           codec{},
		conn:            conn,
		channel:         channel,
		streams:         make(map[streamID]*stream),
		nextStreamID:    1,
		sendLock:        make(chan struct{}, 1),
		closed:          cancel,
		ctx:             ctx,
		userCloseFunc:   func() {},
		userCloseWaitCh: make(chan struct{}),
	}
	c.sendLock <- struct{}{}

	for _, o := range opts {
		o(c)
	}

	if c.interceptor == nil {
		c.interceptor = defaultClientInterceptor
	}

	go c.run()
	return c
}

// acquireSend acquires the connection-wide send serializer. The acquisition is
// cancellable: a request whose context expires while another request holds the
// serializer returns immediately instead of waiting for the writer, and a
// closing connection releases it.
func (c *Client) acquireSend(ctx context.Context) error {
	// Prefer the cancellation check over the lock so a caller which is
	// already done never competes for the serializer.
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-c.sendLock:
		if err := ctx.Err(); err != nil {
			c.releaseSend()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ctx.Done():
		return ErrClosed
	}
}

func (c *Client) releaseSend() {
	select {
	case c.sendLock <- struct{}{}:
	default:
	}
}

func (c *Client) send(ctx context.Context, sid uint32, mt messageType, flags uint8, b []byte) error {
	if err := c.acquireSend(ctx); err != nil {
		return err
	}
	defer c.releaseSend()

	if err := c.ctx.Err(); err != nil {
		return ErrClosed
	}
	return filterCloseErr(c.channel.send(sid, mt, flags, b))
}

// Call makes a unary request and returns with response
func (c *Client) Call(ctx context.Context, service, method string, req, resp any) error {
	payload, err := c.codec.Marshal(req)
	if err != nil {
		return err
	}

	var (
		creq = &Request{
			Service: service,
			Method:  method,
			Payload: payload,
			// TODO: metadata from context
		}

		cresp = &Response{}
	)

	if metadata, ok := GetMetadata(ctx); ok {
		metadata.setRequest(creq)
	}

	if dl, ok := ctx.Deadline(); ok {
		creq.TimeoutNano = time.Until(dl).Nanoseconds()
	}

	info := &UnaryClientInfo{
		FullMethod: fullPath(service, method),
	}
	if err := c.interceptor(ctx, creq, cresp, info, c.dispatch); err != nil {
		return err
	}

	if err := c.codec.Unmarshal(cresp.Payload, resp); err != nil {
		return err
	}

	if cresp.Status != nil && cresp.Status.Code != int32(codes.OK) {
		return status.ErrorProto(cresp.Status)
	}
	return nil
}

// StreamDesc describes the stream properties, whether the stream has
// a streaming client, a streaming server, or both
type StreamDesc struct {
	StreamingClient bool
	StreamingServer bool
}

// ClientStream is used to send or recv messages on the underlying stream
type ClientStream interface {
	CloseSend() error
	SendMsg(m any) error
	RecvMsg(m any) error
}

type clientStream struct {
	ctx          context.Context
	s            *stream
	c            *Client
	desc         *StreamDesc
	localClosed  bool
	remoteClosed bool
}

// StreamBufferStats returns a snapshot of the stream's receive buffer,
// allowing callers to observe per-stream backpressure.
func (cs *clientStream) StreamBufferStats() StreamBufferStats {
	return cs.s.stats()
}

func (cs *clientStream) CloseSend() error {
	if !cs.desc.StreamingClient {
		return fmt.Errorf("%w: cannot close non-streaming client", ErrProtocol)
	}
	if cs.localClosed {
		return ErrStreamClosed
	}
	err := cs.s.send(cs.ctx, messageTypeData, flagRemoteClosed|flagNoData, nil)
	if err != nil {
		return filterCloseErr(err)
	}
	cs.localClosed = true
	return nil
}

func (cs *clientStream) SendMsg(m any) error {
	if !cs.desc.StreamingClient {
		return fmt.Errorf("%w: cannot send data from non-streaming client", ErrProtocol)
	}
	if cs.localClosed {
		return ErrStreamClosed
	}

	var (
		payload []byte
		err     error
	)
	if m != nil {
		payload, err = cs.c.codec.Marshal(m)
		if err != nil {
			return err
		}
	}

	err = cs.s.send(cs.ctx, messageTypeData, 0, payload)
	if err != nil {
		return filterCloseErr(err)
	}

	return nil
}

func (cs *clientStream) RecvMsg(m any) error {
	if cs.remoteClosed {
		return io.EOF
	}

	msg, err := cs.s.queue.next(cs.ctx)
	if err != nil {
		// A cancelled call returns the context error; a torn-down connection
		// returns the terminal stream error. Neither path consumes a message,
		// so a read either succeeds with data or reports the close error.
		if cs.ctx.Err() != nil {
			// Nobody will consume this stream anymore, so unregister it and
			// release whatever it buffered instead of retaining it until the
			// peer finishes the stream.
			cs.c.deleteStream(cs.s)
		}
		return err
	}

	switch msg.header.Type {
	case messageTypeResponse:
		resp := &Response{}
		err := proto.Unmarshal(msg.payload[:msg.header.Length], resp)
		// return the payload buffer for reuse
		cs.c.channel.putmbuf(msg.payload)
		if err != nil {
			return err
		}

		if err := cs.c.codec.Unmarshal(resp.Payload, m); err != nil {
			return err
		}

		if resp.Status != nil && resp.Status.Code != int32(codes.OK) {
			return status.ErrorProto(resp.Status)
		}

		cs.c.deleteStream(cs.s)
		cs.remoteClosed = true

		return nil
	case messageTypeData:
		if !cs.desc.StreamingServer {
			cs.c.channel.putmbuf(msg.payload)
			cs.c.deleteStream(cs.s)
			cs.remoteClosed = true
			return fmt.Errorf("received data from non-streaming server: %w", ErrProtocol)
		}
		if msg.header.Flags&flagRemoteClosed == flagRemoteClosed {
			cs.c.deleteStream(cs.s)
			cs.remoteClosed = true

			if msg.header.Flags&flagNoData == flagNoData {
				cs.c.channel.putmbuf(msg.payload)
				return io.EOF
			}
		}

		err := cs.c.codec.Unmarshal(msg.payload[:msg.header.Length], m)
		cs.c.channel.putmbuf(msg.payload)
		if err != nil {
			return err
		}
		return nil
	default:
		cs.c.channel.putmbuf(msg.payload)
		cs.c.deleteStream(cs.s)
		cs.remoteClosed = true
		return fmt.Errorf("unexpected %q message received: %w", msg.header.Type, ErrProtocol)
	}
}

// Close closes the ttrpc connection and underlying connection
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed()

		c.conn.Close()
	})
	return nil
}

// UserOnCloseWait is used to block until the user's on-close callback
// finishes.
func (c *Client) UserOnCloseWait(ctx context.Context) error {
	select {
	case <-c.userCloseWaitCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) run() {
	err := c.receiveLoop()
	c.Close()
	c.cleanupStreams(err)

	c.userCloseFunc()
	close(c.userCloseWaitCh)
}

func (c *Client) receiveLoop() error {
	for {
		select {
		case <-c.ctx.Done():
			return ErrClosed
		default:
			var (
				msg = &streamMessage{}
				err error
			)

			msg.header, msg.payload, err = c.channel.recv()
			if err != nil {
				_, ok := status.FromError(err)
				if !ok {
					// treat all errors that are not an rpc status as terminal.
					// all others poison the connection.
					return filterCloseErr(err)
				}
			}
			sid := streamID(msg.header.StreamID)
			s := c.getStream(sid)
			if s == nil {
				if msg.payload != nil {
					c.channel.putmbuf(msg.payload)
				}
				log.G(c.ctx).WithField("stream", sid).Error("ttrpc: received message on inactive stream")
				continue
			}

			if err != nil {
				s.closeWithError(err)
			} else if err := s.receive(c.ctx, msg); err != nil {
				// If the stream became terminal, stop routing frames to it
				// while keeping its buffered messages for the consumer. On a
				// connection shutdown cleanupStreams still owns the stream.
				if s.terminalError() != nil {
					c.removeStream(s)
				}
				log.G(c.ctx).WithFields(log.Fields{"error": err, "stream": sid}).Error("ttrpc: failed to handle message")
			}
		}
	}
}

// createStream creates a new stream and registers it with the client.
// Introduce stream types for multiple or single response.
func (c *Client) createStream(ctx context.Context, flags uint8, b []byte, policy StreamBufferPolicy) (*stream, error) {
	// The send serializer must be held across both allocation of the stream ID
	// and sending it across the wire. This ensures that new stream IDs sent on
	// the wire are always increasing, which is a requirement of the TTRPC
	// protocol. Acquisition is cancellable so a caller queued behind a stalled
	// writer still observes its own deadline.
	if err := c.acquireSend(ctx); err != nil {
		return nil, err
	}
	defer c.releaseSend()

	// Check if closed since the serializer was acquired to prevent adding
	// anything after cleanup completes
	if err := c.ctx.Err(); err != nil {
		return nil, ErrClosed
	}

	var s *stream
	if err := func() error {
		// In the future this could be replaced with a sync.Map instead of streamLock+map.
		c.streamLock.Lock()
		defer c.streamLock.Unlock()

		// Check if closed since lock acquired to prevent adding
		// anything after cleanup completes
		if err := c.ctx.Err(); err != nil {
			return ErrClosed
		}

		s = newStream(c.nextStreamID, c, policy, func(msg *streamMessage) {
			if msg != nil && msg.payload != nil {
				c.channel.putmbuf(msg.payload)
			}
		})
		c.streams[s.id] = s
		c.nextStreamID = c.nextStreamID + 2

		return nil
	}(); err != nil {
		return nil, err
	}

	if err := c.channel.send(uint32(s.id), messageTypeRequest, flags, b); err != nil {
		c.deleteStream(s)
		return nil, filterCloseErr(err)
	}

	return s, nil
}

// removeStream unregisters a terminal stream while leaving its buffered
// messages for whoever is still consuming it. The stream ID is never reused,
// so no later frame can belong to it.
func (c *Client) removeStream(s *stream) {
	c.streamLock.Lock()
	delete(c.streams, s.id)
	c.streamLock.Unlock()
}

// deleteStream unregisters a finished stream. Buffered messages are released
// because the consumer has observed the end of the stream.
func (c *Client) deleteStream(s *stream) {
	c.removeStream(s)
	s.discard(nil)
}

func (c *Client) getStream(sid streamID) *stream {
	c.streamLock.RLock()
	s := c.streams[sid]
	c.streamLock.RUnlock()
	return s
}

// cleanupStreams terminates every remaining stream with err. Buffered
// messages stay readable for a consumer waiting on them, so a final response
// which raced with the connection close is still delivered before the error.
func (c *Client) cleanupStreams(err error) {
	c.streamLock.Lock()
	defer c.streamLock.Unlock()

	for sid, s := range c.streams {
		s.closeWithError(err)
		delete(c.streams, sid)
	}
}

// filterCloseErr rewrites EOF and EPIPE errors to ErrClosed. Use when
// returning from call or handling errors from main read loop.
//
// This purposely ignores errors with a wrapped cause.
func filterCloseErr(err error) error {
	switch {
	case err == nil:
		return nil
	case err == io.EOF:
		return ErrClosed
	case errors.Is(err, io.ErrClosedPipe):
		return ErrClosed
	case errors.Is(err, io.EOF):
		return ErrClosed
	case strings.Contains(err.Error(), "use of closed network connection"):
		return ErrClosed
	default:
		// if we have an epipe on a write or econnreset on a read , we cast to errclosed
		var oerr *net.OpError
		if errors.As(err, &oerr) {
			if (oerr.Op == "write" && errors.Is(err, syscall.EPIPE)) ||
				(oerr.Op == "read" && errors.Is(err, syscall.ECONNRESET)) {
				return ErrClosed
			}
		}
	}

	return err
}

// NewStream creates a new stream with the given stream descriptor to the
// specified service and method. If not a streaming client, the request object
// may be provided.
func (c *Client) NewStream(ctx context.Context, desc *StreamDesc, service, method string, req any) (ClientStream, error) {
	var payload []byte
	if req != nil {
		var err error
		payload, err = c.codec.Marshal(req)
		if err != nil {
			return nil, err
		}
	}

	request := &Request{
		Service: service,
		Method:  method,
		Payload: payload,
		// TODO: metadata from context
	}
	p, err := c.codec.Marshal(request)
	if err != nil {
		return nil, err
	}

	var flags uint8
	if desc.StreamingClient {
		flags = flagRemoteOpen
	} else {
		flags = flagRemoteClosed
	}
	s, err := c.createStream(ctx, flags, p, c.streamPolicy)
	if err != nil {
		return nil, err
	}

	return &clientStream{
		ctx:  ctx,
		s:    s,
		c:    c,
		desc: desc,
	}, nil
}

func (c *Client) dispatch(ctx context.Context, req *Request, resp *Response) error {
	p, err := c.codec.Marshal(req)
	if err != nil {
		return err
	}

	// A unary response is a single message, so there is no reason to buffer
	// more than one.
	s, err := c.createStream(ctx, 0, p, c.streamPolicy.withCapacity(1))
	if err != nil {
		return err
	}
	defer c.deleteStream(s)

	// Buffered messages are preferred over the terminal error, so a response
	// which raced with the connection close is still delivered to the caller.
	msg, err := s.queue.next(ctx)
	if err != nil {
		return err
	}

	if msg.header.Type == messageTypeResponse {
		err = proto.Unmarshal(msg.payload[:msg.header.Length], resp)
	} else {
		err = fmt.Errorf("unexpected %q message received: %w", msg.header.Type, ErrProtocol)
	}

	// return the payload buffer for reuse
	c.channel.putmbuf(msg.payload)

	return err
}
