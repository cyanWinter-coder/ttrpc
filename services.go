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
	"os"
	"path"
	"sync"
	"unsafe"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Method func(ctx context.Context, unmarshal func(any) error) (any, error)

type StreamHandler func(context.Context, StreamServer) (any, error)

type Stream struct {
	Handler         StreamHandler
	StreamingClient bool
	StreamingServer bool
}

type ServiceDesc struct {
	Methods map[string]Method
	Streams map[string]Stream
}

type serviceSet struct {
	services          map[string]*ServiceDesc
	unaryInterceptor  UnaryServerInterceptor
	streamInterceptor StreamServerInterceptor
}

func newServiceSet(interceptor UnaryServerInterceptor) *serviceSet {
	return &serviceSet{
		services:          make(map[string]*ServiceDesc),
		unaryInterceptor:  interceptor,
		streamInterceptor: defaultStreamServerInterceptor,
	}
}

func (s *serviceSet) register(name string, desc *ServiceDesc) {
	if _, ok := s.services[name]; ok {
		panic(fmt.Errorf("duplicate service %v registered", name))
	}

	s.services[name] = desc
}

func (s *serviceSet) unaryCall(ctx context.Context, method Method, info *UnaryServerInfo, data []byte) (p []byte, st *status.Status) {
	unmarshal := func(obj any) error {
		return protoUnmarshal(data, obj)
	}

	resp, err := s.unaryInterceptor(ctx, unmarshal, info, method)
	if err == nil {
		if isNil(resp) {
			err = errors.New("ttrpc: marshal called with nil")
		} else {
			p, err = protoMarshal(resp)
		}
	}

	st, ok := status.FromError(err)
	if !ok {
		st = status.New(convertCode(err), err.Error())
	}

	return p, st
}

func (s *serviceSet) streamCall(ctx context.Context, stream StreamHandler, info *StreamServerInfo, ss StreamServer) (p []byte, st *status.Status) {
	resp, err := s.streamInterceptor(ctx, ss, info, stream)
	if err == nil {
		p, err = protoMarshal(resp)
	}
	st, ok := status.FromError(err)
	if !ok {
		st = status.New(convertCode(err), err.Error())
	}
	return
}

func (s *serviceSet) handle(ctx context.Context, id uint32, req *Request, policy StreamBufferPolicy, respond func(*status.Status, []byte, bool, bool) error) (*streamHandler, error) {
	srv, ok := s.services[req.Service]
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, "service %v", req.Service)
	}

	if method, ok := srv.Methods[req.Method]; ok {
		go func() {
			ctx, cancel := getRequestContext(ctx, req)
			defer cancel()

			info := &UnaryServerInfo{
				FullMethod: fullPath(req.Service, req.Method),
			}
			p, st := s.unaryCall(ctx, method, info, req.Payload)

			respond(st, p, false, true)
		}()
		return nil, nil
	}
	if stream, ok := srv.Streams[req.Method]; ok {
		ctx, cancel := getRequestContext(ctx, req)
		info := &StreamServerInfo{
			FullMethod:      fullPath(req.Service, req.Method),
			StreamingClient: stream.StreamingClient,
			StreamingServer: stream.StreamingServer,
		}
		streamPolicy := policy
		if !stream.StreamingClient {
			// A non-streaming client sends at most the request payload.
			streamPolicy = streamPolicy.withCapacity(1)
		}
		responder := &streamResponder{respond: respond}
		sh := &streamHandler{
			ctx:     ctx,
			cancel:  cancel,
			respond: responder,
			recv:    newStreamQueue[Unmarshaler](id, streamPolicy, nil),
			info:    info,
		}
		go func() {
			defer cancel()
			p, st := s.streamCall(ctx, stream.Handler, info, sh)
			// The handler has returned, so nothing will consume buffered
			// messages anymore. Release them before the terminal response so
			// they do not outlive the handler and the stream cannot report a
			// successful write after it was torn down.
			sh.close()
			responder.call(st, p, stream.StreamingServer, true)
		}()

		// Empty proto messages serialized to 0 payloads,
		// so signatures like: rpc Stream(google.protobuf.Empty) returns (stream Data);
		// don't get invoked here, which causes hang on client side.
		// See https://github.com/containerd/ttrpc/issues/126
		if req.Payload != nil || !info.StreamingClient {
			unmarshal := func(obj any) error {
				return protoUnmarshal(req.Payload, obj)
			}
			if err := sh.data(ctx, unmarshal); err != nil {
				// The request payload could not be queued. Tear the stream
				// down here and emit the only terminal frame for it, so the
				// handler's goroutine cannot report a second outcome.
				sh.terminate()
				responder.call(status.New(convertCode(err), err.Error()), nil, false, true)
				return nil, errStreamResponded
			}
		}

		return sh, nil
	}
	return nil, status.Errorf(codes.Unimplemented, "method %v", req.Method)
}

// streamRecvBufferSize is the default number of messages buffered per stream.
// It should be large enough to absorb normal bursts and small enough that
// per-stream memory overhead stays trivial when many streams are open.
const streamRecvBufferSize = 64

// streamHandler is the server side of a stream. Received messages are queued
// in a bounded per-stream buffer so a handler which stops consuming applies
// backpressure to its own stream rather than to the connection's receive loop.
type streamHandler struct {
	ctx     context.Context
	cancel  context.CancelFunc
	respond *streamResponder
	recv    *streamQueue[Unmarshaler]
	info    *StreamServerInfo
}

// streamResponder serializes the terminal response of a stream with any data
// frames the handler sends. Exactly one terminal frame is emitted per stream,
// so a stream which is torn down by a data handling error cannot also report a
// successful completion, and the active request count is decremented once.
type streamResponder struct {
	mu      sync.Mutex
	done    bool
	respond func(*status.Status, []byte, bool, bool) error
}

func (r *streamResponder) call(st *status.Status, data []byte, streaming, closeStream bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.done {
		return ErrStreamClosed
	}
	if closeStream {
		r.done = true
	}
	return r.respond(st, data, streaming, closeStream)
}

func (r *streamResponder) closed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done
}

// terminate ends the stream with st unless it already ended. It reports
// whether this call was the one which emitted the terminal response.
func (r *streamResponder) terminate(st *status.Status) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.done {
		return false
	}
	r.done = true
	return r.respond(st, nil, false, true) == nil
}

// terminateAsync ends the stream like terminate, without blocking the caller.
//
// It is used from the connection receive loop, which must keep serving other
// streams even while a handler is blocked sending on this stream. The
// responder lock guarantees exactly one terminal frame is emitted, whichever
// of the handler or this call gets there first.
func (r *streamResponder) terminateAsync(st *status.Status) {
	go r.terminate(st)
}

// StreamBufferStats returns a snapshot of the handler's receive buffer.
func (s *streamHandler) StreamBufferStats() StreamBufferStats {
	return s.recv.snapshot()
}

// closeSend records that the remote will not send more data. Already buffered
// messages stay available so the handler can consume them before observing
// io.EOF.
func (s *streamHandler) closeSend() {
	s.recv.close(io.EOF, false)
}

// close releases messages buffered for a handler which will no longer consume
// them. Buffered entries hold references to the decoded payloads, so they must
// be dropped once the handler has returned or the stream is torn down.
func (s *streamHandler) close() {
	s.recv.close(ErrStreamClosed, true)
}

// terminate tears the stream down after a data handling failure: buffered
// messages are released and the handler's context is cancelled so a handler
// parked on stream state unwinds instead of waiting for the connection to end.
func (s *streamHandler) terminate() {
	s.close()
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *streamHandler) data(ctx context.Context, unmarshal Unmarshaler) error {
	err := s.recv.push(ctx, unmarshal)
	if err == nil {
		return nil
	}
	if errors.Is(err, errStreamBufferClosed) {
		return ErrStreamClosed
	}
	return err
}

func (s *streamHandler) SendMsg(m any) error {
	// A terminated stream reports the close error, so a handler cannot report
	// a successful write after the stream already ended for the client.
	if s.respond.closed() {
		return ErrStreamClosed
	}
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	p, err := protoMarshal(m)
	if err != nil {
		return err
	}
	return s.respond.call(nil, p, true, false)
}

func (s *streamHandler) RecvMsg(m any) error {
	unmarshal, err := s.recv.next(s.ctx)
	if err != nil {
		// Once the stream is terminal, its own error is more informative than
		// the cancellation used to unblock the handler.
		if terr := s.recv.terminalError(); terr != nil {
			return terr
		}
		return err
	}
	return unmarshal(m)
}

func protoUnmarshal(p []byte, obj any) error {
	switch v := obj.(type) {
	case proto.Message:
		if err := proto.Unmarshal(p, v); err != nil {
			return status.Errorf(codes.Internal, "ttrpc: error unmarshalling payload: %v", err.Error())
		}
	default:
		return status.Errorf(codes.Internal, "ttrpc: error unsupported request type: %T", v)
	}
	return nil
}

func protoMarshal(obj any) ([]byte, error) {
	if obj == nil {
		return nil, nil
	}

	switch v := obj.(type) {
	case proto.Message:
		r, err := proto.Marshal(v)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "ttrpc: error marshaling payload: %v", err.Error())
		}

		return r, nil
	default:
		return nil, status.Errorf(codes.Internal, "ttrpc: error unsupported response type: %T", v)
	}
}

// convertCode maps stdlib go errors into grpc space.
//
// This is ripped from the grpc-go code base.
func convertCode(err error) codes.Code {
	switch err {
	case nil:
		return codes.OK
	case io.EOF:
		return codes.OutOfRange
	case io.ErrClosedPipe, io.ErrNoProgress, io.ErrShortBuffer, io.ErrShortWrite, io.ErrUnexpectedEOF:
		return codes.FailedPrecondition
	case os.ErrInvalid:
		return codes.InvalidArgument
	case context.Canceled:
		return codes.Canceled
	case context.DeadlineExceeded:
		return codes.DeadlineExceeded
	}
	switch {
	case os.IsExist(err):
		return codes.AlreadyExists
	case os.IsNotExist(err):
		return codes.NotFound
	case os.IsPermission(err):
		return codes.PermissionDenied
	}
	return codes.Unknown
}

func fullPath(service, method string) string {
	return "/" + path.Join(service, method)
}

func isNil(resp any) bool {
	return (*[2]uintptr)(unsafe.Pointer(&resp))[1] == 0
}
