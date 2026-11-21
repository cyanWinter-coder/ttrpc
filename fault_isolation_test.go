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
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestChannelMessageLengthBoundary ensures that a message exactly at the
// configured maximum is accepted while one byte more is rejected before any
// buffer is allocated.
func TestChannelMessageLengthBoundary(t *testing.T) {
	const max = 64

	w, r := net.Pipe()
	defer w.Close()
	defer r.Close()

	var (
		wch  = newChannelWithMessageLengthMax(w, max)
		rch  = newChannelWithMessageLengthMax(r, max)
		errs = make(chan error, 1)
	)

	// exactly at the limit: must go through
	go func() {
		errs <- wch.send(1, messageTypeRequest, 0, bytes.Repeat([]byte("a"), max))
	}()

	mh, p, err := rch.recv()
	if err != nil {
		t.Fatalf("unexpected error receiving boundary message: %v", err)
	}
	if mh.Length != max || len(p) != max {
		t.Fatalf("unexpected boundary message length: %v, %v", mh.Length, len(p))
	}
	if err := <-errs; err != nil {
		t.Fatalf("unexpected send error at boundary: %v", err)
	}

	// one byte over: rejected on send
	err = wch.send(1, messageTypeRequest, 0, bytes.Repeat([]byte("a"), max+1))
	if err == nil {
		t.Fatal("expected error sending message over the limit")
	}
	var oerr *OversizedMessageErr
	if !errors.As(err, &oerr) {
		t.Fatalf("expected OversizedMessageErr, got %T: %v", err, err)
	}
	if oerr.MaximumLength() != max {
		t.Fatalf("unexpected maximum length: %v != %v", oerr.MaximumLength(), max)
	}
	if oerr.RejectedLength() != max+1 {
		t.Fatalf("unexpected rejected length: %v != %v", oerr.RejectedLength(), max+1)
	}
}

// TestChannelRecvOversizedPoisonsConnection ensures that an oversized frame
// is rejected with a clear status error without waiting for a body that may
// never arrive, and that the connection is not parsed any further.
func TestChannelRecvOversizedPoisonsConnection(t *testing.T) {
	w, r := net.Pipe()
	defer w.Close()
	defer r.Close()

	rch := newChannelWithMessageLengthMax(r, 16)

	// The peer declares a huge frame and then stalls: the body never
	// arrives. The receiver must not block discarding the declared length.
	go func() {
		hdr := make([]byte, messageHeaderLength)
		writeMessageHeader(hdr, messageHeader{Length: 1 << 22, StreamID: 1, Type: messageTypeRequest})
		w.Write(hdr)
		// body is never sent; connection stays open
	}()

	mh, _, err := rch.recv()
	if err == nil {
		t.Fatal("expected error for oversized message")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted status, got: %v", err)
	}
	if mh.StreamID != 1 {
		t.Fatalf("expected valid header for oversized message, got stream %v", mh.StreamID)
	}

	// The channel is poisoned: the next recv must fail immediately with a
	// terminal (non-status) error instead of parsing residual bytes or
	// blocking on the missing body.
	done := make(chan error, 1)
	go func() {
		_, _, err := rch.recv()
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected terminal error on poisoned channel")
		}
		if _, ok := status.FromError(err); ok {
			t.Fatalf("expected terminal non-status error, got: %v", err)
		}
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("expected ErrProtocol, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("recv blocked on poisoned channel waiting for a half-frame")
	}
}

// TestChannelRecvTruncatedFrame ensures that a truncated header or body
// terminates parsing for the connection and that residual bytes are never
// treated as a new frame.
func TestChannelRecvTruncatedFrame(t *testing.T) {
	t.Run("header", func(t *testing.T) {
		w, r := net.Pipe()
		rch := newChannel(r)

		go func() {
			w.Write([]byte{0x00, 0x00, 0x00}) // half of a header
			w.Close()
		}()

		if _, _, err := rch.recv(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected ErrUnexpectedEOF for truncated header, got: %v", err)
		}

		// parsing does not resume: the same terminal error is reported
		// without touching the connection again.
		if _, _, err := rch.recv(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected terminal error to be reported again, got: %v", err)
		}
		r.Close()
	})

	t.Run("body", func(t *testing.T) {
		w, r := net.Pipe()
		rch := newChannel(r)

		go func() {
			hdr := make([]byte, messageHeaderLength)
			writeMessageHeader(hdr, messageHeader{Length: 8, StreamID: 1, Type: messageTypeRequest})
			w.Write(hdr)
			w.Write([]byte("abcd")) // only half of the declared body
			w.Close()
		}()

		if _, _, err := rch.recv(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected ErrUnexpectedEOF for truncated body, got: %v", err)
		}
		if _, _, err := rch.recv(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected terminal error to be reported again, got: %v", err)
		}
		r.Close()
	})
}

// partialWriteConn fails the first write after accepting failAfter bytes.
type partialWriteConn struct {
	net.Conn

	mu        sync.Mutex
	buf       bytes.Buffer
	failAfter int
	failed    bool
}

func (c *partialWriteConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.failed && len(p) > c.failAfter {
		c.failed = true
		n, _ := c.buf.Write(p[:c.failAfter])
		return n, errors.New("write failed")
	}
	return c.buf.Write(p)
}

// TestChannelSendPartialWriteProgress ensures that when a write only
// partially succeeds, the real progress is recorded and a subsequent send
// resumes at the right offset instead of re-writing the frame prefix.
func TestChannelSendPartialWriteProgress(t *testing.T) {
	conn := &partialWriteConn{failAfter: messageHeaderLength + 3}
	ch := newChannel(conn)

	payload := []byte("hello, partial world")
	if err := ch.send(1, messageTypeRequest, 0, payload); err == nil {
		t.Fatal("expected error from partial write")
	}

	// The retry completes the previous frame before writing the new one.
	if err := ch.send(1, messageTypeRequest, 0, payload); err != nil {
		t.Fatalf("expected send to resume after partial write: %v", err)
	}

	frame := make([]byte, messageHeaderLength+len(payload))
	writeMessageHeader(frame, messageHeader{Length: uint32(len(payload)), StreamID: 1, Type: messageTypeRequest})
	copy(frame[messageHeaderLength:], payload)

	expected := append(append([]byte{}, frame...), frame...)
	if !bytes.Equal(conn.buf.Bytes(), expected) {
		t.Fatalf("frame prefix written more than once: got %d bytes, expected %d", conn.buf.Len(), len(expected))
	}
}

// closeCountConn counts Close calls and reports a fixed error.
type closeCountConn struct {
	net.Conn

	closes   int32
	closeErr error
}

func (c *closeCountConn) Close() error {
	atomic.AddInt32(&c.closes, 1)
	return c.closeErr
}

// TestChannelClose ensures that closing a channel wakes blocked receivers,
// closes the underlying connection exactly once, and reports the close error
// only once.
func TestChannelClose(t *testing.T) {
	t.Run("wakes blocked recv", func(t *testing.T) {
		w, r := net.Pipe()
		defer w.Close()

		rch := newChannel(r)

		errs := make(chan error, 1)
		go func() {
			_, _, err := rch.recv()
			errs <- err
		}()

		// give the receiver a chance to block on the read
		time.Sleep(50 * time.Millisecond)

		if err := rch.close(); err != nil {
			t.Fatalf("unexpected close error: %v", err)
		}

		select {
		case err := <-errs:
			if err == nil {
				t.Fatal("expected blocked recv to be woken with an error")
			}
		case <-time.After(time.Second):
			t.Fatal("close did not wake blocked recv")
		}

		// recv on a closed channel returns immediately
		if _, _, err := rch.recv(); err == nil {
			t.Fatal("expected error receiving on closed channel")
		}
	})

	t.Run("error reported once", func(t *testing.T) {
		conn := &closeCountConn{closeErr: errors.New("close failed")}
		ch := newChannel(conn)

		for i := 0; i < 3; i++ {
			if err := ch.close(); err == nil || err.Error() != "close failed" {
				t.Fatalf("expected close error on every call, got: %v", err)
			}
		}
		if n := atomic.LoadInt32(&conn.closes); n != 1 {
			t.Fatalf("underlying connection closed %d times, expected 1", n)
		}
	})
}

// TestServerOversizedRequestRejected ensures that a request exceeding the
// server's configured message limit is rejected with a clear status error
// while requests within the limit are served normally.
func TestServerOversizedRequestRejected(t *testing.T) {
	var (
		ctx             = context.Background()
		server          = mustServer(t)(NewServer(WithServerMessageLengthMax(128)))
		addr, listener  = newTestListener(t)
		errs            = make(chan error, 1)
		client, cleanup = newTestClient(t, addr)
	)
	defer cleanup()
	defer listener.Close()
	go func() {
		errs <- server.Serve(ctx, listener)
	}()

	registerTestingService(server, &testingServer{})

	// a request within the limit works
	tp := &internal.TestPayload{Foo: "ok"}
	if err := client.Call(ctx, serviceName, "Test", tp, tp); err != nil {
		t.Fatalf("unexpected error for request within limit: %v", err)
	}
	if tp.Foo != "okok" {
		t.Fatalf("unexpected response: %q", tp.Foo)
	}

	// a request beyond the limit is rejected with a clear error
	tp = &internal.TestPayload{Foo: strings.Repeat("a", 1024)}
	err := client.Call(ctx, serviceName, "Test", tp, tp)
	if err == nil {
		t.Fatal("expected error from oversized request")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted status, got: %v", err)
	}

	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != ErrServerClosed {
		t.Fatal(err)
	}
}

// TestServerTruncatedFrameStopsParsing ensures that a truncated frame stops
// parsing on the connection: requests received before the corruption still
// get a response, residual bytes produce no further output, and the
// connection is cleaned up once the peer goes away.
func TestServerTruncatedFrameStopsParsing(t *testing.T) {
	var (
		ctx            = context.Background()
		server         = mustServer(t)(NewServer())
		addr, listener = newTestListener(t)
		errs           = make(chan error, 1)
	)
	defer listener.Close()
	go func() {
		errs <- server.Serve(ctx, listener)
	}()

	registerTestingService(server, &testingServer{})

	conn, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatal(err)
	}

	// send a valid request frame
	payload, err := proto.Marshal(&internal.TestPayload{Foo: "bar"})
	if err != nil {
		t.Fatal(err)
	}
	req, err := proto.Marshal(&Request{Service: serviceName, Method: "Test", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, messageHeaderLength+len(req))
	writeMessageHeader(frame, messageHeader{Length: uint32(len(req)), StreamID: 1, Type: messageTypeRequest})
	copy(frame[messageHeaderLength:], req)
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}

	// the valid request receives a valid response
	rmh, err := readMessageHeader(make([]byte, messageHeaderLength), conn)
	if err != nil {
		t.Fatalf("failed reading response header: %v", err)
	}
	body := make([]byte, rmh.Length)
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatalf("failed reading response body: %v", err)
	}
	var resp Response
	if err := proto.Unmarshal(body, &resp); err != nil {
		t.Fatalf("failed unmarshaling response: %v", err)
	}
	if resp.Status != nil && resp.Status.Code != int32(codes.OK) {
		t.Fatalf("unexpected response status: %v", resp.Status)
	}

	// corrupt the stream with a truncated frame
	if _, err := conn.Write([]byte{0xde, 0xad, 0xbe}); err != nil {
		t.Fatal(err)
	}

	// the residual bytes must not be parsed as a new frame: the server
	// sends nothing more.
	conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("unexpected data after truncated frame")
	} else if nerr, ok := err.(net.Error); !ok || !nerr.Timeout() {
		t.Fatalf("expected read timeout after truncated frame, got: %v", err)
	}

	conn.Close()

	// the server connection is cleaned up
	deadline := time.Now().Add(2 * time.Second)
	for server.countConnection() != 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := server.countConnection(); n != 0 {
		t.Fatalf("expected connection cleanup after truncated frame, got %d connections", n)
	}

	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != ErrServerClosed {
		t.Fatal(err)
	}
}

// blockingServer blocks in each call until the request context is canceled.
type blockingServer struct {
	started chan struct{}
}

func (s *blockingServer) Test(ctx context.Context, req *internal.TestPayload) (*internal.TestPayload, error) {
	s.started <- struct{}{}
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestClientConcurrentClose ensures that closing the connection wakes all
// in-flight calls with a definitive error.
func TestClientConcurrentClose(t *testing.T) {
	const calls = 8

	var (
		ctx             = context.Background()
		server          = mustServer(t)(NewServer())
		addr, listener  = newTestListener(t)
		serveErrs       = make(chan error, 1)
		client, cleanup = newTestClient(t, addr)
		impl            = &blockingServer{started: make(chan struct{}, calls)}
	)
	defer cleanup()
	defer listener.Close()
	go func() {
		serveErrs <- server.Serve(ctx, listener)
	}()

	registerTestingService(server, impl)

	var wg sync.WaitGroup
	callErrs := make([]error, calls)
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tp := &internal.TestPayload{Foo: "x"}
			callErrs[i] = client.Call(ctx, serviceName, "Test", tp, tp)
		}(i)
	}

	// wait until all calls are in flight on the server
	for i := 0; i < calls; i++ {
		select {
		case <-impl.started:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for calls to reach the server")
		}
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	wait := make(chan struct{})
	go func() {
		wg.Wait()
		close(wait)
	}()
	select {
	case <-wait:
	case <-time.After(2 * time.Second):
		t.Fatal("close did not wake in-flight calls")
	}

	for i, err := range callErrs {
		if err == nil {
			t.Fatalf("call %d: expected error after close, got nil", i)
		}
	}

	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-serveErrs; err != ErrServerClosed {
		t.Fatal(err)
	}
}
