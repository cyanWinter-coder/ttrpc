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
	"io"
	"strings"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
)

// waitForStreamCount polls the client's stream registry until it holds the
// expected number of streams, failing the test if that does not happen in
// time.
func waitForStreamCount(t *testing.T, c *Client, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.streamLock.RLock()
		n := len(c.streams)
		c.streamLock.RUnlock()
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d registered streams, still have %d", want, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ownershipServer starts a server with an "Echo" method that answers
// immediately and a "Block" method that answers only once the returned
// release channel is closed (or the request context ends). The started
// channel receives one value per Block invocation, after the request has
// been unmarshaled.
func ownershipServer(t *testing.T, serviceName string) (addr string, started chan struct{}, release chan struct{}, cleanup func()) {
	t.Helper()

	server := mustServer(t)(NewServer())
	addr, listener := newTestListener(t)

	started = make(chan struct{}, 16)
	release = make(chan struct{})

	server.RegisterService(serviceName, &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.TestPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				return &internal.TestPayload{Foo: strings.Repeat(req.Foo, 2)}, nil
			},
			"Block": func(ctx context.Context, unmarshal func(any) error) (any, error) {
				var req internal.TestPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				started <- struct{}{}
				select {
				case <-release:
					return &internal.TestPayload{Foo: strings.Repeat(req.Foo, 2)}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	go server.Serve(ctx, listener)

	return addr, started, release, func() {
		cancel()
		server.Close()
		listener.Close()
	}
}

// TestUnaryCancelDiscardsLateResponse verifies that cancelling a unary call
// unregisters its request, and that the server's late response frame is
// discarded in full instead of being delivered to a later request on the
// same connection.
func TestUnaryCancelDiscardsLateResponse(t *testing.T) {
	const serviceName = "ownership"
	addr, started, release, serverCleanup := ownershipServer(t, serviceName)
	defer serverCleanup()

	client, clientCleanup := newTestClient(t, addr)
	defer clientCleanup()

	// Issue a unary call that blocks server-side, then cancel it.
	callCtx, cancel := context.WithCancel(context.Background())
	callErr := make(chan error, 1)
	go func() {
		var req, resp internal.TestPayload
		req.Foo = "first"
		callErr <- client.Call(callCtx, serviceName, "Block", &req, &resp)
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("server never received the blocking request")
	}
	cancel()

	select {
	case err := <-callErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled from cancelled call, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled call did not return")
	}

	// The cancelled request must no longer own a stream: late frames for
	// it have nowhere to be delivered.
	waitForStreamCount(t, client, 0)

	// Let the server send its late response, then give the receive loop a
	// moment to read and discard it.
	close(release)
	time.Sleep(200 * time.Millisecond)

	// A subsequent request on the same connection must get its own
	// response, not the late frame from the cancelled request.
	echoCtx, echoCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer echoCancel()
	var req, resp internal.TestPayload
	req.Foo = "second"
	if err := client.Call(echoCtx, serviceName, "Echo", &req, &resp); err != nil {
		t.Fatalf("call after late response failed: %v", err)
	}
	if resp.Foo != "secondsecond" {
		t.Fatalf("late frame leaked into later request: got %q, want %q", resp.Foo, "secondsecond")
	}
}

// TestClientCloseReturnsAllWaitersOnce verifies that closing the connection
// wakes every in-flight request exactly once with ErrClosed.
func TestClientCloseReturnsAllWaitersOnce(t *testing.T) {
	const (
		serviceName = "ownership-close"
		waiters     = 5
	)
	addr, started, _, serverCleanup := ownershipServer(t, serviceName)
	defer serverCleanup()

	client, clientCleanup := newTestClient(t, addr)
	defer clientCleanup()

	errs := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			var req, resp internal.TestPayload
			req.Foo = "waiter"
			errs <- client.Call(context.Background(), serviceName, "Block", &req, &resp)
		}()
	}

	for i := 0; i < waiters; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("server received only %d of %d requests", i, waiters)
		}
	}
	waitForStreamCount(t, client, waiters)

	if err := client.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Every waiter must return exactly once; the channel capacity
	// guarantees a second return would block and show up as an extra
	// value or a test timeout.
	for i := 0; i < waiters; i++ {
		select {
		case err := <-errs:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("waiter %d: expected ErrClosed, got %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d waiters returned after close", i, waiters)
		}
	}
	select {
	case err := <-errs:
		t.Fatalf("a waiter returned more than once: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	waitForStreamCount(t, client, 0)
}

// TestMaxConcurrentRequestsBackpressure verifies that once the configured
// concurrency limit is reached, new requests visibly block (observable via
// their context deadline), and that completing a request releases its slot.
func TestMaxConcurrentRequestsBackpressure(t *testing.T) {
	const serviceName = "ownership-bp"
	addr, started, release, serverCleanup := ownershipServer(t, serviceName)
	defer serverCleanup()

	client, clientCleanup := newTestClient(t, addr, WithMaxConcurrentRequests(1))
	defer clientCleanup()

	// Occupy the only slot with a blocked request.
	firstErr := make(chan error, 1)
	go func() {
		var req, resp internal.TestPayload
		req.Foo = "first"
		firstErr <- client.Call(context.Background(), serviceName, "Block", &req, &resp)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("server never received the first request")
	}
	waitForStreamCount(t, client, 1)

	// The next request cannot get a slot; its deadline makes the
	// backpressure visible to the caller.
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer blockedCancel()
	var req, resp internal.TestPayload
	req.Foo = "second"
	if err := client.Call(blockedCtx, serviceName, "Echo", &req, &resp); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded under backpressure, got %v", err)
	}

	// Completing the first request releases its slot, so a new request
	// goes through.
	close(release)
	select {
	case err := <-firstErr:
		if err != nil {
			t.Fatalf("first call failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first call did not return after release")
	}
	waitForStreamCount(t, client, 0)

	thirdCtx, thirdCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer thirdCancel()
	req.Foo = "third"
	resp.Foo = ""
	if err := client.Call(thirdCtx, serviceName, "Echo", &req, &resp); err != nil {
		t.Fatalf("call after slot release failed: %v", err)
	}
	if resp.Foo != "thirdthird" {
		t.Fatalf("unexpected response: got %q, want %q", resp.Foo, "thirdthird")
	}
}

// TestRequestTimeoutReleasesSlot verifies that a request which times out
// while waiting for its response releases its concurrency slot.
func TestRequestTimeoutReleasesSlot(t *testing.T) {
	const serviceName = "ownership-timeout"
	addr, started, _, serverCleanup := ownershipServer(t, serviceName)
	defer serverCleanup()

	client, clientCleanup := newTestClient(t, addr, WithMaxConcurrentRequests(1))
	defer clientCleanup()

	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer timeoutCancel()
	var req, resp internal.TestPayload
	req.Foo = "doomed"
	if err := client.Call(timeoutCtx, serviceName, "Block", &req, &resp); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("server never received the timed-out request")
	}

	// The timed-out request's slot must have been released: a new request
	// with the same limit of 1 proceeds instead of blocking forever.
	waitForStreamCount(t, client, 0)
	echoCtx, echoCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer echoCancel()
	req.Foo = "next"
	resp.Foo = ""
	if err := client.Call(echoCtx, serviceName, "Echo", &req, &resp); err != nil {
		t.Fatalf("call after timed-out request failed: %v", err)
	}
	if resp.Foo != "nextnext" {
		t.Fatalf("unexpected response: got %q, want %q", resp.Foo, "nextnext")
	}
}

// TestStreamCancelDiscardsLateFrames verifies that cancelling a stream's
// context unregisters it, that frames the server sends afterwards are
// discarded, and that the connection stays usable for other requests.
func TestStreamCancelDiscardsLateFrames(t *testing.T) {
	const serviceName = "ownership-stream"

	server := mustServer(t)(NewServer())
	addr, listener := newTestListener(t)
	defer listener.Close()

	stop := make(chan struct{})
	server.RegisterService(serviceName, &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.TestPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				return &internal.TestPayload{Foo: strings.Repeat(req.Foo, 2)}, nil
			},
		},
		Streams: map[string]Stream{
			"Chat": {
				Handler: func(ctx context.Context, ss StreamServer) (any, error) {
					// Keep sending frames until the connection or the
					// test stops us; after the client cancels, these
					// frames are late and must be discarded client-side.
					for i := int64(0); ; i++ {
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-stop:
							return nil, nil
						case <-time.After(10 * time.Millisecond):
						}
						if err := ss.SendMsg(&internal.EchoPayload{Seq: i, Msg: "tick"}); err != nil {
							return nil, err
						}
					}
				},
				StreamingServer: true,
			},
		},
	})
	defer close(stop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.Serve(ctx, listener)
	defer server.Close()

	client, clientCleanup := newTestClient(t, addr)
	defer clientCleanup()

	streamCtx, streamCancel := context.WithCancel(context.Background())
	cs, err := client.NewStream(streamCtx, &StreamDesc{StreamingServer: true}, serviceName, "Chat", &internal.TestPayload{Foo: "go"})
	if err != nil {
		t.Fatal(err)
	}

	var first internal.EchoPayload
	if err := cs.RecvMsg(&first); err != nil {
		t.Fatalf("failed to receive first stream message: %v", err)
	}
	waitForStreamCount(t, client, 1)

	// Cancel the stream: the client must stop waiting for its replies and
	// drop the frames the server keeps sending.
	streamCancel()
	waitForStreamCount(t, client, 0)

	// The connection must still be framed correctly and usable.
	echoCtx, echoCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer echoCancel()
	var req, resp internal.TestPayload
	req.Foo = "after"
	if err := client.Call(echoCtx, serviceName, "Echo", &req, &resp); err != nil {
		t.Fatalf("unary call after stream cancel failed: %v", err)
	}
	if resp.Foo != "afterafter" {
		t.Fatalf("unexpected response: got %q, want %q", resp.Foo, "afterafter")
	}

	// A RecvMsg on the cancelled stream must not yield late data.
	var late internal.EchoPayload
	if err := cs.RecvMsg(&late); err == nil {
		t.Fatalf("expected error from cancelled stream, got message %+v", &late)
	} else if err == io.EOF {
		// io.EOF is acceptable: the stream was retired.
	}
}
