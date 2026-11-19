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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestStreamBufferPolicyDefaultsPreserved verifies requirement 6: a
// zero-value policy behaves exactly like the historical fixed buffer and wait,
// so existing callers see no change.
func TestStreamBufferPolicyDefaultsPreserved(t *testing.T) {
	p := StreamBufferPolicy{}.normalize()
	if p.Capacity != streamRecvBufferSize {
		t.Fatalf("capacity = %d, want %d", p.Capacity, streamRecvBufferSize)
	}
	if p.Wait != time.Second {
		t.Fatalf("wait = %v, want 1s", p.Wait)
	}
	if p.FailFast {
		t.Fatal("zero-value policy must not be fail-fast")
	}

	if got := (StreamBufferPolicy{Capacity: 3}).normalize().Wait; got != time.Second {
		t.Fatalf("zero wait should select the default wait, got %v", got)
	}
	if got := (StreamBufferPolicy{FailFast: true}).normalize().Wait; got != 0 {
		t.Fatalf("fail-fast wait = %v, want 0", got)
	}
	if p := (StreamBufferPolicy{Wait: -time.Second}).normalize(); p.Wait != 0 || !p.FailFast {
		t.Fatalf("negative wait should behave like fail-fast, got %+v", p)
	}
	if got := (StreamBufferPolicy{Capacity: 4}).withCapacity(1).normalize().Capacity; got != 1 {
		t.Fatalf("withCapacity = %d, want 1", got)
	}
}

// TestStreamQueueFailFastDoesNotWait verifies requirement 1: a full buffer can
// be turned into an immediate per-stream error instead of a fixed wait.
func TestStreamQueueFailFastDoesNotWait(t *testing.T) {
	q := newStreamQueue[int](1, StreamBufferPolicy{Capacity: 1, FailFast: true}, nil)
	defer q.close(nil, true)

	ctx := context.Background()
	if err := q.push(ctx, 1); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := q.push(ctx, 2); !errors.Is(err, ErrStreamFull) {
		t.Fatalf("expected ErrStreamFull, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("fail-fast push waited %v", elapsed)
	}
	if stats := q.snapshot(); stats.Overflows != 1 || stats.Waits != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

// TestStreamQueueWaitIsCancellable verifies requirement 2: a producer blocked
// on a full buffer returns as soon as its context is cancelled instead of
// waiting out the budget.
func TestStreamQueueWaitIsCancellable(t *testing.T) {
	q := newStreamQueue[int](1, StreamBufferPolicy{Capacity: 1, Wait: time.Minute}, nil)
	defer q.close(nil, true)

	if err := q.push(context.Background(), 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- q.push(ctx, 2) }()

	deadline := time.Now().Add(2 * time.Second)
	for q.snapshot().Waits == 0 {
		if time.Now().After(deadline) {
			t.Fatal("producer never observed the full buffer")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("push did not return after context cancellation")
	}
}

// TestStreamQueueDeliversBufferedBeforeClose verifies requirement 4: a message
// which reached the buffer before the close is delivered, and the same stream
// never reports a successful read together with a close error.
func TestStreamQueueDeliversBufferedBeforeClose(t *testing.T) {
	q := newStreamQueue[int](1, StreamBufferPolicy{Capacity: 4}, nil)
	if err := q.push(context.Background(), 42); err != nil {
		t.Fatal(err)
	}
	q.close(ErrClosed, false)

	v, err := q.next(context.Background())
	if err != nil {
		t.Fatalf("buffered value lost: %v", err)
	}
	if v != 42 {
		t.Fatalf("got %d, want 42", v)
	}

	v, err = q.next(context.Background())
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if v != 0 {
		t.Fatalf("close returned a value %d alongside the error", v)
	}
}

// TestStreamQueueKeepsSpecificError verifies that a specific terminal error is
// not overwritten by the generic ErrClosed used during teardown.
func TestStreamQueueKeepsSpecificError(t *testing.T) {
	q := newStreamQueue[int](1, StreamBufferPolicy{Capacity: 1}, nil)
	q.close(ErrStreamFull, false)
	q.close(ErrClosed, true)

	if _, err := q.next(context.Background()); !errors.Is(err, ErrStreamFull) {
		t.Fatalf("expected ErrStreamFull, got %v", err)
	}
}

// TestStreamQueueDiscardReleasesItems verifies requirement 5 at the queue
// level: tearing a stream down releases queued items rather than leaking them.
func TestStreamQueueDiscardReleasesItems(t *testing.T) {
	var discarded int
	q := newStreamQueue[int](1, StreamBufferPolicy{Capacity: 4}, func(int) { discarded++ })

	for i := range 3 {
		if err := q.push(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	q.close(nil, true)

	if discarded != 3 {
		t.Fatalf("discarded %d items, want 3", discarded)
	}
	if stats := q.snapshot(); stats.Queued != 0 || stats.Discarded != 3 || stats.HighWatermark != 3 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

// TestStreamQueueConcurrentProducersAndConsumer exercises the broadcast wakeup
// path with several producers waiting on a small buffer and one consumer.
func TestStreamQueueConcurrentProducersAndConsumer(t *testing.T) {
	const (
		producers = 8
		perProd   = 25
	)
	q := newStreamQueue[int](1, StreamBufferPolicy{Capacity: 1, Wait: 10 * time.Second}, nil)
	defer q.close(nil, true)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for p := range producers {
		wg.Go(func() {
			for i := range perProd {
				if err := q.push(ctx, p*perProd+i); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		q.close(nil, false)
		close(done)
	}()

	count := 0
	for {
		_, err := q.next(ctx)
		if err != nil {
			break
		}
		count++
	}
	<-done
	if count != producers*perProd {
		t.Fatalf("consumed %d messages, want %d", count, producers*perProd)
	}
}

// TestStreamBufferObserverReportsPressure verifies requirement 2: buffer
// pressure is observable, including the high-watermark and overflow counters.
func TestStreamBufferObserverReportsPressure(t *testing.T) {
	var (
		mu       sync.Mutex
		seen     []StreamBufferStats
		observed = make(chan struct{}, 64)
	)
	observer := func(stats StreamBufferStats) {
		mu.Lock()
		seen = append(seen, stats)
		mu.Unlock()
		select {
		case observed <- struct{}{}:
		default:
		}
	}

	q := newStreamQueue[int](7, StreamBufferPolicy{Capacity: 2, FailFast: true, Observer: observer}, nil)
	defer q.close(nil, true)

	for i := range 2 {
		if err := q.push(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("observer was not notified at capacity")
	}

	if err := q.push(context.Background(), 99); !errors.Is(err, ErrStreamFull) {
		t.Fatalf("expected ErrStreamFull, got %v", err)
	}
	stats := q.snapshot()
	if stats.Overflows != 1 || stats.HighWatermark != stats.Capacity || stats.StreamID != 7 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if len(seen) == 0 {
		t.Fatal("observer never called")
	}
}

// echoStreamService returns a ServiceDesc with a unary Echo method and an
// echoing bidirectional stream, mirroring the generated code shape.
func echoStreamService(handler StreamHandler) *ServiceDesc {
	if handler == nil {
		handler = func(_ context.Context, ss StreamServer) (any, error) {
			for {
				var req internal.EchoPayload
				if err := ss.RecvMsg(&req); err != nil {
					if err == io.EOF {
						return nil, nil
					}
					return nil, err
				}
				req.Seq++
				if err := ss.SendMsg(&req); err != nil {
					return nil, err
				}
			}
		}
	}
	return &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				req.Seq++
				return &req, nil
			},
		},
		Streams: map[string]Stream{
			"EchoStream": {
				Handler:         handler,
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	}
}

// TestClientStreamPolicyFailsSlowStreamOnly verifies requirement 1 end to end:
// a client which stops consuming only stalls its own stream while the
// connection, unary calls, and other streams keep working.
func TestClientStreamPolicyFailsSlowStreamOnly(t *testing.T) {
	var (
		ctx             = context.Background()
		server          = mustServer(t)(NewServer())
		addr, listener  = newTestListener(t)
		client, cleanup = newTestClient(t, addr,
			WithClientStreamBufferPolicy(StreamBufferPolicy{Capacity: 1, FailFast: true}))
		serviceName = "slowClientService"
	)
	defer listener.Close()
	defer cleanup()

	server.RegisterService(serviceName, echoStreamService(nil))
	go server.Serve(ctx, listener)
	defer server.Close()

	abandoned, err := client.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, serviceName, "EchoStream", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Never consume the echoes; the client buffer (capacity 1) fills at once.
	for i := range 10 {
		if err := abandoned.SendMsg(&internal.EchoPayload{Seq: int64(i), Msg: "abandoned"}); err != nil {
			break
		}
	}

	// The abandoned stream must resolve to an error, not hang. Messages which
	// already reached the buffer are delivered first, so read until the
	// terminal error surfaces.
	perr := make(chan error, 1)
	go func() {
		var msg internal.EchoPayload
		for {
			if err := abandoned.RecvMsg(&msg); err != nil {
				perr <- err
				return
			}
		}
	}()
	select {
	case err := <-perr:
		if !errors.Is(err, ErrStreamFull) {
			t.Fatalf("expected ErrStreamFull on the abandoned stream, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RecvMsg on a full stream did not return")
	}

	// Other streams and unary calls on the same connection must be unaffected.
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var req, resp internal.EchoPayload
	req.Seq = 41
	if err := client.Call(callCtx, serviceName, "Echo", &req, &resp); err != nil {
		t.Fatalf("unary call blocked by an abandoned stream: %v", err)
	}
	if resp.Seq != 42 {
		t.Fatalf("unexpected sequence: got %d, want 42", resp.Seq)
	}

	second, err := client.NewStream(callCtx, &StreamDesc{StreamingClient: true, StreamingServer: true}, serviceName, "EchoStream", nil)
	if err != nil {
		t.Fatalf("NewStream blocked by an abandoned stream: %v", err)
	}
	if err := second.SendMsg(&internal.EchoPayload{Seq: 1, Msg: "ok"}); err != nil {
		t.Fatalf("second stream send failed: %v", err)
	}
	var secondResp internal.EchoPayload
	if err := second.RecvMsg(&secondResp); err != nil {
		t.Fatalf("second stream recv failed: %v", err)
	}
	if secondResp.Seq != 2 {
		t.Fatalf("unexpected sequence on second stream: got %d, want 2", secondResp.Seq)
	}
}

// startIdleStreamService starts a server exposing a stream handler that parks
// until its context is done, and returns a connected client.
func startIdleStreamService(t *testing.T, serviceName string, opts ...ClientOpts) *Client {
	t.Helper()

	ctx := context.Background()
	server := mustServer(t)(NewServer())
	addr, listener := newTestListener(t)
	client, cleanup := newTestClient(t, addr, opts...)
	t.Cleanup(func() {
		listener.Close()
		cleanup()
	})

	server.RegisterService(serviceName, &ServiceDesc{
		Streams: map[string]Stream{
			"Idle": {
				Handler: func(hctx context.Context, _ StreamServer) (any, error) {
					<-hctx.Done()
					return nil, hctx.Err()
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	})
	go server.Serve(ctx, listener)
	t.Cleanup(func() { server.Close() })

	return client
}

// TestClientStreamCancelUnblocksRecv verifies requirement 3: cancelling the
// caller's context makes a blocked read return promptly with a clear error.
func TestClientStreamCancelUnblocksRecv(t *testing.T) {
	client := startIdleStreamService(t, "cancelStreamService")

	streamCtx, cancel := context.WithCancel(context.Background())
	stream, err := client.NewStream(streamCtx, &StreamDesc{StreamingClient: true, StreamingServer: true}, "cancelStreamService", "Idle", nil)
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		var msg internal.EchoPayload
		errCh <- stream.RecvMsg(&msg)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RecvMsg did not return after context cancellation")
	}

	// Writes after cancellation must fail fast with a clear error too.
	if err := stream.SendMsg(&internal.EchoPayload{}); err == nil {
		t.Fatal("SendMsg after cancellation should fail")
	}
}

// TestClientCloseUnblocksStreams verifies requirement 3 on connection close:
// pending reads and writes return an explicit error instead of blocking.
func TestClientCloseUnblocksStreams(t *testing.T) {
	client := startIdleStreamService(t, "closeStreamService")

	stream, err := client.NewStream(context.Background(), &StreamDesc{StreamingClient: true, StreamingServer: true}, "closeStreamService", "Idle", nil)
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		var msg internal.EchoPayload
		errCh <- stream.RecvMsg(&msg)
	}()
	time.Sleep(20 * time.Millisecond)

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("RecvMsg returned nil after connection close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RecvMsg did not return after connection close")
	}

	if err := stream.SendMsg(&internal.EchoPayload{}); err == nil {
		t.Fatal("SendMsg after connection close should fail")
	}
}

// TestClientSendSerializerIsCancellable verifies that a request queued behind
// a stalled writer observes its own cancellation instead of waiting for the
// writer to drain.
func TestClientSendSerializerIsCancellable(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer serverSide.Close()
	client := NewClient(clientSide)
	defer client.Close()

	// Hold the serializer as an in-flight request would.
	if err := client.acquireSend(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := client.Call(ctx, "service", "method", &internal.EchoPayload{}, &internal.EchoPayload{})
	client.releaseSend()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("cancellable send wait took %v", elapsed)
	}
}

// TestServerStreamHandlerReleasesOnReturn verifies requirement 5: once the
// handler returns, messages it never consumed are released and the client sees
// the stream complete.
func TestServerStreamHandlerReleasesOnReturn(t *testing.T) {
	var (
		ctx             = context.Background()
		addr, listener  = newTestListener(t)
		client, cleanup = newTestClient(t, addr)
		serviceName     = "releaseService"
		handlerReady    = make(chan struct{})
	)

	var statsMu sync.Mutex
	var lastStats StreamBufferStats
	policy := StreamBufferPolicy{Capacity: 8, Observer: func(stats StreamBufferStats) {
		statsMu.Lock()
		lastStats = stats
		statsMu.Unlock()
	}}

	server, err := NewServer(WithServerStreamBufferPolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterService(serviceName, &ServiceDesc{
		Streams: map[string]Stream{
			"Slow": {
				Handler: func(hctx context.Context, _ StreamServer) (any, error) {
					// Signal readiness, then park without consuming. The
					// handler is released by the per-stream context when the
					// client closes its side or the connection ends.
					close(handlerReady)
					<-hctx.Done()
					return nil, hctx.Err()
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	})
	go server.Serve(ctx, listener)
	defer server.Close()
	defer listener.Close()
	defer cleanup()

	stream, err := client.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, serviceName, "Slow", nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerReady:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not start")
	}

	// Fill the server-side buffer, then close the client side. The handler is
	// parked, so these messages are never consumed by it.
	for i := range 4 {
		if err := stream.SendMsg(&internal.EchoPayload{Seq: int64(i)}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	// Closing the client tears the stream down; the server must release the
	// buffered messages instead of retaining them for the connection.
	client.Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		statsMu.Lock()
		stats := lastStats
		statsMu.Unlock()
		if stats.Closed && stats.Queued == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server stream buffer was not released: %+v", stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestServerStreamFailFastDoesNotStallConnection verifies requirement 1 on the
// server side: a handler which stops consuming ends only its own stream, and
// the connection keeps serving other requests.
func TestServerStreamFailFastDoesNotStallConnection(t *testing.T) {
	var (
		ctx             = context.Background()
		addr, listener  = newTestListener(t)
		client, cleanup = newTestClient(t, addr)
		serviceName     = "serverSlowService"
		handlerReady    = make(chan struct{})
		handlerCtx      = make(chan context.Context, 1)
	)

	server, err := NewServer(WithServerStreamBufferPolicy(StreamBufferPolicy{Capacity: 1, FailFast: true}))
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterService(serviceName, echoStreamService(func(hctx context.Context, _ StreamServer) (any, error) {
		handlerCtx <- hctx
		close(handlerReady)
		<-hctx.Done()
		return nil, hctx.Err()
	}))
	go server.Serve(ctx, listener)
	defer server.Close()
	defer listener.Close()
	defer cleanup()

	slow, err := client.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, serviceName, "EchoStream", nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerReady:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not start")
	}
	hctx := <-handlerCtx

	// Overflow the server buffer (capacity 1). The overflow must terminate
	// the stream instead of parking the receive goroutine.
	for i := range 10 {
		if err := slow.SendMsg(&internal.EchoPayload{Seq: int64(i)}); err != nil {
			break
		}
	}

	select {
	case <-hctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("server stream handler was not unblocked after buffer overflow")
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var req, resp internal.EchoPayload
	req.Seq = 99
	if err := client.Call(callCtx, serviceName, "Echo", &req, &resp); err != nil {
		t.Fatalf("unary call blocked by a full server stream: %v", err)
	}
	if resp.Seq != 100 {
		t.Fatalf("unexpected sequence: got %d, want 100", resp.Seq)
	}
}

// TestClientStreamStats verifies requirement 2: callers can observe a stream's
// buffer pressure through StreamBufferStatser.
func TestClientStreamStats(t *testing.T) {
	client := startIdleStreamService(t, "statsService",
		WithClientStreamBufferPolicy(StreamBufferPolicy{Capacity: 4, FailFast: true}))

	stream, err := client.NewStream(context.Background(), &StreamDesc{StreamingClient: true, StreamingServer: true}, "statsService", "Idle", nil)
	if err != nil {
		t.Fatal(err)
	}

	statser, ok := stream.(StreamBufferStatser)
	if !ok {
		t.Fatal("client stream does not expose StreamBufferStatser")
	}
	stats := statser.StreamBufferStats()
	if stats.Capacity != 4 {
		t.Fatalf("capacity = %d, want 4", stats.Capacity)
	}
	if stats.StreamID == 0 {
		t.Fatal("stream id should be recorded")
	}
}

// TestStreamResponderSingleTerminalFrame verifies requirement 4 on the server
// side: once a stream has been terminated only one terminal frame is emitted,
// and a handler which tries to send afterwards observes a close error instead
// of reporting a successful write.
func TestStreamResponderSingleTerminalFrame(t *testing.T) {
	var (
		mu        sync.Mutex
		responses []bool // closeStream flag per emitted response
	)
	r := &streamResponder{
		respond: func(_ *status.Status, _ []byte, _, closeStream bool) error {
			mu.Lock()
			responses = append(responses, closeStream)
			mu.Unlock()
			return nil
		},
	}

	if !r.terminate(status.New(codes.InvalidArgument, "data handling error")) {
		t.Fatal("first terminate should emit the terminal response")
	}
	if r.terminate(status.New(codes.Internal, "second")) {
		t.Fatal("second terminate must not emit another terminal response")
	}
	if err := r.call(nil, []byte("late"), true, false); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("expected ErrStreamClosed for data after termination, got %v", err)
	}

	if len(responses) != 1 || !responses[0] {
		t.Fatalf("unexpected responses: %v", responses)
	}
}

// TestServerStreamHandlerSendAfterTerminateFails verifies requirement 4: once a
// stream has been terminated, a handler write reports a close error instead of
// succeeding alongside the terminal close.
func TestServerStreamHandlerSendAfterTerminateFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu        sync.Mutex
		terminals int
	)
	sh := &streamHandler{
		ctx:    ctx,
		cancel: cancel,
		respond: &streamResponder{respond: func(*status.Status, []byte, bool, bool) error {
			mu.Lock()
			terminals++
			mu.Unlock()
			return nil
		}},
		recv: newStreamQueue[Unmarshaler](1, StreamBufferPolicy{Capacity: 2}, nil),
	}

	sh.respond.terminate(status.New(codes.InvalidArgument, "data handling error"))
	sh.terminate()

	// A write after termination must report the close rather than succeed next
	// to the terminal close, so the stream never reports both data and a close
	// error.
	if err := sh.SendMsg(&internal.EchoPayload{}); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("expected ErrStreamClosed after terminate, got %v", err)
	}
	if err := sh.RecvMsg(&internal.EchoPayload{}); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("expected ErrStreamClosed from RecvMsg after terminate, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if terminals != 1 {
		t.Fatalf("emitted %d terminal frames, want 1", terminals)
	}
}

// TestClientBufferedResponseSurvivesClose verifies requirement 4: a response
// which reached the client just before the connection closed is delivered
// instead of being replaced by ErrClosed, independent of scheduling.
func TestClientBufferedResponseSurvivesClose(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer serverSide.Close()

	client := NewClient(clientSide)
	defer client.Close()

	// A server which answers every request and then closes the connection.
	go func() {
		defer serverSide.Close()
		ch := newChannel(serverSide)
		for {
			mh, _, err := ch.recv()
			if err != nil {
				return
			}
			ch.send(mh.StreamID, messageTypeResponse, 0, nil)
			time.Sleep(time.Millisecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := range 50 {
		var resp Response
		if err := client.Call(ctx, "service", "method", &Request{}, &resp); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
}
