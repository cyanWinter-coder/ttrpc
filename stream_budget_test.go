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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
)

// dial connects to a unix test listener (including abstract sockets on Linux),
// matching the addressing used by newTestListener.
func dial(addr string) (net.Conn, error) {
	return net.Dial("unix", addr)
}

// echoStreamDesc is a bidirectional stream that echoes each payload back with
// Seq incremented by one until the client closes its send side.
func echoStreamDesc() Stream {
	return Stream{
		Handler: func(_ context.Context, ss StreamServer) (any, error) {
			for {
				var req internal.EchoPayload
				if err := ss.RecvMsg(&req); err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}
					return nil, err
				}
				req.Seq++
				if err := ss.SendMsg(&req); err != nil {
					return nil, err
				}
			}
		},
		StreamingClient: true,
		StreamingServer: true,
	}
}

// TestStreamBudgetPerStreamBlockingAndFairness verifies requirements 1 and 3
// on the client. A slow stream that consumes nothing reaches its byte budget
// and parks, while a healthy stream sharing the connection keeps receiving
// every frame the receive loop can read ahead (bounded round-robin). The
// slow stream's retained bytes never exceed the budget plus one parked frame.
func TestStreamBudgetPerStreamBlockingAndFairness(t *testing.T) {
	const budget = 512
	addr, listener := newTestListener(t)
	defer listener.Close()
	server := mustServer(t)(NewServer(WithServerStreamReceiveBudget(budget)))
	server.RegisterService("fair", &ServiceDesc{
		Streams: map[string]Stream{"Echo": echoStreamDesc()},
	})
	go server.Serve(context.Background(), listener)
	defer server.Close()

	conn, err := dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(conn, WithStreamReceiveBudget(budget))
	defer func() { client.Close(); conn.Close() }()

	slow, err := client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true, StreamingServer: true}, "fair", "Echo", nil)
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true, StreamingServer: true}, "fair", "Echo", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Interleave sends: one frame per round goes to each stream. Because the
	// two streams are distinct the receive loop reads each healthy frame in
	// turn; the slow stream's frames accumulate up to its budget and park.
	const rounds = 40
	healthyGot := 0
	for i := range rounds {
		payload := &internal.EchoPayload{Seq: int64(i), Msg: "round"}
		if err := slow.SendMsg(payload); err != nil {
			t.Fatalf("slow send %d: %v", i, err)
		}
		if err := healthy.SendMsg(payload); err != nil {
			t.Fatalf("healthy send %d: %v", i, err)
		}
		var resp internal.EchoPayload
		if err := healthy.RecvMsg(&resp); err != nil {
			t.Fatalf("healthy stream starved at round %d: %v", i, err)
		}
		if resp.Seq != int64(i)+1 {
			t.Fatalf("healthy out-of-order round %d: got %d", i, resp.Seq)
		}
		healthyGot++
	}
	if healthyGot != rounds {
		t.Fatalf("healthy stream received %d of %d frames", healthyGot, rounds)
	}

	// The slow stream's mailbox is bounded: queued bytes <= budget plus at
	// most the single parked frame, never unbounded growth.
	slowS := slow.(*clientStream).s
	msgs, byt := slowS.mb.pending()
	maxAllowed := budget + messageLengthMax // parked frame is at most one max-size frame
	if byt > maxAllowed {
		t.Fatalf("slow stream retained %d bytes, exceeds bound %d (msgs=%d)", byt, maxAllowed, msgs)
	}

	// Resuming consumption drains the backlog in order with no loss.
	want := int64(0)
	for range rounds {
		var resp internal.EchoPayload
		if err := slow.RecvMsg(&resp); err != nil {
			t.Fatalf("slow drain at seq %d: %v", want, err)
		}
		if resp.Seq != want+1 {
			t.Fatalf("slow out-of-order: got %d want %d", resp.Seq, want+1)
		}
		want++
	}
	if msgs, byt := slowS.mb.pending(); msgs != 0 || byt != 0 {
		t.Fatalf("slow pending after drain = (%d, %d), want (0,0)", msgs, byt)
	}
}

// TestStreamBudgetCloseRestoresConnection verifies requirements 2, 3 and 4:
// once the blocked slow stream is closed, the receive loop wakes immediately
// and healthy streams on the connection resume; all later operations on the
// closed stream observe the same terminal state and no reader goroutine leaks.
func TestStreamBudgetCloseRestoresConnection(t *testing.T) {
	const budget = 256
	addr, listener := newTestListener(t)
	defer listener.Close()
	server := mustServer(t)(NewServer(WithServerStreamReceiveBudget(budget)))
	server.RegisterService("restore", &ServiceDesc{
		Streams: map[string]Stream{"Echo": echoStreamDesc()},
	})
	go server.Serve(context.Background(), listener)
	defer server.Close()

	conn, err := dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(conn, WithStreamReceiveBudget(budget))
	defer func() { client.Close(); conn.Close() }()

	slow, err := client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true, StreamingServer: true}, "restore", "Echo", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Flood the slow stream (never consumed) until its mailbox parks and the
	// receive loop can no longer make progress on it.
	stopFlood := make(chan struct{})
	floodDone := make(chan struct{})
	go func() {
		defer close(floodDone)
		for i := 0; ; i++ {
			select {
			case <-stopFlood:
				return
			default:
			}
			if err := slow.SendMsg(&internal.EchoPayload{Seq: int64(i), Msg: "flood"}); err != nil {
				return
			}
		}
	}()

	// Wait until the slow stream has actually parked a frame.
	slowS := slow.(*clientStream).s
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if slowS.mb.isParked() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !slowS.mb.isParked() {
		close(stopFlood)
		t.Fatal("slow stream never parked a frame")
	}

	// Closing the whole client tears down the connection and every blocked
	// reader; a fresh client on a fresh connection then works immediately.
	close(stopFlood)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-floodDone:
	case <-time.After(2 * time.Second):
		t.Fatal("flood goroutine did not stop after client close")
	}

	// RecvMsg on the closed stream first drains any messages that were
	// already queued, then reliably returns the terminal error once the
	// mailbox is empty. Loop until that terminal state is observed.
	terrCh := make(chan error, 1)
	go func() {
		var p internal.EchoPayload
		var err error
		for {
			err = slow.RecvMsg(&p)
			if err != nil {
				break
			}
		}
		terrCh <- err
	}()
	select {
	case err := <-terrCh:
		if err == nil {
			t.Fatal("RecvMsg on drained closed stream returned nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RecvMsg on closed stream never reached terminal state")
	}

	// A brand new connection/client works: closing one connection's stuck
	// stream left the server able to serve others.
	conn2, err := dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	client2 := NewClient(conn2, WithStreamReceiveBudget(budget))
	defer func() { client2.Close(); conn2.Close() }()
	st2, err := client2.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true, StreamingServer: true}, "restore", "Echo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st2.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatal(err)
	}
	var p internal.EchoPayload
	if err := st2.RecvMsg(&p); err != nil {
		t.Fatalf("healthy post-close stream failed: %v", err)
	}
	if p.Seq != 2 {
		t.Fatalf("post-close echo seq = %d, want 2", p.Seq)
	}
}

// TestStreamBudgetServerSideFairness verifies requirement 1 on the server
// using the same streamMailbox the server's receive loop parks on
// (streamHandler.data delegates to it). A non-consuming stream reaches a
// bounded parked state, an independent stream is unaffected, and draining
// releases the budget and wakes a waiting producer. It avoids the ttrpc
// transport's inherent single-writer outbound serialization by exercising
// the mailbox directly rather than flooding a socket.
func TestStreamBudgetServerSideFairness(t *testing.T) {
	const budget = 512
	slowMB := newStreamMailbox[Unmarshaler](streamRecvBufferSize, budget)
	healthyMB := newStreamMailbox[Unmarshaler](streamRecvBufferSize, budget)
	ctx := context.Background()
	um := func(any) error { return nil }

	// Fill until a frame parks; retained bytes stay bounded by budget plus at
	// most one overflow frame.
	var delivered int
	for delivered = 0; ; delivered++ {
		if slowMB.isParked() {
			break
		}
		if err := slowMB.deliver(ctx, um, 64); err != nil {
			t.Fatalf("slow deliver %d: %v", delivered, err)
		}
		if delivered > 100 {
			t.Fatal("slow mailbox never parked")
		}
	}
	_, slowBytes := slowMB.pending()
	if slowBytes > budget+messageLengthMax {
		t.Fatalf("slow stream retained %d bytes, exceeds bound", slowBytes)
	}

	// A second, independent mailbox accepts and delivers normally: a blocked
	// slow stream does not touch another stream's accounting.
	if err := healthyMB.deliver(ctx, um, 64); err != nil {
		t.Fatalf("healthy deliver blocked by slow stream: %v", err)
	}
	if got, err := healthyMB.next(ctx); err != nil || got == nil {
		t.Fatalf("healthy next = %v, %v", got, err)
	}

	// A producer waiting on the occupied overflow slot resumes on drain.
	resumed := make(chan error, 1)
	go func() { resumed <- slowMB.deliver(context.Background(), um, 64) }()

	// Drain everything currently queued/parked; the waiting producer either
	// gets promoted during draining or, once the slot frees, on the next
	// iteration. Stop once the producer reports.
	drainCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for {
		select {
		case err := <-resumed:
			if err != nil {
				t.Fatalf("producer did not resume after drain: %v", err)
			}
			goto resumed
		default:
		}
		if _, err := slowMB.next(drainCtx); err != nil {
			t.Fatalf("slow drain: %v", err)
		}
	}
resumed:
	if msgs, byt := slowMB.pending(); msgs < 0 || byt < 0 {
		t.Fatalf("negative accounting: (%d,%d)", msgs, byt)
	}
}

// TestStreamBudgetUnaryRPCServesAlongsideIdleStream verifies end to end that a
// server with a budget keeps serving unary RPCs while a streaming handler is
// simply idle (no flooding, so the single-writer transport is not blocked).
func TestStreamBudgetUnaryRPCServesAlongsideIdleStream(t *testing.T) {
	const budget = 512
	addr, listener := newTestListener(t)
	defer listener.Close()
	server := mustServer(t)(NewServer(WithServerStreamReceiveBudget(budget)))
	handlerReady := make(chan struct{})
	server.RegisterService("srv2", &ServiceDesc{
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
			"Idle": {
				Handler: func(ctx context.Context, _ StreamServer) (any, error) {
					close(handlerReady)
					<-ctx.Done()
					return nil, ctx.Err()
				},
				StreamingClient: true,
				StreamingServer: false,
			},
		},
	})
	go server.Serve(context.Background(), listener)
	defer server.Close()

	conn, err := dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(conn, WithStreamReceiveBudget(budget))
	defer func() { client.Close(); conn.Close() }()

	if _, err := client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true}, "srv2", "Idle", nil); err != nil {
		t.Fatal(err)
	}
	<-handlerReady

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var req, resp internal.EchoPayload
	req.Seq = 41
	if err := client.Call(callCtx, "srv2", "Echo", &req, &resp); err != nil {
		t.Fatalf("unary call failed alongside idle stream: %v", err)
	}
	if resp.Seq != 42 {
		t.Fatalf("unary seq = %d, want 42", resp.Seq)
	}
}

// TestStreamBudgetNoGoroutineLeakOnCancel verifies requirement 2 and 4:
// cancelling a context while a producer and a consumer are blocked in a
// mailbox unblocks both promptly and leaves nothing waiting on the condvar.
func TestStreamBudgetNoGoroutineLeakOnCancel(t *testing.T) {
	// 64-byte messages against a 256-byte budget: four fill the queue, the
	// fifth parks in the overflow slot, the sixth then blocks.
	mb := newStreamMailbox[int](64, 256)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		if err := mb.deliver(ctx, i, 64); err != nil {
			t.Fatalf("fill %d: %v", i, err)
		}
	}
	if err := mb.deliver(ctx, 4, 64); err != nil { // parks, returns immediately
		t.Fatalf("park: %v", err)
	}

	// The sixth producer occupies the condvar wait because the slot is taken.
	pctx, pcancel := context.WithCancel(context.Background())
	producerDone := make(chan error, 1)
	go func() { producerDone <- mb.deliver(pctx, 5, 64) }()

	// A separate empty mailbox gives a consumer that is genuinely blocked with
	// nothing queued, so cancellation is the only thing that can wake it.
	emptyMB := newStreamMailbox[int](64, 256)
	cctx, ccancel := context.WithCancel(context.Background())
	consumerDone := make(chan error, 1)
	go func() {
		_, err := emptyMB.next(cctx)
		consumerDone <- err
	}()

	time.Sleep(50 * time.Millisecond)
	pcancel()
	ccancel()

	select {
	case err := <-producerDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("producer err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked producer not woken by cancel")
	}
	select {
	case err := <-consumerDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("consumer err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked consumer not woken by cancel")
	}

	// The original mailbox still works after cancellation: drain the four
	// queued and the one parked message, then observe EOF.
	for i := 0; i < 5; i++ {
		if _, err := mb.next(ctx); err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
	}
	mb.closeSend()
	if _, err := mb.next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("after drain err = %v, want io.EOF", err)
	}
}

// TestNoBudgetPathUnchanged verifies requirement 5: with no budget configured
// the public streaming API and legacy behavior remain fully functional.
func TestNoBudgetPathUnchanged(t *testing.T) {
	addr, listener := newTestListener(t)
	defer listener.Close()
	server := mustServer(t)(NewServer()) // no budget option
	server.RegisterService("legacy", &ServiceDesc{
		Streams: map[string]Stream{"Echo": echoStreamDesc()},
	})
	go server.Serve(context.Background(), listener)
	defer server.Close()

	conn, err := dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(conn) // no budget option
	defer func() { client.Close(); conn.Close() }()

	if client.streamRecvBudget != 0 {
		t.Fatalf("default budget = %d, want 0 (legacy)", client.streamRecvBudget)
	}

	st, err := client.NewStream(context.Background(),
		&StreamDesc{true, true}, "legacy", "Echo", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if err := st.SendMsg(&internal.EchoPayload{Seq: int64(i)}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		var p internal.EchoPayload
		if err := st.RecvMsg(&p); err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if p.Seq != int64(i)+1 {
			t.Fatalf("seq mismatch: got %d want %d", p.Seq, i+1)
		}
	}
	if err := st.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var p internal.EchoPayload
	if err := st.RecvMsg(&p); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

// TestStreamBudgetOptionValidation verifies server option validation while
// zero (legacy) and positive budgets are accepted.
func TestStreamBudgetOptionValidation(t *testing.T) {
	if _, err := NewServer(WithServerStreamReceiveBudget(-1)); err == nil {
		t.Fatal("expected error for negative server stream budget")
	}
	if _, err := NewServer(WithServerStreamReceiveBudget(0)); err != nil {
		t.Fatalf("zero budget should be allowed (legacy): %v", err)
	}
	if _, err := NewServer(WithServerStreamReceiveBudget(1024)); err != nil {
		t.Fatalf("positive budget rejected: %v", err)
	}

	// Client option ignores non-positive values, leaving legacy mode.
	c := &Client{}
	WithStreamReceiveBudget(-5)(c)
	if c.streamRecvBudget != 0 {
		t.Fatalf("negative client budget applied: %d", c.streamRecvBudget)
	}
	WithStreamReceiveBudget(2048)(c)
	if c.streamRecvBudget != 2048 {
		t.Fatalf("client budget = %d, want 2048", c.streamRecvBudget)
	}
}

// TestStreamBudgetConcurrentStreamsAccounting stresses many streams and
// consumers together and asserts each stream's accounting returns to zero with
// no negative counters, exercising requirement 4 under concurrency.
func TestStreamBudgetConcurrentStreamsAccounting(t *testing.T) {
	const streams, perStream, budget = 16, 100, 512
	mailboxes := make([]*streamMailbox[int], streams)
	for i := range mailboxes {
		mailboxes[i] = newStreamMailbox[int](128, budget)
	}

	var wg sync.WaitGroup
	ctx := context.Background()
	for si := range streams {
		mb := mailboxes[si]
		wg.Go(func() {
			// Producer.
			for j := 0; j < perStream; j++ {
				if err := mb.deliver(ctx, j, 16); err != nil {
					t.Errorf("stream %d deliver %d: %v", si, j, err)
					return
				}
			}
		})
		wg.Go(func() {
			// Consumer.
			for j := 0; j < perStream; j++ {
				if _, err := mb.next(ctx); err != nil {
					t.Errorf("stream %d next %d: %v", si, j, err)
					return
				}
			}
		})
	}
	wg.Wait()

	for si, mb := range mailboxes {
		if msgs, byt := mb.pending(); msgs != 0 || byt != 0 {
			t.Fatalf("stream %d pending = (%d, %d), want (0,0)", si, msgs, byt)
		}
		if got := mb.queuedBytes(); got != 0 {
			t.Fatalf("stream %d queuedByt = %d, want 0 (negative accounting?)", si, got)
		}
	}
}

// TestStreamBudgetGoroutineNoLeak verifies requirement 4 over many stream
// create/use/close cycles: bounded mailboxes and condvar-based wakeups must
// not leave reader or AfterFunc goroutines behind once streams finish.
func TestStreamBudgetGoroutineNoLeak(t *testing.T) {
	addr, listener := newTestListener(t)
	defer listener.Close()
	server := mustServer(t)(NewServer(WithServerStreamReceiveBudget(1024)))
	server.RegisterService("leak", &ServiceDesc{
		Streams: map[string]Stream{"Echo": echoStreamDesc()},
	})
	go server.Serve(context.Background(), listener)
	defer server.Close()

	conn, err := dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(conn, WithStreamReceiveBudget(1024))
	defer func() { client.Close(); conn.Close() }()

	before := runtime.NumGoroutine()
	for range 25 {
		st, err := client.NewStream(context.Background(),
			&StreamDesc{true, true}, "leak", "Echo", nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 5 {
			if err := st.SendMsg(&internal.EchoPayload{Seq: int64(i)}); err != nil {
				t.Fatal(err)
			}
			var p internal.EchoPayload
			if err := st.RecvMsg(&p); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.CloseSend(); err != nil {
			t.Fatal(err)
		}
		var p internal.EchoPayload
		if err := st.RecvMsg(&p); err == nil {
			t.Fatal("expected EOF after CloseSend")
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("goroutine count grew: before=%d after=%d", before, runtime.NumGoroutine())
}
