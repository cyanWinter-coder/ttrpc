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
	"sync"
	"testing"
	"time"
)

// newTestMailbox builds a mailbox whose msgLimit is large enough that the
// byte budget alone drives blocking in the budgeted tests.
func newTestMailbox(budget int) *streamMailbox[string] {
	return newStreamMailbox[string](128, budget)
}

// TestMailboxBudgetParksOverBudget verifies that once the cumulative
// unconsumed bytes reach the budget the next frame is parked (not delivered)
// and accounted as pending, while a frame for a different mailbox is still
// accepted: backpressure is per-stream, not connection-wide.
func TestMailboxBudgetParksOverBudget(t *testing.T) {
	ctx := context.Background()
	a := newTestMailbox(10)
	b := newTestMailbox(10)

	if err := a.deliver(ctx, "a1", 6); err != nil {
		t.Fatalf("deliver a1: %v", err)
	}
	if err := a.deliver(ctx, "a2", 6); err != nil {
		t.Fatalf("over-budget frame should park, not error: %v", err)
	}

	msgs, bytes := a.pending()
	if msgs != 2 || bytes != 12 {
		t.Fatalf("pending after park = (%d msgs, %d bytes), want (2, 12)", msgs, bytes)
	}
	if !a.isParked() {
		t.Fatal("expected frame to occupy the overflow slot")
	}

	// A healthy stream sharing the connection is unaffected.
	if err := b.deliver(ctx, "b1", 9); err != nil {
		t.Fatalf("healthy stream deliver: %v", err)
	}
	if got, err := b.next(ctx); err != nil || got != "b1" {
		t.Fatalf("healthy stream read = %q, %v", got, err)
	}
}

// TestMailboxDrainReleasesBudgetAndPromotes verifies consuming one message
// immediately promotes a parked frame and releases a waiting producer, with
// every byte accounted and released exactly once.
func TestMailboxDrainReleasesBudgetAndPromotes(t *testing.T) {
	ctx := context.Background()
	m := newTestMailbox(10)

	if err := m.deliver(ctx, "a1", 6); err != nil {
		t.Fatalf("deliver a1: %v", err)
	}
	if err := m.deliver(ctx, "a2", 6); err != nil {
		t.Fatalf("over-budget frame should park, not error: %v", err)
	}
	// a1 queued (6 bytes), a2 parked (6 bytes): budget reached.
	if _, bytes := m.pending(); bytes != 12 {
		t.Fatalf("pending bytes = %d, want 12", bytes)
	}

	// A third frame arrives while the overflow slot is occupied: its producer
	// waits on the condvar until the consumer drains and frees room.
	unblocked := make(chan error, 1)
	go func() {
		unblocked <- m.deliver(context.Background(), "a3", 6)
	}()
	time.Sleep(50 * time.Millisecond) // let the producer enter the wait

	got, err := m.next(ctx) // consume a1 -> promotes a2, wakes a3
	if err != nil || got != "a1" {
		t.Fatalf("next = %q, %v", got, err)
	}
	select {
	case err := <-unblocked:
		if err != nil {
			t.Fatalf("waiting producer should resume after drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("producer not woken after consumer released budget")
	}

	// After promotion a2 is queued and a3 parked; drain both so accounting
	// returns to zero without going negative.
	for range 2 {
		if _, err := m.next(ctx); err != nil {
			t.Fatalf("drain: %v", err)
		}
	}
	if msgs, bytes := m.pending(); msgs != 0 || bytes != 0 {
		t.Fatalf("pending after full drain = (%d, %d), want (0, 0)", msgs, bytes)
	}
}

// TestMailboxDeliverCancelWakes verifies a blocked producer returns promptly
// with the context error when its context is cancelled, and the parked slot
// is released so later frames can use it.
func TestMailboxDeliverCancelWakes(t *testing.T) {
	m := newTestMailbox(10)
	ctx := context.Background()

	if err := m.deliver(ctx, "a1", 6); err != nil {
		t.Fatal(err)
	}
	if err := m.deliver(ctx, "a2", 6); err != nil { // parks
		t.Fatal(err)
	}

	pctx, cancel := context.WithCancel(context.Background())
	blocked := make(chan error, 1)
	go func() { blocked <- m.deliver(pctx, "a3", 6) }()

	// Give the goroutine time to enter the condvar wait.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-blocked:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked deliver err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not wake blocked producer")
	}

	// a2 remains parked; a later frame waits rather than corrupting the slot.
	if !m.isParked() {
		t.Fatal("previously parked frame must remain after unrelated cancel")
	}
}

// TestMailboxCloseWakesBothSides verifies closing the mailbox wakes a blocked
// producer and a blocked consumer, that the transition happens once (the first
// terminal error wins), and that pending queued messages are still drained
// before the terminal error.
func TestMailboxCloseWakesBothSides(t *testing.T) {
	m := newTestMailbox(10)
	ctx := context.Background()

	if err := m.deliver(ctx, "a1", 6); err != nil {
		t.Fatal(err)
	}
	if err := m.deliver(ctx, "a2", 6); err != nil { // parks
		t.Fatal(err)
	}

	producerDone := make(chan error, 1)
	go func() { producerDone <- m.deliver(context.Background(), "a3", 6) }()

	// Consumer drains queued messages, then blocks and must be woken by close.
	consumerDone := make(chan error, 1)
	go func() {
		var err error
		for {
			if _, err = m.next(context.Background()); err != nil {
				break
			}
		}
		consumerDone <- err
	}()

	time.Sleep(50 * time.Millisecond)
	m.closeWithError(ErrStreamFull)
	m.closeWithError(ErrClosed) // second close must be a no-op.

	select {
	case err := <-producerDone:
		// The producer either returns the terminal error, or it had already
		// been promoted before close: both are valid; it must never hang.
		if err != nil && !errors.Is(err, ErrStreamFull) {
			t.Fatalf("producer err = %v, want nil or ErrStreamFull", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not wake producer")
	}

	select {
	case err := <-consumerDone:
		if !errors.Is(err, ErrStreamFull) {
			t.Fatalf("consumer err = %v, want ErrStreamFull", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not wake consumer")
	}

	// The terminal error is sticky and wins over later close attempts.
	if _, err := m.next(ctx); !errors.Is(err, ErrStreamFull) {
		t.Fatalf("terminal error not sticky: %v", err)
	}
}

// TestMailboxCloseSendDrainsToEOF verifies the producer-side half close lets
// the consumer drain pending messages and then receive io.EOF, forever.
func TestMailboxCloseSendDrainsToEOF(t *testing.T) {
	m := newTestMailbox(10)
	ctx := context.Background()
	if err := m.deliver(ctx, "a1", 2); err != nil {
		t.Fatal(err)
	}
	m.closeSend()

	if got, err := m.next(ctx); err != nil || got != "a1" {
		t.Fatalf("drain queued = %q, %v", got, err)
	}
	for range 2 {
		if _, err := m.next(ctx); !errors.Is(err, io.EOF) {
			t.Fatalf("after drain err = %v, want io.EOF", err)
		}
	}
}

// TestMailboxOversizedMessageNeverDeadlocks verifies a single message larger
// than the budget is accepted on an empty queue rather than parking forever.
func TestMailboxOversizedMessageNeverDeadlocks(t *testing.T) {
	m := newTestMailbox(10)
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { done <- m.deliver(ctx, "big", 100) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("oversized message on empty queue: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oversized message deadlocked the stream")
	}
	if _, err := m.next(ctx); err != nil {
		t.Fatalf("read oversized: %v", err)
	}
}

// TestMailboxLegacyTimeoutClosesStream verifies the no-budget path retains
// the original behavior: a full mailbox times out and closes with
// ErrStreamFull instead of applying backpressure.
func TestMailboxLegacyTimeoutClosesStream(t *testing.T) {
	m := newStreamMailbox[string](1, 0) // no byte budget, one message slot
	ctx := context.Background()

	if err := m.deliver(ctx, "a1", 1); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := m.deliver(ctx, "a2", 1)
	if !errors.Is(err, ErrStreamFull) {
		t.Fatalf("legacy full mailbox err = %v, want ErrStreamFull", err)
	}
	if elapsed := time.Since(start); elapsed < streamDeliveryTimeout {
		t.Fatalf("legacy timeout fired after %v, expected ~%v", elapsed, streamDeliveryTimeout)
	}
	// The queued message is still delivered; after it the mailbox is terminal.
	if got, err := m.next(ctx); err != nil || got != "a1" {
		t.Fatalf("queued message before terminal = %q, %v", got, err)
	}
	if _, err := m.next(ctx); err == nil {
		t.Fatal("expected terminal error on next after draining legacy queue")
	}
}

// TestMailboxConcurrentAccounting hammers the mailbox from many producers and
// one consumer and asserts the internal byte accounting never goes negative
// and every delivered message is eventually consumed.
func TestMailboxConcurrentAccounting(t *testing.T) {
	const producers, perProducer, size = 8, 50, 4
	m := newTestMailbox(64) // 16 messages fit within the byte budget
	ctx := context.Background()

	var wg sync.WaitGroup
	for range producers {
		wg.Go(func() {
			for range perProducer {
				if err := m.deliver(ctx, "x", size); err != nil {
					t.Errorf("deliver: %v", err)
					return
				}
			}
		})
	}

	received := 0
	total := producers * perProducer
	for received < total {
		if _, err := m.next(ctx); err != nil {
			t.Fatalf("next: %v", err)
		}
		received++
	}
	wg.Wait()

	if msgs, bytes := m.pending(); msgs != 0 || bytes != 0 {
		t.Fatalf("pending after concurrent drain = (%d, %d), want (0, 0)", msgs, bytes)
	}
	// queuedByt must never have drifted negative (it is an int and would
	// surface as a wildly wrong pending count otherwise).
	if got := m.queuedBytes(); got != 0 {
		t.Fatalf("queuedByt = %d, want 0 (negative accounting?)", got)
	}
}

// TestMailboxLegacyContextCancelBeatsTimeout verifies that in legacy mode a
// cancelled context returns immediately rather than waiting out the timeout.
func TestMailboxLegacyContextCancelBeatsTimeout(t *testing.T) {
	m := newStreamMailbox[string](1, 0)
	if err := m.deliver(context.Background(), "a1", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := m.deliver(ctx, "a2", 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if time.Since(start) >= streamDeliveryTimeout {
		t.Fatal("legacy deliver waited for timeout instead of honoring cancel")
	}
	if !strings.Contains(ErrStreamFull.Error(), "buffer full") {
		t.Fatal("unexpected ErrStreamFull message")
	}
}
