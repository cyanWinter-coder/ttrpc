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
	"io"
	"sync"
	"time"
)

// streamDeliveryTimeout is the maximum time a producer waits for a consumer
// to make room in a legacy (no byte budget configured) mailbox before the
// stream is closed with ErrStreamFull.
const streamDeliveryTimeout = time.Second

// queuedMessage pairs a delivered message with its accounted payload size.
type queuedMessage[T any] struct {
	msg  T
	size int
}

// streamMailbox is the per-stream handoff point between the connection
// receive loop (the sole producer) and the stream consumer (RecvMsg, the
// sole consumer). It owns the whole resource/cleanup state chain for one
// stream:
//
//   - the pending message queue, bounded by msgLimit,
//   - the cumulative unconsumed payload byte count, bounded by byteBudget
//     when byteBudget > 0,
//   - a single overflow slot (parked) which holds at most one frame that
//     currently exceeds the budget, keeping total retained memory bounded,
//   - the terminal state (closed / sendClosed) and its terminal error.
//
// Fairness (bounded round-robin): when a frame does not fit the budget it is
// moved into the overflow slot and deliver returns immediately, leaving the
// connection receive loop free to keep delivering frames of other, healthy
// streams. The loop only waits when another frame for the same already-parked
// stream arrives; consuming a single message frees room, promotes the parked
// frame, and wakes the loop. A slow stream can therefore delay but never
// permanently starve a healthy stream sharing the connection.
//
// Every state transition broadcasts on the condvar so blocked producers and
// consumers wake immediately; a context cancellation is wired to the same
// broadcast via context.AfterFunc.
//
// When byteBudget is 0 the mailbox runs in legacy mode: a frame that does not
// fit immediately waits streamDeliveryTimeout for the consumer before the
// stream is closed with ErrStreamFull. This preserves the pre-budget behavior
// for callers that do not configure a budget.
type streamMailbox[T any] struct {
	mu   sync.Mutex
	cond sync.Cond // signalled on every state change producers/consumers wait on

	queue  []queuedMessage[T] // pending messages, at most msgLimit entries
	parked *queuedMessage[T]  // single overflow slot, nil unless a frame awaits room
	// queuedByt is the sum of payload sizes of the entries in queue. The
	// total unconsumed bytes are queuedByt plus the parked frame's size.
	queuedByt int

	msgLimit   int
	byteBudget int // 0 disables byte-based bounding and selects legacy timeout mode

	closeOnce  sync.Once
	closed     bool
	sendClosed bool // producer signalled end-of-stream; drain queue, then io.EOF
	recvErr    error
}

func newStreamMailbox[T any](msgLimit, byteBudget int) *streamMailbox[T] {
	m := &streamMailbox[T]{
		queue:      make([]queuedMessage[T], 0, msgLimit),
		msgLimit:   msgLimit,
		byteBudget: byteBudget,
	}
	m.cond.L = &m.mu
	return m
}

// deliver hands a message of the given accounted payload size to the stream.
//
// In budgeted mode a frame that does not fit is moved into the single
// overflow slot and deliver returns at once (the frame is "parked"), so the
// connection receive loop keeps servicing other streams. A subsequent frame
// for the same parked stream makes deliver wait until the consumer makes room
// and the parked frame is promoted, or the stream/context terminates.
//
// In legacy mode (no byte budget) a full queue waits streamDeliveryTimeout for
// the consumer before the stream is closed with ErrStreamFull.
func (m *streamMailbox[T]) deliver(ctx context.Context, msg T, size int) error {
	if m.byteBudget == 0 {
		return m.deliverLegacy(ctx, msg, size)
	}
	return m.deliverBounded(ctx, msg, size)
}

// deliverBounded implements stream-level byte budget backpressure with
// park-and-continue fairness.
func (m *streamMailbox[T]) deliverBounded(ctx context.Context, msg T, size int) error {
	stop := context.AfterFunc(ctx, m.broadcast)
	defer stop()

	m.mu.Lock()
	defer m.mu.Unlock()

	for {
		if m.closed {
			// A frame arriving after termination never enters accounting;
			// a parked frame left over at termination is dropped here so no
			// retained bytes outlive the stream.
			m.parked = nil
			return m.recvErr
		}
		if err := ctx.Err(); err != nil {
			// A waiting deliver does not own the overflow slot: the parked
			// frame belongs to an earlier, already-accepted delivery, so it
			// must be left intact for another frame's producer to wait on.
			return err
		}
		if m.parked == nil {
			if m.fitsLocked(size) {
				m.enqueueLocked(queuedMessage[T]{msg: msg, size: size})
				m.cond.Broadcast()
				return nil
			}
			// Over budget: park the frame in the overflow slot and return so
			// the receive loop can continue with other streams. The frame's
			// bytes are counted as pending but never touch queuedByt.
			parked := queuedMessage[T]{msg: msg, size: size}
			m.parked = &parked
			m.cond.Broadcast()
			return nil
		}
		// Another frame for an already-parked stream: the slot holds the
		// earlier frame, so wait here until the consumer promotes it.
		m.cond.Wait()
	}
}

// deliverLegacy preserves the pre-budget behavior: when the queue is full the
// producer waits up to streamDeliveryTimeout for the consumer, otherwise the
// stream is closed with ErrStreamFull so one stuck stream cannot hold the
// connection forever.
func (m *streamMailbox[T]) deliverLegacy(ctx context.Context, msg T, size int) error {
	tctx, cancel := context.WithTimeout(ctx, streamDeliveryTimeout)
	defer cancel()
	stop := context.AfterFunc(tctx, m.broadcast)
	defer stop()

	m.mu.Lock()
	defer m.mu.Unlock()

	for len(m.queue) >= m.msgLimit {
		if m.closed {
			return m.recvErr
		}
		if err := tctx.Err(); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			m.closeLocked(ErrStreamFull)
			return ErrStreamFull
		}
		m.cond.Wait()
	}

	m.enqueueLocked(queuedMessage[T]{msg: msg, size: size})
	m.cond.Broadcast()
	return nil
}

// next blocks until a message is available, the producer ends the stream
// (io.EOF after the queue is drained), the mailbox closes with an error, or
// ctx ends. As soon as consuming one message makes room for a parked frame,
// the parked frame is promoted immediately so a blocked producer makes
// progress without waiting for another network frame.
func (m *streamMailbox[T]) next(ctx context.Context) (T, error) {
	var zero T

	stop := context.AfterFunc(ctx, m.broadcast)
	defer stop()

	m.mu.Lock()
	defer m.mu.Unlock()

	for len(m.queue) == 0 {
		if m.closed {
			// A parked frame on a closed stream is always dropped by the
			// producer path; discard it defensively before returning.
			m.parked = nil
			return zero, m.recvErr
		}
		if m.sendClosed {
			return zero, io.EOF
		}
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		m.cond.Wait()
	}

	q := m.queue[0]
	m.queue = m.queue[1:]
	m.queuedByt -= q.size

	if m.parked != nil && m.fitsLocked(m.parked.size) {
		m.promoteParkedLocked()
	}
	m.cond.Broadcast()
	return q.msg, nil
}

// closeWithError puts the mailbox into its terminal state. Pending queued
// messages remain consumable; a parked frame is released. The transition
// happens at most once and wakes every blocked producer and consumer. A nil
// error is reported as ErrClosed.
func (m *streamMailbox[T]) closeWithError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeOnce.Do(func() {
		m.closeLocked(err)
	})
}

// closeSend marks the producer side as finished. Consumers drain the queue
// and then receive io.EOF. It is idempotent.
func (m *streamMailbox[T]) closeSend() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.sendClosed {
		m.sendClosed = true
		m.cond.Broadcast()
	}
}

// pending reports the currently retained message count and payload bytes
// (queued messages plus a parked frame). It is intended for diagnostics and
// tests.
func (m *streamMailbox[T]) pending() (msgs int, bytes int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msgs = len(m.queue)
	bytes = m.queuedByt
	if m.parked != nil {
		msgs++
		bytes += m.parked.size
	}
	return msgs, bytes
}

// isParked reports whether a frame currently occupies the overflow slot.
func (m *streamMailbox[T]) isParked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.parked != nil
}

// queuedBytes reports the accounted payload bytes currently held in the
// queue (excluding the parked overflow frame).
func (m *streamMailbox[T]) queuedBytes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.queuedByt
}

func (m *streamMailbox[T]) broadcast() {
	m.mu.Lock()
	m.cond.Broadcast()
	m.mu.Unlock()
}

// closeLocked enters the terminal state. Callers must hold m.mu.
func (m *streamMailbox[T]) closeLocked(err error) {
	if err == nil {
		err = ErrClosed
	}
	m.closed = true
	m.recvErr = err
	m.parked = nil
	m.cond.Broadcast()
}

// fitsLocked reports whether a message of size may enter the queue given the
// current queue occupancy. The overflow slot is not part of the queue and is
// therefore not counted here. An empty queue always accepts a message, even
// one larger than the configured budget or message limit, so a single
// oversized message can neither deadlock a stream nor stay parked forever.
func (m *streamMailbox[T]) fitsLocked(size int) bool {
	if len(m.queue) == 0 {
		return true
	}
	if len(m.queue) >= m.msgLimit {
		return false
	}
	if m.byteBudget > 0 && m.queuedByt+size > m.byteBudget {
		return false
	}
	return true
}

func (m *streamMailbox[T]) enqueueLocked(q queuedMessage[T]) {
	m.queue = append(m.queue, q)
	m.queuedByt += q.size
}

// promoteParkedLocked moves the overflow frame into the queue. Its bytes move
// with it (parked is not counted in queuedByt) so the totals are unchanged.
func (m *streamMailbox[T]) promoteParkedLocked() {
	q := *m.parked
	m.parked = nil
	m.enqueueLocked(q)
}
