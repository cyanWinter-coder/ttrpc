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
	"sync"
	"time"
)

const (
	// defaultStreamBufferCapacity is the historical per-stream receive buffer
	// size. Keeping it as the default means a zero-value StreamBufferPolicy
	// behaves exactly like the fixed channel buffer it replaces.
	defaultStreamBufferCapacity = streamRecvBufferSize

	// defaultStreamBufferWait is the historical time a producer waits for a
	// full per-stream buffer to drain before failing the stream with
	// ErrStreamFull.
	defaultStreamBufferWait = time.Second
)

// StreamBufferStats is a snapshot of a stream's receive buffer.
//
// Queued and HighWatermark show how much headroom is left, Waits shows that a
// slow consumer is applying backpressure, and Overflows shows that a message
// could not be queued within the configured wait budget, which terminates the
// stream with ErrStreamFull. Discarded counts messages released without being
// consumed, for example when a handler returns or a stream is torn down.
type StreamBufferStats struct {
	// StreamID is the protocol stream identifier.
	StreamID uint32
	// Capacity is the maximum number of queued messages.
	Capacity int
	// Queued is the current number of queued messages.
	Queued int
	// HighWatermark is the largest number of queued messages observed.
	HighWatermark int
	// Waits is the number of messages which found the buffer full and had to
	// wait for the consumer to make room.
	Waits uint64
	// Overflows is the number of messages which could not be queued within
	// the wait budget. An overflow terminates the stream with ErrStreamFull.
	Overflows uint64
	// Discarded is the number of queued messages released without being
	// consumed.
	Discarded uint64
	// Closed reports whether the stream has reached a terminal state.
	Closed bool
}

// StreamBufferObserver is called when a stream's receive buffer comes under
// pressure: it reached capacity, a producer had to wait, or a message
// overflowed. Implementations must not block and must tolerate repeated and
// concurrent calls.
type StreamBufferObserver func(StreamBufferStats)

// StreamBufferPolicy configures the bounded receive buffer of a stream.
//
// The zero value preserves the historical behavior: up to
// streamRecvBufferSize messages are buffered per stream and a producer waits
// at most one second for the consumer to make room before the stream fails
// with ErrStreamFull. Setting FailFast, or a negative Wait, removes that wait
// entirely so a slow consumer cannot add latency to the connection's receive
// loop. A positive Wait bounds how long a slow consumer may hold up delivery
// for its own stream.
//
// The policy only affects local buffering. It does not change the wire
// protocol, so peers which do not configure a policy are unaffected.
type StreamBufferPolicy struct {
	// Capacity is the maximum number of messages buffered per stream. Values
	// less than or equal to zero select the default capacity.
	Capacity int
	// Wait is the maximum time a producer waits for space when the buffer is
	// full. Zero selects the default wait and a negative value disables
	// waiting, behaving like FailFast.
	Wait time.Duration
	// FailFast fails a stream with ErrStreamFull as soon as its buffer is
	// full instead of waiting for the consumer. It takes precedence over Wait.
	FailFast bool
	// Observer, when set, is notified about buffer pressure.
	Observer StreamBufferObserver
}

func (p StreamBufferPolicy) normalize() StreamBufferPolicy {
	if p.Capacity <= 0 {
		p.Capacity = defaultStreamBufferCapacity
	}
	switch {
	case p.FailFast:
		p.Wait = 0
	case p.Wait < 0:
		p.FailFast = true
		p.Wait = 0
	case p.Wait == 0:
		p.Wait = defaultStreamBufferWait
	}
	return p
}

// withCapacity returns a copy of the policy with a different capacity. It is
// used for unary calls and non-streaming-client streams, which can receive at
// most one message.
func (p StreamBufferPolicy) withCapacity(capacity int) StreamBufferPolicy {
	p.Capacity = capacity
	return p
}

// StreamBufferStatser is implemented by streams and stream handlers which
// expose the state of their receive buffer, allowing callers to monitor
// per-stream backpressure.
type StreamBufferStatser interface {
	StreamBufferStats() StreamBufferStats
}

// errStreamBufferClosed is returned by streamQueue when a producer tried to
// queue a message on a stream which already reached a terminal state. Callers
// translate it into the stream's public terminal error.
var errStreamBufferClosed = errors.New("ttrpc: stream buffer closed")

// streamQueue is a bounded, per-stream message queue.
//
// Producers never block the connection indefinitely: they either enqueue,
// observe cancellation, or fail the stream once the configured wait budget
// expires. A mutex-protected slice is used rather than a buffered channel so
// that a producer racing with a close is handled exactly: once closed, no new
// message is accepted, and already buffered messages are delivered ahead of
// the terminal error unless the owner explicitly discards them.
//
// Waiters park on broadcast channels which are closed and replaced on every
// relevant state change, so no wakeup can be lost when several producers or
// consumers share a queue.
type streamQueue[T any] struct {
	mu        sync.Mutex
	items     []T
	capacity  int
	wait      time.Duration
	failFast  bool
	streamID  uint32
	observer  StreamBufferObserver
	discardFn func(T)

	itemsCh chan struct{} // broadcast: an item may be available
	spaceCh chan struct{} // broadcast: space may be available
	prods   int           // producers waiting for space
	cons    int           // consumers waiting for an item

	closed         bool
	discardOnClose bool
	err            error

	highWatermark int
	waits         uint64
	overflows     uint64
	discarded     uint64
}

func newStreamQueue[T any](id uint32, policy StreamBufferPolicy, discardFn func(T)) *streamQueue[T] {
	policy = policy.normalize()
	return &streamQueue[T]{
		items:     make([]T, 0, policy.Capacity),
		capacity:  policy.Capacity,
		wait:      policy.Wait,
		failFast:  policy.FailFast,
		streamID:  id,
		observer:  policy.Observer,
		discardFn: discardFn,
		itemsCh:   make(chan struct{}),
		spaceCh:   make(chan struct{}),
	}
}

// push enqueues item, waiting at most the policy's wait budget for space. It
// returns ErrStreamFull if the budget expires, errStreamBufferClosed if the
// stream is already terminal, and ctx.Err() if ctx is cancelled first.
func (q *streamQueue[T]) push(ctx context.Context, item T) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return errStreamBufferClosed
	}
	if len(q.items) < q.capacity {
		atCapacity := q.enqueueLocked(item)
		q.mu.Unlock()
		if atCapacity {
			q.observe()
		}
		return nil
	}

	// The buffer is full, so this message applies backpressure to the stream.
	q.waits++
	if q.failFast {
		q.overflows++
		q.mu.Unlock()
		q.observe()
		return ErrStreamFull
	}
	q.prods++
	spaceCh := q.spaceCh
	q.mu.Unlock()
	q.observe()

	timer := time.NewTimer(q.wait)
	defer timer.Stop()

	for {
		q.mu.Lock()
		if q.closed {
			q.prods--
			q.mu.Unlock()
			return errStreamBufferClosed
		}
		if len(q.items) < q.capacity {
			atCapacity := q.enqueueLocked(item)
			q.prods--
			q.mu.Unlock()
			if atCapacity {
				q.observe()
			}
			return nil
		}
		q.mu.Unlock()

		select {
		case <-spaceCh:
		case <-timer.C:
			q.mu.Lock()
			q.prods--
			if !q.closed {
				q.overflows++
			}
			q.mu.Unlock()
			q.observe()
			return ErrStreamFull
		case <-ctx.Done():
			q.mu.Lock()
			q.prods--
			q.mu.Unlock()
			return ctx.Err()
		}

		q.mu.Lock()
		spaceCh = q.spaceCh
		q.mu.Unlock()
	}
}

// next returns the next queued item, blocking until one is available, the
// queue is closed, or ctx is cancelled.
//
// Buffered items are always delivered before the terminal error, so a final
// message which raced with teardown is not lost. Once the queue discards on
// close, queued data is dropped and the terminal error is returned on every
// subsequent call. It returns either an item or an error, never both, so a
// stream cannot report a successful read together with a close error.
func (q *streamQueue[T]) next(ctx context.Context) (T, error) {
	var zero T
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		q.mu.Lock()
		if q.discardOnClose {
			q.drainLocked()
			err := q.err
			q.mu.Unlock()
			return zero, err
		}
		if len(q.items) > 0 {
			item := q.popLocked()
			q.mu.Unlock()
			return item, nil
		}
		if q.closed {
			err := q.err
			q.mu.Unlock()
			return zero, err
		}
		q.cons++
		itemsCh := q.itemsCh
		q.mu.Unlock()

		select {
		case <-itemsCh:
		case <-ctx.Done():
		}

		q.mu.Lock()
		q.cons--
		q.mu.Unlock()
	}
}

// terminalError reports the queue's terminal error, or nil while the queue is
// still open.
func (q *streamQueue[T]) terminalError() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		return nil
	}
	return q.err
}

// close marks the queue terminal with err. When discard is set, queued
// messages are dropped and their resources released; otherwise they remain
// available to the consumer ahead of the terminal error. Closing an already
// closed queue with discard set drops whatever is still queued. The first
// terminal error wins, so a specific cause is never replaced by the
// ErrStreamClosed used during teardown.
func (q *streamQueue[T]) close(err error, discard bool) {
	if err == nil {
		err = ErrClosed
	}

	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.err = err
	}
	if discard {
		q.discardOnClose = true
		q.drainLocked()
	}
	q.broadcastLocked()
	q.mu.Unlock()
	q.observe()
}

func (q *streamQueue[T]) enqueueLocked(item T) bool {
	q.items = append(q.items, item)
	if len(q.items) > q.highWatermark {
		q.highWatermark = len(q.items)
	}
	q.broadcastLocked()
	return q.observer != nil && len(q.items) >= q.capacity
}

func (q *streamQueue[T]) popLocked() T {
	item := q.items[0]
	var zero T
	q.items[0] = zero
	q.items = q.items[1:]
	if len(q.items) == 0 {
		q.items = nil
	}
	q.broadcastLocked()
	return item
}

func (q *streamQueue[T]) drainLocked() {
	if len(q.items) == 0 {
		return
	}
	var zero T
	for i := range q.items {
		q.discarded++
		if q.discardFn != nil {
			q.discardFn(q.items[i])
		}
		q.items[i] = zero
	}
	q.items = nil
	q.broadcastLocked()
}

// broadcastLocked wakes every waiter. Channels are only reallocated when a
// waiter is actually parked, so the common uncontended path allocates nothing.
func (q *streamQueue[T]) broadcastLocked() {
	if q.cons > 0 {
		close(q.itemsCh)
		q.itemsCh = make(chan struct{})
	}
	if q.prods > 0 {
		close(q.spaceCh)
		q.spaceCh = make(chan struct{})
	}
}

func (q *streamQueue[T]) snapshot() StreamBufferStats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return StreamBufferStats{
		StreamID:      q.streamID,
		Capacity:      q.capacity,
		Queued:        len(q.items),
		HighWatermark: q.highWatermark,
		Waits:         q.waits,
		Overflows:     q.overflows,
		Discarded:     q.discarded,
		Closed:        q.closed,
	}
}

func (q *streamQueue[T]) observe() {
	if q.observer == nil {
		return
	}
	q.observer(q.snapshot())
}
