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
)

type streamID uint32

type streamMessage struct {
	header  messageHeader
	payload []byte
}

type stream struct {
	id      streamID
	sender  sender
	queue   *streamQueue[*streamMessage]
	release func(*streamMessage)

	closeOnce sync.Once
	mu        sync.Mutex
	recvErr   error
}

func newStream(id streamID, send sender, policy StreamBufferPolicy, release func(*streamMessage)) *stream {
	return &stream{
		id:      id,
		sender:  send,
		queue:   newStreamQueue[*streamMessage](uint32(id), policy, release),
		release: release,
	}
}

// closeWithError moves the stream to its terminal state. Messages which were
// already buffered remain available to the consumer; the terminal error is
// reported only once the buffer has been drained. The first error wins, which
// keeps a more specific cause from being replaced by the generic ErrClosed
// used during teardown.
func (s *stream) closeWithError(err error) error {
	s.closeOnce.Do(func() {
		if err == nil {
			err = ErrClosed
		}
		s.mu.Lock()
		s.recvErr = err
		s.mu.Unlock()
		s.queue.close(err, false)
	})
	return nil
}

// discard drops buffered messages and marks the stream terminal. It is used
// when nobody will consume the stream anymore, so the payloads can return to
// the pool immediately.
func (s *stream) discard(err error) {
	s.closeWithError(err)
	s.queue.close(s.terminalError(), true)
}

func (s *stream) terminalError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recvErr
}

// send writes a frame for this stream. A closed connection and a cancelled
// caller are reported by the sender; the stream itself stays writable after a
// remote half-close, matching the protocol's independent local/remote closed
// states.
func (s *stream) send(ctx context.Context, mt messageType, flags uint8, b []byte) error {
	return s.sender.send(ctx, uint32(s.id), mt, flags, b)
}

// receive delivers a message to this stream from the connection receive loop.
//
// The message is queued in the stream's bounded buffer. If the buffer is full
// the receive loop waits up to the configured budget for the consumer to make
// progress; because the budget is per stream, one slow consumer cannot stall
// delivery for other streams on the connection. If the budget expires the
// stream is terminated with ErrStreamFull. Cancelling ctx or closing the
// stream releases the wait immediately.
func (s *stream) receive(ctx context.Context, msg *streamMessage) error {
	err := s.queue.push(ctx, msg)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrStreamFull):
		s.closeWithError(ErrStreamFull)
		s.releaseMessage(msg)
		return ErrStreamFull
	case errors.Is(err, errStreamBufferClosed):
		s.releaseMessage(msg)
		return s.terminalError()
	default:
		// The wait was cancelled; the caller owns the message buffer.
		s.releaseMessage(msg)
		return err
	}
}

func (s *stream) releaseMessage(msg *streamMessage) {
	if s.release != nil && msg != nil {
		s.release(msg)
	}
}

func (s *stream) stats() StreamBufferStats {
	return s.queue.snapshot()
}

type sender interface {
	send(ctx context.Context, streamID uint32, mt messageType, flags uint8, b []byte) error
}
