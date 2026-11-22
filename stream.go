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
	"sync"
)

type streamID uint32

type streamMessage struct {
	header  messageHeader
	payload []byte
}

type stream struct {
	id     streamID
	sender sender
	mb     *streamMailbox[*streamMessage]

	// closeOnce guarantees the mailbox's terminal transition runs a single
	// time regardless of how many close paths trigger it.
	closeOnce sync.Once
}

func newStream(id streamID, send sender, recvBuf, recvBudget int) *stream {
	return &stream{
		id:     id,
		sender: send,
		mb:     newStreamMailbox[*streamMessage](recvBuf, recvBudget),
	}
}

func (s *stream) closeWithError(err error) error {
	s.closeOnce.Do(func() {
		s.mb.closeWithError(err)
	})
	return nil
}

func (s *stream) send(mt messageType, flags uint8, b []byte) error {
	return s.sender.send(uint32(s.id), mt, flags, b)
}

// receive delivers a message to this stream from the connection receive loop.
// All backpressure, byte-budget accounting, fairness and terminal-state
// handling live in the stream's mailbox. The receive loop is never held up
// beyond the budget of the stream a frame belongs to: a blocked frame is
// parked inside the mailbox while the loop continues servicing other streams.
func (s *stream) receive(ctx context.Context, msg *streamMessage) error {
	return s.mb.deliver(ctx, msg, len(msg.payload))
}

type sender interface {
	send(uint32, messageType, uint8, []byte) error
}
