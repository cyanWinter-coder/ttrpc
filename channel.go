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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	messageHeaderLength = 10
	messageLengthMax    = 4 << 20
)

type messageType uint8

const (
	messageTypeRequest  messageType = 0x1
	messageTypeResponse messageType = 0x2
	messageTypeData     messageType = 0x3
)

func (mt messageType) String() string {
	switch mt {
	case messageTypeRequest:
		return "request"
	case messageTypeResponse:
		return "response"
	case messageTypeData:
		return "data"
	default:
		return "unknown"
	}
}

const (
	flagRemoteClosed uint8 = 0x1
	flagRemoteOpen   uint8 = 0x2
	flagNoData       uint8 = 0x4
)

// messageHeader represents the fixed-length message header of 10 bytes sent
// with every request.
type messageHeader struct {
	Length   uint32      // length excluding this header. b[:4]
	StreamID uint32      // identifies which request stream message is a part of. b[4:8]
	Type     messageType // message type b[8]
	Flags    uint8       // type specific flags b[9]
}

func readMessageHeader(p []byte, r io.Reader) (messageHeader, error) {
	_, err := io.ReadFull(r, p[:messageHeaderLength])
	if err != nil {
		return messageHeader{}, err
	}

	return messageHeader{
		Length:   binary.BigEndian.Uint32(p[:4]),
		StreamID: binary.BigEndian.Uint32(p[4:8]),
		Type:     messageType(p[8]),
		Flags:    p[9],
	}, nil
}

// writeMessageHeader serializes mh into p, which must be at least
// messageHeaderLength bytes long.
func writeMessageHeader(p []byte, mh messageHeader) {
	binary.BigEndian.PutUint32(p[:4], mh.Length)
	binary.BigEndian.PutUint32(p[4:8], mh.StreamID)
	p[8] = byte(mh.Type)
	p[9] = mh.Flags
}

var buffers sync.Pool

type channel struct {
	conn net.Conn
	br   *bufio.Reader

	hrbuf [messageHeaderLength]byte // avoid alloc when reading header

	// mlen is the maximum message length accepted on this channel. It is
	// validated before any buffer is allocated for an incoming frame.
	mlen uint32

	// wbuf is a scratch buffer used to assemble outgoing frames.
	wbuf []byte
	// wpending holds the unwritten remainder of a frame whose write only
	// partially succeeded. It records the real write progress so a later
	// send resumes at the correct offset instead of re-writing the frame
	// prefix.
	wpending []byte

	// recvErr is the terminal receive error. Once set, the connection can
	// no longer be framed and recv reports the same error again without
	// touching the connection, so residual bytes are never parsed as a new
	// frame.
	recvErr error

	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error
}

func newChannel(conn net.Conn) *channel {
	return newChannelWithMessageLengthMax(conn, messageLengthMax)
}

// newChannelWithMessageLengthMax creates a channel which rejects messages
// larger than mlen. A zero mlen selects the default maximum message size.
func newChannelWithMessageLengthMax(conn net.Conn, mlen uint32) *channel {
	if mlen == 0 {
		mlen = messageLengthMax
	}
	return &channel{
		conn:   conn,
		br:     bufio.NewReader(conn),
		mlen:   mlen,
		closed: make(chan struct{}),
	}
}

// recv a message from the channel. The returned buffer contains the message.
//
// If a valid grpc status is returned, the message header
// returned will be valid and caller should send that along to
// the correct consumer. The connection is poisoned in that case: the
// declared frame is not consumed and no further messages can be received.
func (ch *channel) recv() (messageHeader, []byte, error) {
	if ch.recvErr != nil {
		return messageHeader{}, nil, ch.recvErr
	}

	select {
	case <-ch.closed:
		return messageHeader{}, nil, ErrClosed
	default:
	}

	mh, err := readMessageHeader(ch.hrbuf[:], ch.br)
	if err != nil {
		// A truncated or corrupt header means the connection can no longer
		// be framed. Stop parsing instead of treating leftover bytes as the
		// start of a new frame.
		ch.recvErr = err
		return messageHeader{}, nil, err
	}

	if mh.Length > ch.mlen {
		// The declared length exceeds what we are willing to allocate.
		// Do not allocate for it and do not try to discard the body: the
		// peer may never send that many bytes, which would wedge the read
		// loop on a half-frame and stall every other request on the
		// connection. Report the message as oversized, then poison the
		// channel so residual bytes are never parsed as a new frame.
		ch.recvErr = fmt.Errorf("ttrpc: %w: message length %v exceeds maximum message size of %v", ErrProtocol, mh.Length, ch.mlen)
		return mh, nil, status.Errorf(codes.ResourceExhausted, "message length %v exceed maximum message size of %v", mh.Length, ch.mlen)
	}

	var p []byte
	if mh.Length > 0 {
		p = ch.getmbuf(int(mh.Length))
		if _, err := io.ReadFull(ch.br, p); err != nil {
			// Truncated frame: release the frame buffer and stop parsing
			// this connection.
			ch.putmbuf(p)
			ch.recvErr = fmt.Errorf("failed reading message: %w", err)
			return messageHeader{}, nil, ch.recvErr
		}
	}

	return mh, p, nil
}

func (ch *channel) send(streamID uint32, t messageType, flags uint8, p []byte) error {
	select {
	case <-ch.closed:
		return ErrClosed
	default:
	}

	if len(p) > int(ch.mlen) {
		return oversizedMessageError(len(p), int(ch.mlen))
	}

	// A previous frame may have been partially written. Finish writing it
	// before starting a new frame so frames stay ordered and no prefix is
	// written twice.
	if err := ch.flushPending(); err != nil {
		return err
	}

	need := messageHeaderLength + len(p)
	if cap(ch.wbuf) < need {
		ch.wbuf = make([]byte, need)
	}
	frame := ch.wbuf[:need]
	writeMessageHeader(frame, messageHeader{Length: uint32(len(p)), StreamID: streamID, Type: t, Flags: flags})
	copy(frame[messageHeaderLength:], p)

	return ch.writeAll(frame)
}

// writeAll writes p completely. If the connection fails after accepting only
// part of p, the unwritten remainder is recorded in ch.wpending so the next
// send resumes at the real offset.
func (ch *channel) writeAll(p []byte) error {
	for len(p) > 0 {
		n, err := ch.conn.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			if len(p) > 0 {
				ch.wpending = append(ch.wpending[:0], p...)
			}
			return err
		}
	}
	return nil
}

// flushPending completes any partially written frame from a previous send.
func (ch *channel) flushPending() error {
	p := ch.wpending
	for len(p) > 0 {
		n, err := ch.conn.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			ch.wpending = append(ch.wpending[:0], p...)
			return err
		}
	}
	ch.wpending = ch.wpending[:0]
	return nil
}

// close shuts down the channel and the underlying connection, waking any
// blocked senders and receivers. It is safe to call multiple times; the
// underlying connection is closed at most once and every caller observes the
// same close error.
func (ch *channel) close() error {
	ch.closeOnce.Do(func() {
		close(ch.closed)
		ch.closeErr = ch.conn.Close()
	})
	return ch.closeErr
}

func (ch *channel) getmbuf(size int) []byte {
	// we can't use the standard New method on pool because we want to allocate
	// based on size.
	b, ok := buffers.Get().(*[]byte)
	if !ok || cap(*b) < size {
		// TODO(stevvooe): It may be better to allocate these in fixed length
		// buckets to reduce fragmentation but its not clear that would help
		// with performance. An ilogb approach or similar would work well.
		bb := make([]byte, size)
		b = &bb
	} else {
		*b = (*b)[:size]
	}
	return *b
}

func (ch *channel) putmbuf(p []byte) {
	buffers.Put(&p)
}
