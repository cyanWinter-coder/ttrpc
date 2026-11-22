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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// ProtocolVersion is the ttrpc protocol version implemented by this package.
// It is the version advertised during capability negotiation.
const ProtocolVersion uint32 = 1

// MinSupportedProtocolVersion is the oldest peer protocol version this
// package is able to interoperate with.
const MinSupportedProtocolVersion uint32 = 1

// negotiationStreamID identifies the single in-band negotiation exchange,
// which takes place before any application stream (which must use odd,
// strictly increasing identifiers) is opened.
const negotiationStreamID uint32 = 0

// defaultNegotiationTimeout bounds a negotiation exchange when the caller
// does not supply a deadline. A hung or truncated peer must not keep the
// connection resources alive indefinitely.
const defaultNegotiationTimeout = 10 * time.Second

// Capability bits exchanged during negotiation. Unknown bits are ignored
// so that new capabilities can be introduced without breaking older peers.
type Capability uint64

const (
	// CapabilityStreaming indicates support for streaming RPCs.
	CapabilityStreaming Capability = 1 << iota
)

// NegotiationResult is the immutable, published outcome of a successful
// capability negotiation. It is published at most once for the lifetime of
// a connection and never changes afterwards, even if new services are
// registered on the server.
type NegotiationResult struct {
	// Version is the mutually agreed protocol version.
	Version uint32

	// Capabilities is the intersection of the capability bits advertised
	// by both peers.
	Capabilities Capability

	// Services is the snapshot of the service set that was visible at the
	// moment the connection negotiated. It is not affected by services
	// registered on the server later.
	Services []string
}

// negotiation state machine states.
type negotiationState int

const (
	negotiationStateInit negotiationState = iota
	negotiationStateRunning
	negotiationStateSucceeded
	negotiationStateFailed
	negotiationStateClosed
)

func (s negotiationState) String() string {
	switch s {
	case negotiationStateInit:
		return "init"
	case negotiationStateRunning:
		return "running"
	case negotiationStateSucceeded:
		return "succeeded"
	case negotiationStateFailed:
		return "failed"
	case negotiationStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// NegotiationError is returned when capability negotiation fails. Wrapping
// the concrete reason lets callers distinguish a refused, truncated or
// incompatible peer from ordinary transport errors.
type NegotiationError struct {
	reason string
	err    error
}

func newNegotiationError(reason string, err error) *NegotiationError {
	return &NegotiationError{reason: reason, err: err}
}

func (e *NegotiationError) Error() string {
	if e.err != nil {
		return fmt.Sprintf("ttrpc: negotiation %s: %v", e.reason, e.err)
	}
	return fmt.Sprintf("ttrpc: negotiation %s", e.reason)
}

func (e *NegotiationError) Unwrap() error {
	return e.err
}

// Reason returns the machine readable reason code of the failure.
func (e *NegotiationError) Reason() string {
	return e.reason
}

// Well-known negotiation failure reason codes.
const (
	NegotiationReasonTimeout   = "timeout"
	NegotiationReasonTruncated = "truncated"
	NegotiationReasonRefused   = "refused"
	NegotiationReasonVersion   = "incompatible-version"
	NegotiationReasonMissing   = "missing-required-service"
	NegotiationReasonMalformed = "malformed"
	NegotiationReasonClosed    = "closed"
	NegotiationReasonDuplicate = "duplicate"
)

var (
	// ErrNegotiationClosed is returned when negotiation is closed before it
	// could complete, for example because the connection was shut down
	// concurrently.
	ErrNegotiationClosed = errors.New("ttrpc: negotiation closed")

	// ErrNegotiationCompleted is returned when a completed negotiation is
	// driven again. Negotiation publishes its result exactly once.
	ErrNegotiationCompleted = errors.New("ttrpc: negotiation already completed")
)

// negotiationSession drives the rollback-able state transition of a single
// connection's capability negotiation.
//
// The lifecycle is: init -> running -> succeeded | failed | closed. Every
// transition happens under mu and the terminal result is published exactly
// once. Failure and close share a single cleanup path which closes the
// socket, cancels the negotiation context and unblocks the reader goroutine
// without waiting for any process to exit.
type negotiationSession struct {
	conn net.Conn
	ch   *channel

	mu    sync.Mutex
	state negotiationState
	done  chan struct{}

	result *NegotiationResult
	err    error

	// publishOnce guarantees the negotiated view is published a single time.
	publishOnce sync.Once

	// closeOnce guarantees the cleanup path (socket close, context
	// cancellation) runs a single time regardless of how many goroutines
	// race to fail or close the session.
	closeOnce sync.Once

	cancel context.CancelFunc
}

func newNegotiationSession(conn net.Conn, cancel context.CancelFunc) *negotiationSession {
	return &negotiationSession{
		conn:   conn,
		ch:     newChannel(conn),
		state:  negotiationStateInit,
		done:   make(chan struct{}),
		cancel: cancel,
	}
}

// begin transitions init -> running. A second attempt to begin, or an
// attempt on a terminal session, fails deterministically.
func (n *negotiationSession) begin() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch n.state {
	case negotiationStateInit:
		n.state = negotiationStateRunning
		return nil
	case negotiationStateRunning:
		return newNegotiationError(NegotiationReasonDuplicate, ErrNegotiationCompleted)
	default:
		if n.err != nil {
			return newNegotiationError(NegotiationReasonDuplicate, n.err)
		}
		return newNegotiationError(NegotiationReasonClosed, ErrNegotiationClosed)
	}
}

// publish transitions running -> succeeded and publishes the result once.
func (n *negotiationSession) publish(result *NegotiationResult) {
	n.mu.Lock()
	if n.state == negotiationStateRunning {
		n.state = negotiationStateSucceeded
		n.result = result
	}
	n.mu.Unlock()

	n.publishOnce.Do(func() {
		close(n.done)
	})
}

// finish is the single terminal transition of the state machine. It is
// serialized by mu; the first caller wins and concurrent failures or closes
// observe the very same result. The cleanup path runs at most once.
func (n *negotiationSession) finish(state negotiationState, neErr *NegotiationError) error {
	n.mu.Lock()
	if n.state == negotiationStateSucceeded || n.state == negotiationStateFailed ||
		n.state == negotiationStateClosed {
		existing := n.err
		n.mu.Unlock()
		if existing != nil {
			return existing
		}
		return neErr
	}
	n.state = state
	n.err = neErr
	n.mu.Unlock()

	n.cleanup()
	n.publishOnce.Do(func() {
		close(n.done)
	})
	return neErr
}

// fail transitions any non-terminal state to failed and runs the single
// cleanup path. Concurrent failures collapse to the first recorded error.
func (n *negotiationSession) fail(reason string, err error) error {
	return n.finish(negotiationStateFailed, newNegotiationError(reason, err))
}

// close transitions any non-terminal state to closed. It is idempotent and
// races safely with fail(): only one terminal outcome is observable.
func (n *negotiationSession) close() {
	_ = n.finish(negotiationStateClosed, newNegotiationError(NegotiationReasonClosed, ErrNegotiationClosed))
}

// cleanup is the single failure/teardown path. Closing the socket unblocks
// any blocked reader or writer immediately; cancelling the context releases
// the negotiation goroutines and RPC work spawned under it. Nothing here
// waits for a process to exit.
func (n *negotiationSession) cleanup() {
	n.closeOnce.Do(func() {
		n.cancel()
		// Closing the conn makes a blocked ch.recv/ch.send return at once,
		// which is what releases the reader goroutine without reaping the
		// whole process.
		_ = n.conn.Close()
	})
}

func (n *negotiationSession) wait(ctx context.Context) (*NegotiationResult, error) {
	// Prefer an already terminal session over an expired context so the
	// published result is deterministic when both are ready.
	select {
	case <-n.done:
	default:
		select {
		case <-n.done:
		case <-ctx.Done():
			// The caller's deadline fired first: drive the session to a
			// single deterministic failed result and release all resources.
			return nil, n.fail(NegotiationReasonTimeout, ctx.Err())
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.result != nil {
		return n.result, nil
	}
	if n.err != nil {
		return nil, n.err
	}
	return nil, newNegotiationError(NegotiationReasonClosed, ErrNegotiationClosed)
}

// ---------------------------------------------------------------------------
// Wire encoding. Negotiation reuses the standard 10 byte message frame with
// dedicated message types and stream ID zero. The payload is a compact,
// length prefixed binary record, intentionally kept free of protobuf so no
// generated code is required.
// ---------------------------------------------------------------------------

type negotiationOffer struct {
	version      uint32
	minVersion   uint32
	capabilities Capability
	required     []string
}

type negotiationAck struct {
	version      uint32
	capabilities Capability
	services     []string
	status       uint32 // 0 = accepted, otherwise a NegotiationStatus* code
	message      string
}

// Negotiation status codes carried in the ack payload.
const (
	negotiationStatusOK             uint32 = 0
	negotiationStatusIncompatible   uint32 = 1
	negotiationStatusMissingService uint32 = 2
	negotiationStatusMalformed      uint32 = 3
)

const (
	offerFixedLen = 4 + 4 + 8 + 4 // version, minVersion, capabilities, service count
	ackFixedLen   = 4 + 8 + 4 + 4 // version, capabilities, service count, status
)

func encodeStringList(items []string) ([]byte, error) {
	size := 0
	for _, item := range items {
		if len(item) > 0xffff {
			return nil, fmt.Errorf("ttrpc: negotiation item too large: %d bytes", len(item))
		}
		size += 2 + len(item)
	}
	buf := make([]byte, 0, size)
	tmp := make([]byte, 2)
	for _, item := range items {
		binary.BigEndian.PutUint16(tmp, uint16(len(item)))
		buf = append(buf, tmp...)
		buf = append(buf, item...)
	}
	return buf, nil
}

func decodeStringList(b []byte, count int) ([]string, []byte, error) {
	items := make([]string, 0, count)
	for i := 0; i < count; i++ {
		if len(b) < 2 {
			return nil, nil, io.ErrUnexpectedEOF
		}
		n := int(binary.BigEndian.Uint16(b))
		b = b[2:]
		if len(b) < n {
			return nil, nil, io.ErrUnexpectedEOF
		}
		items = append(items, string(b[:n]))
		b = b[n:]
	}
	return items, b, nil
}

func (o negotiationOffer) marshal() ([]byte, error) {
	list, err := encodeStringList(o.required)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, offerFixedLen, offerFixedLen+len(list))
	binary.BigEndian.PutUint32(buf[0:4], o.version)
	binary.BigEndian.PutUint32(buf[4:8], o.minVersion)
	binary.BigEndian.PutUint64(buf[8:16], uint64(o.capabilities))
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(o.required)))
	return append(buf, list...), nil
}

func unmarshalNegotiationOffer(p []byte) (negotiationOffer, error) {
	if len(p) < offerFixedLen {
		return negotiationOffer{}, io.ErrUnexpectedEOF
	}
	o := negotiationOffer{
		version:      binary.BigEndian.Uint32(p[0:4]),
		minVersion:   binary.BigEndian.Uint32(p[4:8]),
		capabilities: Capability(binary.BigEndian.Uint64(p[8:16])),
	}
	count := int(binary.BigEndian.Uint32(p[16:20]))
	if count > 0xffff {
		return negotiationOffer{}, fmt.Errorf("ttrpc: negotiation service count out of range: %d", count)
	}
	services, rest, err := decodeStringList(p[offerFixedLen:], count)
	if err != nil {
		return negotiationOffer{}, err
	}
	if len(rest) != 0 {
		return negotiationOffer{}, fmt.Errorf("ttrpc: %d trailing bytes in negotiation offer", len(rest))
	}
	o.required = services
	return o, nil
}

func (a negotiationAck) marshal() ([]byte, error) {
	services, err := encodeStringList(a.services)
	if err != nil {
		return nil, err
	}
	msg, err := encodeStringList([]string{a.message})
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, ackFixedLen+len(services)+len(msg))
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, a.version)
	buf = append(buf, tmp...)
	capTmp := make([]byte, 8)
	binary.BigEndian.PutUint64(capTmp, uint64(a.capabilities))
	buf = append(buf, capTmp...)
	binary.BigEndian.PutUint32(tmp, uint32(len(a.services)))
	buf = append(buf, tmp...)
	binary.BigEndian.PutUint32(tmp, a.status)
	buf = append(buf, tmp...)
	buf = append(buf, services...)
	buf = append(buf, msg...)
	return buf, nil
}

func unmarshalNegotiationAck(p []byte) (negotiationAck, error) {
	if len(p) < ackFixedLen {
		return negotiationAck{}, io.ErrUnexpectedEOF
	}
	a := negotiationAck{
		version:      binary.BigEndian.Uint32(p[0:4]),
		capabilities: Capability(binary.BigEndian.Uint64(p[4:12])),
	}
	serviceCount := int(binary.BigEndian.Uint32(p[12:16]))
	if serviceCount > 0xffff {
		return negotiationAck{}, fmt.Errorf("ttrpc: negotiation service count out of range: %d", serviceCount)
	}
	a.status = binary.BigEndian.Uint32(p[16:20])
	rest := p[ackFixedLen:]
	services, rest, err := decodeStringList(rest, serviceCount)
	if err != nil {
		return negotiationAck{}, err
	}
	a.services = services
	messages, rest, err := decodeStringList(rest, 1)
	if err != nil {
		return negotiationAck{}, err
	}
	if len(rest) != 0 {
		return negotiationAck{}, fmt.Errorf("ttrpc: %d trailing bytes in negotiation ack", len(rest))
	}
	a.message = messages[0]
	return a, nil
}
