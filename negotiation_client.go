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
	"time"
)

// ClientNegotiation configures the client side of capability negotiation.
// The zero value is not valid; use NewClientNegotiation to construct one.
type ClientNegotiation struct {
	version          uint32
	minVersion       uint32
	capabilities     Capability
	requiredServices []string
	timeout          time.Duration
}

// NewClientNegotiation builds a client negotiation configuration. The
// offered protocol version and capability bits default to the values
// implemented by this package.
func NewClientNegotiation(requiredServices []string) ClientNegotiation {
	return ClientNegotiation{
		version:          ProtocolVersion,
		minVersion:       MinSupportedProtocolVersion,
		capabilities:     CapabilityStreaming,
		requiredServices: append([]string(nil), requiredServices...),
		timeout:          defaultNegotiationTimeout,
	}
}

// WithVersion overrides the protocol version advertised to the server.
func (c ClientNegotiation) WithVersion(version, minVersion uint32) ClientNegotiation {
	c.version = version
	c.minVersion = minVersion
	return c
}

// WithCapabilities overrides the capability bits advertised to the server.
func (c ClientNegotiation) WithCapabilities(caps Capability) ClientNegotiation {
	c.capabilities = caps
	return c
}

// WithTimeout bounds the negotiation exchange. A zero timeout restores the
// package default.
func (c ClientNegotiation) WithTimeout(timeout time.Duration) ClientNegotiation {
	if timeout <= 0 {
		timeout = defaultNegotiationTimeout
	}
	c.timeout = timeout
	return c
}

// clientNegotiationHandshaker performs the client half of the negotiation
// state machine. It implements Handshaker so it composes with the existing
// connection setup path.
type clientNegotiationHandshaker struct {
	cfg ClientNegotiation
}

func newClientNegotiationHandshaker(cfg ClientNegotiation) *clientNegotiationHandshaker {
	return &clientNegotiationHandshaker{cfg: cfg}
}

func (h *clientNegotiationHandshaker) Handshake(ctx context.Context, conn net.Conn) (net.Conn, any, error) {
	timeout := h.cfg.timeout
	if timeout <= 0 {
		timeout = defaultNegotiationTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}

	// The session owns the context so its single cleanup path can cancel
	// work spawned under the exchange independently of this call returning.
	session := newNegotiationSession(conn, cancel)
	go watchNegotiationContext(ctx, session)

	if err := session.begin(); err != nil {
		return nil, nil, err
	}

	offer := negotiationOffer{
		version:      h.cfg.version,
		minVersion:   h.cfg.minVersion,
		capabilities: h.cfg.capabilities,
		required:     h.cfg.requiredServices,
	}
	payload, err := offer.marshal()
	if err != nil {
		return nil, nil, session.fail(NegotiationReasonMalformed, err)
	}
	if err := session.ch.send(negotiationStreamID, messageTypeNegotiation, 0, payload); err != nil {
		return nil, nil, session.fail(transportFailureReason(err), err)
	}

	mh, p, err := session.ch.recv()
	if err != nil {
		return nil, nil, session.fail(transportFailureReason(err), err)
	}
	if mh.StreamID != negotiationStreamID || mh.Type != messageTypeNegotiationAck || mh.Flags != 0 {
		return nil, nil, session.fail(NegotiationReasonMalformed,
			errors.New("ttrpc: unexpected frame while expecting negotiation ack"))
	}

	ack, err := unmarshalNegotiationAck(p)
	if err != nil {
		return nil, nil, session.fail(NegotiationReasonMalformed, err)
	}

	switch ack.status {
	case negotiationStatusOK:
	case negotiationStatusIncompatible:
		return nil, nil, session.fail(NegotiationReasonVersion,
			errors.New("ttrpc: server rejected protocol version: "+ack.message))
	case negotiationStatusMissingService:
		return nil, nil, session.fail(NegotiationReasonMissing,
			errors.New("ttrpc: server is missing required service: "+ack.message))
	default:
		return nil, nil, session.fail(NegotiationReasonRefused,
			errors.New("ttrpc: server refused negotiation: "+ack.message))
	}

	if ack.version < h.cfg.minVersion || ack.version > h.cfg.version {
		return nil, nil, session.fail(NegotiationReasonVersion,
			errors.New("ttrpc: server negotiated unsupported protocol version"))
	}

	result := &NegotiationResult{
		Version:      ack.version,
		Capabilities: ack.capabilities & h.cfg.capabilities,
		Services:     ack.services,
	}
	session.publish(result)

	return conn, &handshakeResult{
		negotiation: result,
		channel:     session.ch,
	}, nil
}

// watchNegotiationContext fails the session with a deterministic result when
// the caller's context expires or is canceled while an exchange is blocked.
// It exits once the session reaches any terminal state.
func watchNegotiationContext(ctx context.Context, session *negotiationSession) {
	select {
	case <-session.done:
		return
	case <-ctx.Done():
		session.fail(NegotiationReasonTimeout, ctx.Err())
	}
}

// NewClientWithNegotiation creates a ttrpc client that negotiates the
// protocol version, service set and capability bits with the server before
// the connection is made available for RPCs.
//
// On negotiation failure the underlying connection is closed through the
// session's single cleanup path and the error is returned; no client or
// reader goroutine is started. On success the negotiated view is published
// exactly once for the returned client and remains immutable for its whole
// lifetime.
func NewClientWithNegotiation(ctx context.Context, conn net.Conn, cfg ClientNegotiation, opts ...ClientOpts) (*Client, error) {
	handshaker := newClientNegotiationHandshaker(cfg)
	approved, hs, err := handshaker.Handshake(ctx, conn)
	if err != nil {
		// The session cleanup already closed the conn; keep the call
		// idempotent for callers that defer a conn.Close() themselves.
		_ = conn.Close()
		return nil, err
	}

	result, _ := hs.(*handshakeResult)

	// Reuse the negotiation channel so its buffered reader keeps any bytes
	// already pulled from the socket during the exchange.
	var ch *channel
	if result != nil {
		ch = result.channel
	}
	if ch == nil {
		ch = newChannel(approved)
	}

	client := newClient(approved, ch, opts...)
	if result != nil {
		client.negotiation = result.negotiation
	}
	return client, nil
}

// NegotiationResult returns the immutable view published during negotiation
// and whether negotiation took place on this connection. A client created
// through NewClient reports false.
func (c *Client) NegotiationResult() (*NegotiationResult, bool) {
	if c.negotiation == nil {
		return nil, false
	}
	return c.negotiation, true
}

// transportFailureReason maps a transport error to the deterministic
// negotiation failure reason. Deadlines are reported as timeouts, short
// reads as truncation, everything else as a malformed exchange.
func transportFailureReason(err error) string {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return NegotiationReasonTimeout
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return NegotiationReasonTruncated
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return NegotiationReasonTimeout
	}
	return NegotiationReasonMalformed
}
