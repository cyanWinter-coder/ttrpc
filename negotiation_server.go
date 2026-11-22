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
	"net"
	"time"
)

// handshakeResult is the internal data returned by a negotiation handshaker
// and attached to the resulting connection.
type handshakeResult struct {
	negotiation *NegotiationResult
	// channel is the exact framing channel used during negotiation. It is
	// handed to the connection so buffered bytes already read from the
	// socket are not lost to a second bufio.Reader.
	channel *channel
	// serviceView is the exact snapshot validated during negotiation. It is
	// published to the connection so later registrations cannot alter the
	// already-handshaked view. Nil for connections without negotiation.
	serviceView map[string]*ServiceDesc
}

// serverNegotiationHandshaker wraps an existing (possibly nil) handshaker
// and performs the server half of capability negotiation after the inner
// handshake completes. It is opt-in: servers without negotiation keep the
// original byte-for-byte connection behavior.
type serverNegotiationHandshaker struct {
	inner   Handshaker
	server  *Server
	timeout time.Duration
}

func newServerNegotiationHandshaker(server *Server, inner Handshaker, timeout time.Duration) *serverNegotiationHandshaker {
	if timeout <= 0 {
		timeout = defaultNegotiationTimeout
	}
	return &serverNegotiationHandshaker{
		inner:   inner,
		server:  server,
		timeout: timeout,
	}
}

func (h *serverNegotiationHandshaker) Handshake(ctx context.Context, raw net.Conn) (net.Conn, any, error) {
	// Run the legacy handshaker (e.g. unix socket credential checks) first
	// so negotiation only runs on already approved connections.
	if h.inner != nil {
		conn, data, err := h.inner.Handshake(ctx, raw)
		if err != nil {
			return nil, nil, err
		}
		raw = conn
		_ = data
	}

	timeout := h.timeout
	if timeout <= 0 {
		timeout = defaultNegotiationTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if deadline, ok := ctx.Deadline(); ok {
		_ = raw.SetDeadline(deadline)
		defer raw.SetDeadline(time.Time{})
	}

	session := newNegotiationSession(raw, cancel)
	go watchNegotiationContext(ctx, session)

	if err := session.begin(); err != nil {
		return nil, nil, err
	}

	mh, p, err := session.ch.recv()
	if err != nil {
		return nil, nil, session.fail(transportFailureReason(err), err)
	}
	if mh.StreamID != negotiationStreamID || mh.Type != messageTypeNegotiation || mh.Flags != 0 {
		return nil, nil, session.reject(negotiationStatusMalformed,
			NegotiationReasonMalformed, errors.New("ttrpc: unexpected frame while expecting negotiation offer"))
	}

	offer, err := unmarshalNegotiationOffer(p)
	if err != nil {
		return nil, nil, session.reject(negotiationStatusMalformed,
			NegotiationReasonMalformed, err)
	}

	// Take an immutable snapshot under the service set lock. Services
	// registered after this point cannot alter the published view.
	snapshot := h.server.services.snapshot()

	// Version negotiation: agree on the highest version the server supports
	// that lies within the client's supported window.
	if offer.minVersion > ProtocolVersion || offer.version < MinSupportedProtocolVersion {
		return nil, nil, session.reject(negotiationStatusIncompatible,
			NegotiationReasonVersion,
			errors.New("ttrpc: no common protocol version for client window"))
	}
	agreedVersion := ProtocolVersion
	if offer.version < agreedVersion {
		agreedVersion = offer.version
	}

	// Refuse immediately when a required service is absent from the
	// snapshot, before any application data or RPC work is admitted.
	for _, required := range offer.required {
		if _, ok := snapshot[required]; !ok {
			return nil, nil, session.reject(negotiationStatusMissingService,
				NegotiationReasonMissing,
				errors.New("ttrpc: required service not registered: "+required))
		}
	}

	ack := negotiationAck{
		version:      agreedVersion,
		capabilities: offer.capabilities & CapabilityStreaming,
		services:     serviceNames(snapshot),
	}
	payload, err := ack.marshal()
	if err != nil {
		return nil, nil, session.fail(NegotiationReasonMalformed, err)
	}
	if err := session.ch.send(negotiationStreamID, messageTypeNegotiationAck, 0, payload); err != nil {
		return nil, nil, session.fail(transportFailureReason(err), err)
	}

	result := &NegotiationResult{
		Version:      agreedVersion,
		Capabilities: ack.capabilities,
		Services:     append([]string(nil), ack.services...),
	}
	session.publish(result)

	return raw, &handshakeResult{
		negotiation: result,
		channel:     session.ch,
		serviceView: snapshot,
	}, nil
}

// reject sends an ack carrying the failure status and then drives the
// session into the failed state through the single cleanup path. A failed
// write collapses into the transport failure; the returned error is always
// the deterministic negotiation failure.
func (n *negotiationSession) reject(status uint32, reason string, cause error) error {
	ack := negotiationAck{
		status:  status,
		message: cause.Error(),
	}
	if payload, err := ack.marshal(); err == nil {
		// Best effort: the peer may already be gone. The fail() below closes
		// the socket regardless.
		_ = n.ch.send(negotiationStreamID, messageTypeNegotiationAck, 0, payload)
	}
	return n.fail(reason, cause)
}

func serviceNames(snapshot map[string]*ServiceDesc) []string {
	names := make([]string, 0, len(snapshot))
	for name := range snapshot {
		names = append(names, name)
	}
	return names
}
