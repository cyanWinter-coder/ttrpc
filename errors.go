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
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrProtocol is a general error in the handling the protocol.
	ErrProtocol = errors.New("protocol error")

	// ErrClosed is returned by client methods when the underlying connection is
	// closed.
	ErrClosed = errors.New("ttrpc: closed")

	// ErrServerClosed is returned when the Server has closed its connection.
	ErrServerClosed = errors.New("ttrpc: server closed")

	// ErrStreamClosed is when the streaming connection is closed.
	ErrStreamClosed = errors.New("ttrpc: stream closed")

	// ErrStreamFull is returned when a stream's receive buffer is full
	// and the message cannot be delivered without blocking the
	// connection's receive loop. This prevents a single unconsumed
	// stream from deadlocking all other streams on the same connection.
	ErrStreamFull = errors.New("ttrpc: stream buffer full")

	// ErrPeerCredentialsUnavailable is returned when a connection's peer
	// credentials could not be established, for example because the
	// connection is not a Unix socket or the SO_PEERCRED lookup failed.
	// The server rejects such a connection instead of serving it as an
	// anonymous peer.
	ErrPeerCredentialsUnavailable = errors.New("ttrpc: peer credentials unavailable")

	// ErrPeerCredentialsTruncated is returned when the operating system
	// reports fewer credential bytes than expected. Short credentials are
	// an error, never a valid zeroed identity.
	ErrPeerCredentialsTruncated = errors.New("ttrpc: peer credentials truncated")

	// ErrPeerCredentialsUnsupported is returned when peer credentials are
	// requested on a platform without an implementation. Connections are
	// rejected rather than served anonymously.
	ErrPeerCredentialsUnsupported = errors.New("ttrpc: peer credentials unsupported on this platform")

	// ErrPeerCredentialsRejected is returned when the server's credential
	// verifier rejects a connection's frozen credentials.
	ErrPeerCredentialsRejected = errors.New("ttrpc: peer credentials rejected")
)

// OversizedMessageErr is used to indicate refusal to send an oversized message.
// It wraps a ResourceExhausted grpc Status together with the offending message
// length.
type OversizedMessageErr struct {
	messageLength int
	err           error
}

// OversizedMessageError returns an OversizedMessageErr error for the given message
// length if it exceeds the allowed maximum. Otherwise a nil error is returned.
func OversizedMessageError(messageLength int) error {
	if messageLength <= messageLengthMax {
		return nil
	}

	return &OversizedMessageErr{
		messageLength: messageLength,
		err:           status.Errorf(codes.ResourceExhausted, "message length %v exceed maximum message size of %v", messageLength, messageLengthMax),
	}
}

// Error returns the error message for the corresponding grpc Status for the error.
func (e *OversizedMessageErr) Error() string {
	return e.err.Error()
}

// Unwrap returns the corresponding error with our grpc status code.
func (e *OversizedMessageErr) Unwrap() error {
	return e.err
}

// RejectedLength retrieves the rejected message length which triggered the error.
func (e *OversizedMessageErr) RejectedLength() int {
	return e.messageLength
}

// MaximumLength retrieves the maximum allowed message length that triggered the error.
func (*OversizedMessageErr) MaximumLength() int {
	return messageLengthMax
}
