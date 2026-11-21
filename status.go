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
	"fmt"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// StatusError is the canonical error type returned by ttrpc clients and
// accepted from ttrpc servers. It carries a stable status code, a human
// readable message and optional structured details, while preserving the
// underlying error for diagnostics.
//
// The wire representation of a StatusError is the google.rpc.Status message
// embedded in every ttrpc Response, so the code, message and details survive
// the full server-to-client round trip.
//
// StatusError interoperates with google.golang.org/grpc/status:
//
//   - status.FromError(err) and status.Code(err) work on any StatusError.
//   - Status errors created with the grpc/status package are handled the same
//     way as ttrpc StatusErrors on both the server and the client.
//
// Use errors.Is to compare two status errors (they match when their code,
// message and details are identical) and errors.As to recover the concrete
// *StatusError type from an error chain.
type StatusError struct {
	// st holds the code, message and structured details transmitted on the
	// wire. It is treated as immutable once the StatusError is created.
	st *status.Status

	// cause, when non-nil, is the local error that produced this status. It is
	// never transmitted to the remote peer but is retained on the originating
	// side (for example a transport failure or a context error) so callers can
	// inspect the underlying condition through errors.Is/errors.As.
	cause error
}

// NewStatusError returns a StatusError for the given code and message. It
// panics when called with codes.OK, mirroring the grpc/status invariant that a
// non-nil error must never carry an OK status.
func NewStatusError(code codes.Code, msg string) *StatusError {
	if code == codes.OK {
		panic("ttrpc: cannot create a status error with codes.OK")
	}
	return &StatusError{st: status.New(code, msg)}
}

// NewStatusErrorf returns NewStatusError(code, fmt.Sprintf(format, a...)).
func NewStatusErrorf(code codes.Code, format string, a ...any) *StatusError {
	return NewStatusError(code, fmt.Sprintf(format, a...))
}

// statusErrorFromStatus wraps an existing grpc status. The optional cause is
// retained for local diagnostics and never serialized.
func statusErrorFromStatus(st *status.Status, cause error) *StatusError {
	if st == nil || st.Code() == codes.OK {
		return nil
	}
	return &StatusError{st: st, cause: cause}
}

// statusErrorFromProto rebuilds a StatusError from a wire status. A nil or
// OK status yields a nil error.
func statusErrorFromProto(sp *spb.Status) *StatusError {
	if sp == nil || sp.Code == int32(codes.OK) {
		return nil
	}
	return &StatusError{st: status.FromProto(sp)}
}

// Error implements the error interface. The text matches the grpc/status
// rendering ("rpc error: code = X desc = Y"), keeping existing string-based
// error comparisons compatible.
func (e *StatusError) Error() string {
	return e.st.String()
}

// GRPCStatus exposes the embedded status so that grpc/status helpers
// (status.FromError, status.Code, ...) accept StatusError directly.
func (e *StatusError) GRPCStatus() *status.Status {
	return e.st
}

// Status returns the grpc status carried by the error, including any
// structured details attached by the server.
func (e *StatusError) Status() *status.Status {
	return e.st
}

// Code returns the stable status code of the error.
func (e *StatusError) Code() codes.Code {
	return e.st.Code()
}

// Message returns the status message of the error.
func (e *StatusError) Message() string {
	return e.st.Message()
}

// WithDetails returns a new StatusError with the provided structured detail
// messages appended. Details are serialized as google.protobuf.Any values and
// delivered to the client inside the wire status. It returns an error if one
// of the details cannot be marshaled; the receiver is left untouched.
func (e *StatusError) WithDetails(details ...proto.Message) (*StatusError, error) {
	p := e.st.Proto()
	for _, detail := range details {
		detailAny, err := anypb.New(detail)
		if err != nil {
			return nil, err
		}
		p.Details = append(p.Details, detailAny)
	}
	return &StatusError{st: status.FromProto(p), cause: e.cause}, nil
}

// Details returns the structured details attached to the status. When an
// individual detail cannot be decoded (for example because its message type
// is unknown to this binary or its bytes are corrupted), the decode error is
// returned in its place and the remaining details are still decoded. Details
// never affect the transport: an undecodable detail degrades to a plain
// status and never invalidates the connection.
func (e *StatusError) Details() []any {
	return e.st.Details()
}

// Unwrap exposes the underlying local error, if any. This lets callers use
// errors.Is and errors.As to diagnose the condition (a transport error, a
// context cancellation, ...) that the status was derived from.
func (e *StatusError) Unwrap() error {
	return e.cause
}

// Is reports whether the target is a StatusError with the same code, message
// and details. The diagnostic cause is intentionally excluded from the
// comparison so that a client-side reconstruction still matches the server's
// status error.
func (e *StatusError) Is(target error) bool {
	tse, ok := target.(*StatusError)
	if !ok {
		return false
	}
	return proto.Equal(e.st.Proto(), tse.st.Proto())
}

// codedError turns one of the package sentinel errors into a status-aware
// error without changing its Error() text or its identity: the original
// pointer is still comparable with == and usable with errors.Is.
type codedError struct {
	code codes.Code
	msg  string
}

func (e *codedError) Error() string {
	return e.msg
}

// GRPCStatus maps the sentinel condition onto a stable grpc code so that
// status.Code and status.FromError work on transport-level errors.
func (e *codedError) GRPCStatus() *status.Status {
	return status.New(e.code, e.msg)
}

// mapStatusError maps client-side errors that never came from the wire onto
// StatusErrors with a stable code. Remote status errors are returned as-is,
// plain sentinels keep their identity and context errors keep their
// unwrappable cause so errors.Is(err, context.Canceled) still succeeds.
func mapStatusError(err error) error {
	if err == nil {
		return nil
	}

	var gs interface{ GRPCStatus() *status.Status }
	if errors.As(err, &gs) {
		// Status errors (ttrpc or grpc) and status-aware sentinels already
		// carry a stable code; leave them untouched.
		return err
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return statusErrorFromStatus(status.New(codes.DeadlineExceeded, err.Error()), err)
	case errors.Is(err, context.Canceled):
		return statusErrorFromStatus(status.New(codes.Canceled, err.Error()), err)
	}

	return err
}

// transportStatusError maps a transport failure onto a codes.Unavailable
// status while preserving the original error as the diagnostic cause.
func transportStatusError(err error) *StatusError {
	return statusErrorFromStatus(status.New(codes.Unavailable, err.Error()), err)
}
