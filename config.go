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
)

type serverConfig struct {
	handshaker  Handshaker
	interceptor UnaryServerInterceptor

	capturePeerCredentials PeerCredentialsCaptureFunc
	verifyPeerCredentials  PeerCredentialsVerifierFunc
}

// ServerOpt for configuring a ttrpc server
type ServerOpt func(*serverConfig) error

// WithServerHandshaker can be passed to NewServer to ensure that the
// handshaker is called before every connection attempt.
//
// Only one handshaker is allowed per server.
func WithServerHandshaker(handshaker Handshaker) ServerOpt {
	return func(c *serverConfig) error {
		if c.handshaker != nil {
			return errors.New("only one handshaker allowed per server")
		}
		c.handshaker = handshaker
		return nil
	}
}

// WithUnaryServerInterceptor sets the provided interceptor on the server
func WithUnaryServerInterceptor(i UnaryServerInterceptor) ServerOpt {
	return func(c *serverConfig) error {
		if c.interceptor != nil {
			return errors.New("only one unchained interceptor allowed per server")
		}
		c.interceptor = i
		return nil
	}
}

// WithUnixPeerCredentials makes the server capture the Unix domain socket
// peer credentials (effective UID, GID and PID) of every accepted connection
// exactly once, at accept time.
//
// The captured snapshot is frozen for the lifetime of the connection and is
// attached to the context passed to handlers and interceptors, where it can be
// read with PeerCredentialsFromContext. It is also validated with verify, if
// non-nil, before any request on the connection is served; a verification
// error rejects the connection. Capture errors, missing credentials, truncated
// or unsupported credentials are always fatal for the connection: the server
// never serves a connection whose credentials could not be established, so a
// peer can never be treated as anonymous by accident.
//
// Credentials are Linux-specific. On other platforms connections are rejected
// with ErrPeerCredentialsUnsupported.
func WithUnixPeerCredentials(verify PeerCredentialsVerifierFunc) ServerOpt {
	return func(c *serverConfig) error {
		if c.capturePeerCredentials != nil {
			return errors.New("peer credentials capture already configured")
		}
		c.capturePeerCredentials = captureUnixPeerCredentials
		c.verifyPeerCredentials = verify
		return nil
	}
}

// WithPeerCredentialsCapture configures a custom capture function for peer
// credentials. It is the platform-independent counterpart of
// WithUnixPeerCredentials and lets a server bind credentials from another
// transport, such as TLS, to each connection.
//
// The function is called exactly once per accepted connection. Errors reject
// the connection.
func WithPeerCredentialsCapture(capture PeerCredentialsCaptureFunc, verify PeerCredentialsVerifierFunc) ServerOpt {
	return func(c *serverConfig) error {
		if c.capturePeerCredentials != nil {
			return errors.New("peer credentials capture already configured")
		}
		if capture == nil {
			return errors.New("peer credentials capture must not be nil")
		}
		c.capturePeerCredentials = capture
		c.verifyPeerCredentials = verify
		return nil
	}
}

// WithChainUnaryServerInterceptor sets the provided chain of server interceptors
func WithChainUnaryServerInterceptor(interceptors ...UnaryServerInterceptor) ServerOpt {
	return func(c *serverConfig) error {
		if len(interceptors) == 0 {
			return nil
		}
		if c.interceptor != nil {
			interceptors = append([]UnaryServerInterceptor{c.interceptor}, interceptors...)
		}
		c.interceptor = func(
			ctx context.Context,
			unmarshal Unmarshaler,
			info *UnaryServerInfo,
			method Method) (any, error) {
			return interceptors[0](ctx, unmarshal, info,
				chainUnaryServerInterceptors(info, method, interceptors[1:]))
		}
		return nil
	}
}

func chainUnaryServerInterceptors(info *UnaryServerInfo, method Method, interceptors []UnaryServerInterceptor) Method {
	if len(interceptors) == 0 {
		return method
	}
	return func(ctx context.Context, unmarshal func(any) error) (any, error) {
		return interceptors[0](ctx, unmarshal, info,
			chainUnaryServerInterceptors(info, method, interceptors[1:]))
	}
}
