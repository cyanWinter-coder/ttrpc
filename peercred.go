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
	"fmt"
	"net"
	"sync"
	"syscall"
)

// PeerCredentialsCaptureFunc captures the peer credentials of a freshly
// accepted connection.
//
// The server calls it exactly once per accepted connection, before any request
// is read. Returning an error rejects the connection: the server must never
// fall back to serving the connection as an anonymous peer.
//
// The returned ConnectionID is ignored; the server assigns it.
type PeerCredentialsCaptureFunc func(conn net.Conn) (PeerCredentials, error)

// PeerCredentialsVerifierFunc validates the frozen credentials of a connection
// before it is served. Returning an error rejects the connection.
type PeerCredentialsVerifierFunc func(creds PeerCredentials) error

// PeerCredentials identifies the process at the other end of an accepted Unix
// domain socket connection.
//
// A PeerCredentials value is captured exactly once, when the connection is
// accepted, and is immutable from then on. The server keeps one snapshot per
// connection and attaches that snapshot to the context of every request served
// on the connection, so a request can never observe credentials that belong to
// a different (for example, a later) connection.
//
// Use PeerCredentialsFromContext to read the credentials from a handler.
type PeerCredentials struct {
	// UID is the effective user ID of the peer at the time it connected.
	UID uint32
	// GID is the effective group ID of the peer at the time it connected.
	GID uint32
	// PID is the process ID of the peer at the time it connected.
	PID int32
	// ConnectionID is the server-assigned identity of the connection the
	// credentials were captured for. Every accepted connection, including a
	// reconnect from the same process, receives a new, never reused
	// ConnectionID. A ConnectionID is only unique within a single Server.
	ConnectionID uint64
}

type peerCredentialsKey struct{}

// PeerCredentialsFromContext returns the peer credentials captured for the
// connection that the request arrived on.
//
// The boolean result is false when the connection has no captured
// credentials, for example when the server was not configured with
// WithUnixPeerCredentials and the handshaker did not provide any. Callers must
// not treat the zero value as an authenticated peer.
func PeerCredentialsFromContext(ctx context.Context) (PeerCredentials, bool) {
	creds, ok := ctx.Value(peerCredentialsKey{}).(PeerCredentials)
	return creds, ok
}

// WithPeerCredentials returns a context carrying the given credentials.
//
// It is used by the server to bind a frozen credential snapshot to a
// connection. Applications should normally only read credentials with
// PeerCredentialsFromContext and rely on the server to populate them.
func WithPeerCredentials(ctx context.Context, creds PeerCredentials) context.Context {
	return context.WithValue(ctx, peerCredentialsKey{}, creds)
}

// peerCredentialsFromHandshake extracts a credential snapshot from data
// returned by a Handshaker. It keeps servers configured with the historical
// UnixSocketRequire* handshakers working while still binding their captured
// credentials to the connection generation.
func peerCredentialsFromHandshake(data any) (PeerCredentials, bool) {
	if data == nil {
		return PeerCredentials{}, false
	}
	if creds, ok := data.(PeerCredentials); ok {
		// The handshaker may not choose the connection identity; the
		// server always assigns it.
		creds.ConnectionID = 0
		return creds, true
	}
	return peerCredentialsFromPlatformHandshake(data)
}

// RequirePeerUID returns a verifier that accepts only peers whose effective
// UID matches uid. Pass -1 to skip the UID check.
func RequirePeerUID(uid int) PeerCredentialsVerifierFunc {
	return func(creds PeerCredentials) error {
		if uid != -1 && uint32(uid) != creds.UID {
			return fmt.Errorf("%w: uid %d, want %d: %v", ErrPeerCredentialsRejected, creds.UID, uid, syscall.EPERM)
		}
		return nil
	}
}

// RequirePeerUIDGID returns a verifier that accepts only peers whose effective
// UID and GID match. Pass -1 for either argument to skip that check.
func RequirePeerUIDGID(uid, gid int) PeerCredentialsVerifierFunc {
	return func(creds PeerCredentials) error {
		if err := RequirePeerUID(uid)(creds); err != nil {
			return err
		}
		if gid != -1 && uint32(gid) != creds.GID {
			return fmt.Errorf("%w: gid %d, want %d: %v", ErrPeerCredentialsRejected, creds.GID, gid, syscall.EPERM)
		}
		return nil
	}
}

// credentialCache tracks the frozen credential snapshot of every live
// connection, keyed by connection generation.
//
// Entries are added when a connection is accepted and removed exactly once,
// when the connection is torn down or the server is closed. Because removal is
// by generation rather than by process identity, a stale connection can never
// remove the entry of a newer connection started by the same process.
type credentialCache struct {
	mu    sync.Mutex
	creds map[uint64]PeerCredentials
	// removed counts the generations actually dropped by remove. A second
	// remove for the same generation does not count, which lets tests and
	// diagnostics distinguish exactly-once release from double release.
	removed uint64
}

func newCredentialCache() *credentialCache {
	return &credentialCache{creds: make(map[uint64]PeerCredentials)}
}

func (c *credentialCache) add(creds PeerCredentials) {
	c.mu.Lock()
	c.creds[creds.ConnectionID] = creds
	c.mu.Unlock()
}

func (c *credentialCache) lookup(id uint64) (PeerCredentials, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	creds, ok := c.creds[id]
	return creds, ok
}

// remove drops the entry for the given connection generation. It reports
// whether an entry was present, so callers can tell the first release of a
// generation from repeated ones. It is safe to call concurrently and more than
// once for the same generation.
func (c *credentialCache) remove(id uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.creds[id]; !ok {
		return false
	}
	delete(c.creds, id)
	c.removed++
	return true
}

func (c *credentialCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.creds)
}
