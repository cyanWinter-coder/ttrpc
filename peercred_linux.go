//go:build linux

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
	"fmt"
	"net"
	"unsafe"

	"golang.org/x/sys/unix"
)

// sizeofUcred is the kernel's struct ucred size (three 32-bit fields). The
// kernel reports the number of bytes it copied back through the optlen
// pointer; asking for exactly this many bytes lets us detect a short read.
const sizeofUcred = 4 + 4 + 4

// soPeerCred is the socket option name read by the capture function. It is a
// variable only so tests can substitute a different option and exercise the
// strict size and error handling with real kernel behaviour.
var soPeerCred = unix.SO_PEERCRED

// captureUnixPeerCredentials reads SO_PEERCRED from a freshly accepted Unix
// socket connection.
//
// SO_PEERCRED returns the credentials that were in effect when the peer called
// connect(2), which the kernel records on the socket itself. Reading it at
// accept time therefore freezes the identity of that specific connection: the
// value cannot drift to another process and cannot be affected by later
// connections.
//
// The read is strict. A non-Unix connection, a syscall failure, an option the
// kernel does not know, or a kernel that reports fewer than sizeofUcred bytes
// is an error rather than an anonymous peer.
func captureUnixPeerCredentials(conn net.Conn) (PeerCredentials, error) {
	uc, err := requireUnixSocket(conn)
	if err != nil {
		return PeerCredentials{}, fmt.Errorf("%w: %w", ErrPeerCredentialsUnavailable, err)
	}

	rs, err := uc.SyscallConn()
	if err != nil {
		return PeerCredentials{}, fmt.Errorf("%w: (net.UnixConn).SyscallConn failed: %w", ErrPeerCredentialsUnavailable, err)
	}

	var (
		ucred    unix.Ucred
		ucredErr error
	)
	ctrlErr := rs.Control(func(fd uintptr) {
		ucred, ucredErr = getsockoptUcredStrict(int(fd), soPeerCred)
	})
	if ctrlErr != nil {
		return PeerCredentials{}, fmt.Errorf("%w: (*syscall.RawConn).Control failed: %w", ErrPeerCredentialsUnavailable, ctrlErr)
	}
	if ucredErr != nil {
		return PeerCredentials{}, fmt.Errorf("%w: failed to retrieve socket peer credentials: %w", ErrPeerCredentialsUnavailable, ucredErr)
	}
	if ucred.Pid <= 0 {
		// A connected userspace peer always has a positive pid. A
		// non-positive pid means the kernel did not report a real
		// identity, so refuse to treat the peer as uid 0.
		return PeerCredentials{}, fmt.Errorf("%w: SO_PEERCRED returned pid %d", ErrPeerCredentialsUnavailable, ucred.Pid)
	}

	return PeerCredentials{
		UID: ucred.Uid,
		GID: ucred.Gid,
		PID: ucred.Pid,
	}, nil
}

// getsockoptUcredStrict performs the SO_PEERCRED-equivalent getsockopt with an
// explicit size check.
//
// unix.GetsockoptUcred discards the kernel-reported optlen, so a truncated
// response would silently decode as zeroed fields and look like a valid uid 0.
// Reading the length ourselves turns that into an error.
func getsockoptUcredStrict(fd, opt int) (unix.Ucred, error) {
	var (
		ucred  unix.Ucred
		optlen = uint32(sizeofUcred) // in/out socklen_t
	)

	// The unsafe.Pointer conversions appear directly in the call arguments,
	// which keeps the pointed-to memory alive for the duration of the
	// syscall as required by the unsafe package rules.
	_, _, errno := unix.Syscall6(
		unix.SYS_GETSOCKOPT,
		uintptr(fd),
		uintptr(unix.SOL_SOCKET),
		uintptr(opt),
		uintptr(unsafe.Pointer(&ucred)),
		uintptr(unsafe.Pointer(&optlen)),
		0,
	)
	if errno != 0 {
		return unix.Ucred{}, errno
	}
	if optlen != sizeofUcred {
		return unix.Ucred{}, fmt.Errorf("%w: SO_PEERCRED returned %d bytes, want %d", ErrPeerCredentialsTruncated, optlen, sizeofUcred)
	}

	return ucred, nil
}

// peerCredentialsFromPlatformHandshake lifts credentials returned by the
// historical UnixCredentialsFunc handshaker into a PeerCredentials snapshot.
//
// UnixCredentialsFunc runs at accept time and returns the *unix.Ucred it read
// with SO_PEERCRED. Freezing that value onto the connection keeps the legacy
// handshakers working while giving them the same per-connection binding as the
// capture API.
func peerCredentialsFromPlatformHandshake(data any) (PeerCredentials, bool) {
	ucred, ok := data.(*unix.Ucred)
	if !ok || ucred == nil {
		return PeerCredentials{}, false
	}
	return PeerCredentials{
		UID: ucred.Uid,
		GID: ucred.Gid,
		PID: ucred.Pid,
	}, true
}
