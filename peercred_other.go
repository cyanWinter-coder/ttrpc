//go:build !linux

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

import "net"

// captureUnixPeerCredentials is unsupported off Linux: there is no portable
// SO_PEERCRED equivalent in the standard library, and pretending to capture
// credentials would silently authenticate every peer as the zero user.
func captureUnixPeerCredentials(net.Conn) (PeerCredentials, error) {
	return PeerCredentials{}, ErrPeerCredentialsUnsupported
}

// peerCredentialsFromPlatformHandshake has no platform-specific handshake
// payload to interpret off Linux.
func peerCredentialsFromPlatformHandshake(any) (PeerCredentials, bool) {
	return PeerCredentials{}, false
}
