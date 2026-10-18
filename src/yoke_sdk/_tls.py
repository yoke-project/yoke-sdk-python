"""TLS 1.3 under the interface library, done here rather than by gRPC: the gRPC this family builds on
cannot be held to TLS 1.3 alone, and a channel at a routable address takes nothing below it. The
handshake is the standard library's, with 1.3 as both its floor and its ceiling; once it is done, a pump
carries the plaintext between the TLS connection and a socket in a directory only this process can enter,
and gRPC speaks HTTP/2 on that socket as on any local one. Never installed."""

import os
import shutil
import socket
import ssl
import tempfile
import threading

from yoke_sdk.base import Misuse, TransportError


def _host_of(address):
    host, separator, _ = address.rpartition(":")
    if not separator:
        raise Misuse(f"{address} is not an address with a port")
    return host.strip("[]")


class Tunnel:
    """The TLS connection to a Core, and the local socket gRPC is to speak on."""

    def __init__(self, tls, directory):
        self._tls, self._directory = tls, directory
        self.path = os.path.join(directory, "channel.sock")
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(self.path)
        self._listener.listen(1)
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        """Accepts the one local connection gRPC makes, and carries bytes both ways until either side
        ends; then ends the other."""
        try:
            local, _ = self._listener.accept()
        except OSError:
            return
        # Only one connection is ever carried: nothing reconnects.
        self._listener.close()
        upstream = threading.Thread(target=self._up, args=(local,), daemon=True)
        upstream.start()
        try:
            while data := self._tls.recv(1 << 16):
                local.sendall(data)
        except (OSError, ssl.SSLError):
            pass
        finally:
            self._end(local)

    def _up(self, local):
        try:
            while data := local.recv(1 << 16):
                self._tls.sendall(data)
        except (OSError, ssl.SSLError):
            pass
        finally:
            self._end(local)

    def _end(self, local):
        for end in (local, self._tls):
            try:
                end.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                end.close()
            except OSError:
                pass

    def close(self):
        """Lets everything go."""
        for end in (self._tls, self._listener):
            try:
                end.close()
            except OSError:
                pass
        shutil.rmtree(self._directory, ignore_errors=True)


def open_tunnel(address, roots, name):
    """A TLS 1.3 connection to host:port: the Core's chain verified against `roots`, in PEM, and no
    others, for `name` — the host where it is empty — with ALPN `h2` and no client certificate. A
    failure names what failed, and nothing is attached."""
    host = _host_of(address)
    port = int(address.rpartition(":")[2])
    if not roots or not roots.strip():
        raise Misuse(
            "TLS was asked with no roots, and the Core's chain is verified only against the roots its "
            "caller supplies"
        )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_3
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    try:
        context.load_verify_locations(
            cadata=roots.decode() if isinstance(roots, bytes) else roots
        )
    except ssl.SSLError as failure:
        raise Misuse(f"the roots cannot be used: {failure}") from failure
    context.set_alpn_protocols(["h2"])
    try:
        raw = socket.create_connection((host, port), timeout=10)
        tls = context.wrap_socket(raw, server_hostname=name or host)
    except (OSError, ssl.SSLError) as failure:
        raise TransportError(
            f"{address} cannot be reached over TLS 1.3: {failure}"
        ) from failure
    tls.settimeout(None)
    if tls.version() != "TLSv1.3" or tls.selected_alpn_protocol() != "h2":
        tls.close()
        raise TransportError(f"{address} did not agree to TLS 1.3 and h2")
    directory = tempfile.mkdtemp(prefix="yoke-iface-")
    os.chmod(directory, 0o700)
    return Tunnel(tls, directory)
