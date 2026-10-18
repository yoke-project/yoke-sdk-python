"""The cases of across-a-routable-address.std.md: a Core on loopback over TLS, which records what each
connection negotiated, and the library's attachments to it."""

import asyncio
import os
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import unittest

from interface_core import (
    Core,
    answer,
    opening,
    refusal,
    request_of,
    serve,
)
from yoke.interface.v1 import operations_pb2
from google.protobuf import timestamp_pb2

from yoke_sdk import base
from yoke_sdk._tls import carry
from yoke_sdk.interface import Attachment


class Pki:
    """A root, and a chain for `localhost` issued by it, made by openssl."""

    def __init__(self, directory):
        def run(*args):
            subprocess.run(
                ["openssl", *args],
                cwd=directory,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        ec = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes"]
        run(
            "req", "-x509", *ec, "-keyout", "root.key", "-out", "root.pem",
            "-subj", "/CN=Yoke test root", "-days", "2",
            "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign",
        )  # fmt: skip
        run(
            "req",
            *ec,
            "-keyout",
            "leaf.key",
            "-out",
            "leaf.csr",
            "-subj",
            "/CN=localhost",
        )
        with open(os.path.join(directory, "leaf.ext"), "w") as f:
            f.write(
                "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
                "keyUsage=digitalSignature\nextendedKeyUsage=serverAuth\n"
            )
        run(
            "x509", "-req", "-in", "leaf.csr", "-CA", "root.pem", "-CAkey", "root.key",
            "-CAcreateserial", "-out", "leaf.pem", "-days", "2", "-extfile", "leaf.ext",
        )  # fmt: skip
        self.root = open(os.path.join(directory, "root.pem"), "rb").read()
        self.chain = os.path.join(directory, "leaf.pem")
        self.key = os.path.join(directory, "leaf.key")


class Terminator:
    """TLS on loopback in front of a Core's local socket, limited to one protocol version: what each
    connection negotiated is recorded, and the plaintext is carried to the Core."""

    def __init__(self, pki, version, upstream):
        self.negotiated, self.connections = [], 0
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = context.maximum_version = version
        context.load_cert_chain(pki.chain, pki.key)
        context.set_alpn_protocols(["h2"])
        self._context, self._upstream = context, upstream
        self._listener = socket.socket()
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(8)
        self.address = "127.0.0.1:%d" % self._listener.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                raw, _ = self._listener.accept()
            except OSError:
                return
            self.connections += 1
            threading.Thread(target=self._serve, args=(raw,), daemon=True).start()

    def _serve(self, raw):
        try:
            tls = self._context.wrap_socket(raw, server_side=True)
        except (ssl.SSLError, OSError):
            raw.close()
            return
        self.negotiated.append(
            (tls.version(), tls.selected_alpn_protocol(), tls.getpeercert() is not None)
        )
        local = socket.socket(socket.AF_UNIX)
        local.connect(self._upstream)
        carry(tls, local)

    def close(self):
        self._listener.close()


def greeting():
    async def script(inbox, out):
        out.put_nowait(opening())
        while True:
            await inbox.get()

    return Core(script)


class AcrossARoutableAddress(unittest.IsolatedAsyncioTestCase):
    def pki(self):
        directory = tempfile.mkdtemp(prefix="ykt")
        self.addCleanup(shutil.rmtree, directory, True)
        return Pki(directory)

    async def terminated(self, core, pki, version):
        terminator = Terminator(pki, version, await serve(self, core))
        self.addCleanup(terminator.close)
        return terminator

    # std: yoke-sdk-python:across-a-routable-address.01
    async def test_the_library_dials_tls_13_alone_against_the_roots_its_caller_supplies(
        self,
    ):
        a, b = self.pki(), self.pki()
        roots = (a.root, "localhost")

        good = greeting()
        server = await self.terminated(good, a, ssl.TLSVersion.TLSv1_3)
        attached = await asyncio.wait_for(
            Attachment.attach(server.address, tls=roots), 10
        )
        self.assertEqual(attached.version, 1)
        self.assertEqual(server.negotiated, [("TLSv1.3", "h2", False)])
        await attached.close()

        old = greeting()
        server = await self.terminated(old, a, ssl.TLSVersion.TLSv1_2)
        with self.assertRaises(base.Error):
            await Attachment.attach(server.address, tls=roots)
        self.assertEqual(old.attachments, 0, "something attached over TLS 1.2")

        stranger = greeting()
        server = await self.terminated(stranger, b, ssl.TLSVersion.TLSv1_3)
        with self.assertRaises(base.Error):
            await Attachment.attach(server.address, tls=roots)
        self.assertEqual(
            stranger.attachments, 0, "something attached to a chain of another root"
        )

        untouched = greeting()
        server = await self.terminated(untouched, a, ssl.TLSVersion.TLSv1_3)
        with self.assertRaises(base.Misuse) as refused:
            await Attachment.attach(server.address, tls=(b"", "localhost"))
        self.assertIn("roots", str(refused.exception))
        self.assertEqual(
            server.connections, 0, "a connection was dialled with no roots"
        )

        with self.assertRaises(base.Misuse) as local:
            await Attachment.attach("/tmp/yoke-no-such.sock", tls=roots)
        self.assertIn("local socket", str(local.exception))

    # std: yoke-sdk-python:across-a-routable-address.02
    async def test_the_credential_is_the_attachments_first_frame_and_its_answer_is_surfaced(
        self,
    ):
        async def script(inbox, out):
            # The Core reads the first frame before it opens.
            first = await inbox.get()
            r = request_of(first)
            accepted = False
            if r is not None and r.WhichOneof("operation") == "authenticate":
                auth = r.authenticate
                if auth.WhichOneof("credential") == "password":
                    accepted = (
                        auth.password.account == "alice"
                        and auth.password.secret == "hunter2x"
                    )
                else:
                    accepted = auth.token == "tok-alice"
            out.put_nowait(opening())
            if accepted:
                out.put_nowait(
                    answer(
                        first.call,
                        authenticate=operations_pb2.Authenticated(
                            account="alice",
                            token="tok-alice",
                            expires=timestamp_pb2.Timestamp(seconds=4_000_000_000),
                        ),
                    )
                )
            else:
                out.put_nowait(refusal(first.call, code="auth.invalid", message="no"))
            while True:
                await inbox.get()

        pki = self.pki()
        core = Core(script)
        server = await self.terminated(core, pki, ssl.TLSVersion.TLSv1_3)
        roots = (pki.root, "localhost")

        by_password = await asyncio.wait_for(
            Attachment.attach(
                server.address, tls=roots, password=("alice", "hunter2x")
            ),
            10,
        )
        self.assertEqual(
            (by_password.established.account, by_password.established.token),
            ("alice", "tok-alice"),
        )
        self.assertEqual(by_password.established.expires.seconds, 4_000_000_000)
        await by_password.close()

        by_token = await asyncio.wait_for(
            Attachment.attach(server.address, tls=roots, token="tok-alice"), 10
        )
        self.assertEqual(by_token.established.account, "alice")
        await by_token.close()

        with self.assertRaises(base.Refusal) as refused:
            await Attachment.attach(
                server.address, tls=roots, password=("alice", "other")
            )
        self.assertEqual(refused.exception.code, "auth.invalid")

        firsts = [
            next(f for n, f in core.frames if n == attachment)
            for attachment in (1, 2, 3)
        ]
        for f in firsts:
            r = request_of(f)
            self.assertEqual(r.version, 1)
            self.assertEqual(r.WhichOneof("operation"), "authenticate")
        self.assertEqual(firsts[0].request.authenticate.password.secret, "hunter2x")
        self.assertEqual(firsts[1].request.authenticate.token, "tok-alice")


if __name__ == "__main__":
    unittest.main()
