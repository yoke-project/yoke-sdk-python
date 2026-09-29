"""The cases of the-harness.std.md, one test each. The harness runs as the process the suite launches,
against a plugin channel and the suite's control socket, both made here."""

import ast
import asyncio
import json
import os
import pathlib
import shutil
import signal
import sys
import tempfile
import unittest

import grpc
from google.protobuf import duration_pb2
from yoke.plugin.v1 import (
    families_pb2,
    register_pb2,
    register_pb2_grpc,
    session_pb2,
    session_pb2_grpc,
)

from yoke_sdk import harness, plugin

ROOT = pathlib.Path(__file__).resolve().parent.parent


def restricted(request):
    return register_pb2.RegisterResponse(
        outcome=register_pb2.RegisterResponse.OUTCOME_ACCEPTED_WITH_RESTRICTIONS,
        session_id="sid-1",
        granted=register_pb2.Surface(),
        withheld=request.declared,
        heartbeat=register_pb2.HeartbeatTerms(
            interval=duration_pb2.Duration(seconds=10), tolerance=3
        ),
    )


class Channel(register_pb2_grpc.RegisterServicer, session_pb2_grpc.SessionServicer):
    def __init__(self, answer):
        self.answer = answer
        self.to_unit = None
        self.opened = asyncio.Event()

    async def Register(self, request, context):
        return self.answer(request)

    async def Open(self, request_iterator, context):
        out = asyncio.Queue()
        self.to_unit = out
        self.opened.set()

        async def inbound():
            async for e in request_iterator:
                if (
                    e.WhichOneof("payload") == "session"
                    and e.session.WhichOneof("kind") == "close"
                ):
                    await out.put(None)
                    return

        reading = asyncio.create_task(inbound())
        try:
            while (e := await out.get()) is not None:
                yield e
        finally:
            reading.cancel()


def revoked(cause):
    return session_pb2.Envelope(
        message_id="c-9",
        session_id="sid-1",
        session=families_pb2.SessionMessage(
            revoked=families_pb2.SessionMessage.Revoked(cause=cause)
        ),
    )


class TheHarness(unittest.IsolatedAsyncioTestCase):
    async def started(self, answer=restricted):
        self.dir = tempfile.mkdtemp(prefix="yph")
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.channel = Channel(answer)
        server = grpc.aio.server()
        register_pb2_grpc.add_RegisterServicer_to_server(self.channel, server)
        session_pb2_grpc.add_SessionServicer_to_server(self.channel, server)
        server.add_insecure_port("unix://" + os.path.join(self.dir, "plugin.sock"))
        await server.start()
        self.addAsyncCleanup(server.stop, None)
        connected = asyncio.get_running_loop().create_future()

        async def accept(reader, writer):
            if not connected.done():
                connected.set_result((reader, writer))

        control = await asyncio.start_unix_server(
            accept, path=os.path.join(self.dir, "control.sock")
        )
        self.addAsyncCleanup(control.wait_closed)
        self.addCleanup(control.close)
        env = {
            "PATH": os.environ.get("PATH", ""),
            "CONFORMANCE_SOCKET": os.path.join(self.dir, "control.sock"),
            "YOKE_PLUGIN": "com.yoke.conformance.python",
            "YOKE_UNIT": "harness",
            "YOKE_SOCKET": os.path.join(self.dir, "plugin.sock"),
            "YOKE_BIND": os.path.join(self.dir, "bind.sock"),
            "YOKE_TOKEN": "t",
        }
        self.process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "yoke_sdk.harness", env=env
        )
        self.addAsyncCleanup(self._kill)
        self.reader, self.writer = await asyncio.wait_for(connected, 10)
        self.addCleanup(self.writer.close)
        self.next = 0
        return await self.read()

    async def _kill(self):
        if self.process.returncode is None:
            self.process.kill()
            await self.process.wait()

    async def read(self):
        line = await asyncio.wait_for(self.reader.readline(), 5)
        self.assertTrue(line, "the harness hung up")
        return json.loads(line)

    async def directive(self, verb, args=None):
        self.next += 1
        id = f"d-{self.next}"
        self.writer.write(
            json.dumps(
                {"type": "directive", "id": id, "verb": verb, "args": args or {}}
            ).encode()
            + b"\n"
        )
        await self.writer.drain()
        while True:
            m = await self.read()
            if m["type"] == "result":
                return id, m

    async def send(self, envelope):
        await asyncio.wait_for(self.channel.opened.wait(), 5)
        await self.channel.to_unit.put(envelope)

    async def exits(self, within):
        return await asyncio.wait_for(self.process.wait(), within)

    # std: yoke-sdk-python:the-harness.01
    async def test_hello_first(self):
        hello = await self.started()
        self.assertEqual(
            (
                hello["type"],
                hello["contract"],
                hello["language"],
                hello["sdk"],
                hello["version"],
                hello["unit"],
            ),
            ("hello", "plugin", "python", plugin.SDK_LINE, 1, "harness"),
        )

    # std: yoke-sdk-python:the-harness.02
    async def test_describe_answers_with_the_generated_manifest(self):
        await self.started()
        id, result = await self.directive("describe")
        self.assertEqual(result["id"], id)
        self.assertEqual(result["value"]["manifest"], harness.declaration().manifest())

    # std: yoke-sdk-python:the-harness.03
    async def test_start_reports_what_admission_answered(self):
        await self.started()
        _, result = await self.directive("start")
        self.assertEqual(result["value"]["outcome"], "accepted with restrictions")
        self.assertTrue(result["value"]["withheld"]["streams"])

        def refused(request):
            return register_pb2.RegisterResponse(
                outcome=register_pb2.RegisterResponse.OUTCOME_REFUSED,
                stage=register_pb2.STAGE_AUTHENTICATION,
                code="admission.auth.consumed",
                message="the library's own words",
            )

        await self._kill()
        await self.started(refused)
        _, result = await self.directive("start")
        self.assertEqual(
            (result["refusal"], result["value"]["stage"]),
            ("admission.auth.consumed", "authentication"),
        )
        self.assertNotIn("own words", json.dumps(result))

    # std: yoke-sdk-python:the-harness.04
    async def test_a_verb_it_does_not_know_is_unrecognised(self):
        await self.started()
        id, result = await self.directive("subscribe")
        self.assertEqual((result["id"], result["unrecognised"]), (id, True))

    # std: yoke-sdk-python:the-harness.05
    async def test_what_the_library_surfaces_is_an_observation(self):
        await self.started()
        await self.directive("start")
        await self.send(
            session_pb2.Envelope(
                message_id="c-1",
                session_id="sid-1",
                control=families_pb2.Control(
                    command=families_pb2.Control.Command(type="calibrate")
                ),
            )
        )
        await self.send(
            revoked(families_pb2.SessionMessage.Revoked.CAUSE_PLUGIN_DISABLED)
        )
        first, second = await self.read(), await self.read()
        self.assertEqual((first["type"], first["kind"]), ("observation", "command"))
        self.assertEqual(second["kind"], "session-ended")
        self.assertEqual(
            (second["fields"]["cause"], second["fields"]["closed"]),
            ("plugin disabled", False),
        )
        self.assertEqual(await self.exits(3), 0)

    # std: yoke-sdk-python:the-harness.06
    async def test_finish_ends_the_harness_which_holds_no_wire(self):
        await self.started()
        self.writer.write(b'{"type":"finish"}\n')
        await self.writer.drain()
        self.assertEqual(await self.exits(3), 0)
        source = (ROOT / "src" / "yoke_sdk" / "harness.py").read_text()
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = (
                    [node.module or ""]
                    if isinstance(node, ast.ImportFrom)
                    else [a.name for a in node.names]
                )
                for name in names:
                    self.assertFalse(
                        name.startswith(("yoke.", "grpc", "google")),
                        f"the harness imports {name}",
                    )

    # std: yoke-sdk-python:the-harness.07
    async def test_asked_to_stop_the_harness_reports_the_end_first(self):
        await self.started()
        await self.directive("start")
        self.process.send_signal(signal.SIGTERM)
        await asyncio.sleep(0.1)
        await self.send(
            revoked(families_pb2.SessionMessage.Revoked.CAUSE_LIVENESS_LOST)
        )
        o = await self.read()
        self.assertEqual(o["kind"], "session-ended")
        self.assertEqual(await self.exits(5), 0)

    # std: yoke-sdk-python:the-harness.08
    async def test_the_suite_is_the_published_pair_authenticated_and_never_built(self):
        script = (ROOT / "ci" / "conformance.sh").read_text()
        self.assertIn("yoke-conformance-", script)
        self.assertIn("releases/download", script)
        self.assertIn("manifest.jsonl", script)
        self.assertIn("sha256sum", script)
        words = script.split()
        for first, second in zip(words, words[1:]):
            self.assertNotIn(
                (first, second),
                {("go", "install"), ("go", "build"), ("go", "run")},
                "the script builds yoke",
            )
        workflow = (ROOT / ".github" / "workflows" / "verify.yml").read_text()
        test, conformance = workflow.find("run: just test"), workflow.find(
            "run: ci/conformance.sh"
        )
        self.assertTrue(
            0 <= test < conformance,
            "the workflow does not run the suite after just test",
        )


if __name__ == "__main__":
    unittest.main()
