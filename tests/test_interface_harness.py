"""The cases of the-interface-harness.std.md, one test each. The harness runs against a control socket
made here and a Core that serves the channel `panel` under an instance's root."""

import ast
import asyncio
import json
import os
import pathlib
import shutil
import tempfile
import unittest

import grpc
from interface_core import Core, event, opening, refusal, request_of, unit_record
from yoke.interface.v1 import codes_pb2, interface_pb2_grpc, records_pb2

import yoke_sdk_interface_harness as harness
from yoke_sdk import plugin

ROOT = pathlib.Path(__file__).resolve().parent.parent


def channel_record(name):
    return records_pb2.Record(
        channel=records_pb2.ChannelRecord(
            declared=records_pb2.ChannelRecord.Declared(
                name=name,
                projection="local",
                address_class="local-socket",
                clients="single",
            ),
            observed=records_pb2.ChannelRecord.Observed(attached=True),
        )
    )


class TheInterfaceHarness(unittest.IsolatedAsyncioTestCase):
    async def bench(self, script=None):
        """An instance's root with the channel `panel` served under it, and a control socket."""
        self.root = tempfile.mkdtemp(prefix="yih")
        self.addCleanup(shutil.rmtree, self.root, True)
        os.makedirs(os.path.join(self.root, "interfaces"))
        if script is not None:
            server = grpc.aio.server()
            interface_pb2_grpc.add_InterfaceServicer_to_server(Core(script), server)
            server.add_insecure_port(
                "unix://" + os.path.join(self.root, "interfaces", "panel.sock")
            )
            await server.start()
            self.addAsyncCleanup(server.stop, None)
        self.control = os.path.join(self.root, "control.sock")
        self.connected = asyncio.Future()

        async def accepted(reader, writer):
            self.connected.set_result((reader, writer))

        server = await asyncio.start_unix_server(accepted, self.control)
        self.addAsyncCleanup(self._close, server)
        env = {"CONFORMANCE_SOCKET": self.control, "CONFORMANCE_INSTANCE": self.root}
        self.exit = asyncio.create_task(harness.serve(env.get))
        self.reader, self.writer = await asyncio.wait_for(self.connected, 5)

    async def _close(self, server):
        server.close()
        self.writer.close()

    async def next(self):
        raw = await asyncio.wait_for(self.reader.readline(), 5)
        self.assertTrue(raw, "the harness ended")
        return json.loads(raw)

    async def send(self, line):
        self.writer.write(json.dumps(line).encode() + b"\n")
        await self.writer.drain()

    async def direct(self, n, verb, **args):
        await self.send({"type": "directive", "id": n, "verb": verb, "args": args})
        while True:
            line = await self.next()
            if line["type"] == "result":
                self.assertEqual(line["id"], n)
                return line

    # std: yoke-sdk-python:the-interface-harness.01
    async def test_hello_first_for_the_interface_contract(self):
        await self.bench()
        hello = await self.next()
        self.assertEqual(hello["type"], "hello")
        self.assertEqual(
            (hello["contract"], hello["language"], hello["sdk"], hello["version"]),
            ("interface", "python", plugin.SDK_LINE, 1),
        )
        self.assertNotIn("unit", hello)
        await self.send({"type": "finish"})
        self.assertEqual(await asyncio.wait_for(self.exit, 5), 0)

    # std: yoke-sdk-python:the-interface-harness.02
    async def test_attaching_reports_the_opening_and_the_standing_subscription_in_order(
        self,
    ):
        async def script(inbox, out):
            out.put_nowait(opening("standing", 1, [channel_record("panel")]))
            out.put_nowait(event("standing", 5))
            out.put_nowait(event("standing", 6))
            while True:
                await inbox.get()

        await self.bench(script)
        await self.next()
        result = await self.direct(1, "attach", channel="panel")
        v = result["value"]
        self.assertEqual((v["version"], v["subscription"], v["at"]), (1, "standing", 4))
        self.assertEqual(v["records"][0]["channel"]["declared"]["name"], "panel")
        seen = []
        while len(seen) < 2:
            line = await self.next()
            self.assertEqual((line["type"], line["kind"]), ("observation", "event"))
            seen.append(line["fields"])
        self.assertEqual([f["seq"] for f in seen], [5, 6])
        self.assertTrue(all(f["type"] == "unit.state.changed" for f in seen))
        await self.send({"type": "finish"})
        await asyncio.wait_for(self.exit, 5)

    # std: yoke-sdk-python:the-interface-harness.03
    async def test_a_refusal_is_reported_as_its_code_and_what_it_names(self):
        async def script(inbox, out):
            out.put_nowait(opening())
            while True:
                f = await inbox.get()
                r = request_of(f)
                if r is not None and r.WhichOneof("operation") == "read":
                    out.put_nowait(
                        refusal(
                            f.call,
                            code="subject.unknown",
                            message="no such unit",
                            subject=codes_pb2.Subject(kind="unit", identity="nobody"),
                        )
                    )
                elif r is not None:
                    out.put_nowait(
                        refusal(
                            f.call,
                            code="channel.suspended",
                            message="suspended",
                            suspension=codes_pb2.Suspension(
                                grade="read-only", by="bench"
                            ),
                        )
                    )

        await self.bench(script)
        await self.next()
        await self.direct(1, "attach", channel="panel")
        read = await self.direct(2, "read", kind="unit", identity="nobody")
        self.assertEqual(read["refusal"], "subject.unknown")
        self.assertEqual(read["value"]["subject"]["kind"], "unit")
        self.assertEqual(read["value"]["subject"]["identity"], "nobody")
        command = await self.direct(3, "command", unit="acquire", type="calibrate")
        self.assertEqual(command["refusal"], "channel.suspended")
        self.assertEqual(
            command["value"]["suspension"], {"grade": "read-only", "by": "bench"}
        )
        for line in (read, command):
            self.assertNotIn("no such unit", json.dumps(line))
            self.assertNotIn("message", line)
            self.assertNotIn("message", line["value"])
        await self.send({"type": "finish"})
        await asyncio.wait_for(self.exit, 5)

    # std: yoke-sdk-python:the-interface-harness.04
    async def test_a_verb_it_does_not_know_is_unrecognised_and_it_holds_no_wire(self):
        await self.bench()
        await self.next()
        result = await self.direct(1, "emit", stream="x", payload="y")
        self.assertTrue(result["unrecognised"])
        await self.send({"type": "finish"})
        self.assertEqual(await asyncio.wait_for(self.exit, 5), 0)
        source = (
            ROOT
            / "interface-harness"
            / "src"
            / "yoke_sdk_interface_harness"
            / "__init__.py"
        )
        imported = set()
        for node in ast.walk(ast.parse(source.read_text())):
            if isinstance(node, ast.Import):
                imported |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom):
                imported.add((node.module or "").split(".")[0])
        self.assertFalse(
            imported & {"grpc", "socket", "ssl", "http", "urllib"},
            f"the harness imports a transport: {imported}",
        )

    # std: yoke-sdk-python:the-interface-harness.05
    async def test_the_suite_is_run_against_it_as_against_the_plugin_harness(self):
        script = (ROOT / "ci" / "conformance.sh").read_text()
        self.assertIn("yoke-python-interface-harness", script)
        self.assertIn("yoke-python-plugin-harness", script)
        self.assertIn(
            'run_suite "$bin/yoke-python-interface-harness"',
            script,
            "the script does not run the suite against the interface harness",
        )


if __name__ == "__main__":
    unittest.main()
