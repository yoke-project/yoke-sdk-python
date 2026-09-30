"""The cases of the-plugin-library.std.md, one test each, against a plugin channel that records what
arrives and says what it is told to."""

import asyncio
import os
import pathlib
import shutil
import tempfile
import time
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

from yoke_sdk import base, plugin

SOURCE = (
    pathlib.Path(__file__).resolve().parent.parent / "src" / "yoke_sdk" / "plugin.py"
)


def accepted():
    return register_pb2.RegisterResponse(
        outcome=register_pb2.RegisterResponse.OUTCOME_ACCEPTED,
        session_id="sid-1",
        granted=register_pb2.Surface(),
        heartbeat=register_pb2.HeartbeatTerms(
            interval=duration_pb2.Duration(nanos=100_000_000), tolerance=3
        ),
    )


class Channel(register_pb2_grpc.RegisterServicer, session_pb2_grpc.SessionServicer):
    """A plugin channel: what it saw, and what it will answer."""

    def __init__(self, answer, bind):
        self.answer, self.bind = answer, bind
        self.registrations, self.bound_when_registered = [], []
        self.sessions = 0
        self.received = []  # (arrival, envelope)
        self.to_unit = None

    async def Register(self, request, context):
        self.registrations.append(request)
        self.bound_when_registered.append(os.path.exists(self.bind))
        return self.answer

    async def Open(self, request_iterator, context):
        self.sessions += 1
        out = asyncio.Queue()
        self.to_unit = out

        async def inbound():
            async for e in request_iterator:
                self.received.append((time.monotonic(), e))
                if (
                    e.WhichOneof("payload") == "session"
                    and e.session.WhichOneof("kind") == "close"
                ):
                    # The Core ends the stream on a CLOSE.
                    await out.put(None)
                    return

        reading = asyncio.create_task(inbound())
        try:
            while (e := await out.get()) is not None:
                yield e
        finally:
            reading.cancel()


def station():
    return plugin.Declaration(
        id="com.yoke.station.acquire",
        needs=["device:instrument"],
        streams=[
            plugin.Stream("station.spectra"),
            plugin.Stream(
                "station.diagnostics", tolerates_loss=True, tolerates_reorder=True
            ),
        ],
        commands=["calibrate"],
        queries=["head-status"],
        occurrences=["calibration.drift"],
        capabilities=[
            plugin.Capability(
                "stream.spectra.publish", plugin.Object(stream="station.spectra")
            ),
            plugin.Capability(
                "stream.diagnostics.publish",
                plugin.Object(stream="station.diagnostics"),
            ),
            plugin.Capability(
                "command.calibrate.accept", plugin.Object(command="calibrate")
            ),
            plugin.Capability(
                "query.head-status.answer", plugin.Object(query="head-status")
            ),
            plugin.Capability(
                "event.calibration-drift.report",
                plugin.Object(occurrence="calibration.drift"),
            ),
        ],
    )


def payload(e):
    return e.WhichOneof("payload")


class ThePluginLibrary(unittest.IsolatedAsyncioTestCase):
    async def bench(self, answer):
        self.dir = tempfile.mkdtemp(prefix="ykp")
        self.addCleanup(shutil.rmtree, self.dir, True)
        os.makedirs(os.path.join(self.dir, "plugins"))
        socket = os.path.join(self.dir, "plugin.sock")
        bind = os.path.join(self.dir, "plugins", "acquire.sock")
        self.channel = Channel(answer, bind)
        server = grpc.aio.server()
        register_pb2_grpc.add_RegisterServicer_to_server(self.channel, server)
        session_pb2_grpc.add_SessionServicer_to_server(self.channel, server)
        server.add_insecure_port("unix://" + socket)
        await server.start()
        self.addAsyncCleanup(server.stop, None)
        self.env = {
            "YOKE_PLUGIN": "com.yoke.station.acquire",
            "YOKE_UNIT": "acquire",
            "YOKE_SOCKET": socket,
            "YOKE_BIND": bind,
            "YOKE_TOKEN": "t-1",
        }
        return self.channel

    async def start(self, declaration=None):
        unit = await plugin.start(declaration or station(), self.env.get)
        self.addAsyncCleanup(unit.close)
        return unit

    async def received(self, found, within=2.0):
        deadline = time.monotonic() + within
        while time.monotonic() < deadline:
            for _, e in self.channel.received:
                if found(e):
                    return e
            await asyncio.sleep(0.01)
        self.fail("the channel never received it")

    async def send(self, **fields):
        await self.channel.to_unit.put(
            session_pb2.Envelope(session_id="sid-1", **fields)
        )

    async def opened(self):
        await self.received(lambda e: payload(e) == "session")

    # std: yoke-sdk-python:the-plugin-library.01
    async def test_a_declaration_generates_the_manifest(self):
        self.assertEqual(
            station().manifest(),
            """manifest: 1
id: com.yoke.station.acquire
protocol: 1
needs:
  - device:instrument
streams:
  - id: station.spectra
  - id: station.diagnostics
    tolerates_loss: true
    tolerates_reorder: true
commands:
  - id: calibrate
queries:
  - id: head-status
occurrences:
  - id: calibration.drift
capabilities:
  - name: stream.spectra.publish
    governs:
      stream: station.spectra
  - name: stream.diagnostics.publish
    governs:
      stream: station.diagnostics
  - name: command.calibrate.accept
    governs:
      command: calibrate
  - name: query.head-status.answer
    governs:
      query: head-status
  - name: event.calibration-drift.report
    governs:
      occurrence: calibration.drift
""",
        )

    # std: yoke-sdk-python:the-plugin-library.02
    async def test_nothing_the_model_does_not_have_can_be_declared(self):
        import dataclasses

        for kind in (
            plugin.Declaration,
            plugin.Stream,
            plugin.Capability,
            plugin.Object,
        ):
            for field in dataclasses.fields(kind):
                for forbidden in ("endpoint", "autostart", "digest", "description"):
                    self.assertNotIn(
                        forbidden,
                        field.name,
                        f"{kind.__name__} lets an author set {field.name}",
                    )

    # std: yoke-sdk-python:the-plugin-library.03
    async def test_the_registration_claims_what_the_manifest_declares(self):
        channel = await self.bench(accepted())
        d = station()
        await self.start(d)
        r = channel.registrations[0]
        self.assertEqual(
            (r.plugin, r.unit, r.token, r.protocol),
            ("com.yoke.station.acquire", "acquire", "t-1", 1),
        )
        self.assertEqual((r.language, r.sdk_line), ("python", plugin.SDK_LINE))
        self.assertEqual(
            list(r.declared.capabilities), [c.name for c in d.capabilities]
        )
        self.assertEqual(
            list(r.declared.streams), ["station.spectra", "station.diagnostics"]
        )
        self.assertEqual(list(r.declared.commands), d.commands)
        self.assertEqual(list(r.declared.queries), d.queries)
        self.assertEqual(list(r.declared.occurrences), d.occurrences)

    # std: yoke-sdk-python:the-plugin-library.04
    async def test_the_units_socket_is_bound_before_it_registers(self):
        channel = await self.bench(accepted())
        await self.start()
        self.assertEqual(channel.bound_when_registered, [True])

    # std: yoke-sdk-python:the-plugin-library.05
    async def test_a_refusal_is_surfaced_and_never_retried(self):
        channel = await self.bench(
            register_pb2.RegisterResponse(
                outcome=register_pb2.RegisterResponse.OUTCOME_REFUSED,
                stage=register_pb2.STAGE_AUTHENTICATION,
                code="admission.auth.consumed",
                message="the token was already spent",
            )
        )
        with self.assertRaises(base.Refusal) as refused:
            await plugin.start(station(), self.env.get)
        self.assertEqual(
            (refused.exception.code, refused.exception.stage),
            ("admission.auth.consumed", "authentication"),
        )
        await asyncio.sleep(0.2)
        self.assertEqual((len(channel.registrations), channel.sessions), (1, 0))

    # std: yoke-sdk-python:the-plugin-library.06
    async def test_an_acceptance_with_restrictions_names_what_was_withheld(self):
        answer = accepted()
        answer.outcome = (
            register_pb2.RegisterResponse.OUTCOME_ACCEPTED_WITH_RESTRICTIONS
        )
        answer.granted.CopyFrom(
            register_pb2.Surface(
                streams=["station.spectra"], capabilities=["stream.spectra.publish"]
            )
        )
        answer.withheld.CopyFrom(
            register_pb2.Surface(
                streams=["station.diagnostics"],
                capabilities=["stream.diagnostics.publish"],
                occurrences=["calibration.drift"],
            )
        )
        await self.bench(answer)
        unit = await self.start()
        a = unit.admission
        self.assertTrue(a.restricted)
        self.assertEqual(a.granted.streams, ["station.spectra"])
        self.assertEqual(a.withheld.streams, ["station.diagnostics"])
        self.assertEqual(a.withheld.capabilities, ["stream.diagnostics.publish"])
        self.assertEqual(a.withheld.occurrences, ["calibration.drift"])

    # std: yoke-sdk-python:the-plugin-library.07
    async def test_the_session_opens_and_beats_on_the_cores_terms(self):
        channel = await self.bench(accepted())
        await self.start()
        await asyncio.sleep(0.45)
        first = channel.received[0][1]
        self.assertEqual(
            (payload(first), first.session.WhichOneof("kind"), first.session_id),
            ("session", "open", "sid-1"),
        )
        beats = [at for at, e in channel.received if payload(e) == "health"]
        self.assertGreaterEqual(len(beats), 3, f"{len(beats)} heartbeats in 450 ms")
        for earlier, later in zip(beats, beats[1:]):
            self.assertGreaterEqual(later - earlier, 0.05)

    # std: yoke-sdk-python:the-plugin-library.08
    async def test_the_end_of_a_session_is_surfaced_and_nothing_reconnects(self):
        channel = await self.bench(accepted())
        unit = await self.start()
        await self.opened()
        await self.send(
            message_id="c-1",
            session=families_pb2.SessionMessage(
                revoked=families_pb2.SessionMessage.Revoked(
                    cause=families_pb2.SessionMessage.Revoked.CAUSE_PLUGIN_DISABLED,
                    line="an operator disabled it",
                )
            ),
        )
        end = await asyncio.wait_for(unit.next(), 2)
        self.assertIsInstance(end, plugin.Ended)
        self.assertEqual(
            (end.closed, end.cause, end.line),
            (False, "plugin disabled", "an operator disabled it"),
        )
        self.assertIsNone(await asyncio.wait_for(unit.next(), 2))
        await asyncio.sleep(0.3)
        self.assertEqual((len(channel.registrations), channel.sessions), (1, 1))

    # std: yoke-sdk-python:the-plugin-library.09
    async def test_an_orderly_close_is_the_units(self):
        await self.bench(accepted())
        unit = await self.start()
        await unit.close()
        await self.received(
            lambda e: payload(e) == "session"
            and e.session.WhichOneof("kind") == "close"
        )
        end = await asyncio.wait_for(unit.next(), 3)
        self.assertIsInstance(end, plugin.Ended)
        self.assertTrue(end.closed)

    # std: yoke-sdk-python:the-plugin-library.10
    async def test_what_the_core_sends_is_surfaced_and_answered_correlated(self):
        await self.bench(accepted())
        unit = await self.start()
        await self.opened()
        await self.send(
            message_id="c-1",
            control=families_pb2.Control(
                command=families_pb2.Control.Command(type="calibrate")
            ),
        )
        await self.send(
            message_id="c-2",
            query=families_pb2.Query(
                question=families_pb2.Query.Question(type="head-status")
            ),
        )
        command = await asyncio.wait_for(unit.next(), 2)
        self.assertIsInstance(command, plugin.Command)
        self.assertEqual(command.type, "calibrate")
        question = await asyncio.wait_for(unit.next(), 2)
        self.assertIsInstance(question, plugin.Question)
        self.assertEqual(question.type, "head-status")
        await unit.ack(command, plugin.Outcome.DONE, "calibrated")
        await unit.answer(question, b"42")
        ack = await self.received(lambda e: payload(e) == "ack")
        self.assertEqual(ack.correlation_id, "c-1")
        answer = await self.received(lambda e: payload(e) == "query")
        self.assertEqual(answer.correlation_id, "c-2")

    # std: yoke-sdk-python:the-plugin-library.11
    async def test_an_occurrence_carries_the_authors_severity_or_is_refused(self):
        channel = await self.bench(accepted())
        unit = await self.start()
        with self.assertRaises(base.Error):
            await unit.report("calibration.drift", None, "drifting")
        await asyncio.sleep(0.1)
        self.assertFalse(
            any(payload(e) == "event" for _, e in channel.received),
            "an occurrence with no severity was sent",
        )
        await unit.report("calibration.drift", plugin.Severity.of(40), "drifting")
        event = await self.received(lambda e: payload(e) == "event")
        self.assertEqual(
            (event.event.occurrence, event.event.severity), ("calibration.drift", 40)
        )

    # std: yoke-sdk-python:the-plugin-library.12
    async def test_nothing_is_emitted_on_a_stream_not_activated(self):
        channel = await self.bench(accepted())
        unit = await self.start()
        with self.assertRaises(base.Refusal) as refused:
            await unit.emit("station.spectra", b"x")
        self.assertEqual(refused.exception.code, "stream.inactive")
        self.assertEqual(
            sorted(os.listdir(os.path.join(self.dir, "plugins"))), ["acquire.sock"]
        )
        await asyncio.sleep(0.1)
        self.assertFalse(
            any(payload(e) == "data" for _, e in channel.received),
            "data reached the channel",
        )


if __name__ == "__main__":
    unittest.main()
