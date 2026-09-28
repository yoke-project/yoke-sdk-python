"""The library a Plugin unit is written with.

One declaration is both the Manifest the library generates and the surface its registration claims,
so the two cannot be written apart. Starting a unit performs the first acts in their order: read the
environment, bind the unit's own socket, register, open the Session — and then beats on the terms the
Core assigned. Everything the Session brings is surfaced, its end included: a Session that ends ends
the incarnation, and the library never reconnects, never retries an admission, never polls, never
creates a stream's transport and never chooses a severity.
"""

import asyncio
import dataclasses
import enum
import json
import os
import re

from yoke.plugin.v1 import (
    families_pb2,
    register_pb2,
    register_pb2_grpc,
    session_pb2_grpc,
)

from yoke_sdk.base import (
    PLUGIN_CONTRACT,
    Envelopes,
    Misuse,
    Refusal,
    TransportError,
    dial,
    environment,
    refusal_of,
)

# What this library says it is, at admission.
SDK_LINE = "yoke-sdk-python 0.0.0"


@dataclasses.dataclass
class Stream:
    """A stream the Plugin may publish, and what its data tolerates."""

    id: str
    tolerates_loss: bool = False
    tolerates_reorder: bool = False


@dataclasses.dataclass
class Object:
    """The one thing a capability governs: set exactly one field."""

    stream: str | None = None
    command: str | None = None
    query: str | None = None
    occurrence: str | None = None
    surface: str | None = None


@dataclasses.dataclass
class Capability:
    """What an operator may grant, governing exactly one object."""

    name: str
    governs: Object


def _scalar(value):
    """A value as YAML writes it: plain where it is an identifier, quoted otherwise."""
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]*", value):
        return value
    return json.dumps(value)


@dataclasses.dataclass
class Declaration:
    """What a Plugin says about itself: what is true of the binary wherever it runs."""

    id: str
    needs: list = dataclasses.field(default_factory=list)
    streams: list = dataclasses.field(default_factory=list)
    commands: list = dataclasses.field(default_factory=list)
    queries: list = dataclasses.field(default_factory=list)
    occurrences: list = dataclasses.field(default_factory=list)
    capabilities: list = dataclasses.field(default_factory=list)

    def manifest(self):
        """The document the declaration generates."""
        lines = [
            "manifest: 1",
            f"id: {_scalar(self.id)}",
            f"protocol: {PLUGIN_CONTRACT}",
        ]
        if self.needs:
            lines.append("needs:")
            lines += [f"  - {_scalar(n)}" for n in self.needs]
        if self.streams:
            lines.append("streams:")
            for s in self.streams:
                lines.append(f"  - id: {_scalar(s.id)}")
                if s.tolerates_loss:
                    lines.append("    tolerates_loss: true")
                if s.tolerates_reorder:
                    lines.append("    tolerates_reorder: true")
        for key, ids in (
            ("commands", self.commands),
            ("queries", self.queries),
            ("occurrences", self.occurrences),
        ):
            if ids:
                lines.append(f"{key}:")
                lines += [f"  - id: {_scalar(i)}" for i in ids]
        if self.capabilities:
            lines.append("capabilities:")
            for c in self.capabilities:
                kind, governed = next(
                    (k, v)
                    for k, v in dataclasses.asdict(c.governs).items()
                    if v is not None
                )
                lines += [
                    f"  - name: {_scalar(c.name)}",
                    "    governs:",
                    f"      {kind}: {_scalar(governed)}",
                ]
        return "\n".join(lines) + "\n"

    def _surface(self):
        """What the registration claims: the declaration again, from the same value."""
        return register_pb2.Surface(
            capabilities=[c.name for c in self.capabilities],
            streams=[s.id for s in self.streams],
            commands=self.commands,
            queries=self.queries,
        )


@dataclasses.dataclass
class Scope:
    """Four lists: what was granted, or what was withheld."""

    capabilities: list
    streams: list
    commands: list
    queries: list

    @classmethod
    def of(cls, surface):
        return cls(
            list(surface.capabilities),
            list(surface.streams),
            list(surface.commands),
            list(surface.queries),
        )


@dataclasses.dataclass
class Admission:
    """What the Core answered."""

    restricted: bool
    granted: Scope
    withheld: Scope  # item by item


@dataclasses.dataclass
class Command:
    """An instruction the Core sent, to be acknowledged."""

    id: str
    type: str
    payload: bytes


@dataclasses.dataclass
class Question:
    """A question the Core asked, to be answered."""

    id: str
    type: str
    payload: bytes


@dataclasses.dataclass
class Activated:
    """A stream the unit may now emit on, and where."""

    stream: str
    transport: str
    address: str


@dataclasses.dataclass
class Stopped:
    """A stream the unit may no longer emit on."""

    stream: str


@dataclasses.dataclass
class Refused:
    """An error the Core answered a message with."""

    correlation: str
    error: Refusal


@dataclasses.dataclass
class Ended:
    """The end of the Session: closed by this unit, or revoked by the Core. Nothing follows it."""

    closed: bool
    cause: (
        str | None
    )  # for a revocation: liveness lost, plugin disabled, scope exceeded, protocol failure
    line: str


class Outcome(enum.IntEnum):
    """What became of a command."""

    ACCEPTED = 1
    DONE = 2
    FAILED = 3


class Severity:
    """How routine or alarming an occurrence is, from 0 to 99: the author's statement and nobody
    else's."""

    def __init__(self, value):
        self.value = value

    @classmethod
    def of(cls, n):
        """A severity the author states."""
        return cls(n)


def _words(name, prefix):
    """An enumerator's name as a person reads it: CAUSE_PLUGIN_DISABLED is 'plugin disabled'."""
    return name.removeprefix(prefix).lower().replace("_", " ")


_END = object()


class Unit:
    """A started unit and its Session."""

    def __init__(self, admission, session_id, call, outbound, server, channel):
        self.admission = admission
        self._envelopes = Envelopes(session_id)
        self._call, self._outbound, self._server, self._channel = (
            call,
            outbound,
            server,
            channel,
        )
        self._events = asyncio.Queue()
        self._ended = self._closing = False
        self._active = set()
        self._tasks = []

    def _send(self, family, message, to=None):
        if self._ended or self._outbound is None:
            raise Refusal("session.revoked", "the Session has ended")
        e = (
            self._envelopes.answer(to, family, message)
            if to
            else self._envelopes.seal(family, message)
        )
        self._outbound.put_nowait(e)

    def _finish(self, end):
        """Ends the Session once: the end is surfaced, nothing follows it, and nothing more is sent."""
        if self._ended:
            return
        self._ended = True
        if self._outbound is not None:
            self._outbound.put_nowait(_END)
            self._outbound = None
        self._events.put_nowait(end)
        self._events.put_nowait(_END)
        for task in self._tasks:
            if task is not asyncio.current_task():
                task.cancel()
        self._server.close()

    async def _receive(self):
        try:
            async for e in self._call:
                family = e.WhichOneof("payload")
                if family == "session" and e.session.WhichOneof("kind") == "revoked":
                    r = e.session.revoked
                    cause = _words(
                        families_pb2.SessionMessage.Revoked.Cause.Name(r.cause),
                        "CAUSE_",
                    )
                    self._finish(Ended(False, cause, r.line))
                    return
                if family == "control":
                    kind = e.control.WhichOneof("kind")
                    if kind == "command":
                        c = e.control.command
                        self._events.put_nowait(
                            Command(e.message_id, c.type, c.payload)
                        )
                    elif kind == "activate":
                        a = e.control.activate
                        transport = _words(
                            families_pb2.Control.Activate.Transport.Name(a.transport),
                            "TRANSPORT_",
                        )
                        self._active.add(a.stream)
                        self._events.put_nowait(
                            Activated(a.stream, transport, a.address)
                        )
                    elif kind == "stop":
                        self._active.discard(e.control.stop.stream)
                        self._events.put_nowait(Stopped(e.control.stop.stream))
                elif family == "query" and e.query.WhichOneof("kind") == "question":
                    q = e.query.question
                    self._events.put_nowait(Question(e.message_id, q.type, q.payload))
                elif family == "error":
                    self._events.put_nowait(
                        Refused(e.correlation_id, refusal_of(e.error))
                    )
            ended = "the stream ended"
        except Exception as failure:
            ended = str(failure)
        if self._closing:
            self._finish(Ended(True, None, ""))
        else:
            self._finish(
                Ended(False, "liveness lost", "the Session's stream ended: " + ended)
            )

    async def _beat(self, interval):
        while True:
            await asyncio.sleep(interval)
            try:
                self._send("health", families_pb2.Health(grade=99))
            except Refusal:
                return

    async def next(self):
        """The next thing the Session brings, in order; None once the end has been surfaced. The
        incarnation is then over, and the process should finish."""
        event = await self._events.get()
        if event is _END:
            self._events.put_nowait(_END)
            return None
        return event

    async def close(self):
        """Ends the Session in order: a CLOSE, the unit's own departure."""
        if self._ended or self._closing:
            return
        self._closing = True
        self._send(
            "session",
            families_pb2.SessionMessage(close=families_pb2.SessionMessage.Close()),
        )
        # Nothing more is sent: the Core ends the stream on a CLOSE.
        self._outbound.put_nowait(_END)
        self._outbound = None

        async def depart():
            # If the Core does not end the stream, the departure still is one.
            await asyncio.sleep(2)
            self._finish(Ended(True, None, ""))

        self._tasks.append(asyncio.create_task(depart()))

    async def ack(self, command, outcome, line=""):
        """Says what became of a command, correlated to it."""
        self._send(
            "ack", families_pb2.Ack(outcome=int(outcome), line=line), to=command.id
        )

    async def answer(self, question, payload):
        """Answers a question, correlated to it."""
        self._send(
            "query",
            families_pb2.Query(answer=families_pb2.Query.Answer(payload=payload)),
            to=question.id,
        )

    async def report(self, occurrence, severity, line="", detail=b""):
        """Reports an occurrence of a declared class, with the author's severity. With none it is
        refused: the library never states one on the author's behalf."""
        if not isinstance(severity, Severity):
            raise Misuse(
                "an occurrence is reported with the author's severity, and none was stated"
            )
        if not 0 <= severity.value <= 99:
            raise Misuse(
                f"a severity runs from 0 to 99, and {severity.value} is not one"
            )
        self._send(
            "event",
            families_pb2.Event(
                occurrence=occurrence, severity=severity.value, line=line, detail=detail
            ),
        )

    async def health(self, grade, line=""):
        """Reports how well the unit is: a grade from 0 to 99, and a line."""
        if not 0 <= grade <= 99:
            raise Misuse(f"a grade runs from 0 to 99, and {grade} is not one")
        self._send("health", families_pb2.Health(grade=grade, line=line))

    async def emit(self, stream, payload):
        """Sends data on a stream. Only the Core creates a stream's transport, so a stream it has not
        activated has nowhere to be written, and the library refuses rather than make one.
        """
        if stream not in self._active:
            raise Refusal(
                "stream.inactive", f"the stream {stream} has not been activated"
            )
        raise Refusal(
            "stream.inactive",
            f"the transport of {stream} is not one this library reaches yet",
        )


async def start(declaration, getenv=os.environ.get):
    """Starts a unit from the environment getenv reads: bind, register, open the Session. A refusal is
    raised with its stage and code, and nothing is tried again."""
    env = environment(getenv)
    # Bind before registering: registered and unreachable is the one order that is wrong.
    try:
        os.remove(env.bind)
    except FileNotFoundError:
        pass
    try:
        server = await asyncio.start_unix_server(
            lambda reader, writer: writer.close(), path=env.bind
        )
    except OSError as failure:
        raise TransportError(
            f"the unit's socket {env.bind} cannot be bound: {failure}"
        ) from failure
    channel = dial(env.socket)
    try:
        response = await register_pb2_grpc.RegisterStub(channel).Register(
            register_pb2.RegisterRequest(
                plugin=env.plugin,
                unit=env.unit,
                token=env.token,
                protocol=PLUGIN_CONTRACT,
                language="python",
                sdk_line=SDK_LINE,
                declared=declaration._surface(),
            )
        )
    except Exception as failure:
        server.close()
        await channel.close()
        raise TransportError(str(failure)) from failure
    if response.outcome == register_pb2.RegisterResponse.OUTCOME_REFUSED:
        server.close()
        await channel.close()
        raise Refusal(
            response.code,
            response.message,
            _words(register_pb2.Stage.Name(response.stage), "STAGE_"),
        )

    outbound = asyncio.Queue()

    async def requests():
        while (e := await outbound.get()) is not _END:
            yield e

    call = session_pb2_grpc.SessionStub(channel).Open(requests())
    unit = Unit(
        Admission(
            response.outcome
            == register_pb2.RegisterResponse.OUTCOME_ACCEPTED_WITH_RESTRICTIONS,
            Scope.of(response.granted),
            Scope.of(response.withheld),
        ),
        response.session_id,
        call,
        outbound,
        server,
        channel,
    )
    # The first envelope is the OPEN, carrying the identity admission issued.
    unit._send(
        "session", families_pb2.SessionMessage(open=families_pb2.SessionMessage.Open())
    )
    unit._tasks.append(asyncio.create_task(unit._receive()))
    interval = response.heartbeat.interval.ToTimedelta().total_seconds()
    if interval > 0:
        unit._tasks.append(asyncio.create_task(unit._beat(interval)))
    return unit
