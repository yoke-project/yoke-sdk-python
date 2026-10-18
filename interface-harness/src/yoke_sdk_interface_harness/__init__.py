"""The Python interface library's harness: the thinnest translation between the suite's directives and
the library. Launched by the suite against an instance, it attaches to the channel a directive names at
the address the library computes, turns each further directive into one call of the library, reports what
the library answered in the suite's vocabulary — a refusal as its code and what it names, never the
library's words — and what the standing subscription or a subscription delivers as observations, in
order. It judges nothing, confirms only when told to, and speaks no wire of its own."""

import asyncio
import json
import os

from google.protobuf.json_format import MessageToDict
from yoke.interface.v1 import events_pb2

from yoke_sdk import base, interface
from yoke_sdk.plugin import SDK_LINE


def _plain(message):
    """A message of the contract as the suite reads it: its fields by their names in the contract."""
    return MessageToDict(message, preserving_proto_field_name=True)


def _records(records):
    return [_plain(r) for r in records]


def _event(e):
    return {
        "seq": e.seq,
        "type": e.type,
        "subject_kind": e.subject.kind,
        "subject": e.subject.identity,
        "severity": e.severity,
    }


def _text(args, key):
    value = args.get(key)
    return value if isinstance(value, str) else ""


def _number(args, key):
    """A number the suite sent as a number, or as the string a 64-bit value is written as."""
    value = args.get(key)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _refused(error):
    """A refusal as its code and what it names; anything else the library failed with is a failure the
    harness does not interpret."""
    if not isinstance(error, base.Refusal):
        return {"value": {"failed": True}}
    value = {}
    if error.subject is not None:
        value["subject"] = {
            "kind": error.subject.kind,
            "identity": error.subject.identity,
            "incarnation": error.subject.incarnation,
        }
    if error.item:
        value["item"] = error.item
    if error.suspension is not None:
        value["suspension"] = {
            "grade": error.suspension.grade,
            "by": error.suspension.by,
        }
    return {"refusal": error.code, "value": value}


_KNOWN = (
    "authenticate",
    "read",
    "subscribe",
    "confirm",
    "command",
    "query",
    "stream-start",
    "stream-stop",
    "stream-subscribe",
    "reclaim",
)


class _Harness:
    """What the harness holds between directives: its attachments, the first being the one directed."""

    def __init__(self, writer, getenv):
        self.writer, self.getenv = writer, getenv
        self.attached = []
        self.standing = ""
        self.tasks = []

    async def send(self, line):
        self.writer.write(json.dumps(line).encode() + b"\n")
        await self.writer.drain()

    async def observe(self, kind, fields):
        await self.send({"type": "observation", "kind": kind, "fields": fields})

    async def subscribed(self, sub):
        """Reports what a subscription delivers, in order, until it ends."""
        while True:
            try:
                d = await sub.next()
            except base.Error:
                d = None
            if d is None:
                await self.observe("ended", {})
                return
            kind = d.WhichOneof("carries")
            if kind == "snapshot":
                await self.observe(
                    "snapshot",
                    {"at": d.snapshot.at, "records": _records(d.snapshot.records)},
                )
            elif kind == "overflow":
                await self.observe(
                    "overflow",
                    {"at": d.overflow.at, "records": _records(d.overflow.records)},
                )
            elif kind == "event":
                await self.observe("event", _event(d.event))

    async def delivered(self, delivery):
        while True:
            try:
                f = await delivery.next()
            except base.Error:
                f = None
            if f is None:
                await self.observe("delivery-ended", {"delivery": delivery.id})
                return
            await self.observe(
                "frame",
                {
                    "delivery": delivery.id,
                    "sequence": f.sequence,
                    "sent_at": f.sent_at,
                    "payload": f.payload.decode(errors="replace"),
                },
            )

    async def attach(self, channel):
        """Attaches to the channel named, at the address the library computes under the instance."""
        try:
            address = interface.service(
                self.getenv("CONFORMANCE_INSTANCE") or "", channel
            )
            a = await asyncio.wait_for(interface.Attachment.attach(address), 30)
        except base.Error as error:
            return _refused(error)
        except Exception:
            return {"value": {"failed": True}}
        standing = a.standing
        a.standing = None
        # A second attachment the channel admitted is kept beside the first, which stays the one directed.
        if not self.attached:
            self.standing = standing.id if standing else ""
            if standing is not None:
                self.tasks.append(asyncio.create_task(self.subscribed(standing)))
        self.attached.append(a)
        return {
            "value": {
                "version": a.version,
                "subscription": standing.id if standing else "",
                "at": a.picture.at,
                "records": _records(a.picture.records),
            }
        }

    async def act(self, d):
        verb, args = d.get("verb"), d.get("args") or {}
        if verb == "attach":
            return await self.attach(_text(args, "channel"))
        if verb not in _KNOWN:
            return {"unrecognised": True}
        if not self.attached:
            return {"value": {"failed": True, "attached": False}}
        a = self.attached[0]
        try:
            return await asyncio.wait_for(self._call(a, verb, args), 30)
        except base.Error as error:
            return _refused(error)
        except asyncio.TimeoutError:
            return {"value": {"failed": True}}

    async def _call(self, a, verb, args):
        if verb == "authenticate":
            if _text(args, "token"):
                got = await a.authenticate_token(_text(args, "token"))
            else:
                got = await a.authenticate(
                    _text(args, "account"), _text(args, "secret")
                )
            return {"value": _plain(got)}
        if verb == "read":
            records = await a.read(_text(args, "kind"), _text(args, "identity"))
            return {"value": {"records": _records(records)}}
        if verb == "subscribe":
            sub = await a.subscribe(
                events_pb2.Filter(
                    subject_kind=_text(args, "subject_kind"),
                    subject_identity=_text(args, "subject_identity"),
                    type=_text(args, "type"),
                )
            )
            self.tasks.append(asyncio.create_task(self.subscribed(sub)))
            return {"value": {}}
        if verb == "confirm":
            await a.confirm(
                _text(args, "subscription") or self.standing, _number(args, "sequence")
            )
            return {"value": {}}
        if verb == "command":
            ack = await a.command(
                _text(args, "unit"),
                _text(args, "type"),
                _text(args, "payload").encode(),
            )
            return {"value": _plain(ack)}
        if verb == "query":
            payload = await a.ask(
                _text(args, "unit"),
                _text(args, "type"),
                _text(args, "payload").encode(),
            )
            return {"value": {"answer": payload.decode(errors="replace")}}
        if verb == "stream-start":
            ack = await a.start_stream(_text(args, "unit"), _text(args, "stream"))
            return {"value": _plain(ack)}
        if verb == "stream-stop":
            ack = await a.stop_stream(_text(args, "unit"), _text(args, "stream"))
            return {"value": _plain(ack)}
        if verb == "reclaim":
            return {"value": _plain(await a.reclaim())}
        delivery = await a.subscribe_stream(_text(args, "unit"), _text(args, "stream"))
        self.tasks.append(asyncio.create_task(self.delivered(delivery)))
        return {"value": {"delivery": delivery.id, "flowing": delivery.flowing}}

    async def close(self):
        for task in self.tasks:
            task.cancel()
        for a in self.attached:
            await a.close()


async def serve(getenv=os.environ.get):
    """Runs the harness until it is told to finish, and is its exit status."""
    path = getenv("CONFORMANCE_SOCKET")
    if not path:
        return 1
    try:
        reader, writer = await asyncio.open_unix_connection(path)
    except OSError:
        return 1
    h = _Harness(writer, getenv)
    await h.send(
        {
            "type": "hello",
            "contract": "interface",
            "language": "python",
            "sdk": SDK_LINE,
            "version": interface.CONTRACT,
        }
    )
    try:
        while True:
            raw = await reader.readline()
            if not raw:
                return 0
            try:
                d = json.loads(raw)
            except ValueError:
                continue
            if d.get("type") == "finish":
                return 0
            if d.get("type") != "directive":
                continue
            result = await h.act(d)
            result["type"], result["id"] = "result", d.get("id")
            await h.send(result)
    finally:
        await h.close()
        writer.close()


def main():
    raise SystemExit(asyncio.run(serve()))
