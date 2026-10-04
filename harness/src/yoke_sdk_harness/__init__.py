"""The Python plugin library's harness: the thinnest translation between the suite's directives and
the library. It turns a directive into a library call and what the library surfaces into an
observation, reports a refusal as its code and a verb it does not know as unrecognised, and judges
nothing: what a case requires lives in the suite. It speaks no wire of its own."""

import asyncio
import json
import os
import signal

from yoke_sdk import base, plugin


def declaration():
    """What the harness declares: one object of every kind, each governed by a capability."""
    return plugin.Declaration(
        id="com.yoke.conformance.python",
        streams=[plugin.Stream("conformance.data")],
        commands=["calibrate"],
        queries=["status"],
        occurrences=["conformance.drift"],
        capabilities=[
            plugin.Capability(
                "stream.data.publish", plugin.Object(stream="conformance.data")
            ),
            plugin.Capability(
                "command.calibrate.accept", plugin.Object(command="calibrate")
            ),
            plugin.Capability("query.status.answer", plugin.Object(query="status")),
            plugin.Capability(
                "event.drift.report", plugin.Object(occurrence="conformance.drift")
            ),
        ],
    )


def _refusal(error):
    """A refusal as its code, and never as the library's words."""
    if isinstance(error, base.Refusal):
        line = {"refusal": error.code}
        if error.stage:
            line["value"] = {"stage": error.stage}
        return line
    return {"value": {"failed": True}}


def _scope(s):
    return {
        "capabilities": s.capabilities,
        "streams": s.streams,
        "commands": s.commands,
        "queries": s.queries,
        "occurrences": s.occurrences,
    }


class _Harness:
    def __init__(self, writer, getenv):
        self.writer, self.getenv = writer, getenv
        self.unit = None
        self.commands, self.questions = {}, {}
        self.ended = asyncio.Event()

    async def send(self, line):
        self.writer.write(json.dumps(line).encode() + b"\n")
        await self.writer.drain()

    async def act(self, d):
        args = d.get("args") or {}
        verb = d.get("verb")
        if verb == "describe":
            return {"value": {"manifest": declaration().manifest()}}
        if verb == "start":
            try:
                unit = await plugin.start(declaration(), self.getenv)
            except base.Error as error:
                return _refusal(error)
            if self.unit is None:
                self.unit = unit
                asyncio.create_task(self.observe(unit))
            a = unit.admission
            outcome = "accepted with restrictions" if a.restricted else "accepted"
            return {
                "value": {
                    "outcome": outcome,
                    "granted": _scope(a.granted),
                    "withheld": _scope(a.withheld),
                }
            }
        known = {"close", "emit", "report-health", "report", "ack", "answer"}
        if self.unit is None:
            return (
                {"value": {"failed": True, "started": False}}
                if verb in known
                else {"unrecognised": True}
            )
        try:
            if verb == "close":
                await self.unit.close()
            elif verb == "emit":
                await self.unit.emit(
                    args.get("stream", ""), args.get("payload", "").encode()
                )
            elif verb == "report-health":
                await self.unit.health(int(args.get("grade", 0)), args.get("line", ""))
            elif verb == "report":
                severity = args.get("severity")
                severity = (
                    plugin.Severity.of(int(severity)) if severity is not None else None
                )
                await self.unit.report(
                    args.get("occurrence", ""), severity, args.get("line", "")
                )
            elif verb == "ack":
                command = self.commands.get(args.get("command", ""))
                if command is None:
                    raise base.Misuse("no such command was surfaced")
                await self.unit.ack(command, plugin.Outcome.DONE, args.get("line", ""))
            elif verb == "answer":
                question = self.questions.get(args.get("question", ""))
                if question is None:
                    raise base.Misuse("no such question was surfaced")
                await self.unit.answer(question, args.get("payload", "").encode())
            else:
                return {"unrecognised": True}
        except base.Error as error:
            return _refusal(error)
        return {"value": {}}

    async def observe(self, unit):
        """Reports everything the library surfaces, in the order it surfaced it; the end ends the harness."""
        while (event := await unit.next()) is not None:
            if isinstance(event, plugin.Command):
                self.commands[event.id] = event
                kind, fields = "command", {"id": event.id, "type": event.type}
            elif isinstance(event, plugin.Question):
                self.questions[event.id] = event
                kind, fields = "question", {
                    "id": event.id,
                    "type": event.type,
                    "payload": event.payload.decode("utf-8", "replace"),
                }
            elif isinstance(event, plugin.Activated):
                kind, fields = "activated", {
                    "stream": event.stream,
                    "transport": event.transport,
                }
            elif isinstance(event, plugin.Stopped):
                kind, fields = "stopped", {"stream": event.stream}
            elif isinstance(event, plugin.Refused):
                kind, fields = "refused", {
                    "correlation": event.correlation,
                    "code": event.error.code,
                }
            elif isinstance(event, plugin.Ended):
                await self.send(
                    {
                        "type": "observation",
                        "kind": "session-ended",
                        "fields": {"closed": event.closed, "cause": event.cause or ""},
                    }
                )
                self.ended.set()
                return
            else:
                continue
            await self.send({"type": "observation", "kind": kind, "fields": fields})


async def serve(getenv=os.environ.get):
    """Runs the harness until it is told to finish or its Session ends, and returns its exit status."""
    path = getenv("CONFORMANCE_SOCKET")
    if not path:
        return 1
    reader, writer = await asyncio.open_unix_connection(path)
    h = _Harness(writer, getenv)
    await h.send(
        {
            "type": "hello",
            "contract": "plugin",
            "language": "python",
            "sdk": plugin.SDK_LINE,
            "version": base.PLUGIN_CONTRACT,
            "unit": getenv("YOKE_UNIT") or "",
        }
    )
    asked = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, asked.set)
    ended = asyncio.create_task(h.ended.wait())
    stop = asyncio.create_task(asked.wait())
    while True:
        line = asyncio.create_task(reader.readline())
        done, _ = await asyncio.wait(
            {line, ended, stop}, return_when=asyncio.FIRST_COMPLETED
        )
        if stop in done:
            # Asked to stop, as the Core asks a process whose Session has ended: the end is reported
            # first, and the process leaves within the Core's window.
            try:
                await asyncio.wait_for(h.ended.wait(), 2)
            except asyncio.TimeoutError:
                pass
            return 0
        if ended in done:
            return 0
        raw = line.result()
        if not raw:
            d = {"type": "finish"}
        else:
            try:
                d = json.loads(raw)
            except ValueError:
                continue
        if d.get("type") == "finish":
            if h.unit is not None:
                await h.unit.close()
            return 0
        if d.get("type") != "directive":
            continue
        result = await h.act(d)
        result["type"], result["id"] = "result", d.get("id")
        await h.send(result)


def main():
    raise SystemExit(asyncio.run(serve()))
