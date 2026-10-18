"""What the interface library's cases share: a Core that records what arrives and says what its script
tells it to, and the frames it says it with."""

import asyncio
import os
import shutil
import tempfile

import grpc
from yoke.interface.v1 import (
    codes_pb2,
    events_pb2,
    interface_pb2,
    interface_pb2_grpc,
    operations_pb2,
    records_pb2,
)

# What a script reads when the client ended its requests: it closed.
CLOSED = object()


class Core(interface_pb2_grpc.InterfaceServicer):
    """A script per attachment, and a record of what each attachment's requests came to. A script is
    a coroutine taking the requests' queue and the queue it answers on; it ends the attachment by
    putting None."""

    def __init__(self, script):
        self.script = script
        self.attachments = 0
        self.frames = []
        self.ends = []

    async def Attach(self, request_iterator, context):
        self.attachments += 1
        n = self.attachments
        inbox, out = asyncio.Queue(), asyncio.Queue()

        async def read():
            try:
                async for f in request_iterator:
                    self.frames.append((n, f))
                    inbox.put_nowait(f)
                self.ends.append("closed")
                inbox.put_nowait(CLOSED)
            except Exception:
                self.ends.append("went away")

        reading = asyncio.create_task(read())
        running = asyncio.create_task(self.script(inbox, out))
        try:
            while (frame := await out.get()) is not None:
                yield frame
        finally:
            reading.cancel()
            running.cancel()


class Refusing(interface_pb2_grpc.InterfaceServicer):
    """A Core that refuses every attachment, with the refusal in the status the contract states."""

    def __init__(self, code, message):
        self.code, self.message = code, message

    async def Attach(self, request_iterator, context):
        refusal = codes_pb2.Refusal(code=self.code, message=self.message)
        any_ = _field(1, b"type.googleapis.com/yoke.interface.v1.Refusal") + _field(
            2, refusal.SerializeToString()
        )
        status = (
            _varint_field(1, 9) + _field(2, self.message.encode()) + _field(3, any_)
        )
        context.set_trailing_metadata((("grpc-status-details-bin", status),))
        await context.abort(grpc.StatusCode.FAILED_PRECONDITION, self.message)
        yield  # never reached


def _varint(n):
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _field(number, body):
    return _varint(number << 3 | 2) + _varint(len(body)) + body


def _varint_field(number, value):
    return _varint(number << 3) + _varint(value)


def opening(subscription="standing", version=1, records=()):
    return interface_pb2.CoreFrame(
        opening=interface_pb2.Opening(
            picture=records_pb2.Snapshot(at=4, records=list(records)),
            subscription=subscription,
            version=version,
        )
    )


def answer(call, **answer_):
    return interface_pb2.CoreFrame(call=call, answer=operations_pb2.Response(**answer_))


def refusal(call, **fields):
    return interface_pb2.CoreFrame(call=call, refusal=codes_pb2.Refusal(**fields))


def event(call, seq, kind="unit.state.changed"):
    return interface_pb2.CoreFrame(
        call=call, event=events_pb2.Event(seq=seq, type=kind)
    )


def completion(call):
    return interface_pb2.CoreFrame(
        call=call,
        completion=interface_pb2.Completion(by=interface_pb2.Completion.BY_CORE),
    )


def unit_record(identity):
    return records_pb2.Record(
        unit=records_pb2.UnitRecord(
            declared=records_pb2.UnitRecord.Declared(identity=identity, kind="plugin")
        )
    )


def request_of(frame):
    return frame.request if frame.WhichOneof("carries") == "request" else None


async def serve(testcase, servicer):
    """Serves a Core on a socket of its own, and is the socket's address."""
    directory = tempfile.mkdtemp(prefix="yki")
    testcase.addCleanup(shutil.rmtree, directory, True)
    server = grpc.aio.server()
    interface_pb2_grpc.add_InterfaceServicer_to_server(servicer, server)
    path = os.path.join(directory, "panel.sock")
    server.add_insecure_port("unix://" + path)
    await server.start()
    testcase.addAsyncCleanup(server.stop, None)
    return path
