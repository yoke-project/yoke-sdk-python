"""The library an interface client is written with: the interface contract, on its typed projection,
over the base.

A client attaches to a channel and is given its opening: what the channel may address, the standing
subscription, and the contract's version. Every operation of the contract's union is one method, all on
the one connection, each answered on its own; a refusal is the base's error, carrying its code and its
detail. Nothing is retried and nothing reconnects, and nothing is sent the author did not ask for: a
confirmation is the author's statement of what it has processed, so the library never makes one.

A channel at a routable address is reached over TLS 1.3 against the roots the caller supplies, and
establishes its caller before anything else: the credential is the attachment's first frame, and the
session token its answer carries is the caller's to present to a later attachment.

This module names nothing of the administrative contract, and uses none of its definitions: a program
written with it cannot reach the operations an interface must never issue.
"""

import asyncio
import dataclasses
import os
import socket
import struct

import grpc
from yoke.interface.v1 import (
    codes_pb2,
    contract_pb2,
    events_pb2,
    interface_pb2,
    interface_pb2_grpc,
    operations_pb2,
)

from yoke_sdk import _tls
from yoke_sdk.base import (
    Error,
    Misuse,
    Refusal,
    Subject,
    Suspension,
    TransportError,
    dial,
)

# The version of the interface contract this library covers, stated on every request.
CONTRACT = contract_pb2.CONTRACT_VERSION

# The one filter shape the library builds for the author.
Filter = events_pb2.Filter


class Ended(TransportError):
    """What a call answers once the attachment has ended."""

    def __init__(self):
        super().__init__("the attachment ended")


def _channel_socket(root, channel):
    """A channel's local socket under an instance's root; a name that could choose where the socket is
    is refused."""
    if channel in ("", ".", "..") or "/" in channel or "\0" in channel:
        raise Misuse(f"the channel name {channel!r} cannot name a channel")
    return os.path.join(root, "interfaces", channel + ".sock")


def service(runtime_dir, channel):
    """A channel's address in the service form, under its runtime directory."""
    return _channel_socket(runtime_dir, channel)


def application(name, channel, getenv):
    """A channel's address in an application instance, derived from the instance's name under the
    account's runtime directory."""
    if name in ("", ".", "..") or "/" in name or "\0" in name:
        raise Misuse(f"the instance name {name!r} cannot name an instance")
    runtime = getenv("XDG_RUNTIME_DIR")
    if not runtime:
        raise Error(
            "XDG_RUNTIME_DIR is not set, and an application instance's addresses derive from it"
        )
    return _channel_socket(os.path.join(runtime, "yoke", name), channel)


def managed(getenv):
    """The address of the channel a managed interface was launched for, which the Core hands it."""
    socket_path = getenv("YOKE_SOCKET")
    if not socket_path:
        raise Error(
            "YOKE_SOCKET is not set: a managed interface is handed its channel's address, and this "
            "process was not"
        )
    return socket_path


def name(request):
    """The operation a request carries, as the contract names it."""
    return (request.WhichOneof("operation") or "").replace("_", ".")


def _refusal_of(r):
    """A refusal as the base's error, with what it names."""
    subject = item = suspension = None
    detail = r.WhichOneof("detail")
    if detail == "subject":
        subject = Subject(r.subject.kind, r.subject.identity, r.subject.incarnation)
    elif detail == "item":
        item = r.item
    elif detail == "suspension":
        suspension = Suspension(r.suspension.grade, r.suspension.by)
    return Refusal(r.code, r.message, subject=subject, item=item, suspension=suspension)


def _varint(buf, at):
    value = shift = 0
    while True:
        byte = buf[at]
        at += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, at
        shift += 7


def _fields(buf):
    """The fields of an encoded message, by number: its length-delimited ones, which is all a status's
    details are."""
    at = 0
    while at < len(buf):
        tag, at = _varint(buf, at)
        number, kind = tag >> 3, tag & 7
        if kind == 0:
            _, at = _varint(buf, at)
        elif kind == 2:
            length, at = _varint(buf, at)
            yield number, buf[at : at + length]
            at += length
        elif kind == 1:
            at += 8
        elif kind == 5:
            at += 4
        else:
            return


def _from_status(failure):
    """The refusal a transport's status carries — `google.rpc.Status`, whose details are the refusals
    the Core attaches — or the transport's own error where it carries none."""
    for key, value in failure.trailing_metadata() or ():
        if key != "grpc-status-details-bin":
            continue
        try:
            for number, any_ in _fields(value):
                if number != 3:
                    continue
                url = body = b""
                for n, v in _fields(any_):
                    if n == 1:
                        url = v
                    elif n == 2:
                        body = v
                if url.endswith(b"yoke.interface.v1.Refusal"):
                    return _refusal_of(codes_pb2.Refusal.FromString(body))
        except (IndexError, ValueError):
            break
    return TransportError(f"{failure.code().name}: {failure.details()}")


# The marker a closed queue ends with.
_END = object()

# The call identity of the credential an attachment presents first.
_AUTHENTICATING = "c-1"

# How long a close waits for the Core to end an attachment told it is closing.
_CLOSING = 1.0


async def _requests(queue):
    """The attachment's requests, until the client has closed."""
    while (frame := await queue.get()) is not None:
        yield frame


class Attachment:
    """A client attached to a channel. `picture` is what the channel was given when it attached,
    `standing` the subscription it holds from then on, and `version` the contract's version the Core
    speaks. `established` is the answer to the credential an attachment presented — the account, and the
    session token with its expiry — and None where it presented none."""

    def __init__(self, channel, tunnel, call, outbound, opening):
        self.picture = opening.picture
        self.version = opening.version
        self.established = None
        self.standing = None
        self._channel, self._tunnel, self._call = channel, tunnel, call
        self._outbound = outbound
        self._count = 0
        self._calls = {}
        self._ended = False
        self._received = asyncio.Event()
        self._task = None

    @classmethod
    async def attach(cls, address, *, tls=None, password=None, token=None):
        """Attaches to the channel at `address` and waits for its opening. A Core that speaks another
        version of the contract is refused before any operation, and the connection let go. A
        credential, a `password` as (account, secret) or a `token`, is sent before anything else, and
        attaching waits for its answer too. `tls` as (roots, name) reaches the channel over TLS 1.3 and
        nothing below it, verifying the Core's chain against `roots`, in PEM, and no others, for `name`.
        Nothing times it out: a caller that wants a deadline puts one around it."""
        channel, tunnel = _dial(address, tls)
        outbound = asyncio.Queue()
        credential = None
        if password is not None:
            credential = operations_pb2.Authenticate(
                password=operations_pb2.Authenticate.Password(
                    account=password[0], secret=password[1]
                )
            )
        elif token is not None:
            credential = operations_pb2.Authenticate(token=token)

        async def fail(error):
            await outbound.put(None)
            await channel.close()
            if tunnel is not None:
                tunnel.close()
            return error

        if credential is not None:
            request = operations_pb2.Request(version=CONTRACT, authenticate=credential)
            outbound.put_nowait(
                interface_pb2.ClientFrame(call=_AUTHENTICATING, request=request)
            )
        call = interface_pb2_grpc.InterfaceStub(channel).Attach(_requests(outbound))
        try:
            first = await call.read()
        except grpc.aio.AioRpcError as failure:
            raise await fail(_from_status(failure)) from failure
        if first is grpc.aio.EOF:
            raise await fail(Ended())
        if first.WhichOneof("carries") != "opening":
            raise await fail(
                TransportError(
                    f"the attachment opened with {first!r}, and not an opening"
                )
            )
        opening = first.opening
        if opening.version != CONTRACT:
            call.cancel()
            raise await fail(
                Refusal(
                    "compat.unsupported",
                    f"this library speaks the interface contract's version {CONTRACT}, and the Core "
                    f"{opening.version}",
                )
            )
        a = cls(channel, tunnel, call, outbound, opening)
        if opening.subscription:
            a.standing = Subscription(
                opening.subscription, a, a._register(opening.subscription), None, True
            )
        pending = a._register(_AUTHENTICATING) if credential is not None else None
        a._count = 1 if credential is not None else 0
        a._task = asyncio.create_task(a._receive())
        if pending is not None:
            try:
                answer = await a._first(pending)
            except Error:
                a._forget(_AUTHENTICATING)
                await a.close()
                raise
            a._forget(_AUTHENTICATING)
            a.established = answer.authenticate
        return a

    async def close(self):
        """Ends the attachment in order: it tells the Core the client is done, so the Core reads a
        client that closed rather than one that went away, and lets the connection go once the Core has
        ended it."""
        await self._outbound.put(None)
        try:
            await asyncio.wait_for(self._received.wait(), _CLOSING)
        except asyncio.TimeoutError:
            pass
        if self._task is not None:
            self._task.cancel()
        self._call.cancel()
        await self._channel.close()
        if self._tunnel is not None:
            self._tunnel.close()

    # ---- frames ----

    async def _receive(self):
        """Hands every frame to the call it belongs to, until the attachment ends. A delivery on the
        connection is registered as its answer arrives, so none of its frames can arrive before it is.
        """
        try:
            while (f := await self._call.read()) is not grpc.aio.EOF:
                if f.WhichOneof("carries") == "answer":
                    a = f.answer
                    if (
                        a.WhichOneof("answer") == "stream_subscribe"
                        and a.stream_subscribe.WhichOneof("arrives") == "connection"
                    ):
                        self._register(a.stream_subscribe.delivery)
                q = self._calls.get(f.call)
                if q is not None:
                    q.put_nowait(f)
        except (grpc.aio.AioRpcError, asyncio.CancelledError):
            pass
        finally:
            self._ended = True
            for q in self._calls.values():
                q.put_nowait(_END)
            self._received.set()

    def _register(self, call):
        if self._ended:
            q = asyncio.Queue()
            q.put_nowait(_END)
            return q
        return self._calls.setdefault(call, asyncio.Queue())

    def _forget(self, call):
        self._calls.pop(call, None)

    def _cancel(self, call):
        self._outbound.put_nowait(
            interface_pb2.ClientFrame(call=call, cancel=interface_pb2.Cancel())
        )

    async def _issue(self, operation, message):
        """Sends a request under a new call identity, stating the contract's version, and returns the
        queue that holds the frames that answer it."""
        if self._ended:
            raise Ended()
        self._count += 1
        call = f"c-{self._count}"
        queue = self._register(call)
        request = operations_pb2.Request(version=CONTRACT)
        getattr(request, operation).CopyFrom(message)
        self._outbound.put_nowait(interface_pb2.ClientFrame(call=call, request=request))
        return call, queue

    async def _first(self, queue):
        """A call's first frame as its answer: a refusal is the base's error, and an attachment that
        ended answers that it ended."""
        f = await queue.get()
        if f is _END:
            raise Ended()
        kind = f.WhichOneof("carries")
        if kind == "refusal":
            raise _refusal_of(f.refusal)
        if kind != "answer":
            raise TransportError(
                "the Core answered a call with something that is not an answer"
            )
        return f.answer

    async def _ask(self, operation, message):
        call, queue = await self._issue(operation, message)
        answered = False
        try:
            answer = await self._first(queue)
            answered = True
            return answer
        finally:
            self._forget(call)
            if not answered and not self._ended:
                self._cancel(call)

    # ---- the operations ----

    async def authenticate(self, account, secret):
        """Presents an account and its secret, on a channel whose class takes a credential; on one
        whose class establishes the client, it answers who was established."""
        return (
            await self._ask(
                "authenticate",
                operations_pb2.Authenticate(
                    password=operations_pb2.Authenticate.Password(
                        account=account, secret=secret
                    )
                ),
            )
        ).authenticate

    async def authenticate_token(self, token):
        """Presents a token an earlier authentication issued."""
        return (
            await self._ask("authenticate", operations_pb2.Authenticate(token=token))
        ).authenticate

    async def read(self, kind, identity=""):
        """The records of a kind the channel sees, or of the one subject an identity names."""
        answer = await self._ask(
            "read", operations_pb2.Read(kind=kind, identity=identity)
        )
        return list(answer.read.records)

    async def confirm(self, subscription, sequence):
        """States that the author has processed a subscription up to a sequence. Only the author knows
        that, so the library never confirms on its own."""
        await self._ask(
            "confirm",
            operations_pb2.Confirm(subscription=subscription, sequence=sequence),
        )

    async def command(self, unit, type, payload=b""):
        """Sends a command to a unit, its payload opaque, and returns how the unit acknowledged it."""
        return (
            await self._ask(
                "command",
                operations_pb2.Command(unit=unit, type=type, payload=payload),
            )
        ).command

    async def ask(self, unit, type, payload=b""):
        """Asks a unit a question, its payload opaque, and returns the answer's."""
        return (
            await self._ask(
                "query", operations_pb2.Question(unit=unit, type=type, payload=payload)
            )
        ).query.payload

    async def start_stream(self, unit, stream):
        """Asks for a unit's stream to flow, for everyone."""
        return (
            await self._ask(
                "stream_start", operations_pb2.UnitStream(unit=unit, stream=stream)
            )
        ).stream_start

    async def stop_stream(self, unit, stream):
        """Asks for a unit's stream to stop flowing."""
        return (
            await self._ask(
                "stream_stop", operations_pb2.UnitStream(unit=unit, stream=stream)
            )
        ).stream_stop

    async def reclaim(self):
        """Takes control back for a channel bound as a local socket."""
        return (await self._ask("reclaim", operations_pb2.Reclaim())).reclaim

    async def subscribe(self, filter=None):
        """Opens a subscription narrowed by a filter, and returns once its snapshot or its refusal
        arrives."""
        call, queue = await self._issue(
            "subscribe", operations_pb2.Subscribe(filter=filter or Filter())
        )
        try:
            answer = await self._first(queue)
        except Error:
            self._forget(call)
            raise
        return Subscription(call, self, queue, answer.subscribe, False)

    async def subscribe_stream(self, unit, stream):
        """Asks for a copy of a unit's stream, and opens it where the answer says it arrives: a
        per-subscriber socket the Core listens on, or the attachment's own connection.
        """
        answer = await self._ask(
            "stream_subscribe", operations_pb2.UnitStream(unit=unit, stream=stream)
        )
        d = answer.stream_subscribe
        arrives = d.WhichOneof("arrives")
        if arrives == "socket":
            conn = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            try:
                conn.connect(d.socket)
            except OSError as failure:
                conn.close()
                raise TransportError(
                    f"the delivery's socket {d.socket} cannot be reached: {failure}"
                ) from failure
            conn.setblocking(False)
            return Delivery(d.delivery, d.flowing, self, conn, None)
        if arrives == "connection":
            return Delivery(
                d.delivery, d.flowing, self, None, self._register(d.delivery)
            )
        raise TransportError(
            f"the delivery {d.delivery} arrives where this projection cannot read it: {arrives}"
        )


def _dial(address, tls):
    """Reaches a channel: a path is a local socket, anything else an address, over TLS where asked."""
    if address.startswith("/"):
        if tls is not None:
            raise Misuse(
                f"{address} is a local socket, and a local socket is not reached over TLS"
            )
        return dial(address), None
    if tls is None:
        return grpc.aio.insecure_channel(address), None
    tunnel = _tls.open_tunnel(address, tls[0], tls[1])
    return grpc.aio.insecure_channel("unix://" + tunnel.path), tunnel


class Subscription:
    """A subscription, delivered in order: its snapshot, then events and overflows, until its caller
    ends it, the Core completes it or the attachment ends. Nothing reconnects it."""

    def __init__(self, id, attachment, queue, pending, standing):
        # What a confirmation names.
        self.id = id
        self._a, self._queue, self._pending, self._standing = (
            attachment,
            queue,
            pending,
            standing,
        )

    async def next(self):
        """The next delivery; None once the subscription completed, and an error that it ended once the
        attachment ended."""
        if self._pending is not None:
            pending, self._pending = self._pending, None
            return pending
        while True:
            f = await self._queue.get()
            if f is _END:
                raise Ended()
            kind = f.WhichOneof("carries")
            if kind == "refusal":
                raise _refusal_of(f.refusal)
            if kind == "completion":
                return None
            if kind == "event":
                return operations_pb2.Subscribed(event=f.event)
            if kind == "answer" and f.answer.WhichOneof("answer") == "subscribe":
                return f.answer.subscribe

    def close(self):
        """Ends the subscription on the wire. The standing subscription is the attachment's, and ends
        with it."""
        if self._standing:
            return
        self._a._cancel(self.id)
        self._a._forget(self.id)
        self._queue.put_nowait(_END)


@dataclasses.dataclass(frozen=True)
class Frame:
    """One message of a stream as a delivery carries it: its sequence, the producer's clock, and the
    payload, opaque."""

    sequence: int
    sent_at: int
    payload: bytes


# The size of a frame's header on a per-subscriber socket: the sequence and the clock.
_HEADER = 16

# The largest frame a per-subscriber socket carries: a payload of 1 MiB and its header.
_PACKET = _HEADER + (1 << 20)


class Delivery:
    """A copy of a stream delivered to this client, read where the answer said it arrives. `flowing` is
    whether the stream was flowing when the subscription was answered."""

    def __init__(self, id, flowing, attachment, conn, queue):
        self.id, self.flowing = id, flowing
        self._a, self._conn, self._queue = attachment, conn, queue

    async def next(self):
        """The next frame, in order; None once the delivery ended, and an error that it ended once the
        attachment did."""
        if self._conn is not None:
            loop = asyncio.get_running_loop()
            try:
                packet = await loop.sock_recv(self._conn, _PACKET)
            except OSError:
                packet = b""
            if not packet:
                self._conn.close()
                return None
            if len(packet) < _HEADER:
                raise TransportError(
                    f"a frame of {len(packet)} bytes is shorter than its header"
                )
            sequence, sent_at = struct.unpack_from("<QQ", packet)
            return Frame(sequence, sent_at, packet[_HEADER:])
        while True:
            f = await self._queue.get()
            if f is _END:
                raise Ended()
            kind = f.WhichOneof("carries")
            if kind == "completion":
                return None
            if kind == "refusal":
                raise _refusal_of(f.refusal)
            if kind == "delivery":
                d = f.delivery
                return Frame(d.sequence, d.sent_at, d.payload)

    async def release(self):
        """Ends the delivery: the Core is told, and what was opened for it is let go."""
        try:
            await self._a._ask(
                "stream_unsubscribe",
                operations_pb2.StreamRelease(delivery=self.id),
            )
        finally:
            if self._conn is not None:
                self._conn.close()
            if self._queue is not None:
                self._a._forget(self.id)
