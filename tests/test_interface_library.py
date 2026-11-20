"""The cases of the-interface-library.std.md, one test each, against a Core that records what arrives and
says what its script tells it to."""

import ast
import asyncio
import os
import pathlib
import shutil
import socket
import struct
import tempfile
import threading
import unittest

from interface_core import (
    CLOSED,
    Core,
    Refusing,
    answer,
    completion,
    event,
    opening,
    refusal,
    request_of,
    serve,
    unit_record,
)
from yoke.interface.v1 import codes_pb2, interface_pb2, operations_pb2, records_pb2

from yoke_sdk import base, interface
from yoke_sdk.interface import Attachment

SOURCE = pathlib.Path(__file__).resolve().parent.parent / "src" / "yoke_sdk"


async def within(awaitable, what="the Core"):
    try:
        return await asyncio.wait_for(awaitable, 5)
    except asyncio.TimeoutError:
        raise AssertionError(f"timed out waiting for {what}") from None


def greeting(subscription="standing", version=1, records=()):
    """A script that opens, and then reads whatever comes."""

    async def script(inbox, out):
        out.put_nowait(opening(subscription, version, records))
        while True:
            await inbox.get()

    return script


class TheInterfaceLibrary(unittest.IsolatedAsyncioTestCase):
    async def _until(self, condition):
        while not condition():
            await asyncio.sleep(0.01)

    # std: yoke-sdk-python:the-interface-library.01
    async def test_a_channels_address_is_computed_or_handed_to_a_managed_interface(
        self,
    ):
        self.assertEqual(
            interface.service("/run/yoke", "panel"), "/run/yoke/interfaces/panel.sock"
        )
        env = {"XDG_RUNTIME_DIR": "/run/user/1000"}
        self.assertEqual(
            interface.application("bench-a", "panel", env.get),
            "/run/user/1000/yoke/bench-a/interfaces/panel.sock",
        )
        handed = {"YOKE_SOCKET": "/run/yoke/interfaces/panel.sock"}
        self.assertEqual(
            interface.managed(handed.get), "/run/yoke/interfaces/panel.sock"
        )
        with self.assertRaises(base.Error) as none:
            interface.managed({}.get)
        self.assertIn("YOKE_SOCKET", str(none.exception))
        for bad in ("a/b", "..", ""):
            with self.assertRaises(base.Error):
                interface.service("/run/yoke", bad)
        with self.assertRaises(base.Error):
            interface.application("a/b", "panel", env.get)

    # std: yoke-sdk-python:the-interface-library.02
    async def test_attaching_surfaces_the_opening_and_a_version_it_does_not_speak_is_refused(
        self,
    ):
        good = Core(greeting("standing", 1, [unit_record("acquire")]))
        attached = await within(Attachment.attach(await serve(self, good)))
        self.assertEqual(attached.version, 1)
        self.assertEqual(attached.standing.id, "standing")
        self.assertEqual(list(attached.picture.records), [unit_record("acquire")])
        await attached.close()

        other = Core(greeting("standing", 2))
        with self.assertRaises(base.Refusal) as refused:
            await Attachment.attach(await serve(self, other))
        self.assertEqual(refused.exception.code, "compat.unsupported")
        self.assertTrue(
            "1" in refused.exception.message and "2" in refused.exception.message
        )
        await within(self._until(lambda: other.ends), "the connection to be let go")

        busy = await serve(
            self, Refusing("channel.in_use", "the channel has its client")
        )
        with self.assertRaises(base.Refusal) as refused:
            await Attachment.attach(busy)
        self.assertEqual(refused.exception.code, "channel.in_use")

    # std: yoke-sdk-python:the-interface-library.03
    async def test_one_method_per_operation_each_stating_the_version_on_one_connection(
        self,
    ):
        async def script(inbox, out):
            out.put_nowait(opening())
            slow = None
            while True:
                f = await inbox.get()
                r = request_of(f)
                if r is None:
                    continue
                op = r.WhichOneof("operation")
                if op == "authenticate":
                    out.put_nowait(
                        answer(
                            f.call,
                            authenticate=operations_pb2.Authenticated(
                                account="alice", token="t-1"
                            ),
                        )
                    )
                elif op == "read":
                    out.put_nowait(answer(f.call, read=records_pb2.Records()))
                elif op == "subscribe":
                    out.put_nowait(
                        answer(
                            f.call,
                            subscribe=operations_pb2.Subscribed(
                                snapshot=records_pb2.Snapshot()
                            ),
                        )
                    )
                elif op == "confirm":
                    out.put_nowait(answer(f.call, confirm=operations_pb2.Confirmed()))
                elif op == "command":
                    done = operations_pb2.Acknowledged(
                        outcome=operations_pb2.Acknowledged.OUTCOME_DONE
                    )
                    out.put_nowait(answer(f.call, command=done))
                    if r.command.unit == "fast" and slow:
                        # The question's answer comes after the command's has been taken.
                        await asyncio.sleep(0.15)
                        out.put_nowait(
                            answer(slow, query=operations_pb2.Answered(payload=b"slow"))
                        )
                        slow = None
                elif op == "query":
                    if r.query.unit == "slow":
                        slow = f.call
                    else:
                        out.put_nowait(
                            answer(
                                f.call, query=operations_pb2.Answered(payload=b"answer")
                            )
                        )
                elif op in ("stream_start", "stream_stop"):
                    accepted = operations_pb2.Acknowledged(
                        outcome=operations_pb2.Acknowledged.OUTCOME_ACCEPTED
                    )
                    out.put_nowait(answer(f.call, **{op: accepted}))
                elif op == "stream_subscribe":
                    out.put_nowait(
                        answer(
                            f.call,
                            stream_subscribe=operations_pb2.Delivering(
                                delivery="00000001", connection=True, flowing=True
                            ),
                        )
                    )
                elif op == "stream_unsubscribe":
                    out.put_nowait(
                        answer(f.call, stream_unsubscribe=operations_pb2.Released())
                    )
                elif op == "reclaim":
                    out.put_nowait(
                        answer(f.call, reclaim=operations_pb2.Reclaimed(changed=True))
                    )

        core = Core(script)
        attached = await within(Attachment.attach(await serve(self, core)))
        self.assertEqual(
            (await attached.authenticate("alice", "hunter2x")).account, "alice"
        )
        await attached.read("unit")
        subscription = await attached.subscribe()
        self.assertEqual((await subscription.next()).WhichOneof("carries"), "snapshot")
        await attached.confirm("standing", 1)
        await attached.command("acquire", "calibrate", b"x")
        await attached.ask("acquire", "status")
        await attached.start_stream("acquire", "spectra")
        await attached.stop_stream("acquire", "spectra")
        delivery = await attached.subscribe_stream("acquire", "spectra")
        self.assertTrue(delivery.flowing)
        await delivery.release()
        self.assertTrue((await attached.reclaim()).changed)

        order = []

        async def question():
            result = await attached.ask("slow", "status")
            order.append("question")
            return result

        async def command():
            await asyncio.sleep(0.1)
            result = await attached.command("fast", "calibrate")
            order.append("command")
            return result

        slow, fast = await asyncio.gather(question(), command())
        self.assertEqual(slow, b"slow")
        self.assertEqual(fast.outcome, operations_pb2.Acknowledged.OUTCOME_DONE)
        self.assertEqual(order, ["command", "question"])
        await attached.close()

        self.assertEqual(core.attachments, 1)
        names = set()
        for _, f in core.frames:
            r = request_of(f)
            if r is not None:
                self.assertEqual(r.version, 1, interface.name(r))
                names.add(interface.name(r))
        self.assertEqual(
            names,
            {
                "authenticate",
                "command",
                "confirm",
                "query",
                "read",
                "reclaim",
                "stream.start",
                "stream.stop",
                "stream.subscribe",
                "stream.unsubscribe",
                "subscribe",
            },
        )

    # std: yoke-sdk-python:the-interface-library.04
    async def test_a_refusal_is_the_bases_error_with_its_code_and_its_detail(self):
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
                                grade="read-only", by="service"
                            ),
                        )
                    )

        attached = await within(Attachment.attach(await serve(self, Core(script))))
        with self.assertRaises(base.Refusal) as read:
            await attached.read("unit", "nobody")
        self.assertEqual(read.exception.code, "subject.unknown")
        self.assertEqual(
            (read.exception.subject.kind, read.exception.subject.identity),
            ("unit", "nobody"),
        )
        with self.assertRaises(base.Refusal) as command:
            await attached.command("acquire", "calibrate")
        self.assertEqual(command.exception.code, "channel.suspended")
        self.assertEqual(
            (command.exception.suspension.grade, command.exception.suspension.by),
            ("read-only", "service"),
        )
        await attached.close()

    # std: yoke-sdk-python:the-interface-library.05
    async def test_a_subscription_is_delivered_in_order_until_ended_and_nothing_reconnects(
        self,
    ):
        async def script(inbox, out):
            out.put_nowait(opening())
            for seq in (1, 2, 3):
                out.put_nowait(event("standing", seq))
            out.put_nowait(
                answer(
                    "standing",
                    subscribe=operations_pb2.Subscribed(
                        overflow=records_pb2.Snapshot(
                            at=9, records=[unit_record("acquire")]
                        )
                    ),
                )
            )
            while True:
                f = await inbox.get()
                r = request_of(f)
                if r is not None and r.WhichOneof("operation") == "subscribe":
                    out.put_nowait(
                        answer(
                            f.call,
                            subscribe=operations_pb2.Subscribed(
                                snapshot=records_pb2.Snapshot()
                            ),
                        )
                    )
                    for seq in (10, 11):
                        out.put_nowait(event(f.call, seq))
                elif f.WhichOneof("carries") == "cancel":
                    # The Core ends the attachment once the subscription is cancelled.
                    out.put_nowait(None)
                    return

        core = Core(script)
        attached = await within(Attachment.attach(await serve(self, core)))
        for seq in (1, 2, 3):
            d = await within(attached.standing.next(), "a standing event")
            self.assertEqual((d.WhichOneof("carries"), d.event.seq), ("event", seq))
        d = await within(attached.standing.next(), "the overflow")
        self.assertEqual((d.WhichOneof("carries"), d.overflow.at), ("overflow", 9))
        sub = await attached.subscribe()
        self.assertEqual((await sub.next()).WhichOneof("carries"), "snapshot")
        for seq in (10, 11):
            d = await within(sub.next(), "an event")
            self.assertEqual(d.event.seq, seq)
        call = sub.id
        sub.close()
        await within(
            self._until(
                lambda: any(
                    f.call == call and f.WhichOneof("carries") == "cancel"
                    for _, f in core.frames
                )
            ),
            "the cancel",
        )

        async def after():
            while True:
                try:
                    await attached.read("unit")
                except interface.Ended as ended:
                    return ended
                await asyncio.sleep(0.02)

        self.assertIsInstance(await within(after(), "the end"), interface.Ended)
        self.assertEqual(core.attachments, 1, "something reconnected")

    # std: yoke-sdk-python:the-interface-library.06
    async def test_confirmation_is_sent_only_when_the_author_confirms(self):
        async def script(inbox, out):
            out.put_nowait(opening())
            while True:
                f = await inbox.get()
                r = request_of(f)
                if r is not None and r.WhichOneof("operation") == "confirm":
                    out.put_nowait(answer(f.call, confirm=operations_pb2.Confirmed()))

        core = Core(script)
        attached = await within(Attachment.attach(await serve(self, core)))
        await asyncio.sleep(0.4)
        self.assertEqual(core.frames, [], "the library sent something unasked")
        await attached.confirm("standing", 7)
        self.assertEqual(len(core.frames), 1)
        confirm = request_of(core.frames[0][1]).confirm
        self.assertEqual((confirm.subscription, confirm.sequence), ("standing", 7))
        await attached.close()

    # std: yoke-sdk-python:the-interface-library.07
    async def test_a_streams_delivery_is_read_where_its_answer_says_it_arrives(self):
        directory = tempfile.mkdtemp(prefix="ykd")
        self.addCleanup(shutil.rmtree, directory, True)
        path = os.path.join(directory, "delivery.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        listener.bind(path)
        listener.listen(1)
        self.addCleanup(listener.close)

        def deliver():
            conn, _ = listener.accept()
            for n in (1, 2):
                conn.send(struct.pack("<QQ", n, 1000 + n) + f"frame {n}".encode())
            conn.close()

        threading.Thread(target=deliver, daemon=True).start()

        async def script(inbox, out):
            out.put_nowait(opening())
            asked = 0
            while True:
                f = await inbox.get()
                r = request_of(f)
                op = r.WhichOneof("operation") if r is not None else None
                if op == "stream_subscribe":
                    asked += 1
                    if asked == 1:
                        d = operations_pb2.Delivering(
                            delivery="00000001", socket=path, flowing=True
                        )
                    else:
                        d = operations_pb2.Delivering(
                            delivery="00000002", connection=True, flowing=False
                        )
                    out.put_nowait(answer(f.call, stream_subscribe=d))
                    if asked == 2:
                        for n in (1, 2):
                            out.put_nowait(
                                interface_pb2.CoreFrame(
                                    call="00000002",
                                    delivery=interface_pb2.StreamDelivery(
                                        delivery="00000002",
                                        sequence=n,
                                        sent_at=2000 + n,
                                        payload=f"frame {n}".encode(),
                                    ),
                                )
                            )
                        out.put_nowait(completion("00000002"))
                elif op == "stream_unsubscribe":
                    out.put_nowait(
                        answer(f.call, stream_unsubscribe=operations_pb2.Released())
                    )

        core = Core(script)
        attached = await within(Attachment.attach(await serve(self, core)))
        by_socket = await attached.subscribe_stream("acquire", "spectra")
        self.assertTrue(by_socket.flowing)
        for n in (1, 2):
            f = await within(by_socket.next(), "a frame on the socket")
            self.assertEqual(
                (f.sequence, f.sent_at, f.payload),
                (n, 1000 + n, f"frame {n}".encode()),
            )
        self.assertIsNone(await within(by_socket.next(), "the socket's end"))
        by_connection = await attached.subscribe_stream("acquire", "spectra")
        self.assertFalse(by_connection.flowing)
        for n in (1, 2):
            f = await within(by_connection.next(), "a frame on the connection")
            self.assertEqual(
                (f.sequence, f.sent_at, f.payload),
                (n, 2000 + n, f"frame {n}".encode()),
            )
        self.assertIsNone(await within(by_connection.next(), "the delivery's end"))
        await by_connection.release()
        released = [
            request_of(f).stream_unsubscribe.delivery
            for _, f in core.frames
            if request_of(f) is not None
            and request_of(f).WhichOneof("operation") == "stream_unsubscribe"
        ]
        self.assertEqual(released, ["00000002"])
        await attached.close()

    # std: yoke-sdk-python:the-interface-library.08
    async def test_closing_an_attachment_ends_it_in_order(self):
        async def script(inbox, out):
            out.put_nowait(opening())
            while True:
                f = await inbox.get()
                if f is CLOSED:
                    # The Core ends the stream once the client's requests have ended.
                    out.put_nowait(None)
                    return
                r = request_of(f)
                if r is not None and r.WhichOneof("operation") == "read":
                    out.put_nowait(answer(f.call, read=records_pb2.Records()))

        core = Core(script)
        attached = await within(Attachment.attach(await serve(self, core)))
        await attached.read("unit")
        await within(attached.close(), "the close")
        self.assertEqual(core.ends, ["closed"])

    # std: yoke-sdk-python:the-interface-library.09
    async def test_a_panel_built_on_the_library_names_nothing_of_the_administrative_contract(
        self,
    ):
        for module in ("interface.py", "_tls.py"):
            tree = ast.parse((SOURCE / module).read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""] + [a.name for a in node.names]
                elif isinstance(node, ast.Attribute):
                    names = [node.attr]
                elif isinstance(node, ast.Name):
                    names = [node.id]
                else:
                    continue
                for name in names:
                    self.assertNotIn(
                        "administrative",
                        name,
                        f"{module}:{node.lineno} reaches the administrative contract",
                    )

    # std: yoke-sdk-python:the-interface-library.10
    async def test_a_reader_of_the_ring_waits_on_its_wake_word(self):
        import ctypes
        import mmap
        import platform
        import time

        # The ring, as the Core makes it: four slots of 64 bytes, handed on the lifeline at the activation.
        slots, capacity = 4, 64
        size = 64 + slots * (32 + capacity)
        memory = os.memfd_create("yoke-test")
        self.addCleanup(os.close, memory)
        os.ftruncate(memory, size)
        mem = mmap.mmap(memory, size)
        self.addCleanup(mem.close)
        mem[:8] = b"YOKERING"
        struct.pack_into("<III", mem, 8, 1, slots, capacity)
        word = ctypes.c_uint32.from_buffer(mem, 32)
        libc = ctypes.CDLL(None, use_errno=True)
        futex = {"x86_64": 202, "aarch64": 98, "riscv64": 98, "armv7l": 240}[
            platform.machine()
        ]

        def wake():
            """Moves the word and wakes every waiter, as a producer and the Core do."""
            word.value = (word.value + 1) & 0xFFFFFFFF
            libc.syscall(
                ctypes.c_long(futex),
                ctypes.byref(word),
                ctypes.c_int(1),
                ctypes.c_int(1 << 30),
                None,
                None,
                ctypes.c_int(0),
            )

        directory = tempfile.mkdtemp(prefix="ykd")
        self.addCleanup(shutil.rmtree, directory, True)
        path = os.path.join(directory, "00000001.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(path)
        listener.listen(1)
        self.addCleanup(listener.close)
        accepted = []

        def hand():
            conn, _ = listener.accept()
            socket.send_fds(conn, [b"\x01\x00\x00\x00\x00"], [memory])
            accepted.append(conn)

        threading.Thread(target=hand, daemon=True).start()

        async def script(inbox, out):
            out.put_nowait(opening())
            while True:
                f = await inbox.get()
                r = request_of(f)
                op = r.WhichOneof("operation") if r is not None else None
                if op == "stream_subscribe":
                    d = operations_pb2.Delivering(
                        delivery="00000001", shared=path, flowing=True
                    )
                    out.put_nowait(answer(f.call, stream_subscribe=d))
                elif op == "stream_unsubscribe":
                    out.put_nowait(
                        answer(f.call, stream_unsubscribe=operations_pb2.Released())
                    )

        def waits():
            """The context switches of the thread reading the ring, so far."""
            for task in os.listdir("/proc/self/task"):
                with open(f"/proc/self/task/{task}/comm") as comm:
                    if comm.read().strip() != "yoke-ring":
                        continue
                with open(f"/proc/self/task/{task}/status") as status:
                    for line in status:
                        if line.startswith("voluntary_ctxt_switches:"):
                            return int(line.split()[1])
            return None

        core = Core(script)
        attached = await within(Attachment.attach(await serve(self, core)))
        delivery = await attached.subscribe_stream("acquire", "spectra")
        await self._until(lambda: accepted)
        reading = asyncio.ensure_future(delivery.next())
        await asyncio.sleep(0.3)
        before = waits()
        await asyncio.sleep(1)
        idle = waits() - before

        # The producer publishes one frame, and wakes whoever waits on the word.
        struct.pack_into("<QI", mem, 64 + 8, 1001, 3)
        mem[64 + 32 : 64 + 35] = b"one"
        struct.pack_into("<Q", mem, 64, 1)
        struct.pack_into("<Q", mem, 24, 1)
        published = time.monotonic()
        wake()
        f = await within(reading, "the frame")
        took = time.monotonic() - published

        # The lifeline closes and the ring is woken, as the Core does at a delivery's end.
        accepted[0].close()
        wake()
        ended = await within(delivery.next(), "the delivery's end")

        self.assertLessEqual(
            idle, 5, f"waiting a second on an empty ring, the reader woke {idle} times"
        )
        self.assertEqual((f.sequence, f.sent_at, f.payload), (1, 1001, b"one"))
        self.assertLess(
            took, 0.1, f"the frame was read {took:.3f} s after it was published"
        )
        self.assertIsNone(ended)
        del word
        await attached.close()

    # std: yoke-sdk-python:the-interface-library.11
    async def test_a_question_may_name_its_cause(self):
        async def script(inbox, out):
            out.put_nowait(opening())
            while True:
                f = await inbox.get()
                r = request_of(f)
                if r is None or r.WhichOneof("operation") != "query":
                    continue
                if r.query.cause:
                    out.put_nowait(
                        refusal(
                            f.call,
                            code="use.cause.unknown",
                            message="no such question",
                            item=r.query.cause,
                        )
                    )
                else:
                    out.put_nowait(
                        answer(f.call, query=operations_pb2.Answered(payload=b"42"))
                    )

        core = Core(script)
        attached = await within(Attachment.attach(await serve(self, core)))
        self.assertEqual(await within(attached.ask("source", "status", b"p")), b"42")
        with self.assertRaises(base.Refusal) as refused:
            await within(attached.ask("source", "status", b"p", cause="q-7"))
        self.assertEqual(refused.exception.code, "use.cause.unknown")
        self.assertEqual(refused.exception.item, "q-7")
        asked = [
            (request_of(f).query.cause, request_of(f).query.payload)
            for _, f in core.frames
            if request_of(f) is not None
            and request_of(f).WhichOneof("operation") == "query"
        ]
        self.assertEqual(asked, [("", b"p"), ("q-7", b"p")])
        await attached.close()

    # std: yoke-sdk-python:the-interface-library.12
    async def test_a_stream_followed_is_subscribed_again_when_its_delivery_ends(self):
        async def script(inbox, out):
            out.put_nowait(opening())
            starts = subscribes = 0
            while True:
                f = await inbox.get()
                r = request_of(f)
                op = r.WhichOneof("operation") if r is not None else None
                if op == "stream_start":
                    starts += 1
                    if starts in (2, 3):
                        out.put_nowait(
                            refusal(
                                f.call, code="unit.not_running", message="restarting"
                            )
                        )
                    else:
                        out.put_nowait(
                            answer(f.call, stream_start=operations_pb2.Acknowledged())
                        )
                elif op == "stream_subscribe":
                    subscribes += 1
                    d = f"d-{subscribes}"
                    out.put_nowait(
                        answer(
                            f.call,
                            stream_subscribe=operations_pb2.Delivering(
                                delivery=d, connection=True, flowing=True
                            ),
                        )
                    )
                    out.put_nowait(
                        interface_pb2.CoreFrame(
                            call=d,
                            delivery=interface_pb2.StreamDelivery(
                                delivery=d,
                                sequence=1,
                                sent_at=subscribes,
                                payload=f"frame of {d}".encode(),
                            ),
                        )
                    )
                    if subscribes == 1:
                        # The unit's Session ended: so does its delivery.
                        out.put_nowait(completion(d))
                elif op == "stream_unsubscribe":
                    out.put_nowait(
                        answer(f.call, stream_unsubscribe=operations_pb2.Released())
                    )

        core = Core(script)
        attached = await within(Attachment.attach(await serve(self, core)))
        follow = await within(attached.follow("acquire", "spectra"))
        first = await within(follow.next(), "the first delivery's frame")
        self.assertEqual((first.payload, follow.delivery), (b"frame of d-1", "d-1"))
        second = await within(follow.next(), "the second delivery's frame")
        self.assertEqual((second.payload, follow.delivery), (b"frame of d-2", "d-2"))
        await within(follow.close())
        ops = [
            request_of(f).WhichOneof("operation")
            for _, f in core.frames
            if request_of(f) is not None
        ]
        self.assertEqual(ops.count("stream_start"), 4)
        self.assertEqual(ops.count("stream_subscribe"), 2)
        self.assertEqual(ops.count("stream_unsubscribe"), 2)
        await attached.close()


if __name__ == "__main__":
    unittest.main()
