"""The producer's side of the shared ring, layout 1: a frame is published into the slot its sequence
selects, and every reader waiting on the shared word is woken."""

import ctypes
import mmap
import os
import platform
import struct
import threading

_HEADER = 64
_SLOT_HEADER = 32
_MAGIC = b"YOKERING"
_VERSION = 1
_OFF_PUBLISHED = 24
_OFF_WAKE = 32
_FUTEX_WAIT = 0
_FUTEX_WAKE = 1
# The futex system call's number is the architecture's; where this library does not know it the readers
# are not woken, and no ring is read: a reader that cannot wait on the word would have to poll it.
_SYS_FUTEX = {"x86_64": 202, "aarch64": 98, "riscv64": 98, "armv7l": 240}.get(
    platform.machine()
)
_libc = ctypes.CDLL(None, use_errno=True)


class Writer:
    """Publishes into a ring the producer was handed."""

    def __init__(self, fd):
        """Maps the ring a descriptor names, for writing, and reads its geometry from its header."""
        size = os.fstat(fd).st_size
        if size < _HEADER:
            raise ValueError(f"the ring is {size} bytes, shorter than its header")
        self._mem = mmap.mmap(fd, size)
        magic = self._mem[:8]
        version, slots, capacity = struct.unpack_from("<III", self._mem, 8)
        if magic != _MAGIC:
            fault = "the memory handed is not a ring"
        elif version != _VERSION:
            fault = f"the ring is of layout {version}; this library writes layout {_VERSION}"
        elif slots == 0 or _HEADER + slots * (_SLOT_HEADER + capacity) > size:
            fault = f"the ring's geometry, {slots} slots of {capacity} bytes, does not fit its {size} bytes"
        else:
            fault = None
        if fault:
            self._mem.close()
            raise ValueError(fault)
        self._slots, self._capacity = slots, capacity
        self._published = struct.unpack_from("<Q", self._mem, _OFF_PUBLISHED)[0]
        self._wake = ctypes.c_uint32.from_buffer(self._mem, _OFF_WAKE)
        # Locking and unlocking is the barrier between writes, which Python has no other word for: what
        # a reader sees of a slot is its payload before its sequence.
        self._barrier = threading.Lock()

    def _fence(self):
        with self._barrier:
            pass

    def publish(self, clock, payload):
        """Writes one frame and wakes every waiter. A frame larger than a slot is refused."""
        if len(payload) > self._capacity:
            raise ValueError(
                f"the frame is {len(payload)} bytes, and a slot of this ring holds {self._capacity}"
            )
        n = self._published + 1
        at = _HEADER + ((n - 1) % self._slots) * (_SLOT_HEADER + self._capacity)
        mem = self._mem
        struct.pack_into("<Q", mem, at, 0)
        self._fence()
        struct.pack_into("<QI", mem, at + 8, clock, len(payload))
        mem[at + _SLOT_HEADER : at + _SLOT_HEADER + len(payload)] = payload
        self._fence()
        struct.pack_into("<Q", mem, at, n)
        self._fence()
        struct.pack_into("<Q", mem, _OFF_PUBLISHED, n)
        self._wake.value = (self._wake.value + 1) & 0xFFFFFFFF
        self._fence()
        if _SYS_FUTEX is not None:
            libc = ctypes.CDLL(None, use_errno=True)
            libc.syscall(
                _SYS_FUTEX,
                ctypes.byref(self._wake),
                _FUTEX_WAKE,
                0x7FFFFFFF,
                None,
                None,
                0,
            )
        self._published = n

    def close(self):
        """Lets the mapping go."""
        self._wake = None
        self._mem.close()


class Reader:
    """One reader of a ring, at its own pace: a client the Core handed the ring for reading alone. It waits
    on the ring's word with no bound; the producer wakes it at every frame, and the Core of 0.16.0 after
    every stop and every end the lifeline carries, so a reader woken with nothing published reads its
    lifeline then. Where this library does not know the futex system call, it offers no reading of a ring.
    """

    def __init__(self, fd):
        """Maps the ring a descriptor names, for reading alone, and reads its geometry from its header."""
        if _SYS_FUTEX is None:
            raise OSError(f"this library cannot wait on a ring on {platform.machine()}")
        size = os.fstat(fd).st_size
        if size < _HEADER:
            raise ValueError(f"the ring is {size} bytes, shorter than its header")
        _libc.mmap.restype = ctypes.c_void_p
        _libc.mmap.argtypes = [
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_long,
        ]
        address = _libc.mmap(None, size, mmap.PROT_READ, mmap.MAP_SHARED, fd, 0)
        if address in (None, ctypes.c_void_p(-1).value):
            raise OSError(ctypes.get_errno(), "the ring cannot be mapped")
        self._address, self._size = address, size
        self._mem = (ctypes.c_char * size).from_address(address)
        version, slots, capacity = struct.unpack_from("<III", self._mem, 8)
        if (
            self._mem[:8] != _MAGIC
            or version != _VERSION
            or slots == 0
            or _HEADER + slots * (_SLOT_HEADER + capacity) > size
        ):
            self.close()
            raise ValueError("the memory handed is not a ring this library reads")
        self._slots, self._capacity, self._next = slots, capacity, 1

    def next(self):
        """The next frame the reader can still read, as (sequence, clock, payload), waiting on the ring's
        word with no bound; None when the word was woken with nothing published. A lap by the producer, or a
        slot overwritten while it was copied, is a gap the sequence shows."""
        mem = self._mem
        waited = False
        while True:
            wake = struct.unpack_from("<I", mem, _OFF_WAKE)[0]
            published = struct.unpack_from("<Q", mem, _OFF_PUBLISHED)[0]
            if published < self._next:
                if waited:
                    return None
                # Shared, not private: the producer and the Core that wake it are other processes. The
                # call releases the interpreter's lock while it waits.
                _libc.syscall(
                    ctypes.c_long(_SYS_FUTEX),
                    ctypes.c_void_p(self._address + _OFF_WAKE),
                    ctypes.c_int(_FUTEX_WAIT),
                    ctypes.c_uint32(wake),
                    None,
                    None,
                    ctypes.c_int(0),
                )
                waited = True
                continue
            if published >= self._slots:
                self._next = max(self._next, published - self._slots + 1)
            n = self._next
            self._next += 1
            at = _HEADER + ((n - 1) % self._slots) * (_SLOT_HEADER + self._capacity)
            before = struct.unpack_from("<Q", mem, at)[0]
            clock, length = struct.unpack_from("<QI", mem, at + 8)
            length = min(length, self._capacity)
            payload = bytes(mem[at + _SLOT_HEADER : at + _SLOT_HEADER + length])
            after = struct.unpack_from("<Q", mem, at)[0]
            if before != n or after != n:
                continue
            return n, clock, payload

    def close(self):
        """Lets the mapping go."""
        if self._address is not None:
            _libc.munmap(ctypes.c_void_p(self._address), ctypes.c_size_t(self._size))
            self._address, self._mem = None, None


def can_read():
    """Whether this library can wait on a ring here, and so read one."""
    return _SYS_FUTEX is not None
