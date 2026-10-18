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
_FUTEX_WAKE = 1
# The futex system call's number is the architecture's; where this library does not know it the readers
# are not woken, and a reader that cannot wait on the word polls it.
_SYS_FUTEX = {"x86_64": 202, "aarch64": 98, "riscv64": 98, "armv7l": 240}.get(
    platform.machine()
)


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
