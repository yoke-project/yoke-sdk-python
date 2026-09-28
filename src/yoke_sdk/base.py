"""What the three libraries of this project share and nothing else: connecting to a socket, the
envelope and its correlation, a refusal as a Python exception carrying its code, the addresses a party
computes from its environment, and the contract version each library states.

A concept that exists on one contract only does not live here: a candidate is in the base only if all
three libraries would otherwise implement it.
"""

import dataclasses
import itertools
import threading
import time

import grpc
from yoke.plugin.v1 import contract_pb2, session_pb2

# The version of the plugin contract the definitions carry, which the plugin library declares.
PLUGIN_CONTRACT = contract_pb2.CONTRACT_VERSION


class Error(Exception):
    """What can go wrong: a refusal from the other party, an environment that does not carry what a
    party needs, a transport that failed, or something the library refused to do for the author.
    """


class Refusal(Error):
    """A refusal as it travels: a code from the one namespace, a message for a person, and the stage
    where there is one."""

    def __init__(self, code, message, stage=None):
        self.code, self.message, self.stage = code, message, stage
        where = f" at {stage}" if stage else ""
        super().__init__(f"{code}{where}: {message}")


class Misuse(Error):
    """Something the library refuses to do on the author's behalf."""


class TransportError(Error):
    """A transport that failed."""


@dataclasses.dataclass(frozen=True)
class Env:
    """What a party is handed, and all it may assume."""

    plugin: str
    unit: str
    socket: str
    bind: str
    token: str


def environment(getenv):
    """Reads the reserved variables. A missing one is an error naming it; no path is assumed."""
    read = lambda name: getenv(name) or ""
    missing = [
        name for name in ("YOKE_UNIT", "YOKE_SOCKET", "YOKE_BIND") if not read(name)
    ]
    if missing:
        raise Error("the environment does not carry " + ", ".join(missing))
    return Env(
        read("YOKE_PLUGIN"),
        read("YOKE_UNIT"),
        read("YOKE_SOCKET"),
        read("YOKE_BIND"),
        read("YOKE_TOKEN"),
    )


def dial(path):
    """A channel to a Unix socket."""
    return grpc.aio.insecure_channel("unix://" + path)


def refusal_of(error):
    """The refusal an error envelope carries."""
    return Refusal(error.code, error.message)


class Envelopes:
    """Fills the header of every envelope one party sends in one Session: a message identity no other
    of its envelopes has, the Session's identity, and the sender's clock."""

    def __init__(self, session):
        self._session = session
        self._next = itertools.count(1)
        self._lock = threading.Lock()

    def seal(self, family, message):
        """An envelope carrying the message as the family named, with its header filled."""
        with self._lock:
            n = next(self._next)
        e = session_pb2.Envelope(
            message_id=f"u-{n}",
            session_id=self._session,
            sent_at_unix_nano=time.time_ns(),
        )
        getattr(e, family).CopyFrom(message)
        return e

    def answer(self, to, family, message):
        """An envelope answering the message identified by `to`."""
        e = self.seal(family, message)
        e.correlation_id = to
        return e
