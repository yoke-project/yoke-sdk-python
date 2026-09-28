"""The library a Plugin unit is written with."""

import dataclasses
import enum

SDK_LINE = "yoke-sdk-python 0.0.0"


@dataclasses.dataclass
class Stream:
    id: str
    tolerates_loss: bool = False
    tolerates_reorder: bool = False


@dataclasses.dataclass
class Object:
    stream: str | None = None
    command: str | None = None
    query: str | None = None
    occurrence: str | None = None
    surface: str | None = None


@dataclasses.dataclass
class Capability:
    name: str
    governs: Object


@dataclasses.dataclass
class Declaration:
    id: str
    needs: list = dataclasses.field(default_factory=list)
    streams: list = dataclasses.field(default_factory=list)
    commands: list = dataclasses.field(default_factory=list)
    queries: list = dataclasses.field(default_factory=list)
    occurrences: list = dataclasses.field(default_factory=list)
    capabilities: list = dataclasses.field(default_factory=list)

    def manifest(self):
        return ""


class Outcome(enum.IntEnum):
    ACCEPTED = 1
    DONE = 2
    FAILED = 3


class Severity:
    @classmethod
    def of(cls, n):
        return cls()


class Command:
    pass


class Question:
    pass


class Ended:
    pass


async def start(declaration, getenv):
    raise NotImplementedError
