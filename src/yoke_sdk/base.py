"""What the three libraries of this project share and nothing else."""

from yoke.plugin.v1 import families_pb2, session_pb2

PLUGIN_CONTRACT = 0


class Error(Exception):
    """What can go wrong."""


class Refusal(Error):
    def __init__(self, code, message, stage=None):
        super().__init__("")
        self.code, self.message, self.stage = code, message, stage


class Misuse(Error):
    pass


class Env:
    plugin = unit = socket = bind = token = ""


def environment(getenv):
    return Env()


def refusal_of(error):
    return Error("")


class Envelopes:
    def __init__(self, session):
        self.session = session

    def seal(self, family, message):
        return session_pb2.Envelope()

    def answer(self, to, family, message):
        return session_pb2.Envelope()
