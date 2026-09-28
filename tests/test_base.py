"""The cases of the-base.std.md, one test each."""

import ast
import pathlib
import unittest

from yoke.plugin.v1 import families_pb2

from yoke_sdk import base

SOURCE = pathlib.Path(__file__).resolve().parent.parent / "src" / "yoke_sdk" / "base.py"


class TheBase(unittest.TestCase):
    # std: yoke-sdk-python:the-base.01
    def test_the_base_holds_nothing_of_one_contract(self):
        source = SOURCE.read_text()
        for forbidden in (
            "RegisterRequest",
            "RegisterResponse",
            "RegisterStub",
            "SessionMessage",
            "SessionStub",
            "Surface",
            "HeartbeatTerms",
            "Stage",
            "register_pb2",
            "session_pb2_grpc",
        ):
            self.assertNotIn(forbidden, source, f"the base refers to {forbidden}")
        self.assertNotIn("subscri", source.lower())
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.ImportFrom) and node.module:
                self.assertFalse(
                    node.module.startswith("yoke_sdk"),
                    f"the base imports {node.module}",
                )
                self.assertNotEqual(
                    node.level, 1, "the base imports a module of this project"
                )

    # std: yoke-sdk-python:the-base.02
    def test_a_refusal_is_an_exception_carrying_its_code_and_envelopes_are_correlated(
        self,
    ):
        error = base.refusal_of(
            families_pb2.Error(
                code="session.correlation.unknown", message="names nothing"
            )
        )
        self.assertIsInstance(error, base.Refusal)
        self.assertIsInstance(error, Exception)
        self.assertEqual(
            (error.code, error.message),
            ("session.correlation.unknown", "names nothing"),
        )
        self.assertIn("session.correlation.unknown", str(error))

        envelopes = base.Envelopes("sid-1")
        first = envelopes.seal("health", families_pb2.Health(grade=99))
        second = envelopes.seal("health", families_pb2.Health(grade=99))
        answer = envelopes.answer(
            first.message_id, "health", families_pb2.Health(grade=99)
        )
        ids = {e.message_id for e in (first, second, answer)}
        self.assertEqual(len(ids), 3)
        for e in (first, second, answer):
            self.assertEqual(e.session_id, "sid-1")
            self.assertGreater(e.sent_at_unix_nano, 0)
        self.assertEqual(answer.correlation_id, first.message_id)
        self.assertNotEqual(answer.correlation_id, answer.message_id)
        self.assertEqual(first.correlation_id, "")

    # std: yoke-sdk-python:the-base.03
    def test_the_addresses_come_from_the_environment(self):
        variables = {
            "YOKE_PLUGIN": "com.yoke.station.acquire",
            "YOKE_UNIT": "acquire",
            "YOKE_SOCKET": "/run/yoke/plugin.sock",
            "YOKE_BIND": "/run/yoke/plugins/acquire.sock",
            "YOKE_TOKEN": "t-1",
        }
        env = base.environment(variables.get)
        self.assertEqual(
            (env.plugin, env.unit, env.socket, env.bind, env.token),
            (
                "com.yoke.station.acquire",
                "acquire",
                "/run/yoke/plugin.sock",
                "/run/yoke/plugins/acquire.sock",
                "t-1",
            ),
        )
        del variables["YOKE_SOCKET"]
        with self.assertRaises(base.Error) as refused:
            base.environment(variables.get)
        self.assertIn("YOKE_SOCKET", str(refused.exception))


if __name__ == "__main__":
    unittest.main()
