"""Runs this repository's Python tests and writes their results as the lines the record writer reads:
one {"Action": …, "Test": …} per test, keyed by the test method's own name, which is what a marker
names; a failure carries its traceback. It exits non-zero if any test failed.

Usage: unittest-results.py <tests directory> <output file>
"""

import json
import sys
import unittest


class Recorder(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lines = []

    def _record(self, test, action, detail=None):
        name = test._testMethodName
        if detail:
            self.lines.append({"Action": "output", "Test": name, "Output": detail})
        self.lines.append({"Action": action, "Test": name})

    def addSuccess(self, test):
        super().addSuccess(test)
        self._record(test, "pass")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self._record(test, "fail", self._exc_info_to_string(err, test))

    def addError(self, test, err):
        super().addError(test, err)
        self._record(test, "fail", self._exc_info_to_string(err, test))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self._record(test, "skip", reason)


def main():
    tests, output = sys.argv[1], sys.argv[2]
    suite = unittest.defaultTestLoader.discover(tests, top_level_dir=tests)
    result = unittest.TextTestRunner(resultclass=Recorder, verbosity=2).run(suite)
    with open(output, "w") as out:
        for line in result.lines:
            out.write(json.dumps(line) + "\n")
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
