# yoke-sdk-python

## What it is

The Python family of Yoke's SDKs: the library a Python program uses to run as a Plugin unit in a Yoke
instance, over a base it shares with the libraries to come, and the harness that holds it to the
contract under the conformance suite. It is published as one wheel, `yoke-sdk`, with the modules
`yoke_sdk.base` and `yoke_sdk.plugin`.

It is not the Core, and it holds no contract definitions of its own: it takes them from the
`yoke-proto` wheel.

## What is where

| Path | What it holds |
| --- | --- |
| `src/yoke_sdk/base.py` | What the libraries share: the connection, the envelope, a refusal as an exception |
| `src/yoke_sdk/plugin.py` | The library a Plugin unit is written with |
| `harness/` | The library's harness for the conformance suite, a project of its own that is never uploaded |
| `tests/` | The libraries' tests; each description (`.std.md`) sits beside the code it describes |
| `checks/`, `ci/` | This repository's own checks, and the scripts continuous integration runs |

## Working on it

Python 3.10 or newer, [`black`](https://black.readthedocs.io) for `just fmt`, and
[`just`](https://just.systems) 1.58.0 or newer. `just build` installs the libraries and the harness
into a virtual environment of the repository's own, under the user's cache directory. `just develop`
also puts the published verification tool, `yoke-verify`, on the `PATH`, which `just test` needs. The
`Containerfile` builds a container with the toolchain.

| Command | What it does |
| --- | --- |
| `just build` | Installs the libraries and the harness into the repository's virtual environment |
| `just test` | Runs the checks and every test, and checks each description against its tests |
| `just lint` | Parses every shell script |
| `just fmt` | Fails, naming each file, when a Python file is not formatted |
| `just develop` | Verifies the installed `just` against a floor, given as `just develop 1.58.0`, and installs `yoke-verify` |
| `just release` | Publishes the wheel at the tag that names this commit; run by the release, not by hand |

## Where the depth is

The documentation at [docs.yoke-project.dev](https://docs.yoke-project.dev/): the
[reference](https://docs.yoke-project.dev/reference/) for the contract the library speaks, and the
[contributors' section](https://docs.yoke-project.dev/contributing/) for how the work is done. The
wheel is on [PyPI](https://pypi.org/project/yoke-sdk/).

## Licence

Apache 2.0, in [`LICENSE`](LICENSE), with a [`NOTICE`](NOTICE).
