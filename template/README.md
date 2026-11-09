# @YOKE_ID@

A Yoke Plugin written in Python.

| Path | What it holds |
| --- | --- |
| `yoke-plugin.yaml` | the file every step of `yoke-compose` reads: the identity, the build, the architectures |
| `plugin/` | the Plugin's source, its `Containerfile`, and its Manifest, `@YOKE_ID@.yaml`, kept beside it |
| `try/` | an application project that uses the Plugin, to see it work |

Build it with `yoke-compose build`; publish it by tagging this repository `vX.Y.Z`.

A Python Plugin travels as an image, so its build needs Podman or Docker, named by `CONTAINER_ENGINE` where
both are there, and it builds for this machine's architecture alone. Docker's classic image store does not
keep an image's digest: use the containerd store, or Podman.
