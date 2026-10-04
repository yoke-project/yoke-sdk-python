#!/usr/bin/env bash
# Packages this family's wheel, yoke-sdk, at the version the tree states and no other — the verb `yoke`'s
# release verb asks a family's script for; the verb uploads the wheel itself.
# Usage: package.sh package <version> <dir>      write yoke_sdk-<version>-py3-none-any.whl into <dir>
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

case "${1:-}" in
  package)
    version="${2:?usage: package.sh package <version> <dir>}"
    out="${3:?usage: package.sh package <version> <dir>}"
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "package: $version is not a version: it is written X.Y.Z" >&2; exit 2; }
    # A version asked for that is not the tree's is refused before anything is written: the tag, the
    # wheel and the line the library says it is cannot disagree.
    tree="$(sed -n 's/^__version__ = "\(.*\)"$/\1/p' "$root/src/yoke_sdk/__init__.py")"
    [[ "$version" == "$tree" ]] || { echo "package: the tree states $tree and $version was asked for: nothing is packaged" >&2; exit 1; }
    mkdir -p "$out"
    python -m pip wheel -q --disable-pip-version-check --no-deps -w "$out" "$root"
    ;;
  *)
    echo "usage: package.sh package <version> <dir>" >&2
    exit 2
    ;;
esac
