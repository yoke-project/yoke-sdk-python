#!/usr/bin/env bash
# The checks described by the-release.std.md, one function per case.

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# The version the tree states, which the wheel's project reads.
tree_version() { sed -n 's/^__version__ = "\(.*\)"$/\1/p' "$root/src/yoke_sdk/__init__.py"; }

# std: yoke-sdk-python:the-release.01
check_one_wheel_published() {
  [[ -f "$root/ci/package.sh" ]] || { echo "no ci/package.sh"; return 1; }
  local version out said listed
  version="$(tree_version)"
  out="$(mktemp -d)"
  said="$(bash "$root/ci/package.sh" package "$version" "$out" 2>&1)" || { echo "the wheel was not built: $said"; return 1; }
  listed="$(python -c 'import sys, zipfile; print("\n".join(zipfile.ZipFile(sys.argv[1]).namelist()))' "$out/yoke_sdk-$version-py3-none-any.whl")"
  rm -rf "$out"
  local file
  for file in yoke_sdk/__init__.py yoke_sdk/base.py yoke_sdk/plugin.py "yoke_sdk-$version.dist-info/licenses/LICENSE" "yoke_sdk-$version.dist-info/licenses/NOTICE"; do
    grep -qx "$file" <<<"$listed" || { echo "the wheel does not carry $file"; return 1; }
  done
  local stray
  stray="$(grep -E 'harness|\.std\.md$|entry_points\.txt$' <<<"$listed" || true)"
  [[ -z "$stray" ]] || { echo "the wheel carries $stray"; return 1; }
  grep -qF 'Private :: Do Not Upload' "$root/harness/pyproject.toml" 2>/dev/null || { echo "the harness is not a project that says it is never uploaded"; return 1; }
  if grep -qF 'Private :: Do Not Upload' "$root/pyproject.toml"; then echo "the wheel's project says it is never uploaded"; return 1; fi
}

# std: yoke-sdk-python:the-release.02
check_packaged_at_the_trees_version() {
  [[ -f "$root/ci/package.sh" ]] || { echo "no ci/package.sh"; return 1; }
  local version out said
  version="$(tree_version)"
  [[ -n "$version" ]] || { echo "yoke_sdk states no __version__"; return 1; }
  out="$(mktemp -d)"
  said="$(bash "$root/ci/package.sh" package "$version" "$out" 2>&1)" || { echo "packaging at $version failed: $said"; return 1; }
  [[ -f "$out/yoke_sdk-$version-py3-none-any.whl" ]] || { echo "packaging at $version wrote no wheel"; return 1; }
  rm -rf "$out" && out="$(mktemp -d)"
  if said="$(bash "$root/ci/package.sh" package 9.9.9 "$out" 2>&1)"; then
    echo "packaging at 9.9.9 was not refused"; return 1
  fi
  [[ "$said" == *9.9.9* && "$said" == *"$version"* ]] || { echo "the refusal does not name both versions: $said"; return 1; }
  [[ -z "$(ls -A "$out")" ]] || { echo "a refused packaging wrote $(ls "$out")"; return 1; }
  rm -rf "$out"
  grep -qE '^dynamic = \["version"\]' "$root/pyproject.toml" && grep -qF 'path = "src/yoke_sdk/__init__.py"' "$root/pyproject.toml" \
    || { echo "the project's version is not read from yoke_sdk"; return 1; }
  grep -qE '^SDK_LINE = "yoke-sdk-python " \+ __version__$' "$root/src/yoke_sdk/plugin.py" \
    || { echo "the SDK line is not the package's version"; return 1; }
}

# std: yoke-sdk-python:the-release.03
check_released_by_yokes_verb() {
  local verb workflow="$root/.github/workflows/release.yml"
  verb="$(cd "$root" && just --show release)"
  [[ "$verb" == *"github.com/yoke-project/yoke/cmd/yoke-release@"* && "$verb" == *"-wheel yoke-sdk"* ]] \
    || { echo "the release verb does not run yoke's verb for the wheel yoke-sdk"; return 1; }
  [[ -f "$workflow" ]] || { echo "no release.yml"; return 1; }
  grep -qE "tags: \['v\*'\]" "$workflow" || { echo "the release run does not run at a v tag"; return 1; }
  grep -qE '^[[:space:]]+id-token: write' "$workflow" || { echo "the release run may not request an identity token"; return 1; }
  grep -qF 'just release > manifest-lines.jsonl' "$workflow" || { echo "the verb's lines are not kept"; return 1; }
  grep -qE '^[[:space:]]+name: manifest-lines$' "$workflow" || { echo "no artifact manifest-lines"; return 1; }
  if grep -qE 'secrets\.|PYPI_TOKEN' "$workflow"; then echo "the release run reads a stored credential"; return 1; fi
}
