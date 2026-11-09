#!/usr/bin/env bash
# The checks described by the-plugin-template.std.md, for Python's template.

pt_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# std: yoke-sdk-python:the-plugin-template.01
check_the_template_holds_the_plugin_projects_tree() {
  local t="$pt_root/template" missing="" each
  for each in yoke-plugin.yaml plugin/Containerfile plugin/main.py plugin/requirements.txt 'plugin/@YOKE_ID@.yaml' try .github/workflows/manifest.yml; do
    [[ -e "$t/$each" ]] || missing+=" $each"
  done
  [[ -z "$missing" ]] || { echo "the template has no$missing"; return 1; }
  grep -qx 'language: python' "$t/yoke-plugin.yaml" || { echo "yoke-plugin.yaml does not name language: python"; return 1; }
  [[ ! -e "$t/try/yoke.yaml" ]] || { echo "try/ holds a descriptor, which new plugin makes"; return 1; }
  grep -q '^FROM .*@sha256:' "$t/plugin/Containerfile" || { echo "the base image is not pinned by digest"; return 1; }
  grep -q -- '--manifest' "$t/.github/workflows/manifest.yml" && grep -q 'diff' "$t/.github/workflows/manifest.yml" \
    || { echo "the workflow does not compare the Manifest printed with the one kept"; return 1; }
  local others
  others="$( { grep -rhoE '@YOKE_[A-Z_]+@' "$t"; find "$t" | grep -oE '@YOKE_[A-Z_]+@'; } | sort -u | grep -vxE '@YOKE_(ID|PROGRAM)@')"
  [[ -z "$others" ]] || { echo "the template writes values nothing substitutes: $others"; return 1; }
}

# std: yoke-sdk-python:the-plugin-template.02
check_the_template_requires_this_releases_library() {
  local stated required
  stated="$(sed -n 's/^__version__ = "\(.*\)"$/\1/p' "$pt_root/src/yoke_sdk/__init__.py")"
  required="$(sed -n 's/^yoke-sdk==//p' "$pt_root/template/plugin/requirements.txt")"
  [[ -n "$stated" && "$required" == "$stated" ]] || { echo "the template requires yoke-sdk ${required:-at no exact version} and the tree states $stated"; return 1; }
}

# std: yoke-sdk-python:the-plugin-template.03
check_generated_the_plugin_prints_the_manifest_it_keeps() {
  local dir verdict=0; dir="$(mktemp -d)"
  cp "$pt_root/template/plugin/main.py" "$dir/main.py"
  sed -i 's/@YOKE_ID@/com.example.greeter/g; s/@YOKE_PROGRAM@/greeter/g' "$dir/main.py"
  sed 's/@YOKE_ID@/com.example.greeter/g' "$pt_root/template/plugin/@YOKE_ID@.yaml" > "$dir/kept"
  PYTHONPATH="$pt_root/src" python "$dir/main.py" --manifest > "$dir/printed" 2> "$dir/err" || { echo "it does not run: $(tail -3 "$dir/err")"; verdict=1; }
  cmp -s "$dir/kept" "$dir/printed" || { echo "the Manifest printed differs from the one kept"; verdict=1; }
  rm -rf "$dir"; return "$verdict"
}
