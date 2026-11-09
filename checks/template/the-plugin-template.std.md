# Python's Plugin template

| | |
| --- | --- |
| **Feature** | `template/`, the starting tree of a Plugin project in Python that this family hands over with each of its releases: a project whose build leaves an OCI image archive |
| **Planning item** | yoke-project/yoke-sdk-python#45 |

## yoke-sdk-python:the-plugin-template.01 — the template holds the Plugin project's tree, with two values to substitute

| Field | Value |
| --- | --- |
| **Cites** | arch/20-composer/07 §The template · arch/20-composer/07 §The build contract |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a clean checkout |
| **Action** | read `template/` |
| **Expected** | it holds `yoke-plugin.yaml` naming `language: python`, `plugin/` with its source, a `Containerfile` whose base image is pinned by digest, its requirements and `@YOKE_ID@.yaml`, `try/` with no `yoke.yaml`, and a workflow that compares the Manifest printed with the one kept; nothing but the two values is left to substitute |

## yoke-sdk-python:the-plugin-template.02 — the template requires this release's library

| Field | Value |
| --- | --- |
| **Cites** | arch/20-composer/07 §The template |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a clean checkout |
| **Action** | read the version the tree states and the one the template requires |
| **Expected** | the two are the same |

## yoke-sdk-python:the-plugin-template.03 — generated, the Plugin prints the Manifest it keeps

| Field | Value |
| --- | --- |
| **Cites** | arch/20-composer/07 §The template |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | the template generated as the greeter, and this checkout's library |
| **Action** | run the program with `--manifest` |
| **Expected** | it prints the Manifest kept |
