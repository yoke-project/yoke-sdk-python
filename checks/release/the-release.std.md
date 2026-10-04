# The release

| | |
| --- | --- |
| **Feature** | this family publishes one wheel, `yoke-sdk`, carrying the base and the plugin library as `yoke_sdk.base` and `yoke_sdk.plugin`, at the version the tree states — which is the version the library says it is — and it publishes it with `yoke`'s published release verb, at its release tag, by trusted publishing; the harness is never published |
| **Planning item** | yoke-project/yoke-sdk-python#25 |

## yoke-sdk-python:the-release.01 — one wheel, yoke-sdk, carries the base and the plugin library, the licence and the notice; the harness is not published

| Field | Value |
| --- | --- |
| **Cites** | prj_structure/80 §The layout · arch/90-sdks/01 §One project, and what the base may hold · prj_structure/95 §The licences |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a clean checkout |
| **Action** | build the wheel `yoke-sdk` and list what it carries; read the harness's project |
| **Expected** | it carries `yoke_sdk/base.py` and `yoke_sdk/plugin.py`, `LICENSE` and `NOTICE`, and nothing of the harness, no test description and no command; the harness is a project of its own that says it is never uploaded, and the wheel's project says no such thing |

## yoke-sdk-python:the-release.02 — the wheel is packaged at the tree's version and no other, and the library says that version

| Field | Value |
| --- | --- |
| **Cites** | prj_structure/85 §What a release is · prj_structure/85 §The release record · arch/90-sdks/01 §What each library declares |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a clean checkout |
| **Action** | package the wheel at the version the tree states; package it at another; read where the project's version and the library's line come from |
| **Expected** | the first writes `yoke_sdk-<version>-py3-none-any.whl`; the second is refused, naming both versions, and writes nothing; the project's version is read from `yoke_sdk`'s `__version__`, and the line is `yoke-sdk-python` followed by it, so a tag, the wheel and the line cannot disagree |

## yoke-sdk-python:the-release.03 — the release verb publishes the wheel with yoke's published verb, and the release run offers trusted publishing

| Field | Value |
| --- | --- |
| **Cites** | prj_structure/95 §The release command · prj_structure/97 §The definitions, and the four families that are not Go |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | the `release` verb, and the workflow `release.yml` |
| **Action** | read them |
| **Expected** | the verb runs `yoke`'s published release verb, asked for the wheel `yoke-sdk`; the workflow runs at a `v` tag, may request an identity token — which the index exchanges for a credential — and keeps the verb's output as the artifact `manifest-lines`; no credential is stored anywhere |
