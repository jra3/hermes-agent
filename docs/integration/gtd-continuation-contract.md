# GTD continuation contract: proof against current upstream

Prerequisite: https://github.com/jra3/gtdd/issues/108. Base: upstream `fd905d2926933a74a1f33ca6c8598992ea757ccb`
(main, 2026-10-18), chosen as the newest upstream commit at the time of the proof; the last release
tag, `v0.22.2`, predates the canonical session authority's API admission (`history: null`) and is
not a candidate. This branch adds tests and a proof plugin; it changes nothing in core.

## What runs

`tests/gateway/fixtures/gtd_continuation_peer.py` boots a real `GatewayRunner`, the canonical
session authority, and the real `APIServerAdapter` on a loopback port, as the gateway does, in a
subprocess with its own `HERMES_HOME`. Only the model provider is replaced: a loopback OpenAI-wire
server that echoes the last user text and records every request's `messages`, so a test can see
exactly which history the execution loaded. Fault injection (an unreadable transcript, a failing
admission write) patches the disposable store object inside that process; the admission and
execution paths are the production ones.

`tests/gateway/fixtures/gtd_continuation_plugin/` is a standalone plugin installed under the
scratch home's `plugins/` and enabled through `plugins.enabled`. It registers two routes on the
API server with `ctx.register_platform_handler("api_server", ...)`.

```
TZ=UTC LANG=C.UTF-8 PYTHONHASHSEED=0 .venv/bin/python -m pytest tests/gateway/test_gtd_continuation_contract.py -q -p no:cacheprovider
10 passed in 32.17s
```

The venv is `uv venv --python 3.14 .venv && uv sync --frozen --group dev --extra messaging`.
`scripts/run_tests.sh` also runs it, but its `pm` activation installs tools under `~/.hermes`;
invoke pytest through the venv directly on a machine that hosts a live agent.

## What the tests pin

Each test's docstring states the contract and, where current Hermes falls short, the observed gap:

- creation versus continuation: an explicit `session_id` that never existed is created silently and
  executes on empty history (gap);
- a delta executes with the baseline loaded from the session at execution time;
- exact replay by `Idempotency-Key` names the same run; a changed body or another target is 409;
- a deleted session is retired: later sends are 409 `not_found` before execution;
- a delete during an executing turn is 409 `session_active_turn`; the queued delta keeps its baseline;
- an unreadable transcript settles the admitted turn `failed` before the model runs;
- an admission write failure is 503 `storage_unavailable` with no row and no run;
- concurrent sends serialize in server order; an interleaved third-party turn is undetectable (gap);
- compaction keeps the root id as the admission target and the lineage in the loaded history;
- a restart marks the executing turn `unknown`, keeps every run readable and replayable, queues new
  sends, and moves only on `POST /v1/runs/{id}/resolve-unknown` with the exact admission and generation;
- the plugin's routes are served and authenticated; its guard is a preflight, not an atomic
  precondition, and every read it makes imports internal modules.

The write-up with the decision and the proposed GTD-to-Hermes contract lives in the gtdd repository,
`docs/research/108-plugin-proof.md`.
