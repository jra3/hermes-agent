# GTD continuation proof

Prerequisite: https://github.com/jra3/gtdd/issues/108

Base: `f97608f178d1ffeca59860195ab7da295f7c8e5f` (the deployed Hermes revision).
This branch is an isolated proof, not a rollout or a finished gateway extension.

## Observed

`tests/gateway/test_api_server_continuity.py` sends authenticated requests to the
real aiohttp `/v1/runs` handler, using a temporary Hermes home and real SQLite
SessionDB. Only agent construction/execution is replaced by a recorder. No
provider request or external tool runs.

Two cases pass as **characterizations of undesirable behavior**:

- An explicit `session_id` that has never existed is accepted with HTTP 202;
  execution receives the delta with empty history and the run reports completed.
- A session with an existing two-message baseline whose history read raises
  `sqlite3.OperationalError` is likewise accepted and executed with empty history.

Command: `scripts/run_tests.sh tests/gateway/test_api_server_continuity.py -q`.
Observed result: two tests passed. This is evidence of the gap, not evidence that
strict continuation is implemented. Neither test proves real provider behavior,
input persistence, compaction, recovery, or exactly-once tool effects.

## Source findings that constrain the change

`_handle_runs` loads history before scheduling execution. An HTTP-only preflight
cannot close a subsequent deletion/reset race. `_conversation_history_for_session`
returns an empty list on database failure. `_resolve_live_session_id` and the
underlying resume resolver also have fallback paths.

`agent/turn_facade_lease.py` already owns a durable cross-process session lease.
It skips admission when the session is absent and reloads history only when it
waited. Strict continuation must validate/reload under that lease even when it
acquired immediately. Reusing the lease is preferable to a new parallel lock.

`agent/turn_context.py::_persist_turn_start` flushes inbound context before the
first model request, but `_persist_under_lock` logs and swallows exceptions.
The flush also has best-effort branches. A strict receipt must be based on
independent durable readback, not the return of this method or run completion.

## Required implementation boundary

Keep strict delivery opt-in for the GTD client; preserve existing Signal and
other clients. The interface needs explicit creation versus continuation,
expected conversation generation and previous context receipt, immutable input
identity, and durable receipt readback. Continue to use authenticated API access
and the existing run idempotency store. A context receipt says input was
persisted; it never says external tool effects completed exactly once.

Validation belongs at actual turn admission and input persistence. A missing or
reset generation, unreadable history, stale context receipt, or failed input
readback must prevent model/tool execution. Normal compaction needs a tested
lineage rule; active transcript search alone cannot prove older input delivery.
The client must not manufacture a successful receipt from HTTP 202.

## Rollout gates (not yet satisfied)

Before preparing a production image, prove: ordinary two-turn continuation,
same-Action concurrent sends, duplicate idempotency replay, deletion/reset
between HTTP admission and execution, storage failures before/after input write,
interruption, process restart, and normal compaction. Use disposable state and
fake model/tool boundaries, with real HTTP, database, and lease paths.

Only then build a pinned derivative image and document its exact source/image
identity, backup and restoration procedure. Test the image against a disposable
profile before scheduling the personal gateway rollout. Rollback restores the
previous image and disables GTD admission; it must not replay unsettled turns.
No personal gateway restart or configuration change was made for this proof.
