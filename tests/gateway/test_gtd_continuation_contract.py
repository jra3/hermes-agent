"""GTD continuation contract against the production admission path (jra3/gtdd#108).

Each test boots the disposable gateway in ``fixtures/gtd_continuation_peer.py`` (real runner,
authority, API adapter over real HTTP, loopback model) in a subprocess with its own HERMES_HOME,
runs one scenario and asserts on the receipt. An assertion here states the contract GTD relies on;
where current Hermes falls short the test pins the observed behavior and says so in its docstring,
so a later upstream change is noticed either way.
"""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[2]
PEER = Path(__file__).parent / 'fixtures' / 'gtd_continuation_peer.py'
PLUGIN = Path(__file__).parent / 'fixtures' / 'gtd_continuation_plugin'
SID = 'gtd-A-7-g1'


def _env(tmp_path, scenario, **extra):
    home, state = tmp_path / 'home', tmp_path / 'state'
    home.mkdir(exist_ok=True)
    state.mkdir(exist_ok=True)
    env = {key: os.environ[key] for key in ('PATH', 'LANG', 'TZ', 'SYSTEMROOT') if key in os.environ}
    env.update(HOME=str(home), USERPROFILE=str(home), HERMES_HOME=str(state), PYTHONPATH=str(REPO),
               GTD_SCENARIO=scenario, **extra)
    return env


def _probe(tmp_path, scenario, **extra):
    result = subprocess.run([sys.executable, str(PEER)], cwd=REPO, env=_env(tmp_path, scenario, **extra),
                            stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=120, check=False)
    assert result.returncode == 0, result.stdout + '\n' + result.stderr
    receipt = json.loads((tmp_path / 'state' / 'receipt.json').read_text())
    print(json.dumps({k: v for k, v in receipt.items() if k != 'model_requests'}, indent=1, default=str))
    return receipt


def _completed(status):
    assert status['status'] == 'completed', status
    return status


def test_initial_context_then_delta_sees_the_baseline(tmp_path):
    """Normal continuation: the delta executes with the initial context in its history, loaded from
    the session at execution (the admission records ``history: null``), and the receipt names the
    admission and its execution generation.

    Pinned gap: an explicit ``session_id`` that never existed is accepted and created silently
    (HTTP 202, executes on empty history). Current Hermes has no creation-versus-continuation
    distinction a client can ask for."""
    r = _probe(tmp_path, 'create_and_continue')
    assert r['first']['http'] == 202 and r['first']['body']['replayed'] is False
    _completed(r['first_settled'])
    assert r['model_after_first'] == [['CTX initial Action context']]  # created silently, empty history
    assert r['second']['http'] == 202
    settled = _completed(r['second_settled'])
    assert settled['session_id'] == SID and settled['admission_id'] and settled['execution_generation'] == 2
    assert r['model_after_second'][1] == ['CTX initial Action context', 'DELTA title changed']
    roles = [(m['role'], m['content']) for m in r['transcript']['data'] if m['role'] in ('user', 'assistant')]
    assert roles == [('user', 'CTX initial Action context'), ('assistant', 'ACK[CTX initial Action context]'),
                     ('user', 'DELTA title changed'), ('assistant', 'ACK[DELTA title changed]')], roles
    assert [(row['request_id'], row['status'], row['outcome']) for row in r['rows']] == [
        (r['first']['body']['run_id'], 'terminal', 'completed'), (r['second']['body']['run_id'], 'terminal', 'completed')]
    # Restore: a new generation id starts from nothing, as GTD intends when it resends the context.
    assert r['restore']['http'] == 202 and r['restore']['model'] == ['CTX restored context']


def test_exact_replay_names_the_same_run_and_a_changed_body_is_refused(tmp_path):
    """Duplicate replay: the same Idempotency-Key with the same body returns the same run without
    a second execution; the same key with another body, or aimed at another session, is refused."""
    r = _probe(tmp_path, 'replay')
    assert r['first']['http'] == 202
    assert r['replay']['http'] == 202 and r['replay']['body']['run_id'] == r['first']['body']['run_id']
    assert r['replay']['body']['replayed'] is True and r['replay']['replayed_header'] == 'true'
    assert r['conflict']['http'] == 409 and r['conflict']['body']['error']['code'] == 'idempotency_key_conflict'
    assert r['other_target']['http'] == 409, r['other_target']
    assert r['model_calls'] == 1
    assert len(r['rows']) == 1


def test_deleted_conversation_refuses_the_delta_before_execution(tmp_path):
    """Missing history, the explicit form: a deleted session is retired for good. A later send to
    its id is refused at admission (409 ``not_found``), nothing executes, the old run's receipt
    still answers by its key, and the run's status is still readable."""
    r = _probe(tmp_path, 'deleted_session')
    _completed(r['first_settled'])
    assert r['delete'] == [200, {'object': 'hermes.session.deleted', 'id': SID, 'deleted': True}]
    assert r['get_after_delete'][0] == 404
    assert r['send_after_delete']['http'] == 409, r['send_after_delete']
    assert r['send_after_delete']['body']['error']['code'] == 'not_found'
    assert r['model_calls'] == 1 and r['model_user_texts'] == [['CTX']]
    assert r['replay_after_delete']['http'] == 202 and r['replay_after_delete']['body']['replayed'] is True
    assert r['run1_status_after_delete'][0] == 200 and r['run1_status_after_delete'][1]['status'] == 'completed'


def test_delete_between_admission_and_execution(tmp_path):
    """Reset between HTTP admission and execution. While a turn executes the delete is refused
    (409 ``session_active_turn``), so the queued delta runs with its baseline intact. With the FIFO
    idle, the delete retires the id and the next send is refused at admission."""
    r = _probe(tmp_path, 'delete_while_queued')
    assert r['queued']['http'] == 202 and r['queued']['status']['status'] == 'queued'
    assert r['delete_during_turn'][0] == 409, r['delete_during_turn']
    assert r['delete_during_turn'][1]['error']['code'] == 'session_active_turn'
    _completed(r['first_settled'])
    _completed(r['second_settled'])
    assert r['model_user_texts'] == [['BLOCK first turn'], ['BLOCK first turn', 'DELTA queued behind']]
    assert r['delete_idle'][0] == 200 and r['delete_idle'][1]['deleted'] is True
    assert r['send_after_idle_delete']['http'] == 409
    assert r['send_after_idle_delete']['body']['error']['code'] == 'not_found'
    assert r['model_calls'] == 2


def test_unreadable_history_fails_the_turn_before_the_model_runs(tmp_path):
    """Unreadable context: with the transcript read raising at execution, the admitted delta settles
    ``failed`` and the model is never called; the FIFO continues, and once storage heals the next
    delta sees the whole baseline. Hermes does not replay the failed input."""
    r = _probe(tmp_path, 'history_read_error')
    _completed(r['first_settled'])
    assert r['second']['http'] == 202  # admission is durable, so acceptance precedes the read
    assert r['second_settled']['status'] == 'failed', r['second_settled']
    assert r['injected_reads'] >= 1
    assert r['model_calls_after_failure'] == 1
    failed = next(row for row in r['rows_after_failure'] if row['request_id'] == r['second']['body']['run_id'])
    assert (failed['status'], failed['outcome']) == ('terminal', 'failed')
    _completed(r['third_settled'])
    assert r['model_third'] == ['CTX', 'DELTA after heal']
    users = [m['content'] for m in r['transcript']['data'] if m['role'] == 'user']
    assert users == ['CTX', 'DELTA after heal'], users  # the failed input left no transcript row


def test_admission_storage_failure_refuses_without_a_run(tmp_path):
    """Failed persistence at admission: a storage error answers 503 ``storage_unavailable`` with
    Retry-After, leaves no admission row and runs nothing; the same key retried after the outage
    is a fresh admission that executes once."""
    r = _probe(tmp_path, 'admission_storage_failure')
    _completed(r['first_settled'])
    d = r['during_outage']
    assert d['http'] == 503 and d['body']['error']['code'] == 'storage_unavailable' and d['retry_after'] == '1', d
    assert d['writes_attempted'] >= 1
    assert len(r['rows_after_outage']) == 1 and r['model_calls_after_outage'] == 1
    assert r['retry']['http'] == 202 and r['retry']['body']['replayed'] is False
    _completed(r['retry_settled'])
    assert r['model_retry'] == ['CTX', 'DELTA during outage']
    assert len(r['rows_after_retry']) == 2


def test_concurrent_sends_serialize_and_a_stranger_is_not_detected(tmp_path):
    """Concurrent sends on one conversation are both admitted and run in FIFO order, the second
    seeing the first. Pinned gap: a turn admitted by another writer between GTD's sends is simply
    part of the history; nothing lets GTD name the predecessor it expects."""
    r = _probe(tmp_path, 'concurrent')
    assert [a['http'] for a in r['admitted']] == [202, 202]
    for s in r['settled']:
        _completed(s)
    # Two requests in flight at once commit in the order the server reads them, not the order the
    # client issued them: GTD must serialize its own sends per Action and never rely on this order.
    first, second = r['model_user_texts']
    assert len(first) == 1 and second == [first[0], ({'CTX one', 'DELTA two'} - {first[0]}).pop()], r['model_user_texts']
    assert [row['generation'] for row in r['rows']] == [1, 2]
    assert [row['text'] for row in r['rows']] == [first[0], second[1]]
    _completed(r['after_stranger'])
    assert r['model_after_stranger'] == [*second, 'OTHER writer not from GTD', 'DELTA three']


def test_compaction_keeps_the_root_id_and_the_lineage(tmp_path):
    """Normal compaction: after the compressor publishes a child and advances the route, a send to
    the root id is admitted on the root, executes with the compacted lineage, and the transcript
    read on the root follows root to tip. A send aimed at the child id lands on the root too."""
    r = _probe(tmp_path, 'compaction')
    _completed(r['first_settled'])
    assert r['advanced'] is True and r['tip'] == 'gtd-A-7-g1-c1'
    assert r['second']['http'] == 202
    _completed(r['second_settled'])
    assert r['model_second'] == ['CTX before compaction', 'SUMMARY of the earlier turns', 'DELTA after compaction']
    assert [row['request_id'] for row in r['rows_root']] == [r['first_settled']['run_id'], r['second']['body']['run_id']]
    assert r['rows_child'] == []
    assert r['child_addressed']['http'] == 202 and r['child_addressed']['settled']['session_id'] == SID
    assert len(r['child_addressed']['rows_root']) == 3


def test_restart_leaves_the_lost_turn_unknown_and_nothing_replays(tmp_path):
    """Restart: the owner dies with one turn executing and one queued. The new owner marks the
    executing turn ``unknown``, both runs stay readable by id, exact replays name them without new
    work, a new send is admitted but waits, and only the explicit resolve-unknown with the exact
    admission and generation lets the FIFO move. The lost input is closed with a failed-turn
    boundary; Hermes never re-runs it."""
    env = _env(tmp_path, 'restart_crash', GTD_INSTANCE='1')
    ready = tmp_path / 'state' / 'crash-ready.json'
    owner = subprocess.Popen([sys.executable, str(PEER)], cwd=REPO, env=env, stdin=subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 60
        while not ready.exists():
            assert owner.poll() is None, owner.communicate()
            assert time.monotonic() < deadline, 'owner never reached the crash point'
            time.sleep(.05)
        owner.send_signal(signal.SIGKILL)
        owner.wait(10)
    finally:
        if owner.poll() is None:
            owner.kill()
    before = json.loads(ready.read_text())
    assert before['status1']['status'] == 'running' and before['status2']['status'] == 'queued', before
    r = _probe(tmp_path, 'restart_recover', GTD_INSTANCE='2')
    run1, run2 = before['run1']['run_id'], before['run2']['run_id']
    boot = {row['request_id']: row['status'] for row in r['rows_at_boot']}
    assert boot == {run1: 'unknown', run2: 'queued'}, boot
    assert r['status1'][0] == 200 and r['status1'][1]['status'] == 'unknown'
    assert r['status2'][0] == 200 and r['status2'][1]['status'] == 'queued'
    assert r['replay1'][0] == 202 and r['replay1'][1]['run_id'] == run1 and r['replay1'][1]['replayed'] is True
    assert r['replay2'][0] == 202 and r['replay2'][1]['run_id'] == run2 and r['replay2'][1]['replayed'] is True
    assert r['third']['http'] == 202
    assert r['model_calls_before_resolution'] == 0
    assert r['third_status_before_resolution']['status'] == 'queued'
    assert r['resolve_wrong_generation'][0] == 409 and r['resolve_wrong_generation'][1]['error']['code'] == 'stale_generation'
    assert r['resolve'][0] == 200, r['resolve']
    _completed(r['run2_settled'])
    _completed(r['third_settled'])
    assert r['model_user_texts'] == [['BLOCK first turn', 'DELTA queued behind'],
                                     ['BLOCK first turn', 'DELTA queued behind', 'DELTA three after restart']]
    after = {row['request_id']: (row['status'], row['outcome']) for row in r['rows_after']}
    assert after[run1] == ('terminal', 'interrupted'), after
    roles = [(m['role'], m['content']) for m in r['transcript']['data'] if m['role'] in ('user', 'assistant')]
    assert roles[0] == ('user', 'BLOCK first turn') and roles[1][0] == 'assistant'  # the lost turn's boundary
    assert ('user', 'DELTA queued behind') in roles and ('user', 'DELTA three after restart') in roles


def test_plugin_route_is_served_and_its_guard_is_a_preflight(tmp_path):
    """A standalone plugin installed under HERMES_HOME/plugins and enabled in config registers
    routes on the API server through the supported seam. Its continuation read works; its guarded
    send refuses a missing conversation and a stale predecessor and otherwise forwards to the core
    door. The plugin needs internal imports for every read (see its docstring)."""
    state = tmp_path / 'state'
    state.mkdir()
    shutil.copytree(PLUGIN, state / 'plugins' / 'gtd-continuation')
    r = _probe(tmp_path, 'plugin_endpoint', GTD_PLUGIN='1')
    loaded = next(p for p in r['plugins'] if p['name'] == 'gtd-continuation')
    assert loaded['enabled'] and not loaded.get('error'), loaded
    _completed(r['first_settled'])
    status, facts = r['continuation']
    assert status == 200 and facts['exists'] and not facts['retired'] and facts['pending'] == []
    assert facts['latest']['run_id'] == r['first_settled']['run_id'] and facts['latest']['status'] == 'terminal'
    assert r['continuation_missing'][1]['exists'] is False
    assert r['unauthenticated']['http'] == 401
    assert r['guarded_missing'][0] == 409 and r['guarded_missing'][1]['error']['code'] == 'continuation_missing'
    assert r['guarded_stale'][0] == 409 and r['guarded_stale'][1]['error']['code'] == 'continuation_stale'
    assert r['guarded_ok']['http'] == 202
    _completed(r['guarded_ok']['settled'])
    assert r['guarded_ok']['model'] == ['CTX', 'DELTA via plugin']
    assert r['model_calls'] == 2
