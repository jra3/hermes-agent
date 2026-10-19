"""Disposable production-runtime probe for the GTD continuation contract (jra3/gtdd#108).

Boots a real ``GatewayRunner`` with the canonical session authority and the real API adapter on a
loopback port, as the gateway does, with only the model provider replaced by a loopback HTTP peer
that records every request it receives. Every scenario drives the adapter over real HTTP and reads
state back over HTTP and from the disposable ``state.db``. Nothing here reaches a real provider or
tool, and ``HERMES_HOME`` is a scratch directory the test wrapper owns.

Run through ``tests/gateway/test_gtd_continuation_contract.py``; the scenario is ``GTD_SCENARIO``.
The receipt is ``$HERMES_HOME/receipt.json``; assertions live in the test, not here, so an
unexpected observation is still recorded.
"""
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
import traceback

KEY = 'gtd-probe-owned-secret'
SID = 'gtd-A-7-g1'  # the id GTD mints for Action A-7's first conversation generation


class ModelPeer(BaseHTTPRequestHandler):
    """OpenAI-wire stub: echoes the last user text; ``BLOCK`` holds the turn until released."""

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'data': [{'id': 'local-wire-stub', 'object': 'model'}]}).encode())

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if 'messages' not in request:
            self.server.metadata_requests.append(request)
            reply = ''
        else:
            last = next((m.get('content', '') for m in reversed(request['messages']) if m.get('role') == 'user'), '')
            text = last if isinstance(last, str) else json.dumps(last)
            if 'BLOCK' in text:
                self.server.blocked.set()
                assert self.server.release.wait(30), 'fixture model gate timed out'
            self.server.requests.append(request['messages'])
            reply = f'ACK[{text}]'
        base = {'id': 'chatcmpl-local', 'model': 'local-wire-stub', 'created': 1}
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream' if request.get('stream') else 'application/json')
        self.end_headers()
        if request.get('stream'):
            for delta, finish in [({'role': 'assistant', 'content': reply}, None), ({}, 'stop')]:
                chunk = {**base, 'object': 'chat.completion.chunk',
                         'choices': [{'index': 0, 'delta': delta, 'finish_reason': finish}]}
                self.wfile.write(('data: ' + json.dumps(chunk) + '\n\n').encode())
                self.wfile.flush()
            self.wfile.write(b'data: [DONE]\n\n')
        else:
            self.wfile.write(json.dumps({**base, 'object': 'chat.completion', 'choices': [
                {'index': 0, 'message': {'role': 'assistant', 'content': reply}, 'finish_reason': 'stop'}],
                'usage': {'prompt_tokens': 10, 'completion_tokens': 5, 'total_tokens': 15}}).encode())


def user_texts(messages):
    """The user turns the model saw, in order: the history the execution actually loaded."""
    out = []
    for m in messages:
        if m.get('role') != 'user':
            continue
        content = m.get('content')
        out.append(content if isinstance(content, str) else json.dumps(content))
    return out


class Hermes:
    """One disposable gateway process: runner, authority, API adapter, loopback model."""

    def __init__(self, peer):
        self.peer = peer
        self.runner = self.adapter = self.port = None

    async def start(self, *, instance_id):
        from gateway.config import Platform, PlatformConfig
        from gateway.platforms.api_server import APIServerAdapter
        from gateway.run import GatewayRunner
        from gateway.session_authority import initialize_session_authority
        from hermes_cli.plugins import discover_plugins
        discover_plugins()  # the gateway loads plugins from HERMES_HOME/plugins while reading its config
        self.runner = GatewayRunner()
        await initialize_session_authority(self.runner, profile_id='default', instance_id=instance_id)
        self.adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={'key': KEY, 'port': 0}))
        self.adapter.gateway_runner = self.runner
        self.adapter.set_message_handler(self.runner._handle_message)
        assert await self.adapter.connect(), 'API adapter failed to connect'
        # What the gateway does once the adapter is up: publish it and recover committed API work.
        self.runner._publish_primary_adapter(Platform.API_SERVER, self.adapter)
        self.port = self.adapter._site._server.sockets[0].getsockname()[1]
        return self

    @property
    def authority(self):
        return self.runner.session_authority

    @property
    def db(self):
        return self.authority.db

    def rows(self, sid):
        from hermes_state_runtime import list_session_admissions
        return [{k: v for k, v in r.items() if k != 'payload'} | {'text': r['payload'].get('text')}
                for r in list_session_admissions(self.db, session_id=sid, pending_only=False)]

    async def stop(self):
        await self.adapter.disconnect()


class Client:
    def __init__(self, session, port):
        self.session, self.port = session, port
        self.headers = {'Authorization': f'Bearer {KEY}'}

    def url(self, path):
        return f'http://127.0.0.1:{self.port}{path}'

    async def run(self, sid, text, *, key=None, headers=None, body=None):
        """POST /v1/runs -> (status, json). ``key`` is the Idempotency-Key GTD mints per message."""
        hdrs = {**self.headers, **(headers or {})}
        if key:
            hdrs['Idempotency-Key'] = key
        payload = {'session_id': sid, 'input': text} if body is None else body
        async with self.session.post(self.url('/v1/runs'), json=payload, headers=hdrs) as resp:
            return resp.status, await resp.json(), dict(resp.headers)

    async def get(self, path, headers=None):
        async with self.session.get(self.url(path), headers={**self.headers, **(headers or {})}) as resp:
            return resp.status, await resp.json()

    async def post(self, path, body):
        async with self.session.post(self.url(path), json=body, headers=self.headers) as resp:
            return resp.status, await resp.json()

    async def delete(self, path):
        async with self.session.delete(self.url(path), headers=self.headers) as resp:
            return resp.status, await resp.json()

    async def wait_terminal(self, run_id, *, timeout=20, also=()):
        deadline = time.monotonic() + timeout
        while True:
            status, body = await self.get(f'/v1/runs/{run_id}')
            if status != 200 or body.get('status') in ('completed', 'failed', 'cancelled', *also):
                return status, body
            assert time.monotonic() < deadline, ('run never settled', body)
            await asyncio.sleep(.02)

    async def messages(self, sid):
        return await self.get(f'/api/sessions/{sid}/messages?order=oldest&limit=100')


def _receipt(evidence, name='receipt.json'):
    out = Path(os.environ['HERMES_HOME'], name)
    out.with_suffix('.tmp').write_text(json.dumps(evidence, indent=2, default=str))
    os.replace(out.with_suffix('.tmp'), out)


# ── scenarios ──────────────────────────────────────────────────────────────────────────────────────

async def create_and_continue(h, c):
    """Initial context on a never-existing explicit id, then a delta on the same id."""
    ev = {}
    s1, r1, _ = await c.run(SID, 'CTX initial Action context', key='msg-1')
    ev['first'] = {'http': s1, 'body': r1}
    ev['first_settled'] = (await c.wait_terminal(r1['run_id']))[1]
    ev['model_after_first'] = [user_texts(m) for m in h.peer.requests]
    s2, r2, _ = await c.run(SID, 'DELTA title changed', key='msg-2')
    ev['second'] = {'http': s2, 'body': r2}
    ev['second_settled'] = (await c.wait_terminal(r2['run_id']))[1]
    ev['model_after_second'] = [user_texts(m) for m in h.peer.requests]
    ev['transcript'] = (await c.messages(SID))[1]
    ev['rows'] = h.rows(SID)
    # A new generation (GTD's explicit Restore) is a different id with no history behind it.
    s3, r3, _ = await c.run('gtd-A-7-g2', 'CTX restored context', key='msg-3')
    ev['restore'] = {'http': s3, 'settled': (await c.wait_terminal(r3['run_id']))[1],
                     'model': user_texts(h.peer.requests[-1])}
    return ev


async def replay(h, c):
    """The same Idempotency-Key with the same body names the same run; a changed body is refused."""
    ev = {}
    s1, r1, hd1 = await c.run(SID, 'CTX', key='msg-1')
    ev['first'] = {'http': s1, 'body': r1}
    ev['first_settled'] = (await c.wait_terminal(r1['run_id']))[1]
    s2, r2, hd2 = await c.run(SID, 'CTX', key='msg-1')
    ev['replay'] = {'http': s2, 'body': r2, 'replayed_header': hd2.get('Idempotency-Replayed')}
    s3, r3, _ = await c.run(SID, 'CTX but different', key='msg-1')
    ev['conflict'] = {'http': s3, 'body': r3}
    # The same key aimed at another session is a conflict too (the ledger keys identity by target).
    s4, r4, _ = await c.run('gtd-A-8-g1', 'CTX', key='msg-1')
    ev['other_target'] = {'http': s4, 'body': r4}
    ev['model_calls'] = len(h.peer.requests)
    ev['rows'] = h.rows(SID)
    return ev


async def deleted_session(h, c):
    """A deleted conversation refuses later sends instead of executing them on empty history."""
    ev = {}
    s1, r1, _ = await c.run(SID, 'CTX', key='msg-1')
    ev['first_settled'] = (await c.wait_terminal(r1['run_id']))[1]
    ev['delete'] = await c.delete(f'/api/sessions/{SID}')
    ev['get_after_delete'] = await c.get(f'/api/sessions/{SID}')
    s2, r2, _ = await c.run(SID, 'DELTA after delete', key='msg-2')
    ev['send_after_delete'] = {'http': s2, 'body': r2}
    await asyncio.sleep(.5)
    ev['model_calls'] = len(h.peer.requests)
    ev['model_user_texts'] = [user_texts(m) for m in h.peer.requests]
    # Replaying the FIRST turn's key after deletion: the tombstone still answers it.
    s3, r3, _ = await c.run(SID, 'CTX', key='msg-1')
    ev['replay_after_delete'] = {'http': s3, 'body': r3}
    ev['run1_status_after_delete'] = await c.get(f"/v1/runs/{r1['run_id']}")
    return ev


async def delete_while_queued(h, c):
    """Reset/deletion between HTTP admission and execution: a turn runs, a second is queued."""
    ev = {}
    h.peer.blocked.clear()
    h.peer.release.clear()
    s1, r1, _ = await c.run(SID, 'BLOCK first turn', key='msg-1')
    assert await asyncio.to_thread(h.peer.blocked.wait, 10)
    s2, r2, _ = await c.run(SID, 'DELTA queued behind', key='msg-2')
    ev['queued'] = {'http': s2, 'body': r2, 'status': (await c.get(f"/v1/runs/{r2['run_id']}"))[1]}
    ev['delete_during_turn'] = await c.delete(f'/api/sessions/{SID}')
    ev['rows_during'] = h.rows(SID)
    h.peer.release.set()
    ev['first_settled'] = (await c.wait_terminal(r1['run_id']))[1]
    ev['second_settled'] = (await c.wait_terminal(r2['run_id'], also=('unknown', 'queued')))[1]
    await asyncio.sleep(.5)
    ev['rows_after'] = h.rows(SID)
    ev['model_user_texts'] = [user_texts(m) for m in h.peer.requests]
    # Now the FIFO is idle: a delete that lands between two sends.
    ev['delete_idle'] = await c.delete(f'/api/sessions/{SID}')
    s3, r3, _ = await c.run(SID, 'DELTA after idle delete', key='msg-3')
    ev['send_after_idle_delete'] = {'http': s3, 'body': r3}
    await asyncio.sleep(.3)
    ev['model_calls'] = len(h.peer.requests)
    return ev


async def history_read_error(h, c):
    """The transcript becomes unreadable between two turns: the delta must not run on []."""
    ev = {}
    s1, r1, _ = await c.run(SID, 'CTX', key='msg-1')
    ev['first_settled'] = (await c.wait_terminal(r1['run_id']))[1]
    db = h.db
    original = db.get_messages_as_conversation
    broken = {'on': True, 'reads': 0}

    def unreadable(session_id, *args, **kwargs):
        if broken['on'] and session_id == SID:
            broken['reads'] += 1
            raise sqlite3.OperationalError('injected: messages table unreadable')
        return original(session_id, *args, **kwargs)
    db.get_messages_as_conversation = unreadable
    try:
        s2, r2, _ = await c.run(SID, 'DELTA on unreadable history', key='msg-2')
        ev['second'] = {'http': s2, 'body': r2}
        ev['second_settled'] = (await c.wait_terminal(r2['run_id']))[1]
        ev['injected_reads'] = broken['reads']
        ev['model_calls_after_failure'] = len(h.peer.requests)
    finally:
        broken['on'] = False
        db.get_messages_as_conversation = original
    ev['rows_after_failure'] = h.rows(SID)
    # Storage healed: the next delta sees the whole baseline, the failed input is not replayed by Hermes.
    s3, r3, _ = await c.run(SID, 'DELTA after heal', key='msg-3')
    ev['third_settled'] = (await c.wait_terminal(r3['run_id']))[1]
    ev['model_third'] = user_texts(h.peer.requests[-1]) if len(h.peer.requests) > 1 else None
    ev['transcript'] = (await c.messages(SID))[1]
    return ev


async def admission_storage_failure(h, c):
    """The admission write itself fails: no run, no row, no execution; the retry is a fresh admission."""
    ev = {}
    s1, r1, _ = await c.run(SID, 'CTX', key='msg-1')
    ev['first_settled'] = (await c.wait_terminal(r1['run_id']))[1]
    db = h.db
    original = db._execute_write
    broken = {'on': True, 'writes': 0}

    def failing(*args, **kwargs):
        if broken['on']:
            broken['writes'] += 1
            raise sqlite3.OperationalError('injected: database is locked')
        return original(*args, **kwargs)
    db._execute_write = failing
    try:
        s2, r2, hd2 = await c.run(SID, 'DELTA during outage', key='msg-2')
        ev['during_outage'] = {'http': s2, 'body': r2, 'retry_after': hd2.get('Retry-After'),
                               'writes_attempted': broken['writes']}
    finally:
        broken['on'] = False
        db._execute_write = original
    await asyncio.sleep(.3)
    ev['rows_after_outage'] = h.rows(SID)
    ev['model_calls_after_outage'] = len(h.peer.requests)
    s3, r3, _ = await c.run(SID, 'DELTA during outage', key='msg-2')
    ev['retry'] = {'http': s3, 'body': r3}
    ev['retry_settled'] = (await c.wait_terminal(r3['run_id']))[1]
    ev['model_retry'] = user_texts(h.peer.requests[-1])
    ev['rows_after_retry'] = h.rows(SID)
    return ev


async def concurrent(h, c):
    """Two sends at once on one conversation, and an interleaved writer GTD did not send."""
    ev = {}
    (s1, r1, _), (s2, r2, _) = await asyncio.gather(c.run(SID, 'CTX one', key='msg-1'),
                                                    c.run(SID, 'DELTA two', key='msg-2'))
    ev['admitted'] = [{'http': s1, 'body': r1}, {'http': s2, 'body': r2}]
    ev['settled'] = [(await c.wait_terminal(r1['run_id']))[1], (await c.wait_terminal(r2['run_id']))[1]]
    ev['model_user_texts'] = [user_texts(m) for m in h.peer.requests]
    ev['rows'] = h.rows(SID)
    # A third party (another API client, the dashboard) writes to the same session between GTD's turns.
    s3, r3, _ = await c.run(SID, 'OTHER writer not from GTD', key='stranger-1')
    await c.wait_terminal(r3['run_id'])
    s4, r4, _ = await c.run(SID, 'DELTA three', key='msg-3')
    ev['after_stranger'] = (await c.wait_terminal(r4['run_id']))[1]
    ev['model_after_stranger'] = user_texts(h.peer.requests[-1])
    return ev


async def compaction(h, c):
    """Normal history compaction: the root id keeps admitting; the delta sees the compacted lineage."""
    ev = {}
    s1, r1, _ = await c.run(SID, 'CTX before compaction', key='msg-1')
    ev['first_settled'] = (await c.wait_terminal(r1['run_id']))[1]
    db, authority = h.db, h.authority
    # The compaction publication the compressor performs, as upstream's own continuity test drives it.
    db.publish_compression_child(parent_session_id=SID, child_session_id='gtd-A-7-g1-c1', source='api_server',
                                 messages=[{'role': 'user', 'content': 'CTX before compaction'},
                                           {'role': 'assistant', 'content': 'SUMMARY of the earlier turns'}],
                                 require_compression_lease=False)
    route = authority.sessions[SID].route
    advanced = h.runner.session_store.advance_compression_session(route, SID, 'gtd-A-7-g1-c1')
    ev['advanced'] = advanced is not None
    ev['tip'] = db.get_compression_tip(SID)
    s2, r2, _ = await c.run(SID, 'DELTA after compaction', key='msg-2')
    ev['second'] = {'http': s2, 'body': r2}
    ev['second_settled'] = (await c.wait_terminal(r2['run_id']))[1]
    ev['model_second'] = [m.get('content') for m in h.peer.requests[-1] if m.get('role') in ('user', 'assistant')]
    ev['rows_root'] = h.rows(SID)
    ev['rows_child'] = h.rows('gtd-A-7-g1-c1')
    ev['transcript_root'] = (await c.messages(SID))[1]
    s3, r3, _ = await c.run('gtd-A-7-g1-c1', 'DELTA aimed at the child id', key='msg-3')
    ev['child_addressed'] = {'http': s3, 'body': r3}
    if s3 == 202:
        ev['child_addressed']['settled'] = (await c.wait_terminal(r3['run_id']))[1]
        ev['child_addressed']['rows_root'] = h.rows(SID)
    return ev


async def plugin_endpoint(h, c):
    """A standalone plugin's route on the API server, through real discovery from HERMES_HOME/plugins."""
    ev = {}
    from hermes_cli.plugins import get_plugin_manager
    ev['plugins'] = [{k: p[k] for k in ('name', 'enabled', 'error') if k in p}
                     for p in get_plugin_manager().list_plugins()]
    s1, r1, _ = await c.run(SID, 'CTX', key='msg-1')
    ev['first_settled'] = (await c.wait_terminal(r1['run_id']))[1]
    ev['continuation'] = await c.get(f'/v1/gtd/continuation/{SID}')
    ev['continuation_missing'] = await c.get('/v1/gtd/continuation/never-existed')
    async with c.session.get(c.url(f'/v1/gtd/continuation/{SID}')) as resp:
        ev['unauthenticated'] = {'http': resp.status, 'body': await resp.json()}
    # The plugin's guarded send: refuses a missing conversation and a stale predecessor BEFORE admitting.
    ev['guarded_missing'] = await c.post('/v1/gtd/send', {
        'session_id': 'never-existed', 'input': 'DELTA', 'idempotency_key': 'g-1', 'after_run_id': r1['run_id']})
    ev['guarded_stale'] = await c.post('/v1/gtd/send', {
        'session_id': SID, 'input': 'DELTA', 'idempotency_key': 'g-2', 'after_run_id': 'run_not_the_latest'})
    s3, r3 = await c.post('/v1/gtd/send', {
        'session_id': SID, 'input': 'DELTA via plugin', 'idempotency_key': 'g-3', 'after_run_id': r1['run_id']})
    ev['guarded_ok'] = {'http': s3, 'body': r3}
    if s3 == 202:
        ev['guarded_ok']['settled'] = (await c.wait_terminal(r3['run_id']))[1]
        ev['guarded_ok']['model'] = user_texts(h.peer.requests[-1])
    ev['model_calls'] = len(h.peer.requests)
    ev['rows'] = h.rows(SID)
    return ev


async def restart_crash(h, c):
    """Phase 1: a turn is executing and a second is queued when the owner dies."""
    h.peer.blocked.clear()
    h.peer.release.clear()
    s1, r1, _ = await c.run(SID, 'BLOCK first turn', key='msg-1')
    assert await asyncio.to_thread(h.peer.blocked.wait, 10)
    s2, r2, _ = await c.run(SID, 'DELTA queued behind', key='msg-2')
    ev = {'run1': r1, 'run2': r2, 'rows': h.rows(SID),
          'status1': (await c.get(f"/v1/runs/{r1['run_id']}"))[1], 'status2': (await c.get(f"/v1/runs/{r2['run_id']}"))[1]}
    _receipt(ev, 'crash-ready.json')
    parent = os.getppid()
    while os.getppid() == parent:  # the parent SIGKILLs this process once crash-ready.json exists
        time.sleep(.05)
    raise AssertionError('parent exited without killing the owner')


async def restart_recover(h, c):
    """Phase 2: the same home, a new owner. Nothing replays; uncertainty is explicit; the FIFO waits."""
    before = json.loads(Path(os.environ['HERMES_HOME'], 'crash-ready.json').read_text())
    run1, run2 = before['run1']['run_id'], before['run2']['run_id']
    ev = {'before': before}
    ev['rows_at_boot'] = h.rows(SID)
    ev['status1'] = await c.get(f'/v1/runs/{run1}')
    ev['status2'] = await c.get(f'/v1/runs/{run2}')
    # Exact replays of both keys after restart: same runs, no new work.
    ev['replay1'] = await c.run(SID, 'BLOCK first turn', key='msg-1')
    ev['replay2'] = await c.run(SID, 'DELTA queued behind', key='msg-2')
    # A new send while the head is unknown.
    s3, r3, _ = await c.run(SID, 'DELTA three after restart', key='msg-3')
    ev['third'] = {'http': s3, 'body': r3}
    await asyncio.sleep(1.0)
    ev['model_calls_before_resolution'] = len(h.peer.requests)
    ev['rows_before_resolution'] = h.rows(SID)
    ev['third_status_before_resolution'] = (await c.get(f'/v1/runs/{r3["run_id"]}'))[1] if s3 == 202 else None
    # Explicit operator acknowledgement of the lost turn, with the exact admission and generation.
    unknown = next(r for r in h.rows(SID) if r['request_id'] == run1)
    ev['resolve_wrong_generation'] = await c.post(f'/v1/runs/{run1}/resolve-unknown', {
        'admission_id': unknown['admission_id'], 'execution_generation': unknown['generation'] + 7})
    ev['resolve'] = await c.post(f'/v1/runs/{run1}/resolve-unknown', {
        'admission_id': unknown['admission_id'], 'execution_generation': unknown['generation']})
    ev['run2_settled'] = (await c.wait_terminal(run2))[1]
    if s3 == 202:
        ev['third_settled'] = (await c.wait_terminal(r3['run_id']))[1]
    await asyncio.sleep(.3)
    ev['model_user_texts'] = [user_texts(m) for m in h.peer.requests]
    ev['rows_after'] = h.rows(SID)
    ev['transcript'] = (await c.messages(SID))[1]
    return ev


SCENARIOS = {f.__name__: f for f in (create_and_continue, replay, deleted_session, delete_while_queued,
                                      history_read_error, admission_storage_failure, concurrent, compaction,
                                      plugin_endpoint, restart_crash, restart_recover)}


async def main(peer):
    import aiohttp
    scenario = SCENARIOS[os.environ['GTD_SCENARIO']]
    h = await Hermes(peer).start(instance_id=f"gtd-probe-{os.environ.get('GTD_INSTANCE', '1')}")
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            evidence = await scenario(h, Client(session, h.port))
            evidence['model_requests'] = h.peer.requests
            _receipt(evidence)
    finally:
        if scenario is not restart_crash:
            await h.stop()


if __name__ == '__main__':
    peer = ThreadingHTTPServer(('127.0.0.1', 0), ModelPeer)
    peer.requests, peer.metadata_requests = [], []
    peer.blocked, peer.release = threading.Event(), threading.Event()
    threading.Thread(target=peer.serve_forever, daemon=True).start()
    base = f'http://127.0.0.1:{peer.server_port}/v1'
    os.environ.update(OPENAI_API_KEY='loopback-only', OPENAI_BASE_URL=base)
    home = Path(os.environ['HERMES_HOME'])
    plugins = 'plugins:\n  enabled: [gtd-continuation]\n' if os.environ.get('GTD_PLUGIN') else ''
    (home / 'config.yaml').write_text(
        f'model:\n  default: local-wire-stub\n  provider: custom\n  base_url: {base}\n'
        f'auxiliary:\n  title_generation:\n    enabled: false\nterminal:\n  cwd: {home}\n{plugins}')
    code = 0
    try:
        asyncio.run(main(peer))
    except BaseException:
        traceback.print_exc()
        code = 1
    finally:
        peer.shutdown()
        peer.server_close()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)
