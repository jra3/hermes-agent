"""gtd-continuation: the smallest standalone endpoint plugin that could carry GTD's contract.

What it proves: ``ctx.register_platform_handler("api_server", factory)`` is a supported seam that
hands a plugin the live aiohttp application before the router freezes, so a GTD-specific route can
exist without touching core.

What it cannot do on supported surfaces, marked ``INTERNAL`` below: every read of conversation
state (the admission ledger, retirement) and the request's authentication go through modules the
plugin policy calls internal (``plugins/AGENTS.md``: "Internal import paths are not API"; the
adapter handed to the factory is read-only). The guarded send is a preflight followed by a loopback
POST to the core ``/v1/runs``: two operations, so a writer can land between them. Closing that
window needs the precondition inside the admission transaction, which only core owns.
"""
import json
import logging

logger = logging.getLogger(__name__)


def _authority(adapter):
    from gateway.session_authorities import active_authority  # INTERNAL
    return active_authority(adapter.gateway_runner)


def _continuation(adapter, session_id):
    """Durable facts a client needs before continuing: existence, retirement, the latest admission."""
    from hermes_state_mutation_retirement import retired_session  # INTERNAL
    from hermes_state_runtime import list_session_admissions  # INTERNAL
    authority = _authority(adapter)
    db = authority.db
    row = db.get_session(session_id)
    rows = list_session_admissions(db, session_id=session_id, pending_only=False)
    latest = rows[-1] if rows else None
    return {
        'session_id': session_id,
        'exists': row is not None,
        'retired': bool(retired_session(db, session_id)),
        'pending': [r['request_id'] for r in rows if r['status'] in ('queued', 'started', 'unknown')],
        'latest': None if latest is None else {
            'run_id': latest['request_id'], 'admission_id': latest['admission_id'],
            'status': latest['status'], 'outcome': latest['outcome'],
            'execution_generation': latest['generation']},
        'runtime_revision': None if row is None else row['runtime_revision'],
    }


def register(ctx):
    def wire(app, adapter):
        from aiohttp import web

        def refused(status, code, message):
            return web.json_response({'error': {'message': message, 'type': 'invalid_request_error',
                                                'param': None, 'code': code}}, status=status)

        async def continuation(request):
            denied = adapter._check_auth(request)  # INTERNAL: the API key check is a private method
            if denied is not None:
                return denied
            return web.json_response(_continuation(adapter, request.match_info['session_id']))

        async def send(request):
            """Preflight the continuation, then forward to the core run door. NOT atomic."""
            denied = adapter._check_auth(request)  # INTERNAL
            if denied is not None:
                return denied
            body = await request.json()
            sid, after = body['session_id'], body.get('after_run_id')
            facts = _continuation(adapter, sid)
            if not facts['exists'] or facts['retired']:
                return refused(409, 'continuation_missing', f'{sid} does not exist or was deleted')
            if facts['pending']:
                return refused(409, 'continuation_busy', f'{sid} has unsettled turns: {facts["pending"]}')
            latest = facts['latest']
            if after is not None and (latest is None or latest['run_id'] != after or latest['status'] != 'terminal'):
                return refused(409, 'continuation_stale', f'the latest turn on {sid} is not {after}')
            # ---- window: another admission can commit here, before the forward below ----
            import aiohttp
            port = adapter._site._server.sockets[0].getsockname()[1]  # INTERNAL
            async with aiohttp.ClientSession() as session:
                async with session.post(f'http://127.0.0.1:{port}/v1/runs',
                                        json={'session_id': sid, 'input': body['input']},
                                        headers={'Authorization': request.headers.get('Authorization', ''),
                                                 'Idempotency-Key': body['idempotency_key']}) as resp:
                    return web.json_response(await resp.json(), status=resp.status)

        app.router.add_get('/v1/gtd/continuation/{session_id}', continuation)
        app.router.add_post('/v1/gtd/send', send)
        logger.info('gtd-continuation routes registered')

    ctx.register_platform_handler('api_server', wire)
