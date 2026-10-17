"""GTD continuation prerequisite (jra3/gtdd#108).

Characterize the deployed API using real HTTP and a disposable SessionDB.
Only agent execution is replaced: no provider, tool, or personal state is used.
These assertions document the gap, not the desired strict continuation contract.
"""
import asyncio
import sqlite3
from unittest.mock import MagicMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['missing', 'read_error'])
async def test_stock_continuation_executes_without_baseline(failure, monkeypatch):
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={'key': 'test-only'}))
    db = await adapter._ensure_session_db_async()
    sid = 'gtd-disposable-conversation'
    if failure == 'read_error':
        db.create_session(sid, source='api_server')
        db.append_message(sid, 'user', 'Original Action context')
        db.append_message(sid, 'assistant', 'I have the context')
        def unreadable(*args, **kwargs):
            raise sqlite3.OperationalError('injected history read failure')
        monkeypatch.setattr(db, 'get_messages_as_conversation', unreadable)
    agent = MagicMock()
    agent.run_conversation.return_value = {'final_response': 'recorded', 'completed': True}
    agent.session_prompt_tokens = agent.session_completion_tokens = agent.session_total_tokens = 0
    monkeypatch.setattr(adapter, '_create_agent', lambda **kwargs: agent)
    app = web.Application()
    app.router.add_post('/v1/runs', adapter._handle_runs)
    async with TestClient(TestServer(app)) as client:
        response = await client.post('/v1/runs', json={
            'session_id': sid, 'input': 'Only the delta: title changed',
        }, headers={'Authorization': 'Bearer test-only', 'Idempotency-Key': 'turn-2'})
        assert response.status == 202
        run_id = (await response.json())['run_id']
        await asyncio.wait_for(adapter._active_run_tasks[run_id], timeout=10)
    agent.run_conversation.assert_called_once()
    assert agent.run_conversation.call_args.kwargs['conversation_history'] == []
    assert adapter._run_statuses[run_id]['status'] == 'completed'
