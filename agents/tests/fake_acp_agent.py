"""A real JSON-RPC subprocess used to test both sides of ACP, without model credits."""
import json
import sys


def send(message):
    print(json.dumps({'jsonrpc': '2.0', **message}), flush=True)


def ask(request_id, method, params):
    send({'id': request_id, 'method': method, 'params': params})
    response = json.loads(sys.stdin.readline())
    return response['result']


for line in sys.stdin:
    req = json.loads(line)
    method = req.get('method')
    if method == 'initialize':
        assert req['params']['clientCapabilities']['elicitation']['form'] == {}
        result = {'protocolVersion': 1, 'agentCapabilities': {'loadSession': True}}
    elif method == 'session/new':
        send({'method': 'session/update', 'params': {'sessionId': 'test-session',
            'update': {'sessionUpdate': 'available_commands_update', 'availableCommands': [
                {'name': 'test', 'description': 'Run tests', 'input': {'hint': 'path'}}]}}})
        result = {'sessionId': 'test-session'}
    elif method == 'session/load':
        assert req['params']['sessionId'] == 'test-session'
        send({'method': 'session/update', 'params': {'sessionId': 'test-session',
            'update': {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'Past response'}}}})
        result = {}
    elif method == 'session/prompt':
        permission = ask('permission', 'session/request_permission', {
            'sessionId': 'test-session', 'toolCall': {'toolCallId': 'tool-1', 'title': 'Run tests', 'kind': 'execute'},
            'options': [{'optionId': 'once', 'name': 'Allow once', 'kind': 'allow_once'},
                        {'optionId': 'deny', 'name': 'Deny', 'kind': 'reject_once'}]})
        assert permission['outcome'] == {'outcome': 'selected', 'optionId': 'once'}
        answer = ask('question', 'elicitation/create', {
            'sessionId': 'test-session', 'mode': 'form', 'message': 'Which style?',
            'requestedSchema': {'type': 'object', 'properties': {'style': {'type': 'string', 'enum': ['short', 'long']}}, 'required': ['style']}})
        assert answer == {'action': 'accept', 'content': {'style': 'short'}}
        send({'method': 'session/update', 'params': {'sessionId': 'test-session',
            'update': {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'It works.'}}}})
        result = {'stopReason': 'end_turn'}
    else:
        continue
    send({'id': req['id'], 'result': result})
