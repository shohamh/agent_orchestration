"""An ACP agent that runs a terminal through its client, streams tool updates and honours cancel. No model credits."""
import json
import sys
import threading

SESSION = 'terminal-session'
replies = {}
ready = threading.Condition()
cancelled = threading.Event()


def send(message):
    print(json.dumps({'jsonrpc': '2.0', **message}), flush=True)


def update(**fields):
    send({'method': 'session/update', 'params': {'sessionId': SESSION, 'update': fields}})


def ask(number, method, params):
    send({'id': number, 'method': method, 'params': {'sessionId': SESSION, **params}})
    with ready:
        ready.wait_for(lambda: number in replies)
        return replies.pop(number)


def run_prompt(request):
    text = request['params']['prompt'][0]['text']
    if text == 'wait':
        cancelled.wait()
        send({'id': request['id'], 'result': {'stopReason': 'cancelled'}})
        return
    if text == 'crash':
        sys.stderr.write('the adapter broke\n')
        sys.stderr.flush()
        raise SystemExit(3)
    made = ask(100, 'terminal/create', {'command': sys.executable, 'args': ['-c', "print('from the terminal')"]})
    terminal = made['result']['terminalId']
    waited = ask(101, 'terminal/wait_for_exit', {'terminalId': terminal})
    assert waited['result']['exitCode'] == 0, waited
    output = ask(102, 'terminal/output', {'terminalId': terminal})
    assert 'from the terminal' in output['result']['output'], output
    assert ask(103, 'terminal/release', {'terminalId': terminal})['result'] == {}
    update(sessionUpdate='tool_call', toolCallId='t1', title='Edit a file', kind='edit', status='in_progress', rawInput={'path': 'x'})
    update(sessionUpdate='tool_call_update', toolCallId='t1', status='completed', rawOutput='wrote x')
    update(sessionUpdate='plan', entries=[{'content': 'one step', 'priority': 'high', 'status': 'completed'}])
    update(sessionUpdate='usage_update', used=5, size=10)
    update(sessionUpdate='agent_message_chunk', content={'type': 'text', 'text': 'Part one, '})
    update(sessionUpdate='agent_message_chunk', content={'type': 'text', 'text': 'part two.'})
    send({'id': request['id'], 'result': {'stopReason': 'end_turn'}})


for line in sys.stdin:
    message = json.loads(line)
    if 'method' not in message:
        with ready:
            replies[message['id']] = message
            ready.notify_all()
        continue
    method = message['method']
    if method == 'initialize':
        send({'id': message['id'], 'result': {'protocolVersion': 1, 'agentCapabilities': {'loadSession': False, 'promptCapabilities': {'image': True}}}})
    elif method == 'session/new':
        send({'id': message['id'], 'result': {'sessionId': SESSION, 'modes': {'currentModeId': 'a', 'availableModes': [{'id': 'a', 'name': 'A'}, {'id': 'b', 'name': 'B'}]},
                                              'configOptions': [{'id': 'model', 'name': 'Model', 'type': 'select', 'currentValue': 'x', 'options': []}]}})
    elif method == 'session/set_mode':
        update(sessionUpdate='current_mode_update', currentModeId=message['params']['modeId'])
        send({'id': message['id'], 'result': {}})
    elif method == 'session/set_config_option':
        send({'id': message['id'], 'result': {'configOptions': [{'id': 'model', 'name': 'Model', 'type': 'select', 'currentValue': message['params']['value'], 'options': []}]}})
    elif method == 'session/cancel':
        cancelled.set()
    elif method == 'session/prompt':
        threading.Thread(target=run_prompt, args=(message,), daemon=True).start()
