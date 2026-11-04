"""Exercise portable step chat, live events, program logs and restart on real local backends.

python board/step_smoke_test.py --base http://127.0.0.1:8092
Creates only simulated agents, under a dedicated board and temporary flow definition.
Requires idle portable-flow workers: saving/deleting a definition re-registers those workers.
"""
import argparse
import asyncio
import json
import time

import aiohttp

FLOW = '''name: step-view-smoke
description: Disposable step-view integration check
input: {topic: step smoke}
steps:
  prepare:
    kind: program
    run: python -c "from pathlib import Path; Path('context.txt').write_text('preserved context'); print('prepared context')"
  talk:
    kind: agent
    needs: [prepare]
    agent: demo
    review: true
    prompt: Explain the context and wait for review.
  verify:
    kind: program
    needs: [talk]
    run: python -c "from pathlib import Path; print(Path('context.txt').read_text())"
'''


async def main(args):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as client:
        async def call(method, path, body=None):
            async with client.request(method, args.base + path, json=body) as response:
                data = await response.json()
                if response.status >= 400:
                    raise RuntimeError(f'{response.status} {path}: {data}')
                return data

        async def activate(name):
            # Saving is a draft; activation applies it. This disposable fixture accepts the interruption
            # of any running flow steps, as re-registering did before.
            preview = await call('GET', f'/api/flow-documents/{name}/activation')
            return await call('POST', f'/api/flow-documents/{name}/activate', {'revision': preview['revision'], 'impact': preview['impact'], 'confirm': True})

        async def graph(system, run):
            return await call('GET', f'/api/cards/{system}/{run}/dag?task=flow-step-view-smoke' if system.startswith('trigger')
                              else f'/api/cards/{system}/{run}/dag?task=step-view-smoke')

        async def drive(system, run):
            deadline = time.monotonic() + 160
            ref = None
            while time.monotonic() < deadline:
                dag = await graph(system, run)
                failed = next((n for n in dag['nodes'] if n['column'] == 'stopped'), None)
                if failed:
                    raise RuntimeError(f"{system}: step {failed['label']} failed: {failed.get('error')}")
                node = next((n for n in dag['nodes'] if n['step'] == 'talk' and n['ref']), None)
                if node:
                    ref = node['ref']; break
                await asyncio.sleep(1)
            assert ref, f'{system}: agent step never started'
            seen, continued, finished = set(), False, False
            params = {'system': system, 'board': args.board, 'card': run, 'node': ref, 'kind': 'agent'}
            async with client.get(args.base + '/api/live', params=params, timeout=aiohttp.ClientTimeout(total=160)) as response:
                async for line in response.content:
                    if not line.startswith(b'data: '): continue
                    message = json.loads(line[6:])
                    if message['type'] == 'error': raise RuntimeError(message['message'])
                    if message['type'] != 'node-rows': continue
                    for row in message['rows']:
                        if row['type'] not in ('approval', 'question') or row.get('resolved') or row['key'] in seen: continue
                        seen.add(row['key'])
                        if row['type'] == 'approval':
                            option = next(o for o in row['options'] if o['kind'] == 'allow_once')
                            body = {'type': 'approval', 'id': row['id'], 'optionId': option['id']}
                        else:
                            body = {'type': 'answer', 'id': row['id'], 'answers': {'style': 'brief', 'checks': ['tests']}}
                        await call('POST', f'/api/cards/{system}/{run}/nodes/{ref}/input', body)
                    if message.get('state') == 'awaiting_review':
                        if not continued:
                            await call('POST', f'/api/cards/{system}/{run}/nodes/{ref}/input', {'type': 'message', 'text': 'continue with a concise summary'})
                            continued = True
                        elif not finished:
                            await call('POST', f'/api/cards/{system}/{run}/nodes/{ref}/input', {'type': 'done'})
                            finished = True
                    if message.get('state') == 'done': break
            assert continued and finished and len(seen) >= 2, f'{system}: missed chat interactions'
            while time.monotonic() < deadline:
                cards = (await call('GET', f'/api/boards/{system}/{args.board}/cards'))['cards']
                card = next(c for c in cards if c['id'] == run)
                if card['column'] == 'stopped': raise RuntimeError(f'{system}: {card}')
                if card['column'] == 'done':
                    dag = await graph(system, run)
                    verify = next(n for n in dag['nodes'] if n['step'] == 'verify')
                    for _ in range(15):
                        logs = await call('GET', f"/api/cards/{system}/{run}/nodes/{verify['ref']}/logs")
                        if any('preserved context' in line for line in logs['lines']): break
                        await asyncio.sleep(1)  # engine log batches can land after terminal state
                    else: raise AssertionError(logs)
                    assert card['backend_url'] and all(n['backend_url'] for n in dag['nodes'])
                    return ref
                await asyncio.sleep(1)
            raise TimeoutError(f'{system}: flow did not finish')

        if not args.resume_run and not args.reuse_flow:
            await call('PUT', '/api/flows/step-view-smoke', {'yaml': FLOW})
            await activate('step-view-smoke')
        # Workers re-register asynchronously; their startup includes SDK and deployment discovery.
            await asyncio.sleep(15)
        for system in args.systems.split(','):
            run = args.resume_run or (await call('POST', f'/api/boards/{system}/{args.board}/cards', {'kind': 'flow', 'flow': 'step-view-smoke', 'input': {}}))['id']
            print(f'{system}: created {run}', flush=True)
            ref = await drive(system, run)
            if system == 'hatchet':
                dag = await graph(system, run)
                assert next(n for n in dag['nodes'] if n['ref'] == ref)['restart_supported']
                # Native replay propagation is checked separately with quiet steps, so deleting
                # this interactive fixture cannot interrupt an agent waiting for review.
            else:
                result = await call('POST', f'/api/cards/{system}/{run}/nodes/{ref}/restart', {})
                assert result['mode'] == 'runtime' and result['source'] == run, result
                rerun = result['id']
                print(f'{system}: runtime restart {rerun}', flush=True)
                await drive(system, rerun)
                dag = await graph(system, rerun)
                first = next(n for n in dag['nodes'] if n['step'] == 'prepare')
                logs = await call('GET', f"/api/cards/{system}/{rerun}/nodes/{first['ref']}/logs")
                assert any('"reused": true' in line for line in logs['lines']), logs
            print(f'{system}: PASS chat, live rows, logs and restart support', flush=True)
        await call('DELETE', '/api/flows/step-view-smoke')
        await activate('step-view-smoke')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--base', default='http://127.0.0.1:8092')
    parser.add_argument('--board', default='step-smoke')
    parser.add_argument('--systems', default='hatchet,prefect,trigger-dev')
    parser.add_argument('--resume-run', help='Resume a disposable fixture after a failed test (one --systems backend)')
    parser.add_argument('--reuse-flow', action='store_true', help='Use the already-registered disposable definition')
    asyncio.run(main(parser.parse_args()))
