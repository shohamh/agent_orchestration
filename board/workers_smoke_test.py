"""Real-engine, two-service setup/transfer/restart smoke test; disposable flows and runs only.

Requires idle portable coordinators. The two execution services run on this PC, not two PCs.
python board/workers_smoke_test.py --base http://127.0.0.1:8092
"""
import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import aiohttp
import yaml
from aiohttp import web
from aiohttp.test_utils import TestServer

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'agents'))
from flowkit.worker_service import make_app
from flowkit.restart import workspace_path
sys.path.insert(0,str(ROOT/'agents/tests'))
from confluence_fixture import confluence_app


async def main(args):
    with tempfile.TemporaryDirectory(prefix='workers-native-') as tmp:
        root=Path(tmp)
        repo=root/'repo'; repo.mkdir(); (repo/'seed.txt').write_text('repository context')
        for command in (['init'],['add','.'],['-c','user.name=Smoke','-c','user.email=smoke@example.invalid','commit','-m','seed']):
            subprocess.run(['git','-C',str(repo),*command],check=True,capture_output=True)
        note=root/'notes.txt'; note.write_text('file context')
        wiki_app=confluence_app()
        services=[TestServer(make_app(worker_id=name,root=root/name,pool='smoke',tags=['test'],slots=2)) for name in ('smoke-a','smoke-b')]
        services.append(TestServer(wiki_app))
        for service in services: await service.start_server()
        registry=ROOT/'workers.local.yaml'
        original=registry.read_bytes() if registry.exists() else None
        previous=yaml.safe_load(original) if original else {'workers':[{'id':'local','url':'http://127.0.0.1:8096','pool':'default','tags':[],'token_env':'FLOW_WORKER_TOKEN'}]}
        previous['workers'] += [{'id':name,'url':str(service.make_url('')).rstrip('/'),'pool':'smoke','tags':['test']} for name,service in zip(('smoke-a','smoke-b'),services)]
        registry.write_text(yaml.safe_dump(previous),encoding='utf-8')
        wiki=({'base_url':'https://shibboleth.atlassian.net/wiki','page_id':'2067399657','target':'context/wiki'}
              if args.public_confluence else
              {'base_url':str(services[2].make_url('')).rstrip('/'),'page_id':'1','target':'context/wiki'})
        wiki_check=("assert 'SystemRequirements' in Path('context/wiki/2067399657.md').read_text(encoding='utf-8')"
                    if args.public_confluence else
                    "assert '**content 2**' in Path('context/wiki/2.md').read_text(); assert Path('context/wiki/attachments/1/att10.png').is_file()")
        verify_command=("python -c \"from pathlib import Path; assert Path('repos/demo/seed.txt').read_text()=='repository context'; "
                        "assert Path('context/notes.txt').read_text()=='file context'; "+wiki_check+"; "
                        "Path('result.txt').write_text('cross-worker verified'); print('all context arrived')\"")
        flow={'name':'workers-context-smoke','description':'Disposable native worker/context verification',
              'environment':{'agent':'demo','worker':{'id':'smoke-a'},
                  'repositories':[{'url':str(repo),'target':'repos/demo'}],
                  'files':[{'source':str(note),'target':'context/notes.txt'}],
                  'confluence':[wiki]},
              'steps':{
                  'verify':{'kind':'program','worker':{'id':'smoke-b'},'run':verify_command},
                  'finish':{'kind':'program','needs':['verify'],'worker':{'id':'smoke-a'},'run':'''python -c "from pathlib import Path; print(Path('result.txt').read_text())"'''}}}
        runs=[]
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as client:
            async def call(method,path,body=None):
                async with client.request(method,args.base+path,json=body) as response:
                    data=await response.json()
                    if response.status>=400: raise RuntimeError(f'{response.status} {path}: {data}')
                    return data
            async def graph(system,run):
                task=('flow-' if system=='trigger-dev' else '')+flow['name']
                return await call('GET',f'/api/cards/{system}/{run}/dag?task={task}')
            async def wait_done(system,run,review=False):
                deadline=time.monotonic()+220
                marked=False
                while time.monotonic()<deadline:
                    dag=await graph(system,run)
                    failed=[n for n in dag['nodes'] if n['column']=='stopped']
                    if failed: raise RuntimeError(f'{system} failed: {failed}')
                    if review and not marked:
                        setup=next(n for n in dag['nodes'] if n['step']=='setup_environment')
                        if setup['ref']:
                            logs=await call('GET',f'/api/cards/{system}/{run}/nodes/{setup["ref"]}/logs')
                            events=[json.loads(line.split('EVT ',1)[1]) for line in logs['lines'] if 'EVT ' in line]
                            statuses=[e for e in events if e.get('type')=='status']
                            if statuses and statuses[-1]['state']=='awaiting_review':
                                # Confirm native dependencies are blocked on the setup agent's review.
                                assert next(n for n in dag['nodes'] if n['step']=='verify')['column'] in ('pending','queued','scheduled')
                                await call('POST',f'/api/cards/{system}/{run}/nodes/{setup["ref"]}/input',{'type':'done'})
                                marked=True
                    if dag['nodes'] and all(n['column']=='done' for n in dag['nodes']):
                        if review: assert marked
                        # Native state can become visible just before the runtime metadata refresh.
                        return dag
                    await asyncio.sleep(1)
                raise TimeoutError(f'{system} timed out: {dag}')
            try:
                await call('PUT','/api/flows/'+flow['name'],{'yaml':yaml.safe_dump(flow,sort_keys=False)})
                await asyncio.sleep(25)  # native registration / Trigger rebuild
                for system in args.systems:
                    run=(await call('POST',f'/api/boards/{system}/workers-context-smoke/cards',{'kind':'flow','flow':flow['name'],'execution':{'default':{'pool':'smoke'}}}))['id']
                    runs.append((system,run))
                    dag=await wait_done(system,run,review=True)
                    expected={'setup_environment':'smoke-a','verify':'smoke-b','finish':'smoke-a'}
                    assert {n['step']:n['execution']['worker']['id'] for n in dag['nodes']}==expected,dag
                    verify=next(n for n in dag['nodes'] if n['step']=='verify')
                    # Trigger's parent finishes after its last child; do not race the engine's
                    # terminal-state preflight merely because all task nodes are complete.
                    for _ in range(40):
                        cards=(await call('GET',f'/api/boards/{system}/workers-context-smoke/cards'))['cards']
                        if next(c for c in cards if c['id']==run)['column']=='done': break
                        await asyncio.sleep(1)
                    else: raise TimeoutError(f'{system} parent did not finish')
                    # Change the selected step's actual execution PC, keeping descendants on their selection.
                    restarted=await call('POST',f'/api/cards/{system}/{run}/nodes/{verify["ref"]}/restart',{'worker':{'id':'smoke-a'}})
                    replay_id=restarted['id']
                    if replay_id!=run: runs.append((system,replay_id))
                    await asyncio.sleep(2)
                    replay=await wait_done(system,replay_id)
                    assert next(n for n in replay['nodes'] if n['step']=='verify')['execution']['worker']['id']=='smoke-a',replay
                    print(json.dumps({'system':system,'run':run,'restart':replay_id,'workers':expected,'context':'git + file + Confluence','restart_mode':restarted.get('mode','native') }),flush=True)
            finally:
                # Stop only disposable runs created by this script before removing their definition.
                for system,run in runs:
                    try:
                        cards=(await call('GET',f'/api/boards/{system}/workers-context-smoke/cards'))['cards']
                        active=next((c for c in cards if c['id']==run and c['column'] not in ('done','stopped')),None)
                        if active: await call('POST',f'/api/cards/{system}/{run}/cancel',{})
                    except Exception as exc: print(f'cleanup {system}: {type(exc).__name__}',file=sys.stderr)
                try:
                    await call('DELETE','/api/flows/'+flow['name'])
                finally:
                    if original is None: registry.unlink(missing_ok=True)
                    else: registry.write_bytes(original)
                    for service in services: await service.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--base',default='http://127.0.0.1:8092')
    parser.add_argument('--systems',nargs='+',default=['hatchet','prefect','trigger-dev'])
    parser.add_argument('--public-confluence',action='store_true',help='Use the public Shibboleth Cloud page instead of a local Confluence fixture')
    asyncio.run(main(parser.parse_args()))
