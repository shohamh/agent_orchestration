import asyncio
import base64
import os
import sys
import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flowkit.definition import parse
from flowkit.artifacts import snapshot, restore, ancestor_files, publish
from agents.tests.worker_fixture import make_app
from flowkit.distributed import execute_step
from aiohttp.test_utils import TestServer


def definition():
    return parse({'name': 'worker-test', 'steps': {
        'a': {'kind': 'program', 'run': 'python -c "from pathlib import Path; Path(\'hello.txt\').write_text(\'across PCs\')"'},
        'b': {'kind': 'program', 'needs': ['a'], 'run': 'python -c "from pathlib import Path; print(Path(\'hello.txt\').read_text())"'},
        'c': {'kind': 'program', 'needs': ['a'], 'run': 'echo c'},
        'd': {'kind': 'program', 'needs': ['b', 'c'], 'run': 'echo d'}}})


def files(value):
    return {'hello.txt': {'data': base64.b64encode(value.encode()).decode(), 'mode': 420}}


class ArtifactTests(unittest.TestCase):
    def test_step_tags_keep_default_pool_but_explicit_worker_changes_target(self):
        from flowkit.workers import selection_for
        f=definition(); f.worker={'pool':'builds'}; f.steps['b'].worker={'tags':['gpu']}
        self.assertEqual(selection_for(f,'b',{}),{'pool':'builds','tags':['gpu']})
        self.assertEqual(selection_for(f,'b',{'default':{'pool':'builds'},'steps':{'b':{'id':'review-pc'}}}),{'id':'review-pc'})
    def test_causal_merge_parallel_conflict_and_deletion(self):
        f = definition()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            publish(root, 'a', {}, files('a'))
            self.assertEqual(ancestor_files(f, 'b', root), files('a'))
            publish(root, 'b', files('a'), files('b'))
            publish(root, 'c', files('a'), files('c'))
            with self.assertRaisesRegex(ValueError, 'conflict.*hello.txt'):
                ancestor_files(f, 'd', root)
            publish(root, 'c', files('a'), files('b'))
            self.assertEqual(ancestor_files(f, 'd', root), files('b'))
            publish(root, 'b', files('a'), {})
            publish(root, 'c', files('a'), files('a'))
            self.assertEqual(ancestor_files(f, 'd', root), {})

    def test_rejects_traversal_case_collisions_and_symlinks(self):
        with tempfile.TemporaryDirectory() as tmp:
            for bad in ({'../outside': files('x')['hello.txt']},
                        {'A': files('x')['hello.txt'], 'a': files('x')['hello.txt']},
                        {'a': files('x')['hello.txt'], 'a/b': files('x')['hello.txt']}):
                with self.assertRaises(ValueError): restore(bad, Path(tmp))


class RemoteTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_does_not_confirm_when_cleanup_fails(self):
        from aiohttp import ClientSession
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as tmp:
            app = make_app(worker_id='pc', root=Path(tmp))
            async with TestServer(app) as service, ClientSession() as client:
                async def broken_cleanup(): raise RuntimeError('process cleanup failed')
                task = asyncio.create_task(broken_cleanup())
                await asyncio.sleep(0)
                app['jobs']['job'] = SimpleNamespace(task=task, state='running', error=None, process=SimpleNamespace(returncode=None))
                async with client.delete(service.make_url('/jobs/job')) as response:
                    self.assertNotEqual(response.status, 200)

    async def test_failed_setup_remains_interactive_until_repaired(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); source=root/'missing.txt'
            app=make_app(worker_id='repair',root=root/'jobs')
            async with TestServer(app) as service:
                registry=root/'workers.yaml'; registry.write_text(f'workers:\n- id: repair\n  url: {service.make_url("")}\n')
                f=parse({'name':'repair-setup','environment':{'agent':'demo','files':[{'source':str(source),'target':'context/note.txt'}]},'steps':{'after':{'kind':'program','run':'echo after'}}})
                inbox=asyncio.Queue(); reviews=0; repaired=False
                async def sink(event):
                    nonlocal reviews,repaired
                    if event.get('type')=='status' and event.get('state')=='awaiting_review':
                        reviews+=1; await inbox.put({'type':'done'})
                    if event.get('type')=='status' and event.get('reason','').startswith('Setup validation failed'):
                        self.assertEqual(reviews,1)
                        source.write_text('repaired source')
                        job=next(iter(app['jobs'].values()))
                        target=app['root']/job.id/'workspaces/repair-run/context/note.txt'
                        target.parent.mkdir(parents=True,exist_ok=True); target.write_text('repaired source')
                        repaired=True
                with patch.dict(os.environ,{'FLOW_WORKERS_FILE':str(registry),'FLOW_WORKSPACES':str(root/'canonical')}):
                    output=await asyncio.wait_for(execute_step(f,'setup_environment',{'input':{},'steps':{}},'repair-run',inbox.get,sink),30)
                self.assertTrue(repaired)
                self.assertEqual(reviews,2)
                self.assertTrue(output['validated'])

    async def test_revocation_wins_against_submission_paused_in_inventory(self):
        from aiohttp import ClientSession
        from flowkit.capabilities import probe
        import threading, uuid
        with tempfile.TemporaryDirectory() as tmp:
            app = make_app(worker_id='pc', root=Path(tmp))
            async with TestServer(app) as service, ClientSession() as client:
                waiting, release = threading.Event(), threading.Event()
                def slow_probe(kind, name):
                    if kind == 'command' and name == 'python':
                        waiting.set(); release.wait(10)
                    return probe(kind, name)
                app['probe'] = slow_probe
                jid = str(uuid.uuid4())
                flow = parse({'name': 'late', 'steps': {'run': {'kind': 'program', 'run': 'python -c "print(1)"'}}})
                body = {'id': jid, 'files': {}, 'assets': {}, 'start': {'flow': flow.to_json(), 'step': 'run', 'runId': 'late-run', 'context': {'input': {}, 'steps': {}}}}
                post = asyncio.create_task(client.post(service.make_url('/jobs'), json=body))
                try:
                    self.assertTrue(await asyncio.to_thread(waiting.wait, 10))
                    async with client.delete(service.make_url('/jobs/' + jid)) as response:
                        self.assertEqual(response.status, 200)
                    release.set()
                    response = await asyncio.wait_for(post, 10)
                    async with response: self.assertNotEqual(response.status, 200)
                    self.assertNotIn(jid, app['jobs'])
                finally:
                    release.set()
                    await asyncio.gather(post, return_exceptions=True)

    async def test_revoke_requires_expected_capacity_gate_identity(self):
        from aiohttp import ClientSession
        import uuid
        with tempfile.TemporaryDirectory() as tmp:
            app = make_app(worker_id='pc', root=Path(tmp))
            async with TestServer(app) as service, ClientSession() as client:
                jid = str(uuid.uuid4())
                async with client.delete(service.make_url('/jobs/' + jid), headers={'X-Flow-Gate-ID': 'wrong-gate'}) as response:
                    self.assertNotEqual(response.status, 200)
                self.assertFalse((app['root']/'.terminated'/(jid + '.json')).exists())

    async def test_revoke_absent_job_prevents_late_submission(self):
        from aiohttp import ClientSession
        import uuid
        with tempfile.TemporaryDirectory() as tmp:
            app = make_app(worker_id='pc', root=Path(tmp))
            async with TestServer(app) as service, ClientSession() as client:
                jid = str(uuid.uuid4())
                async with client.delete(service.make_url('/jobs/' + jid)) as response:
                    self.assertEqual(response.status, 200)
                    self.assertTrue((await response.json())['terminated'])
                flow = parse({'name': 'late', 'steps': {'run': {'kind': 'program', 'run': 'python -c "print(1)"'}}})
                async with client.post(service.make_url('/jobs'), json={'id': jid, 'files': {}, 'assets': {}, 'start': {'flow': flow.to_json(), 'step': 'run', 'runId': 'late-run', 'context': {'input': {}, 'steps': {}}}}) as response:
                    self.assertNotEqual(response.status, 200)
                self.assertNotIn(jid, app['jobs'])

    async def test_immediately_cancelled_job_retains_termination_proof(self):
        from aiohttp import ClientSession
        from flowkit.worker_service import Job, termination_path
        import uuid
        with tempfile.TemporaryDirectory() as tmp:
            app = make_app(worker_id='pc', root=Path(tmp))
            async with TestServer(app) as service, ClientSession() as client:
                jid = str(uuid.uuid4())
                job = Job(app, jid, {})
                app['jobs'][jid] = job
                job.task.cancel()  # before Job.run gets a chance to execute
                await asyncio.gather(job.task, return_exceptions=True)
                async with client.delete(service.make_url('/jobs/' + jid)) as response:
                    self.assertEqual(response.status, 200)
                self.assertTrue(termination_path(app, jid).exists())
                del app['jobs'][jid]
                async with client.delete(service.make_url('/jobs/' + jid)) as response:
                    self.assertEqual(response.status, 200)

    async def test_reaped_completed_job_retains_termination_proof(self):
        from aiohttp import ClientSession
        import uuid
        with tempfile.TemporaryDirectory() as tmp:
            app = make_app(worker_id='pc', root=Path(tmp))
            async with TestServer(app) as service, ClientSession() as client:
                jid = str(uuid.uuid4())
                flow = parse({'name': 'complete', 'steps': {'run': {'kind': 'program', 'run': 'python -c "print(1)"'}}})
                await client.post(service.make_url('/jobs'), json={'id': jid, 'files': {}, 'assets': {}, 'start': {'flow': flow.to_json(), 'step': 'run', 'runId': 'finished-run', 'context': {'input': {}, 'steps': {}}}})
                await asyncio.wait_for(app['jobs'][jid].task, 30)
                del app['jobs'][jid]
                async with client.delete(service.make_url('/jobs/' + jid)) as response:
                    self.assertEqual(response.status, 200)
                    self.assertTrue((await response.json())['terminated'])

    async def test_cleanup_failure_never_reports_done(self):
        from aiohttp import ClientSession
        import uuid
        with tempfile.TemporaryDirectory() as tmp:
            app = make_app(worker_id='pc', root=Path(tmp))
            async with TestServer(app) as service, ClientSession() as client:
                jid = str(uuid.uuid4())
                flow = parse({'name': 'cleanup', 'steps': {'run': {'kind': 'program', 'run': 'python -c "print(1)"'}}})
                with patch.object(app['capacity'], 'finish', side_effect=RuntimeError('release failed')):
                    await client.post(service.make_url('/jobs'), json={'id': jid, 'files': {}, 'assets': {}, 'start': {'flow': flow.to_json(), 'step': 'run', 'runId': 'cleanup-run', 'context': {'input': {}, 'steps': {}}}})
                    await asyncio.gather(app['jobs'][jid].task, return_exceptions=True)
                    self.assertNotEqual(app['jobs'][jid].state, 'done')
                    async with client.delete(service.make_url('/jobs/' + jid)) as response:
                        self.assertNotEqual(response.status, 200)

    async def test_worker_identity_survives_verbose_event_history(self):
        from aiohttp import ClientSession
        import uuid
        with tempfile.TemporaryDirectory() as tmp:
            app=make_app(worker_id='verbose',root=Path(tmp))
            async with TestServer(app) as service, ClientSession() as client:
                jid=str(uuid.uuid4())
                flow=parse({'name':'verbose-flow','steps':{'log':{'kind':'program','run':'python -c "for i in range(6000): print(i)"'}}})
                response=await client.post(service.make_url('/jobs'),json={'id':jid,'files':{},'assets':{},'start':{'flow':flow.to_json(),'step':'log','runId':'verbose-run','context':{'input':{},'steps':{}}}})
                self.assertEqual(response.status,200)
                await asyncio.wait_for(app['jobs'][jid].task,30)
                data=await (await client.get(service.make_url('/jobs/'+jid))).json()
                self.assertEqual(data['state'],'done',data.get('error'))
                self.assertEqual(data['worker']['id'],'verbose')
                self.assertGreater(data['cursor'],5000)
    @unittest.skipIf(os.name=='nt','POSIX process-group regression')
    async def test_cancel_kills_command_grandchild_on_posix(self):
        from aiohttp import ClientSession
        import uuid
        script="import subprocess,sys,time\nfrom pathlib import Path\np=subprocess.Popen([sys.executable,'-c',\"import time; from pathlib import Path; time.sleep(2); Path('leaked.txt').write_text('orphan')\"])\nprint('spawned',flush=True)\ntime.sleep(30)\n"
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); jid=str(uuid.uuid4())
            async with TestServer(make_app(root=root)) as service, ClientSession() as client:
                f=parse({'name':'cancel-tree','steps':{'wait':{'kind':'program','run':'python {flow_dir}/spawn.py'}}})
                await client.post(service.make_url('/jobs'),json={'id':jid,'files':{},'assets':{'spawn.py':{'data':base64.b64encode(script.encode()).decode(),'mode':420}},'start':{'flow':f.to_json(),'step':'wait','runId':'posix-run','context':{'input':{},'steps':{}}}})
                for _ in range(80):
                    data=await (await client.get(service.make_url('/jobs/'+jid))).json()
                    if any(e.get('text')=='spawned' for e in data['events']): break
                    await asyncio.sleep(.1)
                else: self.fail(data)
                await client.delete(service.make_url('/jobs/'+jid))
                await asyncio.sleep(2.5)
                self.assertFalse((root/jid/'workspaces/posix-run/leaked.txt').exists())

    async def test_setup_git_runs_inside_interactive_stdio_worker(self):
        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); repo=root/'source'; repo.mkdir(); (repo/'seed').write_text('seed')
            for args in (['init'],['add','.'],['-c','user.name=Test','-c','user.email=test@example.invalid','commit','-m','seed']):
                subprocess.run(['git','-C',str(repo),*args],check=True,capture_output=True)
            async with TestServer(make_app(worker_id='setup',root=root/'jobs')) as service:
                registry=root/'workers.yaml'; registry.write_text(f'workers:\n- id: setup\n  url: {service.make_url("")}\n')
                flow=parse({'name':'setup-worker-test','environment':{'agent':'demo','repositories':[{'url':str(repo),'target':'repo'}]},'steps':{'after':{'kind':'program','run':'echo after'}}})
                messages=asyncio.Queue()
                async def sink(event):
                    if event.get('type')=='status' and event.get('state')=='awaiting_review': await messages.put({'type':'done'})
                with patch.dict(os.environ,{'FLOW_WORKERS_FILE':str(registry),'FLOW_WORKSPACES':str(root/'canonical')}):
                    result=await asyncio.wait_for(execute_step(flow,'setup_environment',{'input':{},'steps':{}},'setup-run',messages.get,sink),20)
                    self.assertTrue(result['validated'])

    async def test_authentication_and_queued_job_cancellation(self):
        from aiohttp import ClientSession
        with tempfile.TemporaryDirectory() as tmp:
            app=make_app(worker_id='protected',root=Path(tmp),slots=1,token='test-token')
            async with TestServer(app) as server, ClientSession() as client:
                self.assertEqual((await client.get(server.make_url('/health'))).status,401)
                headers={'Authorization':'Bearer test-token'}
                self.assertEqual((await client.get(server.make_url('/health'),headers=headers)).status,200)
                f=parse({'name':'slow-flow','steps':{'wait':{'kind':'program','run':'python -c "import time; time.sleep(30)"'}}})
                import uuid
                ids=[str(uuid.uuid4()),str(uuid.uuid4())]
                try:
                    for jid in ids:
                        response=await client.post(server.make_url('/jobs'),headers=headers,json={'id':jid,'files':{},'assets':{},'start':{'flow':f.to_json(),'step':'wait','runId':'queued-test','context':{'input':{},'steps':{}}}})
                        self.assertEqual(response.status,200,await response.text())
                    data=await (await client.get(server.make_url('/jobs/'+ids[1]),headers=headers)).json()
                    self.assertEqual(data['state'],'queued')
                    await client.delete(server.make_url('/jobs/'+ids[1]),headers=headers)
                    data=await (await client.get(server.make_url('/jobs/'+ids[1]),headers=headers)).json()
                    self.assertEqual(data['state'],'cancelled')
                finally:
                    for jid in ids: await client.delete(server.make_url('/jobs/'+jid),headers=headers)

    async def test_two_services_transfer_files_and_identify_real_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            services = [TestServer(make_app(worker_id=name, root=root/name, slots=1, pool='test', tags=['linux'])) for name in ('pc-a', 'pc-b')]
            for service in services: await service.start_server()
            registry = root/'workers.yaml'
            registry.write_text('workers:\n' + ''.join(f'  - id: pc-{letter}\n    url: {service.make_url("")}\n    pool: test\n    tags: [linux]\n' for letter, service in zip(('a', 'b'), services)))
            events = []
            assets=root/'assets'; assets.mkdir()
            helper=assets/'helper.py'
            helper.write_text("from pathlib import Path; print(Path('hello.txt').read_text()); print('original helper')")
            async def sink(event): events.append(event)
            async def receive(): await asyncio.Event().wait()
            try:
                with patch.dict(os.environ, {'FLOW_WORKERS_FILE': str(registry), 'FLOW_WORKSPACES': str(root/'canonical')}), patch('flowkit.distributed.FLOWS_DIR',assets):
                    f = definition()
                    f.steps['b'].run='python {flow_dir}/helper.py'
                    await execute_step(f, 'a', {'input': {}, 'steps': {}, '_flow_execution': {'steps': {'a': {'id': 'pc-a'}}}}, 'test-run', receive, sink)
                    helper.write_text("print('changed helper')")
                    result = await execute_step(f, 'b', {'input': {}, 'steps': {}, '_flow_execution': {'steps': {'b': {'id': 'pc-b'}}}}, 'test-run', receive, sink)
                    self.assertIn('across PCs', result['stdout'])
                    self.assertIn('original helper',result['stdout'])
                    self.assertEqual([e['worker']['id'] for e in events if e.get('type') == 'worker'], ['pc-a', 'pc-b'])
                    f.steps['b'].run='echo changed definition'
                    with self.assertRaisesRegex(ValueError,'definition.*changed'):
                        await execute_step(f,'b',{'input':{},'steps':{},'_flow_execution':{}},'test-run',receive,sink)
            finally:
                for service in services: await service.close()


if __name__ == '__main__': unittest.main()
