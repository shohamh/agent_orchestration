import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import os
import time
from unittest.mock import patch
from agents.flowkit.capacity import CapacityGate

class CapacityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 100.
        self.path = Path(self.tmp.name)/'gate.db'
        self.g = self.gate()
    def gate(self, **kw):
        return CapacityGate(self.path, clock=lambda:self.now, slots=1, **kw)
    def sample(self, cpu=10, ram=20):
        for i in range(11): self.g.observe(cpu, ram, self.now-10+i)
    def test_unknown_threshold_stale(self):
        self.assertEqual(self.g.try_acquire('a')['reason'], 'missing_metrics')
        self.g.configure(slots=1,cpu_limit=60, ram_limit=70)
        self.sample(60, 20)
        self.assertEqual(self.g.try_acquire('a')['reason'], 'high_cpu')
        self.sample(10,70)
        self.assertEqual(self.g.try_acquire('a')['reason'], 'high_ram')
        self.now += 4
        self.assertEqual(self.g.try_acquire('a')['reason'], 'missing_metrics')
    def test_shared_slots_fencing_cooldown(self):
        self.sample()
        other = self.gate()
        lease = self.g.try_acquire('a')
        self.assertTrue(lease['acquired'])
        self.assertFalse(other.try_acquire('b')['acquired'])
        self.g.confirm_started('a',lease['fence'],os.getpid())
        self.g.release('a')
        self.assertEqual(other.try_acquire('b')['reason'], 'cooldown')
        self.now += 11
        self.sample()
        self.assertTrue(other.try_acquire('b')['acquired'])
        self.assertFalse(self.g.valid('a', lease['fence']))
    def test_expiry_quarantines_slot(self):
        self.sample()
        lease = self.g.try_acquire('a')
        self.now += 31
        self.sample()
        self.assertFalse(self.g.valid('a',lease['fence']))
        self.assertEqual(self.g.try_acquire('b')['reason'],'full_slots')
    def test_gaps_and_recovery(self):
        self.sample()
        self.now += 5
        self.g.observe(10,20,self.now)
        self.assertEqual(self.g.metrics()['state'],'unknown')
        self.now += 10
        self.sample()
        self.assertEqual(self.g.metrics()['state'],'known')
    def test_weighting(self):
        for i in range(11): self.g.observe(0 if i<5 else 100,20,90+i)
        self.assertEqual(self.g.metrics()['cpu_percent'],60)

def _compete(path, start, results, finished):
    import time
    gate = CapacityGate(path, slots=1)
    start.wait()
    results.put(gate.try_acquire(str(__import__('os').getpid()))['acquired'])
    finished.wait(15)

class ConcurrencyTests(unittest.TestCase):
    def test_distinct_processes_one_slot(self):
        import multiprocessing
        import time
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'capacity.db'
            gate = CapacityGate(path, slots=1)
            now=time.monotonic()
            for i in range(11): gate.observe(5,5,now-10+i)
            ctx=multiprocessing.get_context('spawn')
            start=ctx.Event(); results=ctx.Queue(); finished=ctx.Event()
            processes=[ctx.Process(target=_compete,args=(path,start,results,finished)) for _ in range(2)]
            for process in processes: process.start()
            start.set()
            values=[results.get(timeout=15) for _ in processes]
            finished.set()
            for process in processes:
                process.join(15)
                self.assertEqual(process.exitcode,0)
            self.assertEqual(sum(values),1)

    def test_stale_publication_and_owner_release(self):
        with tempfile.TemporaryDirectory() as directory:
            now=[100.]
            gate=CapacityGate(Path(directory)/'gate.db',clock=lambda:now[0])
            for i in range(11): gate.observe(1,1,90+i)
            lease=gate.try_acquire('job')
            other=CapacityGate(gate.path,clock=lambda:now[0])
            other.release('job')
            self.assertTrue(gate.valid('job',lease['fence']))
            now[0]=131
            writes=[]
            with self.assertRaisesRegex(RuntimeError,'fenced'):
                gate.publish('job',lease['fence'],lambda:writes.append(True))
            self.assertEqual(writes,[])
            self.assertFalse(gate.renew('job',lease['fence']))

class CancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_queued_cancellation_never_starts(self):
        import asyncio
        from agents.flowkit.worker_service import Job
        with tempfile.TemporaryDirectory() as directory:
            gate=CapacityGate(Path(directory)/'gate.db')
            job=Job({'slots':asyncio.Semaphore(1),'capacity':gate,'root':Path(directory)},'queued',{'files':{},'assets':{},'runId':'test'})
            await asyncio.sleep(.02)
            job.task.cancel()
            await asyncio.gather(job.task,return_exceptions=True)
            self.assertEqual(job.state,'cancelled')
            self.assertIsNone(job.process)
            with gate.transaction() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM leases').fetchone()[0],0)
    def test_running_lease_survives_high_usage_and_settings_change(self):
        with tempfile.TemporaryDirectory() as directory:
            gate=CapacityGate(Path(directory)/'gate.db',clock=lambda:100)
            for i in range(11): gate.observe(89,89,90+i)
            lease=gate.try_acquire('running')
            self.assertTrue(lease['acquired'])
            gate.configure(slots=1,cpu_limit=60,ram_limit=70)
            for i in range(11): gate.observe(100,100,90+i)
            self.assertTrue(gate.valid('running',lease['fence']))

    def test_machine_sampler_returns_physical_percentages(self):
        import time
        from agents.flowkit.capacity import MachineSampler
        sampler=MachineSampler()
        self.assertIsNone(sampler.sample())
        time.sleep(.05)
        cpu,ram=sampler.sample()
        self.assertTrue(0<=cpu<=100)
        self.assertTrue(0<=ram<=100)


class RecoveryAndWindowTests(unittest.TestCase):
    def test_delayed_start_blocks_then_starts_fresh_cooldown(self):
        with tempfile.TemporaryDirectory() as directory:
            now=[100.]
            gate=CapacityGate(Path(directory)/'gate.db',slots=2,clock=lambda:now[0])
            for i in range(11): gate.observe(1,1,90+i)
            lease=gate.try_acquire('slow-launch')
            now[0]=120.
            for i in range(11): gate.observe(1,1,110+i)
            self.assertEqual(gate.try_acquire('second')['reason'],'pending_start')
            gate.confirm_started('slow-launch',lease['fence'],os.getpid())
            self.assertEqual(gate.try_acquire('second')['reason'],'cooldown')
            now[0]=129.
            for i in range(10): gate.observe(1,1,120+i)
            self.assertEqual(gate.try_acquire('second')['reason'],'cooldown')
            now[0]=130.; gate.observe(1,1,130.)
            self.assertTrue(gate.try_acquire('second')['acquired'])

    def test_preceding_interval_nonuniform_weights_equal_threshold(self):
        with tempfile.TemporaryDirectory() as directory:
            gate=CapacityGate(Path(directory)/'gate.db',cpu_limit=60,clock=lambda:100.)
            # Each CPU value describes the interval ENDING at its timestamp.
            for stamp,cpu in [(90,99),(91,0),(93,0),(94,0),(97,100),(100,100)]:
                gate.observe(cpu,20,stamp)
            self.assertEqual(gate.metrics()['cpu_percent'],60.)
            self.assertEqual(gate.try_acquire('job')['reason'],'high_cpu')

    def test_reboot_reclaims_old_owner_and_invalidates_window(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'gate.db'
            old=CapacityGate(path,boot='boot-a',clock=lambda:100.)
            for i in range(11): old.observe(1,1,90+i)
            lease=old.try_acquire('old')
            new=CapacityGate(path,boot='boot-b',clock=lambda:5.)
            self.assertFalse(old.valid('old',lease['fence']))
            with new.transaction() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM leases').fetchone()[0],0)
            self.assertEqual(new.metrics()['state'],'unknown')

    def test_dead_pending_owner_reclaimed_but_unknown_owner_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            gate=CapacityGate(Path(directory)/'gate.db',slots=1,clock=lambda:100.)
            for i in range(11): gate.observe(1,1,90+i)
            gate.try_acquire('dead')
            with patch('agents.flowkit.capacity.process_identity',side_effect=PermissionError('unknown')):
                self.assertEqual(gate.try_acquire('next')['reason'],'full_slots')
            with patch('agents.flowkit.capacity.process_identity',return_value=None):
                self.assertTrue(gate.try_acquire('next')['acquired'])

class AsyncSupervisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_sqlite_contention_keeps_loop_live_and_retries(self):
        import asyncio
        from agents.flowkit.worker_service import capacity_call
        with tempfile.TemporaryDirectory() as directory:
            gate=CapacityGate(Path(directory)/'gate.db')
            with gate.transaction() as db:
                pending=asyncio.create_task(capacity_call(gate.metrics))
                ticks=0
                for _ in range(12):
                    await asyncio.sleep(.03); ticks+=1
                self.assertFalse(pending.done())
                self.assertEqual(ticks,12)
            self.assertEqual((await asyncio.wait_for(pending,2))['state'],'unknown')
            with gate.transaction():
                with self.assertRaisesRegex(RuntimeError,'lease lost'):
                    await capacity_call(gate.metrics,deadline=time.monotonic()+.05)

    async def test_repeated_cancellation_waits_for_child_kill_and_release(self):
        import asyncio
        from agents.flowkit.worker_service import Job
        class Stream:
            async def readline(self): await asyncio.Event().wait()
            def write(self,value): pass
            async def drain(self): pass
        class Process:
            pid=os.getpid()
            stdin=Stream(); stdout=Stream(); stderr=Stream()
            def __init__(self): self.dead=asyncio.Event(); self.returncode=None
            async def wait(self): await self.dead.wait(); return 0
        process=Process(); killed=asyncio.Event()
        def kill(*args):
            killed.set()
            asyncio.get_running_loop().call_later(.2,process.dead.set)
        with tempfile.TemporaryDirectory() as directory:
            gate=CapacityGate(Path(directory)/'gate.db')
            now=time.monotonic()
            for i in range(11): gate.observe(1,1,now-10+i)
            async def launch(*args,**kwargs): return process
            with patch('agents.flowkit.worker_service.asyncio.create_subprocess_exec',side_effect=launch), patch('agents.flowkit.executor._kill',side_effect=kill), patch('agentdriver.process_tree.ProcessTree'):
                job=Job({'slots':asyncio.Semaphore(1),'capacity':gate,'root':Path(directory),'stats':{'active':0},'identity':{}},'job',{'files':{},'assets':{},'runId':'test','start':{}})
                for _ in range(100):
                    await asyncio.sleep(.01)
                    if job.process: break
                job.task.cancel()
                await asyncio.wait_for(killed.wait(),2)
                for _ in range(5): job.task.cancel(); await asyncio.sleep(.02)
                await asyncio.gather(job.task,return_exceptions=True)
                self.assertTrue(process.dead.is_set())
                with gate.transaction() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM leases').fetchone()[0],0)


def _crashed_owner(path, connection):
    import subprocess
    from agentdriver.process_tree import ProcessTree
    gate=CapacityGate(path,slots=1)
    now=time.monotonic()
    for i in range(11): gate.observe(1,1,now-10+i)
    lease=gate.try_acquire('crashed')
    process=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])
    name='Local\\FlowCapacity-'+lease['fence']
    tree=ProcessTree(process.pid,name=name)
    gate.confirm_started('crashed',lease['fence'],process.pid,name)
    connection.send(process.pid)
    connection.close()
    os._exit(7)


class CrashIntegrationTests(unittest.TestCase):
    @unittest.skipUnless(os.name=='nt','Windows job object recovery')
    def test_crashed_owner_tree_verified_dead_and_reclaimed(self):
        import multiprocessing
        from agents.flowkit.capacity import process_identity
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'gate.db'
            ctx=multiprocessing.get_context('spawn')
            receiving,sending=ctx.Pipe(duplex=False)
            owner=ctx.Process(target=_crashed_owner,args=(path,sending))
            owner.start()
            self.assertTrue(receiving.poll(10))
            child=receiving.recv()
            owner.join(10)
            self.assertEqual(owner.exitcode,7)
            gate=CapacityGate(path,slots=1)
            for _ in range(100):
                with gate.transaction() as db:
                    gate._reclaim(db)
                    count=db.execute('SELECT COUNT(*) FROM leases').fetchone()[0]
                if count==0: break
                time.sleep(.02)
            self.assertEqual(count,0)
            self.assertIsNone(process_identity(child))

    def test_nonuniform_machine_samples_feed_preceding_window(self):
        from agents.flowkit.capacity import MachineSampler
        sampler=MachineSampler()
        with tempfile.TemporaryDirectory() as directory:
            gate=CapacityGate(Path(directory)/'gate.db',cpu_limit=60,clock=lambda:100.)
            counters=[(0,0,20),(1,1,20),(2,2,20),(4,4,20),(5,5,20),(5,8,20),(5,11,20)]
            with patch.object(sampler,'_read',side_effect=counters):
                self.assertIsNone(sampler.sample())
                for stamp in [90,91,93,94,97,100]: gate.observe(*sampler.sample(),stamp)
            self.assertEqual(gate.metrics()['cpu_percent'],60.)
            self.assertEqual(gate.try_acquire('job')['reason'],'high_cpu')


class LegacySafetyTests(unittest.TestCase):
    def test_unverified_legacy_boot_marker_cannot_release_live_leases(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            gate=CapacityGate(Path(directory)/'gate.db',clock=lambda:100.)
            for i in range(11): gate.observe(1,1,90+i)
            lease=gate.try_acquire('active')
            with gate.transaction() as db:
                state=gate._state(db)
                state['boot']='windows-conservative'
                state.pop('boot_verified',None)
                gate._save(db,state)
            upgraded=CapacityGate(gate.path,clock=lambda:100.)
            self.assertTrue(gate.valid('active',lease['fence']))
            with upgraded.transaction() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM leases').fetchone()[0],1)

class RoundTwoTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_queued_acquisition_while_database_stays_locked(self):
        import asyncio
        from agents.flowkit.worker_service import Job
        with tempfile.TemporaryDirectory() as directory:
            gate=CapacityGate(Path(directory)/'gate.db')
            now=time.monotonic()
            for i in range(11): gate.observe(1,1,now-10+i)
            with gate.transaction():
                job=Job({'slots':asyncio.Semaphore(1),'capacity':gate,'root':Path(directory)},'queued',{'files':{},'assets':{},'runId':'test'})
                await asyncio.sleep(.04)
                job.task.cancel()
                await asyncio.wait_for(asyncio.gather(job.task,return_exceptions=True),.8)
                self.assertEqual(job.state,'cancelled')
                self.assertIsNone(job.process)
            with gate.transaction() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM leases').fetchone()[0],0)

    @unittest.skipUnless(os.name=='nt','Windows session namespace')
    def test_other_session_absence_is_not_proof_of_tree_death(self):
        from agents.flowkit.capacity import execution_dead
        details={'phase':'started','tree':'Local\\FlowCapacity-test','session':123}
        with patch('agents.flowkit.capacity.session_id',return_value=456), patch('agentdriver.process_tree.reclaim_named_tree',return_value=True) as reclaim:
            self.assertFalse(execution_dead(details))
            reclaim.assert_not_called()
        with patch('agents.flowkit.capacity.session_id',return_value=123), patch('agentdriver.process_tree.reclaim_named_tree',return_value=True):
            self.assertTrue(execution_dead(details))
        details.pop('session')
        with patch('agentdriver.process_tree.reclaim_named_tree',return_value=True) as reclaim:
            self.assertFalse(execution_dead(details))
            reclaim.assert_not_called()

    async def test_delayed_child_handshake_blocks_admission_until_workload_start(self):
        import asyncio
        import json
        from agents.flowkit.worker_service import Job
        original=asyncio.create_subprocess_exec
        script='import sys,time,json; sys.stdin.readline(); time.sleep(.3); print(json.dumps({"type":"capacity_ready"}),flush=True); sys.stdin.readline(); time.sleep(.4); print(json.dumps({"type":"capacity_started"}),flush=True); time.sleep(60)'
        async def delayed(*args,**kwargs):
            return await original(sys.executable,'-u','-c',script,**kwargs)
        with tempfile.TemporaryDirectory() as directory:
            gate=CapacityGate(Path(directory)/'gate.db',slots=2)
            now=time.monotonic()
            for i in range(11): gate.observe(1,1,now-10+i)
            with patch('agents.flowkit.worker_service.asyncio.create_subprocess_exec',side_effect=delayed):
                job=Job({'slots':asyncio.Semaphore(2),'capacity':gate,'root':Path(directory),'stats':{'active':0},'identity':{}},'slow',{'files':{},'assets':{},'runId':'test','start':{}})
                try:
                    phases=[]; launching_at=None; started_at=None
                    for _ in range(200):
                        await asyncio.sleep(.01)
                        with gate.transaction() as db:
                            row=db.execute('SELECT details FROM leases WHERE job=?',('slow',)).fetchone()
                            if row:
                                phase=json.loads(row[0])['phase']
                                phases.append(phase)
                                if phase=='launching' and launching_at is None: launching_at=time.monotonic()
                                if phase=='started':
                                    started_at=gate._state(db)['last_start']; break
                        if row and phase in ('pending','launching'):
                            self.assertEqual(gate.try_acquire('other')['reason'],'pending_start')
                    self.assertIn('pending',phases)
                    self.assertIn('launching',phases)
                    self.assertIsNotNone(started_at)
                    self.assertGreaterEqual(started_at-launching_at,.3)
                    self.assertEqual(gate.try_acquire('other')['reason'],'cooldown')
                finally:
                    job.task.cancel()
                    await asyncio.gather(job.task,return_exceptions=True)
