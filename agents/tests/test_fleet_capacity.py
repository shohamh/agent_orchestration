import tempfile
import unittest
from pathlib import Path
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
        self.assertEqual(self.g.metrics()['cpu_percent'],50)

def _compete(path, start, results):
    import time
    gate = CapacityGate(path, slots=1)
    start.wait()
    results.put(gate.try_acquire(str(__import__('os').getpid()))['acquired'])

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
            start=ctx.Event(); results=ctx.Queue()
            processes=[ctx.Process(target=_compete,args=(path,start,results)) for _ in range(2)]
            for process in processes: process.start()
            start.set()
            values=[results.get(timeout=15) for _ in processes]
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
