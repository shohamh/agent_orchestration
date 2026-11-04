"""Per-PC admission. Sample every second; gaps/freshness >3s invalidate a window.

SQLite BEGIN IMMEDIATE serializes every admission and fenced publication. All
services on a PC MUST use the same database (never a network filesystem). Leases
expire after 30 seconds without renewal, but expired reservations stay quarantined
until their owner stops execution and releases them. This deliberately fails closed
on a crash; a supervisor must establish process-tree death before manual cleanup.
Cooldown and samples survive reconnects; OS boot identity invalidates old samples.
RAM is (total-available)/total physical memory, including reclaimable memory as free.
"""
import contextlib
import ctypes
import json
import math
import os
from pathlib import Path
import sqlite3
import time
import uuid


def default_path():
    base = os.environ.get('PROGRAMDATA') if os.name == 'nt' else '/var/tmp'
    return Path(os.environ.get('FLOW_CAPACITY_DB', str(Path(base or '/var/tmp')/'agent-orchestration'/'capacity.db')))


def boot_id():
    if os.name != 'nt':
        try: return Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        except OSError: pass
    # Windows uses monotonic regression/gaps to reset samples; leases stay quarantined.
    return 'windows-conservative'


class CapacityGate:
    def __init__(self, path=None, *, slots=4, cpu_limit=90, ram_limit=90, clock=time.monotonic, boot=None):
        if not 1 <= slots <= 100 or not 0 < cpu_limit <= 100 or not 0 < ram_limit <= 100:
            raise ValueError('Invalid capacity settings')
        self.path = Path(path) if path is not None else default_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock, self.owner = clock, uuid.uuid4().hex
        self.boot = boot or boot_id()
        self.slots, self.cpu_limit, self.ram_limit = slots, cpu_limit, ram_limit
        with self.transaction() as db:
            db.execute('CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY, value TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS leases (job TEXT PRIMARY KEY, owner TEXT, fence TEXT, expires REAL)')
            row = db.execute('SELECT value FROM state WHERE id=1').fetchone()
            state = json.loads(row[0]) if row else {}
            settings = state.get('settings', {'slots':slots,'cpu_limit':cpu_limit,'ram_limit':ram_limit})
            if state.get('boot') != self.boot:
                state = {'boot':self.boot, 'samples':[], 'last_start':self.clock()-10}
            state['settings'] = settings
            self._save(db,state)

    @contextlib.contextmanager
    def transaction(self):
        db = sqlite3.connect(self.path, timeout=10)
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally: db.close()

    def _state(self, db): return json.loads(db.execute('SELECT value FROM state WHERE id=1').fetchone()[0])
    def _save(self, db, state): db.execute('INSERT OR REPLACE INTO state VALUES (1,?)',(json.dumps(state),))

    def configure(self, *, slots, cpu_limit=90, ram_limit=90):
        if not 1<=slots<=100 or not 0<cpu_limit<=100 or not 0<ram_limit<=100: raise ValueError('Invalid capacity settings')
        with self.transaction() as db:
            s=self._state(db); s['settings']={'slots':slots,'cpu_limit':cpu_limit,'ram_limit':ram_limit}; self._save(db,s)

    def observe(self, cpu_percent, ram_percent, monotonic_time):
        if not all(math.isfinite(x) for x in (cpu_percent,ram_percent,monotonic_time)) or not all(0<=x<=100 for x in (cpu_percent,ram_percent)):
            raise ValueError('Invalid sample')
        with self.transaction() as db:
            s = self._state(db)
            samples = s['samples']
            if monotonic_time < s['last_start']:
                s['last_start'] = monotonic_time
            if samples and (monotonic_time <= samples[-1][0] or monotonic_time-samples[-1][0]>3): samples=[]
            samples.append([monotonic_time,cpu_percent,ram_percent])
            while len(samples)>2 and samples[1][0]<=monotonic_time-10: samples.pop(0)
            s['samples']=samples
            self._save(db,s)

    def _metrics(self,s,now):
        rows=s['samples']
        result={'state':'unknown','cpu_percent':None,'ram_percent':None,'sampled_at':rows[-1][0] if rows else None,
                'observed_at':time.time(),'window_seconds':10,'ram_basis':'physical total minus available'}
        if not rows or not 0<=now-rows[-1][0]<=3 or rows[0][0]>rows[-1][0]-10: return result
        end=rows[-1][0]; start=end-10
        totals=[0.,0.]
        for a,b in zip(rows,rows[1:]):
            dt=max(0,b[0]-max(start,a[0]))
            for j in range(2): totals[j]+=a[j+1]*dt
        result.update(state='known',cpu_percent=totals[0]/10,ram_percent=totals[1]/10)
        return result

    def metrics(self):
        with self.transaction() as db: return self._metrics(self._state(db),self.clock())

    def try_acquire(self,job_id):
        with self.transaction() as db:
            now=self.clock(); s=self._state(db); m=self._metrics(s,now); settings=s['settings']
            reason=None
            if db.execute('SELECT COUNT(*) FROM leases').fetchone()[0]>=settings['slots']: reason='full_slots'
            elif now-s['last_start']<10 or (s['samples'] and s['samples'][-1][0]-s['last_start']<10): reason='cooldown'
            elif m['state']=='unknown': reason='missing_metrics'
            elif m['cpu_percent']>=settings['cpu_limit']: reason='high_cpu'
            elif m['ram_percent']>=settings['ram_limit']: reason='high_ram'
            if reason: return {'acquired':False,'reason':reason,'metrics':m}
            fence=uuid.uuid4().hex
            db.execute('INSERT INTO leases VALUES (?,?,?,?)',(job_id,self.owner,fence,now+30))
            s['last_start']=now; self._save(db,s)
            return {'acquired':True,'job_id':job_id,'fence':fence,'expires':now+30}

    def _valid(self,db,job,fence):
        return db.execute('SELECT 1 FROM leases WHERE job=? AND owner=? AND fence=? AND expires>?',(job,self.owner,fence,self.clock())).fetchone() is not None

    def valid(self,job,fence):
        with self.transaction() as db: return self._valid(db,job,fence)

    def renew(self,job,fence):
        with self.transaction() as db:
            if not self._valid(db,job,fence): return False
            db.execute('UPDATE leases SET expires=? WHERE job=?',(self.clock()+30,job))
            return True

    def publish(self,job,fence,callback):
        with self.transaction() as db:
            if not self._valid(db,job,fence): raise RuntimeError('Capacity lease expired; publication fenced')
            return callback()

    def release(self,job_id):
        with self.transaction() as db: db.execute('DELETE FROM leases WHERE job=? AND owner=?',(job_id,self.owner))


class MachineSampler:
    def __init__(self): self.previous=None
    def sample(self):
        if os.name=='nt':
            from ctypes import wintypes
            idle,kernel,user=wintypes.FILETIME(),wintypes.FILETIME(),wintypes.FILETIME()
            if not ctypes.windll.kernel32.GetSystemTimes(ctypes.byref(idle),ctypes.byref(kernel),ctypes.byref(user)): raise OSError('GetSystemTimes failed')
            value=lambda f:(f.dwHighDateTime<<32)+f.dwLowDateTime
            idle,total=value(idle),value(kernel)+value(user)
            class Memory(ctypes.Structure):
                _fields_=[('length',wintypes.DWORD),('load',wintypes.DWORD)]+[(n,ctypes.c_ulonglong) for n in ('total','available','page','freepage','virtual','freevirtual','extended')]
            mem=Memory(); mem.length=ctypes.sizeof(mem)
            if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(mem)): raise OSError('GlobalMemoryStatusEx failed')
            ram=100*(1-mem.available/mem.total)
        else:
            values=list(map(int,Path('/proc/stat').read_text().splitlines()[0].split()[1:]))
            idle=values[3]+values[4]; total=sum(values[:8])
            mem={line.split(':')[0]:int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()}
            ram=100*(1-mem['MemAvailable']/mem['MemTotal'])
        previous=self.previous; self.previous=(idle,total)
        if previous is None or total<=previous[1]: return None
        return max(0,min(100,100*(1-(idle-previous[0])/(total-previous[1])))),ram
