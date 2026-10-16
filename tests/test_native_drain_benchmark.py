"""Counter accounting and the actual bounded native kernel (not a Cargo speed test)."""
import json
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
import unittest

from experiments.native_drain_qualification import summarize_logs, ROOT


class AccountingTest(unittest.TestCase):
    def test_retry_work_is_charged_and_completion_status_is_separate(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'log'
            def row(epoch, completed, at):
                return 'KYOTO_PROGRESS ' + json.dumps(dict(epoch=epoch*32, completed=completed,
                    total=100, reported_at=at, phase='atomic-work')) + '\n'
            p.write_text(row('a',0,0)+row('a',30,3)+row('a',30,3)+row('b',0,10)+row('b',20,12))
            r = summarize_logs([p], [dict(started=10., status='running')])
            self.assertEqual(r['reported_work_units'],50)
            self.assertEqual(r['retained_reported_work_units'],20)
            self.assertEqual(r['discarded_reported_work_units'],30)
            for status in ('queued','failed','cancelled'):
                r = summarize_logs([p], [dict(started=None, status=status)])
                self.assertEqual(r['retained_reported_work_units'],0)
            p.write_text(p.read_text()+row('b',100,15))
            r = summarize_logs([p],[dict(started=10.,status='succeeded')])
            self.assertEqual(r['retained_reported_work_units'],100)

    def test_malformed_or_incomplete_records_are_not_work(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'log'
            values=[{},[],dict(phase='atomic-work'),dict(phase='atomic-work',epoch='a'*32,
                completed=float('nan'),total=100,reported_at=1),dict(phase='atomic-work',epoch='a'*32,
                completed=True,total=100,reported_at=1)]
            p.write_text(''.join('KYOTO_PROGRESS '+json.dumps(v)+'\n' for v in values)+'KYOTO_PROGRESS {')
            self.assertEqual(summarize_logs([p],[dict(started=0,status='running')])['reported_work_units'],0)
            with self.assertRaises(ValueError):summarize_logs([p],[])

    def test_counter_reset_in_same_epoch_is_reported_not_double_counted(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'log'
            p.write_text(''.join('KYOTO_PROGRESS '+json.dumps(dict(phase='atomic-work',epoch='a'*32,
                completed=n,total=100,reported_at=n))+'\n' for n in (30,20)))
            with self.assertRaises(ValueError):summarize_logs([p],[dict(started=0,status='running')])


@unittest.skipUnless(shutil.which('cc'), 'C compiler required for native kernel test')
class NativeCounterTest(unittest.TestCase):
    def test_actual_atomic_work_shared_and_independent(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);exe=root/'counter';shared=root/'data';marker=root/'released'
            subprocess.run([shutil.which('cc'),'-std=c11','-O2',str(ROOT/'experiments/workloads/shared_counter.c'),
                '-o',str(exe)],check=True,capture_output=True,timeout=30)
            for independent in (False,True):
                shared.write_bytes(bytes(4096))
                if independent:marker.touch()
                children=[]
                try:
                    for i in range(2):
                        children.append(subprocess.Popen([str(exe),str(shared),'10',str(i),str(marker),str(i)*32],
                            stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True))
                    for p in children:
                        out,err=p.communicate(timeout=10)
                        self.assertEqual(p.returncode,0,err)
                        records=[json.loads(line[15:]) for line in out.splitlines()]
                        self.assertEqual(records[0]['completed'],0)
                        self.assertEqual(records[-1]['completed'],10)
                finally:
                    for p in children:
                        if p.poll() is None:p.kill()
                        p.communicate(timeout=5)
                b=shared.read_bytes()
                self.assertEqual(struct.unpack_from('=Q',b,0)[0],0 if independent else 2000000)
                if independent:
                    self.assertEqual(struct.unpack_from('=Q',b,128)[0],1000000)
                    self.assertEqual(struct.unpack_from('=Q',b,256)[0],1000000)
