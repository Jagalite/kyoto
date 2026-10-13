import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import kyoto
from worker_restart import restart_worker


class WorkerRestartTest(unittest.TestCase):
    def test_old_restart_schema_preserves_pending_requests(self):
        import sqlite3
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = sqlite3.connect(root/'queue.sqlite')
            db.execute('CREATE TABLE worker_restart (session TEXT PRIMARY KEY, expires REAL NOT NULL)')
            db.execute("INSERT INTO worker_restart VALUES ('old',12345)")
            db.commit(); db.close()
            db = kyoto.connect(root)
            row = db.execute('SELECT * FROM worker_restart').fetchone()
            self.assertEqual(row['session'], 'old')
            self.assertEqual(row['expires'], 12345)
            self.assertEqual(len(row['request_id']), 32)
            db.execute("INSERT INTO worker_restart VALUES ('another','old',12346)")
            db.commit(); db.close()

    def test_unsupported_worker_is_not_signalled(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with patch('kyoto.runtime_for', return_value={'worker_session': 'old'}), patch('kyoto.worker_ready', return_value=True), patch('kyoto.ensure_worker') as ensure:
                with self.assertRaisesRegex(RuntimeError, 'predates graceful restart'):
                    restart_worker(root, 1)
                ensure.assert_not_called()

    def test_timeout_removes_only_own_request(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = kyoto.connect(root)
            db.execute('INSERT INTO worker_restart VALUES (?,?,?)', ('other', 'old', time.time()+60))
            db.commit()
            previous = dict(worker_session='old', supports_restart=True)
            with patch('kyoto.runtime_for', return_value=previous), patch('kyoto.worker_ready', return_value=True), patch('kyoto.ensure_worker'):
                with self.assertRaisesRegex(TimeoutError, 'not verified'):
                    restart_worker(root, .01)
            self.assertEqual([r[0] for r in db.execute('SELECT request_id FROM worker_restart')], ['other'])
            db.close()

    def test_permission_failure_keeps_job_running_and_backs_off(self):
        with tempfile.TemporaryDirectory() as folder:
            db = kyoto.connect(Path(folder))
            db.execute("INSERT INTO jobs(id,status) VALUES ('a','running')")
            db.commit()
            process = Mock()
            process.signal.side_effect = PermissionError('denied')
            state = dict(process=process)
            with patch('kyoto.time.monotonic', return_value=10):
                kyoto.signal_attempt(db, 'a', state, signal.SIGTERM)
                kyoto.signal_attempt(db, 'a', state, signal.SIGTERM)
            self.assertEqual(process.signal.call_count, 1)
            row = db.execute("SELECT status,error FROM jobs WHERE id='a'").fetchone()
            self.assertEqual(row['status'], 'running')
            self.assertIn('Cannot signal', row['error'])
            process.signal.side_effect = None
            with patch('kyoto.time.monotonic', return_value=21):
                kyoto.signal_attempt(db, 'a', state, signal.SIGTERM)
            self.assertIsNone(db.execute("SELECT error FROM jobs WHERE id='a'").fetchone()[0])
            db.close()

    def test_live_restart_preserves_command_pid_attempt_queue_and_exit_status(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = kyoto.connect(root)
            for key, value in dict(max_running=1, pressure_aware=False, auto_defer=False).items():
                db.execute('INSERT INTO settings VALUES (?,?)', (key, json.dumps(value)))
            code = "from pathlib import Path; import time; p=Path('starts'); p.write_text(p.read_text()+'x' if p.exists() else 'x');\nwhile not Path('release').exists(): time.sleep(.05)\nraise SystemExit(7)"
            for job_id in ('active', 'queued'):
                db.execute('INSERT INTO jobs(id,command,cwd,environment,status,submitted,estimate,samples,resources) VALUES (?,?,?,?,?,?,?,?,?)',
                           (job_id, json.dumps([sys.executable, '-c', code]), folder, json.dumps(dict(os.environ)), 'queued', time.time(), 60, 0, '[]'))
            db.commit()
            worker = subprocess.Popen([sys.executable, kyoto.__file__, '--state', folder, '_worker'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            def wait_for(predicate):
                deadline = time.monotonic()+20
                while time.monotonic()<deadline:
                    value = predicate()
                    if value:
                        return value
                    time.sleep(.1)
                self.fail('timed out waiting for worker')
            child_pid = None
            try:
                wait_for(lambda: (root/'starts').exists())
                db.execute("INSERT INTO settings VALUES ('paused','true')")
                db.commit()
                wait_for(lambda: (kyoto.runtime_for(db) or {}).get('recovery_ready'))
                before = dict(db.execute("SELECT * FROM jobs WHERE id='active'").fetchone())
                record_path = root/'attempts'/before['attempt_token']/'process.json'
                record = wait_for(lambda: kyoto.recovery.read_json(record_path))
                child_pid = record['pid']
                old = kyoto.runtime_for(db)['worker_session']
                result = subprocess.run([sys.executable, kyoto.__file__, '--state', folder, 'restart', '--timeout', '20'], capture_output=True, text=True, timeout=25)
                self.assertEqual(result.returncode, 0, result.stderr)
                report = json.loads(result.stdout)
                self.assertNotEqual(report['worker_session'], old)
                after = dict(db.execute("SELECT * FROM jobs WHERE id='active'").fetchone())
                self.assertEqual(after['status'], 'running')
                self.assertEqual(after['attempt_token'], before['attempt_token'])
                self.assertEqual(kyoto.recovery.read_json(record_path), record)
                os.kill(child_pid, 0)
                self.assertEqual((root/'starts').read_text(), 'x')
                self.assertEqual(db.execute("SELECT status FROM jobs WHERE id='queued'").fetchone()[0], 'queued')
                self.assertEqual(kyoto.config_for(db)['paused'], True)
                (root/'release').touch()
                wait_for(lambda: db.execute("SELECT status FROM jobs WHERE id='active'").fetchone()[0] == 'failed')
                self.assertEqual(db.execute("SELECT exit_code FROM jobs WHERE id='active'").fetchone()[0], 7)
            finally:
                (root/'release').touch()
                runtime = kyoto.runtime_for(db)
                for pid in {worker.pid, (runtime or {}).get('worker_pid')} - {None}:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                worker.wait(timeout=5)
                if child_pid:
                    try:
                        os.killpg(child_pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                db.close()
