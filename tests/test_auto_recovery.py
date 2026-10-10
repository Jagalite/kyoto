import json
import contextlib
import io
import threading
import time
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import dashboard
import kyoto


class AutoRecoveryTest(unittest.TestCase):
    def test_observers_and_preview_recover_stale_jobs_before_output(self):
        for command in [('watch', '--once'), ('health',), ('show', 'lost'), ('plan', '--', 'cargo', 'check')]:
            with self.subTest(command=command), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                db = kyoto.connect(root)
                (root / 'attempts' / 'lost').mkdir(parents=True)
                db.execute("INSERT INTO settings VALUES ('paused','true')")
                db.execute("INSERT INTO jobs(id,command,cwd,environment,status,submitted,started,estimate,samples,attempt_token) VALUES ('lost','[\"echo\"]',?,'{}','running',1,1,1,0,'lost')", (folder,))
                db.commit()
                try:
                    result = subprocess.run([sys.executable, kyoto.__file__, '--state', folder, *command], capture_output=True, text=True, timeout=20)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(db.execute("SELECT status FROM jobs WHERE id='lost'").fetchone()[0], 'interrupted')
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0], 1)
                    if command[0] == 'watch':
                        self.assertIn('interrupted', result.stdout)
                        self.assertNotIn('missing/stale', result.stdout)
                finally:
                    row = db.execute('SELECT value FROM runtime WHERE id=1').fetchone()
                    if row:
                        try:
                            os.kill(json.loads(row[0])['worker_pid'], signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                    db.close()

    def test_dashboard_checks_worker_on_refresh(self):
        screen = Mock()
        screen.getmaxyx.return_value = (10, 50)
        screen.getch.side_effect = [-1, ord('q')]
        snapshot = dict(config=dict(kyoto.DEFAULT_CONFIG, paused=False), runtime=None,
                        jobs=[], now=0, fresh=False, reason='', active_count=0)
        with patch('dashboard.curses.curs_set'), patch('dashboard.palette', return_value={'accent': 0, 'muted': 0}), patch('dashboard.read_snapshot', return_value=snapshot), patch('dashboard.WorkerRecovery') as recover:
            recover.return_value.refresh.return_value = None
            dashboard.screen_loop(screen, Path('/unused'), 0, None, Mock())
            self.assertEqual(recover.return_value.refresh.call_count, 2)

    def test_old_heartbeat_does_not_match_replacement_session(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            kyoto.recovery.write_json(root / 'coordinator.json', dict(pid=os.getpid(), session='new'))
            runtime = dict(worker_pid=os.getpid(), worker_session='old', recovery_ready=True, sampled_at=time.time())
            self.assertFalse(kyoto.worker_ready(root, runtime))
            runtime['worker_session'] = 'new'
            self.assertTrue(kyoto.worker_ready(root, runtime))
            with patch('kyoto.os.kill', side_effect=ProcessLookupError):
                self.assertFalse(kyoto.worker_ready(root, runtime))

    def test_timeout_is_explicit_and_cli_does_not_print_saved_status(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            kyoto.connect(root).close()
            with patch('kyoto.ensure_worker'):
                self.assertIs(kyoto.recover_existing(root, timeout=0), False)
            with patch('sys.argv', ['kyoto', '--state', folder, 'status']), patch('kyoto.recover_existing', return_value=False), contextlib.redirect_stdout(io.StringIO()) as out, contextlib.redirect_stderr(io.StringIO()) as err:
                with self.assertRaises(SystemExit) as raised:
                    kyoto.main()
                self.assertEqual(raised.exception.code, 3)
                self.assertEqual(out.getvalue(), '')
                self.assertIn('not verified', err.getvalue())

    def test_slow_recovery_does_not_block_dashboard_quit(self):
        entered, release = threading.Event(), threading.Event()
        def slow(*args, **kwargs):
            entered.set()
            release.wait(10)
            return True
        worker = dashboard.WorkerRecovery(Path('/unused'))
        screen = Mock()
        screen.getmaxyx.return_value = (10, 50)
        def quit_key():
            self.assertTrue(entered.wait(2))
            return ord('q')
        screen.getch.side_effect = quit_key
        snapshot = dict(config=dict(kyoto.DEFAULT_CONFIG, paused=False), runtime=None,
                        jobs=[], now=0, fresh=False, reason='', active_count=0)
        try:
            with patch('kyoto.recover_existing', side_effect=slow), patch('dashboard.WorkerRecovery', return_value=worker), patch('dashboard.curses.curs_set'), patch('dashboard.palette', return_value={'accent': 0, 'muted': 0}), patch('dashboard.read_snapshot', return_value=snapshot):
                dashboard.screen_loop(screen, Path('/unused'), 0, None, Mock())
                self.assertTrue(worker.thread.is_alive())
        finally:
            release.set()
            if worker.thread:
                worker.thread.join(2)

    def test_wait_uses_its_retry_loop_not_the_observer_timeout(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = kyoto.connect(root)
            db.execute("INSERT INTO jobs(id,command,cwd,environment,status,submitted,started,finished,exit_code,estimate,samples) VALUES ('done','[\"echo\"]',?,'{}','succeeded',1,1,2,0,1,0)", (folder,))
            db.commit()
            try:
                with patch('sys.argv', ['kyoto', '--state', folder, 'wait', 'done']), patch('kyoto.connect', return_value=db), patch('kyoto.ensure_worker') as ensure, patch('kyoto.recover_existing', return_value=False) as recover, contextlib.redirect_stdout(io.StringIO()) as output:
                    self.assertEqual(kyoto.main(), 0)
                    recover.assert_not_called()
                    ensure.assert_called()
                    self.assertEqual(json.loads(output.getvalue())['status'], 'succeeded')
            finally:
                db.close()
