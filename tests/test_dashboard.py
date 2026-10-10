import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from dashboard import clean, clipped, log_tail, plain, read_snapshot
from kyoto import connect

SCRIPT = str(Path(__file__).resolve().parents[1] / 'kyoto.py')


class DashboardTest(unittest.TestCase):
    def test_empty_dashboard_does_not_create_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'missing'
            result = subprocess.run([sys.executable, SCRIPT, '--state', str(root), 'watch', '--once'], capture_output=True, text=True, check=True)
            self.assertIn('No jobs yet', result.stdout)
            self.assertFalse(root.exists())

    def test_read_only_snapshot_separates_active_history_and_hides_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = connect(root)
            for job_id, state in [('a' * 32, 'queued'), ('b' * 32, 'running'), ('c' * 32, 'succeeded')]:
                db.execute('INSERT INTO jobs (id,command,cwd,environment,status,submitted,started,finished,estimate,samples) VALUES (?,?,?,?,?,?,?,?,?,?)',
                           (job_id, json.dumps(['cargo', 'test']), '/project', '{"SECRET":"do-not-display"}', state, 1, 2 if state != 'queued' else None, 3 if state == 'succeeded' else None, 60, 0))
            db.commit()
            db.close()
            before = (root / 'queue.sqlite').read_bytes()
            snapshot = read_snapshot(root)
            self.assertEqual([j['status'] for j in snapshot['jobs']], ['running', 'queued', 'succeeded'])
            self.assertEqual(snapshot['active_count'], 2)
            self.assertNotIn('environment', snapshot['jobs'][0])
            output = plain(snapshot, root)
            self.assertIn('1m00s*', output)
            self.assertIn('missing/stale', output)
            self.assertNotIn('do-not-display', output)
            self.assertEqual(before, (root / 'queue.sqlite').read_bytes())

    def test_waiting_list_uses_conditional_execution_plan(self):
        import time
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = connect(root)
            for name, estimate, offset in [('long', 60, -1), ('short', 5, 0)]:
                db.execute('INSERT INTO jobs(id,command,cwd,status,submitted,estimate,samples,resources) VALUES (?,?,?,?,?,?,?,?)',
                           (name, '["echo"]', '/project', 'queued', time.time() + offset, estimate, 0, '["/project"]'))
            db.commit()
            db.close()
            snapshot = read_snapshot(root)
            self.assertEqual([j['id'] for j in snapshot['jobs']], ['short', 'long'])
            self.assertTrue(all(j['plan_conditional'] for j in snapshot['jobs']))
            self.assertIn('#1', plain(snapshot, root))
            self.assertNotIn('· wave', plain(snapshot, root))
            self.assertNotIn('after resume +', plain(snapshot, root))

    def test_history_expands_until_all_entries_are_loaded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = connect(root)
            for index in range(35):
                db.execute('INSERT INTO jobs(id,command,cwd,status,submitted,finished,estimate,samples) VALUES (?,?,?,?,?,?,?,?)',
                           (f'{index:032d}', '["echo"]', '/project', 'succeeded', index, index, 1, 0))
            db.commit()
            db.close()
            first = read_snapshot(root, 6)
            expanded = read_snapshot(root, 31)
            last = read_snapshot(root, 56)
            self.assertEqual([len(s['jobs']) for s in (first, expanded, last)], [6, 31, 35])
            self.assertTrue(first['more_history'])
            self.assertTrue(expanded['more_history'])
            self.assertFalse(last['more_history'])
            self.assertEqual([j['id'] for j in first['jobs']], [j['id'] for j in expanded['jobs'][:6]])

    def test_log_tail_is_bounded_and_terminal_control_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            job_id = 'a' * 32
            (root / (job_id + '.log')).write_text('old\n' * 100000 + '\x1b[31merror\x1b[0m\nlast line\n')
            self.assertEqual(log_tail(root, job_id, 2), ['error', 'last line'])
            self.assertEqual(log_tail(root, '../outside'), ['Invalid job ID'])
            self.assertEqual(log_tail(root, 'b' * 32), ['No log yet.'])
            self.assertNotIn('\x1b', clean('\x1b]52;c;payload\x07text\x1b[2J'))
            self.assertNotIn('\x07', clean('\x07alert'))

    def test_wide_characters_clip_to_terminal_width(self):
        self.assertEqual(clipped('abc界d', 5), 'abc界')
        self.assertEqual(clipped('abc界d', 4), 'abc')

    def test_tui_alias_in_pipe_prints_one_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([sys.executable, SCRIPT, '--state', directory, 'tui'], capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0)
            self.assertIn('KYOTO', result.stdout)
            self.assertNotIn('\x1b', result.stdout)


if __name__ == '__main__':
    unittest.main()
