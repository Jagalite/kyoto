import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import kyoto


class PlanTest(unittest.TestCase):
    def invoke(self, root, *options):
        out = io.StringIO()
        with patch('sys.argv', ['kyoto', '--state', str(root), 'plan', *options]), patch.dict(os.environ, {'KYOTO_USER_ID': '00000000-0000-0000-0000-000000000001'}), patch('kyoto.ensure_worker') as worker, contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            kyoto.main()
            worker.assert_not_called()
        return json.loads(out.getvalue())

    def test_absent_state_stays_absent_and_command_does_not_execute(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'absent'
            marker = Path(directory) / 'marker'
            result = self.invoke(root, '--resource', directory, '--', 'touch', str(marker))
            self.assertFalse(root.exists())
            self.assertFalse(marker.exists())
            self.assertFalse(result['enqueued'])
            self.assertEqual(result['planned_order'], 1)
            self.assertTrue(result['plan_conditional'])
            self.assertIsNone(result['planned_queue'][0]['id'])
            self.assertNotIn('environment', result)

    def test_smart_fifo_parallel_and_readonly(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = kyoto.connect(root)
            for job_id, duration in [('long', 60), ('medium', 30)]:
                db.execute('INSERT INTO jobs(id,command,cwd,status,submitted,estimate,resources) VALUES (?,?,?,?,?,?,?)',
                           (job_id, '["echo"]', directory, 'queued', time.time()-1, duration, json.dumps([directory+'/'+job_id])))
            db.commit()
            for mode, limit, expected in [('smart', 1, 1), ('fifo', 1, 3), ('smart', 2, 1)]:
                for key, value in [('mode', mode), ('max_running', limit)]:
                    db.execute('INSERT OR REPLACE INTO settings VALUES (?,?)', (key, json.dumps(value)))
                db.execute('INSERT OR REPLACE INTO runtime VALUES (1,?)', (json.dumps(dict(sampled_at=time.time(), config=kyoto.config_for(db), effective_max_running=limit)),))
                db.commit()
                before = (root/'queue.sqlite').read_bytes()
                result = self.invoke(root, '--estimate', '5', '--resource', directory+'/new', '--', 'echo')
                self.assertEqual(result['planned_order'], expected)
                self.assertFalse(result['plan_conditional'])
                self.assertEqual(len(result['planned_queue']), 3)
                self.assertEqual((root/'queue.sqlite').read_bytes(), before)
                self.assertEqual(db.execute('SELECT count(*) FROM jobs').fetchone()[0], 2)
                if limit == 2:
                    self.assertEqual([j['planned_wave'] for j in result['planned_queue']], [1, 1, 2])
            db.close()

    def test_validation_applies_without_creating_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)/'absent'
            with self.assertRaises(SystemExit):
                self.invoke(root, '--', 'cargo', 'test')
            self.assertFalse(root.exists())
            result = self.invoke(root, '--resource', directory, '--', 'cargo', 'test', '--no-run')
            self.assertEqual(result['command_kind'], 'test-compile')
