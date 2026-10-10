import contextlib
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import uuid

import kyoto


class OwnershipTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.a, self.b = str(uuid.uuid4()), str(uuid.uuid4())

    def tearDown(self):
        self.temp.cleanup()

    def cli(self, *args, actor=None):
        env = dict(os.environ)
        env.pop('KYOTO_USER_ID', None)
        if actor:
            env['KYOTO_USER_ID'] = actor
        out = io.StringIO()
        connections = []
        original = kyoto.connect
        def tracked(root):
            db = original(root)
            connections.append(db)
            return db
        with patch.dict(os.environ, env, clear=True), patch('sys.argv', ['kyoto', '--state', str(self.root), *args]), patch('kyoto.ensure_worker'), patch('kyoto.connect', tracked), contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            try:
                kyoto.main()
            finally:
                for db in connections:
                    db.close()
        return json.loads(out.getvalue())

    def submit(self, actor=None):
        return self.cli('submit', '--resource', self.temp.name, '--', 'echo', 'hello', actor=actor)

    def test_generated_identity_and_listing(self):
        job = self.submit()
        self.assertEqual(str(uuid.UUID(job['owner_id'])), job['owner_id'])
        self.submit(self.a)
        self.assertEqual(len(self.cli('jobs')), 2)
        self.assertEqual(len(self.cli('jobs', '--mine', actor=self.a)), 1)
        self.assertEqual(self.cli('jobs', '--user-id', job['owner_id'])[0]['id'], job['id'])
        with self.assertRaises(SystemExit):
            self.cli('jobs', '--mine')

    def test_cancel_enforcement_and_audit(self):
        job = self.submit(self.a)
        for actor in (None, self.b):
            with self.assertRaises(SystemExit):
                self.cli('cancel', job['id'], actor=actor)
        self.assertEqual(self.cli('show', job['id'])['cancel'], 0)
        self.cli('cancel', job['id'], actor=self.a)
        self.assertEqual(self.cli('show', job['id'])['cancel'], 1)
        audit = self.cli('audit', job['id'])
        self.assertEqual([r['outcome'] for r in audit], ['denied', 'denied', 'requested'])
        self.assertEqual(audit[-1]['actor_id'], self.a)
        self.assertEqual(audit[-1]['forced'], 0)
        other = self.submit(self.a)
        self.cli('cancel', other['id'], '--force', actor=self.b)
        self.assertEqual(self.cli('audit', other['id'])[0]['forced'], 1)

    def test_legacy_and_immutable_owner(self):
        db = kyoto.connect(self.root)
        db.execute("INSERT INTO jobs(id,status) VALUES ('legacy','queued')")
        db.commit()
        self.assertFalse(kyoto.cancel_job(db, 'legacy', self.a, False))
        self.assertTrue(kyoto.cancel_job(db, 'legacy', None, True))
        with self.assertRaises(sqlite3.IntegrityError):
            db.execute("UPDATE jobs SET owner_id=? WHERE id='legacy'", (self.a,))
        db.close()

    def test_identity_does_not_fragment_learning(self):
        first = self.submit(self.a)
        db = kyoto.connect(self.root)
        db.execute("UPDATE jobs SET status='succeeded',started=1,finished=3 WHERE id=?", (first['id'],))
        db.commit()
        db.close()
        second = self.submit(self.b)
        self.assertEqual(second['samples'], 1)
        self.assertEqual(second['estimate'], 2)

    def test_explicit_identity_and_invalid_uuid(self):
        job = self.cli('submit', '--user-id', self.b, '--resource', self.temp.name, '--', 'echo', actor=self.a)
        self.assertEqual(job['owner_id'], self.b)
        self.cli('cancel', job['id'], '--user-id', self.b, actor=self.a)
        with self.assertRaises(SystemExit):
            self.cli('submit', '--user-id', 'bad', '--', 'echo')
        self.assertEqual(len(self.cli('jobs')), 1)
