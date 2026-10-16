"""New Kyoto defaults without silently migrating existing Smart queues."""
import json
import sqlite3
import tempfile
from pathlib import Path
import unittest

import kyoto
from scheduling import DEFAULT_CONFIG


class DefaultSelectionTest(unittest.TestCase):
    def test_new_queue_persists_kyoto_and_keeps_draining_off(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / 'fresh'
            db = kyoto.connect(root)
            self.assertEqual(kyoto.config_for(db)['mode'], 'kyoto')
            self.assertFalse(kyoto.config_for(db)['active_drain'])
            self.assertEqual(json.loads(db.execute(
                "SELECT value FROM settings WHERE key='mode'").fetchone()[0]), 'kyoto')
            db.close()
            reopened = kyoto.connect(root)
            self.assertEqual(kyoto.config_for(reopened)['mode'], 'kyoto')
            reopened.close()

    def test_existing_implicit_smart_queue_preserved_on_upgrade(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            db = sqlite3.connect(root / 'queue.sqlite')
            db.execute('''CREATE TABLE jobs (
                id TEXT PRIMARY KEY, command TEXT, cwd TEXT, environment TEXT,
                signature TEXT, status TEXT, submitted REAL, started REAL,
                finished REAL, exit_code INTEGER, estimate REAL, samples INTEGER,
                cancel INTEGER DEFAULT 0, error TEXT)''')
            db.execute('CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            db.execute("INSERT INTO settings VALUES ('max_running', '2')")
            db.commit()
            db.close()
            upgraded = kyoto.connect(root)
            self.assertEqual(kyoto.config_for(upgraded)['mode'], 'smart')
            self.assertEqual(kyoto.config_for(upgraded)['max_running'], 2)
            self.assertFalse(kyoto.config_for(upgraded)['active_drain'])
            upgraded.close()
            again = kyoto.connect(root)
            self.assertEqual(kyoto.config_for(again)['mode'], 'smart')
            again.close()

    def test_explicit_legacy_mode_and_opt_in_preserved(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            db = kyoto.connect(root)
            db.execute("UPDATE settings SET value=? WHERE key='mode'", (json.dumps('fifo'),))
            db.execute("INSERT INTO settings VALUES ('active_drain','true')")
            db.commit()
            db.close()
            again = kyoto.connect(root)
            config = kyoto.config_for(again)
            self.assertEqual(config['mode'], 'fifo')
            self.assertTrue(config['active_drain'])
            again.close()

    def test_preview_new_queue_is_read_only(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / 'nonexistent'
            db = kyoto.connect(root, preview=True)
            self.assertEqual(kyoto.config_for(db)['mode'], 'kyoto')
            db.close()
            self.assertFalse(root.exists())

    def test_default_choice_does_not_enable_termination(self):
        self.assertEqual(DEFAULT_CONFIG['mode'], 'kyoto')
        self.assertFalse(DEFAULT_CONFIG['active_drain'])


if __name__ == '__main__':
    unittest.main()
