"""Busy recovery reads retry within the existing deadline, not on arbitrary errors."""
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import kyoto


class RecoveryContentionTest(unittest.TestCase):
    def database(self, root):
        db=sqlite3.connect(root/'queue.sqlite', check_same_thread=False)
        db.execute('CREATE TABLE runtime(id INTEGER PRIMARY KEY,value TEXT)')
        db.execute("INSERT INTO runtime VALUES (1,'{}')")
        db.commit()
        return db

    def test_real_exclusive_lock_releases_before_deadline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);writer=self.database(root)
            writer.execute('BEGIN EXCLUSIVE')
            release=threading.Timer(.25, writer.commit);release.start()
            try:
                with patch('kyoto.ensure_worker'),patch('kyoto.worker_ready',return_value=True):
                    self.assertTrue(kyoto.recover_existing(root, timeout=2))
            finally:
                release.join();writer.close()

    def test_busy_timeout_reports_unverified_not_saved_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);writer=self.database(root);writer.execute('BEGIN EXCLUSIVE')
            try:
                start=time.monotonic()
                with patch('kyoto.ensure_worker'),patch('kyoto.worker_ready',return_value=True) as ready:
                    self.assertFalse(kyoto.recover_existing(root, timeout=.08))
                    ready.assert_not_called()
                self.assertLess(time.monotonic()-start,.5)
            finally:writer.rollback();writer.close()

    def test_non_busy_errors_are_not_hidden(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);sqlite3.connect(root/'queue.sqlite').close()
            with patch('kyoto.ensure_worker'),self.assertRaisesRegex(sqlite3.OperationalError,'no such table'):
                kyoto.recover_existing(root, timeout=1)

    def test_python39_message_fallback_is_narrow_and_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.database(root).close()
            for message, retry in [('database is locked',True),('disk I/O error',False)]:
                with patch('kyoto.ensure_worker'),patch('kyoto.sqlite3.connect') as connection:
                    connection.return_value.execute.side_effect=sqlite3.OperationalError(message)
                    if retry:self.assertFalse(kyoto.recover_existing(root,timeout=0))
                    else:
                        with self.assertRaises(sqlite3.OperationalError):kyoto.recover_existing(root,timeout=0)
                    connection.return_value.close.assert_called_once()

if __name__=='__main__':unittest.main()
