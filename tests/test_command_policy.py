import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import kyoto
from command_policy import evaluate, read_policy, preset


class PolicyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = kyoto.connect(self.root)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def rule(self, action, command, param=None, value=None, maximum=None, scope='cargo'):
        self.db.execute('INSERT INTO command_policy_rules(action,command,parameter,value,max_value,scope) VALUES (?,?,?,?,?,?)',
                        (action,command,param,value,maximum,scope))
        self.db.commit()

    def allowed(self, *args):
        return evaluate(list(args), read_policy(self.db))['allowed']

    def test_defaults_and_admin_override(self):
        self.assertFalse(self.allowed('cargo','test'))
        self.assertTrue(self.allowed('cargo','test','--no-run'))
        self.assertFalse(self.allowed('cargo','test','--','--no-run'))
        self.rule('allow','test')
        self.assertTrue(self.allowed('cargo','t'))
        self.assertTrue(self.allowed('rustup','run','stable','cargo','test'))
        self.assertFalse(self.allowed('cargo','bench'))
        preset(self.db,'compile-only')
        self.assertFalse(self.allowed('cargo','test'))

    def test_value_rules_cannot_change_boolean_flag_parsing(self):
        self.rule('deny', '*', 'quiet', 'test')
        self.rule('deny', '*', 'no-run', 'ignored')
        self.assertFalse(self.allowed('cargo', '-q', 'test'))
        self.assertFalse(self.allowed('cargo', 'test', '-q'))
        self.assertTrue(self.allowed('cargo', 'test', '--no-run'))

    def test_equivalent_flags_and_all_occurrences(self):
        self.rule('limit','*','jobs',maximum=4)
        for args in (['-j8'],['-j','8'],['--jobs=8'],['--jobs','8'],['-qj8'],['-j2','-j8'],['--jobs=-1']):
            self.assertFalse(self.allowed('cargo','build',*args), args)
        self.assertTrue(self.allowed('cargo','build','-j4'))
        self.rule('deny','build','profile','release')
        for args in (['--release'],['-r'],['--profile','release'],['--profile=release']):
            self.assertFalse(self.allowed('cargo','b',*args), args)
        self.assertTrue(self.allowed('cargo','build','--profile','dev'))

    def test_scope_values_and_require(self):
        self.rule('allow','test')
        self.rule('deny','test','nocapture',scope='args')
        self.assertFalse(self.allowed('cargo','test','--','--nocapture'))
        self.assertTrue(self.allowed('cargo','test','--nocapture'))
        self.rule('require','build','profile','dev')
        self.assertFalse(self.allowed('cargo','build'))
        self.assertTrue(self.allowed('cargo','build','--profile=dev'))
        self.assertFalse(self.allowed('cargo','build','--config','--profile=dev'))
        self.assertTrue(self.allowed('cargo','build','--help'))

    def test_allowlist_and_persistence(self):
        preset(self.db,'deny-all')
        self.rule('allow','check')
        self.assertTrue(self.allowed('cargo','check'))
        self.assertFalse(self.allowed('cargo','build'))
        self.assertTrue(self.allowed('sh','-c','cargo build'))  # documented boundary
        other = kyoto.connect(self.root)
        self.assertEqual(read_policy(other),read_policy(self.db))
        other.close()

    def cli(self,*args):
        output=io.StringIO()
        connections=[]
        original=kyoto.connect
        def tracked(*args,**kwargs):
            db=original(*args,**kwargs);connections.append(db);return db
        with patch('sys.argv',['kyoto','--state',str(self.root),*args]), patch('kyoto.connect',tracked), patch('kyoto.ensure_worker'), patch('kyoto.recover_existing', return_value=True), contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
            try: kyoto.main()
            finally:
                for db in connections: db.close()
        return json.loads(output.getvalue())

    def test_cli_plan_submit_and_remove(self):
        with self.assertRaises(SystemExit): self.cli('plan','--','cargo','test')
        rules=self.cli('policy','allow','test')
        rule_id=rules['rules'][-1]['id']
        self.assertTrue(self.cli('plan','--','cargo','test')['policy']['allowed'])
        self.assertEqual(self.cli('submit','--','cargo','test')['command_kind'],'test-run')
        changed=self.cli('policy','remove',str(rule_id))
        self.assertEqual(len(changed['rejected_queued_jobs']),1)
        with self.assertRaises(SystemExit): self.cli('submit','--','cargo','test')
        self.cli('policy','limit','*','--param','j','--max-value','4')
        with self.assertRaises(SystemExit): self.cli('plan','--','cargo','build','-j8')

    def test_worker_rechecks_queued_job(self):
        self.cli('policy','allow','test')
        job=self.cli('submit','--','cargo','test')
        preset(self.db,'compile-only')
        self.db.commit()
        for key,value in [('pressure_aware',False),('max_running',1)]:
            self.db.execute('INSERT OR REPLACE INTO settings VALUES (?,?)',(key,json.dumps(value)))
        self.db.commit()
        worker=subprocess.Popen([sys.executable,str(Path(kyoto.__file__)),'--state',str(self.root),'_worker'])
        try:
            deadline=time.monotonic()+10
            while time.monotonic()<deadline:
                row=dict(self.db.execute('SELECT * FROM jobs WHERE id=?',(job['id'],)).fetchone())
                if row['status']=='failed':break
                time.sleep(.1)
            self.assertEqual(row['status'],'failed')
            self.assertIsNone(row['started'])
            self.assertIn('policy',row['error'])
            self.assertFalse((self.root/(job['id']+'.log')).exists())
        finally:
            worker.terminate();worker.wait(timeout=5)
