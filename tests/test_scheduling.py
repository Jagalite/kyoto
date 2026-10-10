import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kyoto import connect
from scheduling import DEFAULT_CONFIG, choose_job, conflicts, discover_resources, forecast


def job(id, submitted=0, estimate=10, cpu=1, memory=100, resources=None, **extra):
    return dict(id=id, submitted=submitted, estimate=estimate, cpu_slots=cpu,
                memory_mb=memory, resources=resources or ['/tmp/' + id], **extra)


class PolicyTest(unittest.TestCase):
    def setUp(self):
        self.config = dict(DEFAULT_CONFIG, mode='smart', max_running=2, cpu_budget=4,
                           memory_budget_mb=1000, aging_seconds=100)

    def test_small_jobs_first_and_fifo_preserves_order(self):
        jobs = [job('large', estimate=50), job('small', submitted=1, estimate=1)]
        self.assertEqual(choose_job(jobs, [], self.config, 2)[0]['id'], 'small')
        self.config['mode'] = 'fifo'
        self.assertEqual(choose_job(jobs, [], self.config, 2)[0]['id'], 'large')

    def test_aged_head_is_not_blocked_by_estimated_cpu_cost(self):
        jobs = [job('large', estimate=50, cpu=8), job('small', submitted=99, estimate=1)]
        self.assertEqual(choose_job(jobs, [job('active')], self.config, 101)[0]['id'], 'large')
        selected, reason = choose_job(jobs, [], self.config, 101)
        self.assertEqual(selected['id'], 'large')
        self.assertIn('aging priority', reason)

    def test_backfill_path_conflicts_and_fifo_head_blocking(self):
        active = job('active', cpu=3, memory=800)
        jobs = [job('big', cpu=2, memory=300, resources=['/tmp/active']), job('small', submitted=1, estimate=1)]
        self.assertEqual(choose_job(jobs, [active], self.config, 2)[0]['id'], 'small')
        self.config['mode'] = 'fifo'
        self.assertIsNone(choose_job(jobs, [active], self.config, 2)[0])

    def test_locks_include_aliases_parent_paths_and_wildcard(self):
        self.assertTrue(conflicts(['/a/target'], ['/a/target/debug']))
        self.assertFalse(conflicts(['/a/target'], ['/a/target-other']))
        self.assertTrue(conflicts(['*'], ['/anything']))
        self.assertTrue(conflicts(['/'], ['/anything']))
        candidate = job('b', resources=['/a/target'])
        self.assertIsNone(choose_job([candidate], [job('a', resources=['/a'])], self.config, 1)[0])

    def test_old_memory_reservations_do_not_block_admission(self):
        self.config['min_available_memory_mb'] = 819.2
        queued = [job('large', memory=3072)]
        selected, reason = choose_job(queued, [], self.config, 500, 1929)
        self.assertEqual(selected['id'], 'large')
        self.assertEqual(reason, 'aging priority')
        self.assertIsNotNone(choose_job(queued, [], self.config, 1, None)[0])

    def test_cost_hints_do_not_set_resource_limits(self):
        selected, _ = choose_job([job('new', memory=999999, cpu=999)],
            [job('active', memory=100, current_rss_mb=900)], self.config, 1)
        self.assertEqual(selected['id'], 'new')

    def test_forecast_matches_parallel_and_lock_scheduling(self):
        jobs = [job('a', estimate=10), job('b', estimate=2), job('c', estimate=3, resources=['/tmp/a'])]
        result = forecast(jobs, [], self.config, 1)
        self.assertEqual(result['b'], (0, 2))
        self.assertEqual(result['c'], (0, 3))
        self.assertEqual(result['a'], (3, 13))

    def test_cargo_metadata_and_fallback(self):
        meta = json.dumps(dict(workspace_root='/repo', target_directory='/shared', build_directory='/build'))
        with patch('scheduling.bounded_output', return_value=meta) as probe:
            paths, _ = discover_resources(['cargo', '+stable', 'build', '--target-dir', 'out', '--config', 'build.jobs=2'], '/repo', {}, [])
            self.assertEqual(paths, ['/build', '/repo', '/repo/out', '/shared'])
            self.assertIn('+stable', probe.call_args.args[0])
            self.assertIn('build.jobs=2', probe.call_args.args[0])
        with patch('scheduling.bounded_output', side_effect=ValueError()):
            self.assertEqual(discover_resources(['cargo', 'check'], '/repo', {}, [])[0], ['*'])
        self.assertEqual(discover_resources(['sh', '-c', 'cargo build'], '/repo', {}, [])[0], ['*'])
        self.assertEqual(discover_resources(['cargo', 'build', '-C', '/other'], '/repo', {}, [])[0], ['*'])

    def test_explicit_paths_resolve_symlinks(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'real').mkdir()
            (root / 'alias').symlink_to(root / 'real', target_is_directory=True)
            paths, _ = discover_resources(['anything'], folder, {}, ['real', 'alias'])
            self.assertEqual(paths, [str((root / 'real').resolve())])

    def test_legacy_schema_migration_preserves_job(self):
        with tempfile.TemporaryDirectory() as folder:
            db = sqlite3.connect(Path(folder) / 'queue.sqlite')
            db.execute('CREATE TABLE jobs (id TEXT PRIMARY KEY, command TEXT, cwd TEXT, environment TEXT, signature TEXT, status TEXT, submitted REAL, started REAL, finished REAL, exit_code INTEGER, estimate REAL, samples INTEGER, cancel INTEGER DEFAULT 0, error TEXT)')
            db.execute("INSERT INTO jobs (id,status) VALUES ('legacy','queued')")
            db.commit()
            db.close()
            migrated = connect(Path(folder))
            row = migrated.execute('SELECT * FROM jobs').fetchone()
            self.assertEqual(row['id'], 'legacy')
            self.assertEqual(row['resources'], '[]')
            self.assertEqual(row['resource_source'], 'command manages its own locks')
            self.assertEqual(row['cpu_slots'], 1)
            migrated.close()


if __name__ == '__main__':
    unittest.main()
