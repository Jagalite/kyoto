"""Real CLI, worker and runner execution for the Kyoto scheduling mode."""
import unittest
import sys

import test_hill_integration as integration


class KyotoWorkerTest(integration.HillIntegrationTest):
    """Only Kyoto-specific cases; the original hill suite runs separately."""

    test_command_progress_reaches_public_status_and_worker_measurements = None
    test_default_mode_and_live_mode_switch_reset_experiment = None
    test_delayed_replacement_progress_preserves_learned_limit = None
    test_hill_growth_obeys_numeric_cap_and_explicit_resource_locks = None
    test_recovery_preserves_command_and_rebaselines_old_log_progress = None

    def test_kyoto_live_mode_applies_smart_ordering_and_measured_learning(self):
        self.configure('--mode', 'kyoto', '--max-running', '1')
        long = self.submit('long', reporting=True, resource='a')
        short = self.submit('short', reporting=True, resource='b')
        # An explicitly estimated short is preferred independent of submit
        # order, while active-process accounting still limits concurrency.
        self.db.execute('UPDATE jobs SET estimate=?, samples=1 WHERE id=?', (900, long))
        self.db.execute('UPDATE jobs SET estimate=?, samples=1 WHERE id=?', (10, short))
        self.db.commit()
        self.configure('--resume')
        self.wait_for(lambda: self.row(short)['status'] == 'running', 'short admission')
        self.assertEqual(self.row(long)['status'], 'queued')
        health = self.cli('health')['runtime']
        self.assertEqual(health['config']['mode'], 'kyoto')
        self.assertEqual(health['kyoto']['queue_policy'], 'smart')
        self.assertEqual(health['kyoto']['admission_limit'], 1)
        self.assertIn('hill_climb', health)
        self.release('short')
        self.finished(short)
        self.wait_for(lambda: self.row(long)['status'] == 'running', 'long admission')
        self.release('long')
        self.finished(long)

    def test_kyoto_worker_restart_keeps_active_attempt_and_reports_mode(self):
        self.configure('--mode', 'kyoto', '--max-running', '1')
        active = self.submit('active', reporting=True, record_starts=True)
        self.configure('--resume')
        self.wait_for(lambda: self.row(active)['status'] == 'running', 'running command')
        self.wait_for(lambda: (self.root / 'starts-active').exists(), 'actual command launch')
        original = self.row(active)['attempt_token']
        session = self.runtime()['worker_session']
        self.worker.terminate()
        self.worker.wait(timeout=5)
        self.start_worker()
        self.wait_for(lambda: self.runtime().get('recovery_ready')
                      and self.runtime()['worker_session'] != session,
                      'replacement coordinator')
        self.assertEqual(self.row(active)['attempt_token'], original)
        self.assertEqual((self.root / 'starts-active').read_text(), 'started\n')
        self.assertIn('kyoto', self.runtime())
        self.release('active')
        self.finished(active)

    def finite_batch(self, *, final_counter=False, fail=False):
        self.configure('--mode', 'kyoto', '--max-running', '4')
        ids=[]
        code="import time; time.sleep(.4)"
        if final_counter:
            code += "; print('KYOTO_PROGRESS {\"completed\":1,\"total\":1}',flush=True)"
        if fail:code += "; raise SystemExit(7)"
        for i in range(12):
            ids.append(self.cli('submit','--resource',str(self.root / str(i)),
                                '--',sys.executable,'-u','-c',code)['id'])
        self.configure('--resume')
        self.wait_for(lambda: all(self.row(j)['status'] not in ('queued','running') for j in ids),
                      'finite short batch',timeout=15)
        rows=[self.row(j) for j in ids]
        self.assertTrue(all(r['status']==('failed' if fail else 'succeeded') for r in rows))
        events=[(r['started'],1) for r in rows]+[(r['finished'],-1) for r in rows]
        actual=maximum=0
        for _,change in sorted(events):
            actual+=change;maximum=max(actual,maximum)
        self.assertLessEqual(maximum,4)
        return maximum

    def test_silent_successful_completions_enable_real_parallel_discovery(self):
        self.assertGreaterEqual(self.finite_batch(),2)

    def test_counter_only_at_completion_does_not_reset_discovery(self):
        self.assertGreaterEqual(self.finite_batch(final_counter=True),2)

    def test_failed_jobs_do_not_supply_fast_discovery_evidence(self):
        self.assertEqual(self.finite_batch(fail=True),1)


if __name__ == '__main__':
    unittest.main()
