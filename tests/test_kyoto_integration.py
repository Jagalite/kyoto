"""Real CLI, worker and runner execution for the Kyoto scheduling mode."""
import unittest

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


if __name__ == '__main__':
    unittest.main()
