"""Real CLI/worker/runner qualification with controlled sensor failures.

Reuse the existing isolated process harness. Only telemetry and controller
intervals are synthetic; admission, process completion, recovery, cancellation,
locks, and public diagnostics run through Kyoto's normal worker.
"""
from pathlib import Path
import subprocess
import sys
import unittest

import test_hill_integration as integration


FAULT_WORKER = integration.WORKER.replace('kyoto.worker(Path(sys.argv[2]))', '''
import hill_telemetry
hill_telemetry.TELEMETRY_GRACE_SECONDS = 3
fault_root = Path(sys.argv[2])
original_sample_host = kyoto.sample_host

def faultable_host():
    sample = original_sample_host()
    if (fault_root / 'disk-missing').exists():
        sample['disk_counters'] = dict(valid=False, monotonic=sample['sample_monotonic'],
                                      devices={}, error='injected sensor failure')
    if (fault_root / 'memory-pressure').exists():
        sample['pressure'] = 'warning'
    return sample

kyoto.sample_host = faultable_host
kyoto.worker(fault_root)
''')


class TelemetryIntegrationTest(integration.HillIntegrationTest):
    def start_worker(self):
        self.worker = subprocess.Popen(
            [sys.executable, '-c', FAULT_WORKER, str(Path(integration.SCRIPT).parent), str(self.root)],
            cwd=self.root, env=self.env, stdout=self.worker_log,
            stderr=self.worker_log, start_new_session=True)

    def qualified_queue(self):
        self.configure('--mode', 'hill-climb', '--max-running', '2')
        first = self.submit('first', reporting=True, resource='one', record_starts=True)
        survivor = self.submit('survivor', reporting=True, resource='two', record_starts=True)
        queued = self.submit('next', reporting=True, resource='three', record_starts=True)
        self.configure('--resume')
        self.wait_for(lambda: self.runtime()['hill_climb']['holding']
                      and self.runtime()['hill_climb']['qualified_target'] == 2,
                      'qualified two-slot controller')
        self.assertEqual(self.row(queued)['status'], 'queued')
        return first, survivor, queued

    def lose_disk(self):
        (self.root / 'disk-missing').touch()
        return self.wait_for(lambda: self.runtime()['hill_climb']['admission_paused_for_telemetry']
                             and self.runtime(), 'closed telemetry admission gate')

    def test_brief_gap_blocks_refill_then_restores_learned_capacity(self):
        first, survivor, queued = self.qualified_queue()
        attempt = self.row(survivor)['attempt_token']
        snapshot = self.lose_disk()
        self.assertEqual(snapshot['effective_max_running'], 0)
        self.assertEqual(snapshot['hill_climb']['target'], 2)
        self.release('first')
        self.wait_for(lambda: self.row(first)['status'] == 'succeeded', 'first completion during gap')
        self.assertEqual(self.row(queued)['status'], 'queued')
        (self.root / 'disk-missing').unlink()
        self.wait_for(lambda: self.row(queued)['status'] == 'running', 'refill after measured recovery')
        self.wait_for(lambda: self.runtime()['hill_climb']['telemetry_state'] == 'measured'
                      and self.runtime()['hill_climb']['qualified_target'] == 2,
                      'productive post-recovery window')
        self.assertEqual(self.row(survivor)['attempt_token'], attempt)
        self.assertEqual((self.root / 'starts-survivor').read_text(), 'started\n')
        health = self.cli('health')['runtime']['hill_climb']
        self.assertIn('per_job_rates', health)
        self.assertFalse(health['admission_paused_for_telemetry'])
        self.assertEqual(health['telemetry_grace_seconds'], 3)

    def test_persistent_gap_expires_without_killing_existing_work(self):
        first, survivor, queued = self.qualified_queue()
        attempts = [self.row(job)['attempt_token'] for job in (first, survivor)]
        self.lose_disk()
        self.wait_for(lambda: self.runtime()['hill_climb']['phase'] == 'unavailable'
                      and self.runtime()['hill_climb']['target'] == 1, 'sensor-grace expiry')
        self.assertEqual(self.runtime()['effective_max_running'], 1)
        self.assertEqual(self.row(queued)['status'], 'queued')
        for job, attempt in zip((first, survivor), attempts):
            self.assertEqual(self.row(job)['status'], 'running')
            self.assertEqual(self.row(job)['attempt_token'], attempt)
        self.release('first')
        self.wait_for(lambda: self.row(first)['status'] == 'succeeded', 'natural drain')
        self.assertEqual(self.row(queued)['status'], 'queued')

    def test_memory_pressure_supersedes_retained_limit(self):
        first, survivor, queued = self.qualified_queue()
        self.lose_disk()
        (self.root / 'memory-pressure').touch()
        self.wait_for(lambda: self.runtime()['hill_climb']['phase'] == 'blocked', 'memory safety override')
        state = self.runtime()
        self.assertEqual(state['effective_max_running'], 0)
        self.assertEqual(state['hill_climb']['target'], 1)
        self.assertFalse(state['hill_climb']['admission_paused_for_telemetry'])
        (self.root / 'disk-missing').unlink()
        self.wait_for(lambda: self.runtime()['disk_io'].get('state') == 'measured', 'disk recovery')
        self.assertEqual(self.runtime()['effective_max_running'], 0)
        self.assertEqual(self.row(queued)['status'], 'queued')
        (self.root / 'memory-pressure').unlink()
        self.wait_for(lambda: self.runtime()['effective_max_running'] == 1, 'pressure recovery')

    def test_coordinator_restart_during_gap_does_not_restore_optimizer_memory(self):
        first, survivor, queued = self.qualified_queue()
        attempts = [self.row(job)['attempt_token'] for job in (first, survivor)]
        before = self.lose_disk()
        self.worker.terminate()
        self.worker.wait(timeout=5)
        self.start_worker()
        self.wait_for(lambda: self.runtime().get('recovery_ready')
                      and self.runtime().get('worker_session') != before['worker_session'],
                      'replacement coordinator')
        self.wait_for(lambda: self.runtime()['hill_climb']['phase'] == 'unavailable', 'fresh optimizer fallback')
        self.assertEqual(self.runtime()['hill_climb']['target'], 1)
        self.assertEqual(self.row(queued)['status'], 'queued')
        for job, attempt in zip((first, survivor), attempts):
            self.assertEqual(self.row(job)['status'], 'running')
            self.assertEqual(self.row(job)['attempt_token'], attempt)
        for name in ('first', 'survivor'):
            self.assertEqual((self.root / ('starts-' + name)).read_text(), 'started\n')

    def test_cancellation_still_works_while_admission_is_closed(self):
        first, survivor, queued = self.qualified_queue()
        before = self.lose_disk()
        last_decision = max(d['at'] for d in before['hill_climb']['decisions'])
        self.cli('cancel', first)
        self.wait_for(lambda: self.row(first)['status'] not in ('queued', 'running'), 'cancellation during gap')
        self.assertNotEqual(self.row(first)['status'], 'succeeded')
        self.assertEqual(self.row(survivor)['status'], 'running')
        self.assertEqual(self.row(queued)['status'], 'queued')
        # Cancellation must not be recast as a successful exploratory probe.
        self.assertFalse(any(d['action'] == 'accepted' and d['at'] > last_decision
                             for d in self.runtime()['hill_climb']['decisions']))


def load_tests(loader, tests, pattern):
    # The original module discovers its original scenarios. Reuse only its
    # harness here, rather than silently running inherited tests a second time.
    return unittest.TestSuite(TelemetryIntegrationTest(name) for name in sorted(
        name for name in TelemetryIntegrationTest.__dict__ if name.startswith('test_')))
