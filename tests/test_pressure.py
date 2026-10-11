import unittest
from unittest.mock import patch

from pressure import AdaptiveController, parse_linux, parse_vm_stat, sample_host
from scheduling import DEFAULT_CONFIG, choose_job
from test_scheduling import job


def host(t, **overrides):
    return dict(valid=True, sample_monotonic=t, sampled_at=t, available_mb=10000,
                total_mb=16000, swap_used_mb=8000, swap_in_mb=100, swap_out_mb=100,
                pressure='normal', load_average_1m=0, **overrides)


class PressureTest(unittest.TestCase):
    def setUp(self):
        self.controller = AdaptiveController()
        self.config = dict(DEFAULT_CONFIG, mode='smart', max_running='auto', cpu_budget=8,
                           memory_budget_mb=8192)

    def update(self, t, changes=None, queued=None, running=None):
        sample = host(t)
        sample.update(changes or {})
        return self.controller.update(self.config, sample, queued or [job('next')], running or [], t)

    def test_swap_occupancy_does_not_block_and_auto_ramps(self):
        policy, snapshot = self.update(0)
        self.assertEqual(policy['max_running'], 1)
        policy, snapshot = self.update(10, running=[job('active')])
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(snapshot['swap_out_mb_per_second'], 0)
        self.assertEqual(snapshot['swap_used_mb'], 8000)

    def test_swap_activity_blocks_and_recovery_is_gradual(self):
        self.update(0)
        policy, snapshot = self.update(2, {'swap_out_mb': 104})
        self.assertEqual(policy['max_running'], 0)
        self.assertEqual(snapshot['admission_reason'], 'active swapping')
        self.assertEqual(snapshot['swap_out_mb_per_second'], 2)
        self.assertEqual(self.update(4, {'swap_out_mb': 104})[0]['max_running'], 0)
        self.assertEqual(self.update(12, {'swap_out_mb': 104})[0]['max_running'], 1)
        self.assertEqual(self.update(22, {'swap_out_mb': 104}, running=[job('active')])[0]['max_running'], 2)

    def test_pressure_reduces_limit_without_mutating_running_jobs(self):
        running = [job('a'), job('b')]
        policy, snapshot = self.update(0, {'pressure': 'warning'}, running=running)
        self.assertEqual(policy['max_running'], 0)
        self.assertIsNone(choose_job([job('next')], running, policy, 0, 10000)[0])
        self.assertEqual(len(running), 2)

    def test_modest_swap_reads_and_writes_block_admission(self):
        for counter in ('swap_in_mb', 'swap_out_mb'):
            self.controller = AdaptiveController()
            self.update(0)
            policy, snapshot = self.update(2, {counter: 100.125})
            self.assertEqual(policy['max_running'], 0)
            self.assertEqual(snapshot['admission_reason'], 'active swapping')

    def test_fixed_cap_and_fifo_auto(self):
        self.config['mode'] = 'fifo'
        self.assertEqual(self.update(0)[0]['max_running'], 1)
        self.config['max_running'] = 3
        self.assertEqual(self.update(2)[0]['max_running'], 3)
        self.assertEqual(self.update(4, {'pressure': 'critical'})[0]['max_running'], 0)
        self.config['pressure_aware'] = False
        self.assertEqual(self.update(6, {'pressure': 'critical'})[0]['max_running'], 3)

    def test_load_bounds_ceiling_without_memory_reservations(self):
        _, snapshot = self.update(0, {'load_average_1m': 7})
        self.assertEqual(snapshot['concurrency_ceiling'], 1)
        _, snapshot = self.update(2, {'available_mb': 2500}, queued=[job('large', memory=1024)])
        self.assertEqual(snapshot['concurrency_ceiling'], 8)
        self.assertEqual(snapshot['effective_memory_reserve_mb'], 0)
        policy, snapshot = self.update(4, {'available_mb': 1}, queued=[job('large', memory=99999)])
        self.assertEqual(policy['max_running'], 1)

    def test_unknown_telemetry_blocks_when_enabled(self):
        policy, snapshot = self.update(0, {'valid': False, 'available_mb': None})
        self.assertEqual(policy['max_running'], 0)
        self.assertIn('unavailable', snapshot['admission_reason'])

    def test_counter_reset_is_not_negative_activity(self):
        self.update(0)
        _, snapshot = self.update(2, {'swap_out_mb': 0})
        self.assertIsNone(snapshot['swap_out_mb_per_second'])
        self.assertEqual(snapshot['effective_max_running'], 1)

    def test_idle_does_not_ramp_up(self):
        sample = host(0)
        self.controller.update(self.config, sample, [], [], 0)
        sample = host(100)
        policy, _ = self.controller.update(self.config, sample, [], [], 100)
        self.assertEqual(policy['max_running'], 1)

    def test_linux_parser_counts_swap_pages_and_psi(self):
        mem = 'MemTotal: 8192000 kB\nMemAvailable: 2048000 kB\nSwapTotal: 1024000 kB\nSwapFree: 512000 kB\n'
        vm = 'pswpin 100\npswpout 200\n'
        result = parse_linux(mem, vm, 4096, 'some avg10=12.00 avg60=0.00 total=100\nfull avg10=0.00 avg60=0.00 total=0\n')
        self.assertEqual(result['available_mb'], 2000)
        self.assertEqual(result['swap_used_mb'], 500)
        self.assertEqual(result['swap_out_mb'], 200 / 256)
        self.assertEqual(result['pressure'], 'warning')
        self.assertEqual(parse_linux(mem, vm, 4096)['pressure'], 'unknown')

    def test_macos_parser_page_size(self):
        output = 'Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: 100.\nPages inactive: 200.\nPages speculative: 20.\nSwapins: 10.\nSwapouts: 30.\n'
        result = parse_vm_stat(output)
        self.assertEqual(result['available_mb'], 5)
        self.assertEqual(result['swap_out_mb'], 30 / 64)

    def test_old_reservations_ignored_while_path_locks_remain(self):
        policy = dict(self.config, max_running=4, min_available_memory_mb=500)
        running = [job('recent', memory=800, current_rss_mb=700, host_accounted_rss_mb=0)]
        self.assertIsNotNone(choose_job([job('next', memory=400)], running, policy, 0, 1600)[0])
        self.assertIsNone(choose_job([job('next', resources=['/tmp/recent'])], running, policy, 0, 1600)[0])

    def test_sample_failure_is_explicit(self):
        with patch('pressure.sys.platform', 'darwin'), patch('pressure.bounded_output', side_effect=OSError('unavailable')):
            self.assertFalse(sample_host()['valid'])


class CPUUsageTest(unittest.TestCase):
    def test_cpu_tick_deltas_and_unavailable_samples(self):
        from cpu import utilization
        self.assertEqual(utilization([100, 100, 100, 0], [125, 125, 150, 0], [2]), 50)
        self.assertEqual(utilization([0] * 8, [20, 0, 10, 50, 20, 0, 0, 0], [3, 4]), 30)
        self.assertIsNone(utilization(None, [1, 2, 3, 4], [2]))
        self.assertIsNone(utilization([1, 2, 3, 4], [1, 2, 3, 4], [2]))
        self.assertIsNone(utilization([100, 0, 0, 0], [1, 0, 0, 0], [2]))

    def test_controller_reports_cpu_percentage(self):
        controller = AdaptiveController()
        controller.update(DEFAULT_CONFIG, host(0, cpu_ticks=[0, 0, 0, 0], cpu_idle_indices=[2]), [], [], 0)
        _, result = controller.update(DEFAULT_CONFIG, host(2, cpu_ticks=[20, 10, 70, 0], cpu_idle_indices=[2]), [], [], 2)
        self.assertEqual(result['cpu_percent'], 30)

    def test_linux_sampler_excludes_double_counted_guest_ticks(self):
        from cpu import sample_ticks
        with patch('cpu.sys.platform', 'linux'), patch('cpu.Path.read_text', return_value='cpu 10 20 30 40 50 60 70 80 90 100\n'):
            ticks, idle = sample_ticks()
        self.assertEqual(ticks, [10, 20, 30, 40, 50, 60, 70, 80])
        self.assertEqual(idle, [3, 4])


if __name__ == '__main__':
    unittest.main()
