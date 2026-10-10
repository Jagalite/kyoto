import unittest
from scheduling import DEFAULT_CONFIG, execution_plan


class ExecutionPlanTest(unittest.TestCase):
    def job(self, name, duration, submitted, resource):
        return dict(id=name, estimate=duration, submitted=submitted, resources=[resource])

    def test_smart_order_parallel_waves_and_conflicts(self):
        jobs = [self.job('long', 30, 99, '/a'), self.job('short', 5, 100, '/b'), self.job('medium', 10, 100, '/b')]
        plan = execution_plan(jobs, [], dict(DEFAULT_CONFIG, max_running=2), 101)
        self.assertEqual([j['id'] for j in plan], ['short', 'long', 'medium'])
        self.assertEqual([j['planned_start_in_seconds'] for j in plan], [0, 0, 5])
        self.assertEqual([j['planned_wave'] for j in plan], [1, 1, 2])

    def test_fifo_and_running_remaining_time(self):
        running = dict(self.job('running', 20, 80, '/a'), started=90)
        jobs = [self.job('long', 30, 99, '/b'), self.job('short', 5, 100, '/c')]
        plan = execution_plan(jobs, [running], dict(DEFAULT_CONFIG, mode='fifo', max_running=1), 100)
        self.assertEqual([j['id'] for j in plan], ['long', 'short'])
        self.assertEqual([j['planned_start_in_seconds'] for j in plan], [10, 40])
