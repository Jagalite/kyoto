import unittest
from short_job_discovery import ShortJobDiscovery

class ShortNumericTest(unittest.TestCase):
    def test_unresolvable_monotonic_interval_is_not_a_rate(self):
        e=ShortJobDiscovery()
        for name in ('a','b'):
            e.observe_result(dict(id=name,attempt_token=name,command_kind='build',
                status='succeeded',exit_code=0,started=0.,finished=1.),1e100)
        self.assertIsNone(e.evidence([dict(command_kind='build')],[],1e100,1e100))

    def test_overflowing_rate_is_not_exported(self):
        e=ShortJobDiscovery()
        for name in ('a','b'):
            e.observe_result(dict(id=name,attempt_token=name,command_kind='build',
                status='succeeded',exit_code=0,started=0.,finished=1e-320),0.)
        self.assertIsNone(e.evidence([dict(command_kind='build')],[],0.,0.))
