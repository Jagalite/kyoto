import copy
import json
import unittest

from progress_objective import measure_progress


def samples(values, totals=None):
    totals = totals or {}
    return {(name, 'attempt-1', '0', 'explicit', 'phase-1'):
            dict(value=value, total=totals.get(name, 1000000), source='explicit',
                 epoch='phase-1', confidence='high') for name, value in values.items()}


def observation(rates, totals=None):
    return measure_progress(samples(dict.fromkeys(rates, 0), totals), samples(rates, totals), 1)


class ProgressObjectiveTest(unittest.TestCase):
    def test_rejects_the_old_39_percent_loss(self):
        baseline = observation({'a': 100})
        trial = observation({'a': 60, 'b': 1})
        self.assertAlmostEqual(trial['progress_per_second'] / baseline['progress_per_second'], .61)

    def test_counts_the_newcomers_actual_40_percent_gain(self):
        baseline = observation({'a': 100})
        trial = observation({'a': 40, 'b': 100})
        self.assertAlmostEqual(trial['progress_per_second'] / baseline['progress_per_second'], 1.4)

    def test_slow_incumbent_doubling_does_not_hide_57_percent_aggregate_loss(self):
        baseline = observation({'a': 100, 'b': 1})
        trial = observation({'a': 40, 'b': 2, 'c': 1})
        self.assertAlmostEqual(trial['progress_per_second'] / baseline['progress_per_second'], 43 / 101)

    def test_removing_slow_job_preserves_actual_8_9_percent_gain(self):
        baseline = observation({'a': 100, 'b': 1})
        trial = observation({'a': 110})
        self.assertAlmostEqual(trial['progress_per_second'] / baseline['progress_per_second'], 110 / 101)

    def test_unequal_totals_measure_completed_fractions_not_raw_count_units(self):
        baseline = observation({'a': 100}, {'a': 1000})
        trial = observation({'a': 40, 'b': 100}, {'a': 1000, 'b': 1000000})
        self.assertAlmostEqual(trial['progress_per_second'] / baseline['progress_per_second'], .401)
        self.assertAlmostEqual(trial['per_job_rates']['b']['fraction_per_second'], .0001)
        self.assertEqual(trial['per_job_rates']['b']['rate_per_second'], 100)

    def test_rescaling_one_commands_counter_and_total_preserves_objective(self):
        original = observation({'a': 5, 'b': 10}, {'a': 100, 'b': 100})
        rescaled = observation({'a': 5, 'b': 100000}, {'a': 100, 'b': 1000000})
        self.assertAlmostEqual(original['progress_per_second'], rescaled['progress_per_second'])
        self.assertNotEqual(original['per_job_rates']['b']['rate_per_second'],
                            rescaled['per_job_rates']['b']['rate_per_second'])

    def test_missing_total_preserves_raw_diagnostics_but_prevents_scoring(self):
        measured = observation({'a': 5, 'b': 10000}, {'b': None})
        self.assertTrue(measured['valid'])
        self.assertFalse(measured['eligible'])
        self.assertIsNone(measured['progress_per_second'])
        self.assertEqual(measured['per_job_rates']['b']['rate_per_second'], 10000)
        self.assertIsNone(measured['per_job_rates']['b']['fraction_per_second'])
        self.assertIn('unknown', measured['reason'])

    def test_unusable_totals_do_not_enter_scoring_or_serializable_diagnostics(self):
        for total in (None, 0, -1, float('inf'), 10 ** 400, True, '100'):
            with self.subTest(total=total):
                measured = observation({'a': 1}, {'a': total})
                self.assertTrue(measured['valid'])
                self.assertFalse(measured['eligible'])
                self.assertIsNone(measured['per_job_rates']['a']['total'])
                json.dumps(measured['per_job_rates'], allow_nan=False)

    def test_nan_total_invalidates_window(self):
        measured = observation({'a': 1}, {'a': float('nan')})
        self.assertFalse(measured['valid'])
        self.assertFalse(measured['eligible'])

    def test_unknown_total_sources_remain_displayable(self):
        for source in ('cargo-artifacts', 'cargo-starts'):
            with self.subTest(source=source):
                start, end = samples({'a': 1}, {'a': None}), samples({'a': 4}, {'a': None})
                for sample in (start, end):
                    next(iter(sample.values())).update(source=source, confidence='low')
                measured = measure_progress(start, end, 3)
                self.assertEqual(measured['per_job_rates']['a']['source'], source)
                self.assertEqual(measured['per_job_rates']['a']['rate_per_second'], 1)
                self.assertFalse(measured['eligible'])

    def test_zero_progress_is_an_eligible_measurement(self):
        measured = observation({'a': 0, 'b': 0})
        self.assertTrue(measured['valid'])
        self.assertTrue(measured['eligible'])
        self.assertEqual(measured['progress_per_second'], 0)

    def test_elapsed_and_nonzero_initial_counter_are_respected(self):
        start, end = samples({'a': 20}, {'a': 100}), samples({'a': 50}, {'a': 100})
        measured = measure_progress(start, end, 10)
        self.assertAlmostEqual(measured['progress_per_second'], .03)
        self.assertEqual(measured['per_job_rates']['a']['rate_per_second'], 3)
        self.assertEqual(measured['per_job_rates']['a']['confidence'], 'high')

    def test_counter_reset_does_not_credit_negative_work(self):
        measured = measure_progress(samples({'a': 20}), samples({'a': 1}), 10)
        self.assertFalse(measured['valid'])
        self.assertIsNone(measured['progress_per_second'])
        self.assertEqual(measured['per_job_rates'], {})

    def test_source_phase_total_and_attempt_changes_invalidate_window(self):
        for changes in (dict(source='libtest'), dict(epoch='phase-2'), dict(total=2000000), dict(total=None)):
            with self.subTest(changes=changes):
                start, end = samples({'a': 1}), samples({'a': 10})
                next(iter(end.values())).update(changes)
                self.assertFalse(measure_progress(start, end, 10)['valid'])
        start, end = samples({'a': 1}), samples({'a': 10})
        end[('a', 'attempt-2', '0', 'explicit', 'phase-1')] = end.pop(next(iter(end)))
        self.assertFalse(measure_progress(start, end, 10)['valid'])

    def test_unknown_total_becoming_known_needs_a_fresh_window(self):
        measured = measure_progress(samples({'a': 1}, {'a': None}), samples({'a': 10}), 10)
        self.assertFalse(measured['valid'])

    def test_boolean_total_cannot_alias_a_new_known_numeric_total(self):
        measured = measure_progress(samples({'a': 0}, {'a': True}),
                                    samples({'a': 1}, {'a': 1}), 10)
        self.assertFalse(measured['valid'])

    def test_empty_or_changed_cohort_is_not_a_complete_window(self):
        for start, end in (({}, {}), (samples({'a': 0}), samples({'a': 1, 'b': 1})),
                           (samples({'a': 0, 'b': 0}), samples({'a': 1}))):
            with self.subTest(start=start, end=end):
                self.assertFalse(measure_progress(start, end, 10)['valid'])

    def test_invalid_elapsed_values_cannot_score(self):
        for elapsed in (0, -1, float('nan'), float('inf'), 10 ** 400, True, '10'):
            with self.subTest(elapsed=elapsed):
                self.assertFalse(measure_progress(samples({'a': 0}), samples({'a': 1}), elapsed)['valid'])

    def test_nonfinite_invalid_or_excessive_counters_cannot_score(self):
        for value in (-1, float('nan'), float('inf'), 10 ** 400, True, '10', 1000001):
            with self.subTest(value=value):
                self.assertFalse(measure_progress(samples({'a': 0}), samples({'a': value}), 10)['valid'])

    def test_raw_rate_overflow_invalidates_window(self):
        measured = measure_progress(samples({'a': 0}, {'a': 1e308}),
                                    samples({'a': 1e308}, {'a': 1e308}), .1)
        self.assertFalse(measured['valid'])

    def test_normalized_rate_overflow_is_not_a_score(self):
        measured = measure_progress(samples({'a': 0}, {'a': 1e-308}),
                                    samples({'a': 1e-308}, {'a': 1e-308}), 1e-309)
        self.assertTrue(measured['valid'])
        self.assertFalse(measured['eligible'])
        self.assertIsNone(measured['per_job_rates']['a']['fraction_per_second'])
        json.dumps(measured['per_job_rates'], allow_nan=False)

    def test_aggregate_overflow_is_not_a_score(self):
        measured = measure_progress(samples({'a': 0, 'b': 0}, {'a': 1, 'b': 1}),
                                    samples({'a': 1, 'b': 1}, {'a': 1, 'b': 1}), 1e-308)
        self.assertTrue(measured['valid'])
        self.assertFalse(measured['eligible'])
        self.assertIsNone(measured['progress_per_second'])

    def test_measurement_is_pure(self):
        start, end = samples({'a': 0}), samples({'a': 20})
        before = copy.deepcopy((start, end))
        measure_progress(start, end, 10)
        self.assertEqual((start, end), before)


if __name__ == '__main__':
    unittest.main()
