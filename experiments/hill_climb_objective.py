#!/usr/bin/env python3
"""Check four corrected objective decisions through the production controller.

Every command has the same total, so the measured phase-completion ratio must
equal the aggregate raw-work ratio. Assertions check numerical and decision
correctness for these counterexamples, not general scheduling convergence or
hardware performance. The last case explicitly starts a measured downward trial
to isolate its score from the policy deciding when to explore downward.
No commands, worker processes, or shared queue state are used.
"""
import json
import math
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hill_climbing import HillClimbingController, _measurements
from progress_objective import measure_progress

HOST = dict(valid=True, cpu_percent=40, available_mb=10000, total_mb=16000,
    pressure='normal', swap_in_mb_per_second=0, swap_out_mb_per_second=0,
    disk_pressure=False, effective_max_running=8, concurrency_ceiling=8,
    disk_io=dict(valid=True, state='measured', devices={'disk0': dict(
        read_ms=2, read_iops=50, write_ms=2, write_iops=50)}))
TOTAL = 10000

def job(name, value):
    return dict(id=name, started=0, attempt_token='attempt-1', progress=dict(
        measurable=True, confidence='high', measurement=dict(
            value=value, total=TOTAL, source='explicit', epoch='same-work-unit')))

def update(controller, now, values, cap=8):
    config = dict(mode='hill-climb', max_running=cap, cpu_budget=cap, pressure_aware=True)
    return controller.update(config, HOST, [dict(id='next')],
        [job(name, value) for name, value in values.items()], now)[1]['hill_climb']

def result(name, view, before, after, expected_action):
    expected_ratio = after / before
    decision = view['decisions'][-1]
    assert decision['action'] == expected_action, (name, decision)
    assert math.isclose(view['objective'], expected_ratio, rel_tol=1e-12), (name, view)
    assert math.isclose(decision['progress_ratio'], expected_ratio, rel_tol=1e-12), (name, decision)
    return dict(scenario=name, resource_factor=view['resource_factor'],
        equal_total_per_phase=TOTAL,
        actual_before_units_per_second=before,
        actual_after_units_per_second=after,
        actual_before_phase_fractions_per_second=before/TOTAL,
        actual_after_phase_fractions_per_second=after/TOTAL,
        actual_ratio=expected_ratio,
        reported_ratio=view['objective'], target=view['target'],
        decision=decision)

results=[]
for name, incumbent_rate, newcomer_rate, expected_action in (
        ('rejects_39_percent_loss', 60, 1, 'rejected'),
        ('accepts_40_percent_gain', 40, 100, 'accepted')):
    controller=HillClimbingController()
    update(controller, 0, {'a':0})
    update(controller, 10, {'a':1000})
    update(controller, 11, {'a':1000+incumbent_rate,'b':0})
    view=update(controller, 21, {'a':1000+11*incumbent_rate,'b':10*newcomer_rate})
    results.append(result(name,view,100,incumbent_rate+newcomer_rate,expected_action))

controller=HillClimbingController()
update(controller,0,{'a':0})
update(controller,10,{'a':1000})
update(controller,11,{'a':1100,'b':0})
# First qualify two slots with a real 10% gain. The next baseline has unequal
# incumbent rates (100 and 1), where mean relative speed previously mis-scored.
update(controller,21,{'a':2100,'b':100})
update(controller,22,{'a':2200,'b':101})
update(controller,42,{'a':4200,'b':121})
update(controller,43,{'a':4240,'b':123,'c':0})
view=update(controller,53,{'a':4640,'b':143,'c':10})
results.append(result('rejects_57_percent_loss_heterogeneous_incumbents',view,101,43,'rejected'))

controller=HillClimbingController()
update(controller,0,{'a':0},2)
update(controller,10,{'a':1000},2)
# b's first observed counter is near completion; it completes naturally at t=43.
update(controller,11,{'a':1100,'b':TOTAL-122},2)
update(controller,21,{'a':2100,'b':TOTAL-22},2)
before = {'a':2200,'b':TOTAL-21}
after = {'a':4200,'b':TOTAL-1}
update(controller,22,before,2)
update(controller,42,after,2)
start = _measurements([job(name,value) for name,value in before.items()])
end = _measurements([job(name,value) for name,value in after.items()])
baseline = dict(measure_progress(start,end,20), count=len(end), elapsed=20,
                factor=1, measurements=end)
assert baseline['eligible'] and controller.target == 2
controller._begin_probe(42,baseline,'down')
update(controller,43,{'a':4310},2)
view=update(controller,53,{'a':5410},2)
results.append(result('accepts_8_9_percent_gain_after_low_rate_job_drains',view,101,110,'accepted'))

print(json.dumps(results,indent=2,allow_nan=False))
