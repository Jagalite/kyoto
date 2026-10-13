#!/usr/bin/env python3
"""Reproduce objective failures at 4c8e37b using comparable work units.

Assertions identify the known failures; passing this audit reproduces the bugs,
not scheduler correctness. Rerun after a fix and revise the expected outcomes.
No commands, worker processes, or shared queue state are used.
"""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hill_climbing import HillClimbingController

HOST = dict(valid=True, cpu_percent=40, available_mb=10000, total_mb=16000,
    pressure='normal', swap_in_mb_per_second=0, swap_out_mb_per_second=0,
    disk_pressure=False, effective_max_running=8, concurrency_ceiling=8,
    disk_io=dict(valid=True, state='measured', devices={'disk0': dict(
        read_ms=2, read_iops=50, write_ms=2, write_iops=50)}))

def job(name, value):
    return dict(id=name, started=0, attempt_token='attempt-1', progress=dict(
        measurable=True, confidence='high', measurement=dict(
            value=value, total=10000, source='explicit', epoch='same-work-unit')))

def update(controller, now, values, cap=8):
    config = dict(mode='hill-climb', max_running=cap, cpu_budget=cap, pressure_aware=True)
    return controller.update(config, HOST, [dict(id='next')],
        [job(name, value) for name, value in values.items()], now)[1]['hill_climb']

def result(name, view, before, after):
    return dict(scenario=name, resource_factor=view['resource_factor'],
        actual_before_units_per_second=before,
        actual_after_units_per_second=after,
        actual_ratio=after/before,
        reported_ratio=view['objective'], target=view['target'],
        decision=view['decisions'][-1])

results=[]
for name, incumbent_rate, newcomer_rate in (
        ('accepts_39_percent_loss', 60, 1),
        ('rejects_40_percent_gain', 40, 100)):
    controller=HillClimbingController()
    update(controller, 0, {'a':0})
    update(controller, 10, {'a':1000})
    update(controller, 11, {'a':1000+incumbent_rate,'b':0})
    view=update(controller, 21, {'a':1000+11*incumbent_rate,'b':10*newcomer_rate})
    results.append(result(name,view,100,incumbent_rate+newcomer_rate))

controller=HillClimbingController()
update(controller,0,{'a':0})
update(controller,10,{'a':1000})
update(controller,11,{'a':1100,'b':0})
update(controller,21,{'a':2100,'b':10})
update(controller,22,{'a':2200,'b':11})
update(controller,42,{'a':4200,'b':31})
update(controller,43,{'a':4240,'b':33,'c':0})
view=update(controller,53,{'a':4640,'b':53,'c':10})
results.append(result('accepts_57_percent_loss_heterogeneous_incumbents',view,101,43))

controller=HillClimbingController()
update(controller,0,{'a':0},2)
update(controller,10,{'a':1000},2)
# b will complete naturally at t=43; its current value reflects work completed before admission.
update(controller,11,{'a':1100,'b':9968},2)
update(controller,21,{'a':2100,'b':9978},2)
update(controller,22,{'a':2200,'b':9979},2)
update(controller,42,{'a':4200,'b':9999},2)
update(controller,43,{'a':4310},2)
view=update(controller,53,{'a':5410},2)
results.append(result('rejects_8_9_percent_gain_after_low_rate_job_drains',view,101,110))

assert results[0]['decision']['action']=='accepted' and results[0]['actual_ratio']<1
assert results[1]['decision']['action']=='rejected' and results[1]['actual_ratio']>1.05
assert results[2]['decision']['action']=='accepted' and results[2]['actual_ratio']<1
assert results[3]['decision']['action']=='rejected' and results[3]['actual_ratio']>1.05
print(json.dumps(results,indent=2))
