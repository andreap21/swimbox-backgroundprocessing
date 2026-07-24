"""Swimbox Points calculator tests (Trello #490).

Self-contained assert-style script (house style: swimboxapis/tests).
Run:  venv/bin/python tests/test_swimbox_points.py
"""
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.ritmi import reference_paces
from services.swimbox_points import (
    calculate_swimbox_points,
    has_hr_signal,
    zone_coefficients,
    _assemble,
    _hr_engine,
    _pace_engine,
)

PASS = FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        print(f'  FAIL: {name}')


def zones_from_bounds(bounds, max_hr=None, resting_hr=None):
    return {
        'zones': [{'zone': i + 1, 'label': '', 'lower_bpm': lo, 'upper_bpm': hi,
                   'lower_pct': None, 'upper_pct': None}
                  for i, (lo, hi) in enumerate(bounds)],
        'source': 'derived',
        'derived_from': {'max_hr': max_hr, 'resting_hr': resting_hr},
    }


# Reference athlete (spec §2.2): Max 175, Rest 38, default HRR anchors →
# exactly what swimboxapis derive_zones produces.
HRR_ZONES = zones_from_bounds(
    [(107, 120), (121, 134), (135, 148), (149, 161), (162, 175)], 175, 38)
# Same athlete, %HRmax anchors (no resting HR known — tier 1/2 derivation).
HRMAX_ZONES = zones_from_bounds(
    [(88, 105), (106, 123), (124, 140), (141, 158), (159, 175)], 175, None)

CTX_HRR = {'hr_zones': HRR_ZONES, 'max_hr': 175, 'resting_hr': 38, 'critical_speed': None}


# ── Coefficients (§2) ────────────────────────────────────────────────────────
print('coefficients')
c = zone_coefficients(HRR_ZONES)
check('hrr coefficients', [round(x, 3) for x in c] == [1.256, 1.804, 2.591, 3.675, 5.211])
# Continuous function reproduces Stagno's published table (1.25/1.71/2.54/
# 3.61/5.16) within a few % at default boundaries — §2.3 allows ~4%; the
# small extra drift comes from bpm rounding of the stored bounds.
for got, published in zip(c, [1.25, 1.71, 2.54, 3.61, 5.16]):
    check(f'near stagno {published}', abs(got - published) / published < 0.06)
c_hrmax = zone_coefficients(HRMAX_ZONES)
check('hrmax fallback near stagno',
      all(abs(g - p) / p < 0.06 for g, p in zip(c_hrmax, [1.25, 1.71, 2.54, 3.61, 5.16])))
# Manually widened Z4 (upper stretched 161→170) raises its midpoint → higher
# c4: the continuous function tracks edited boundaries (§2.3).
widened = zones_from_bounds(
    [(107, 120), (121, 134), (135, 148), (149, 170), (171, 175)], 175, 38)
check('manual wide z4 raises c4', zone_coefficients(widened)[3] > c[3])
check('missing container -> None', zone_coefficients(None) is None)
check('degenerate max<=rest -> None',
      zone_coefficients(zones_from_bounds([(1, 2)] * 5, 40, 60)) is None)

# ── §8 worked examples (continuous coefficients shift them slightly from the
# spec's fixed-table 70/134/166/73 — within the declared ~4%) ────────────────
print('§8 worked examples')
for minutes, expected in [
    ({1: 15, 2: 30, 3: 0, 4: 0, 5: 0}, 73),    # 45 min easy technique
    ({1: 10, 2: 20, 3: 20, 4: 10, 5: 0}, 137),  # 60 min mixed aerobic
    ({1: 10, 2: 10, 3: 15, 4: 20, 5: 5}, 169),  # 60 min threshold
    ({1: 10, 2: 5, 3: 5, 4: 8, 5: 2}, 74),      # 30 min sprint set
]:
    total = sum(minutes.values()) * 60.0
    result = _assemble({z: minutes[z] * 60.0 for z in range(1, 6)}, total, c, 'hr')
    check(f'example -> {expected}', result['points'] == expected)
    check(f'example ppm {expected}',
          abs(result['points_per_minute'] - result['points'] / (total / 60)) < 0.05)

# ── Ritmi port golden values (captured from swimboxapis
# calculate_reference_paces(100, 90) — keep in sync guard) ───────────────────
print('ritmi port')
table = reference_paces(90)
GOLDEN = {
    25:  {'A1': 24.3432, 'A2': 21.735, 'B1': 20.7, 'B2': 19.665,
          'C1': 18.583425, 'C2': 17.468419, 'C3': 16.332972},
    100: {'A1': 99.4896, 'A2': 88.83, 'B1': 84.6, 'B2': 80.37,
          'C1': 75.94965, 'C2': 71.392671, 'C3': None},
    400: {'A1': 417.0096, 'A2': 372.33, 'B1': 354.6, 'B2': 336.87,
          'C1': None, 'C2': None, 'C3': None},
}
for d, row in GOLDEN.items():
    for zone, golden in row.items():
        got = table[d][zone]
        ok = (got is None) if golden is None else (got is not None and abs(got - golden) < 1e-4)
        check(f'ritmi {d}m {zone}', ok)

# ── HR engine (§3) ───────────────────────────────────────────────────────────
print('hr engine')
# Irregular dt, interior None (interpolates to 125 → Z2), above-max clamp,
# below-Z1 tail, trailing gap extended at last reading.
activity = {
    'duration': 360,
    'heart_rate_stream': {'time': [0, 60, 120, 180, 240],
                          'heartrate': [110, None, 140, 180, 100]},
}
zone_seconds, total = _hr_engine(activity, HRR_ZONES)
check('hr total = duration', total == 360)
check('hr z1', zone_seconds[1] == 60)   # [0,60) @110
check('hr z2', zone_seconds[2] == 60)   # [60,120) @interp 125
check('hr z3', zone_seconds[3] == 60)   # [120,180) @140
check('hr z4 empty', zone_seconds[4] == 0)
check('hr z5 clamp', zone_seconds[5] == 60)  # [180,240) @180 > zone bands → z5? (180 in 162-175? no → clamp)
check('below z1 unscored', sum(zone_seconds.values()) == 240)  # tail @100 < 107
result = calculate_swimbox_points(activity, CTX_HRR)
check('method hr', result['method'] == 'hr')
check('hr points', result['points'] == round(
    (60 * c[0] + 60 * c[1] + 60 * c[2] + 60 * c[4]) / 60))
check('ppm denominator includes unscored tail',
      abs(result['points_per_minute'] - result['points'] / 6.0) < 0.05)

# Leading edge: stream starts at t=30 → [0,30) scored at first sample.
leading = {'duration': 90, 'heart_rate_stream': {'time': [30, 90], 'heartrate': [110, 110]}}
zone_seconds, total = _hr_engine(leading, HRR_ZONES)
check('leading edge extended', zone_seconds[1] == 90 and total == 90)

# Average-only fallback: arrays unusable, summary average present.
avg_only = {'duration': 600, 'heart_rate_stream': {'time': [], 'heartrate': [],
                                                   'average_heartrate': 150}}
result = calculate_swimbox_points(avg_only, CTX_HRR)
check('avg fallback method hr', result['method'] == 'hr')
check('avg fallback all z4', result['zone_minutes'] == {'z1': 0.0, 'z2': 0.0, 'z3': 0.0,
                                                        'z4': 10.0, 'z5': 0.0})
check('has_hr_signal avg', has_hr_signal(avg_only))
check('has_hr_signal none', not has_hr_signal({'heart_rate_stream': {'heartrate': [None]}}))

# ── Pace engine (§4-5) ───────────────────────────────────────────────────────
print('pace engine')


def sprint_set(send_off, n=16):
    """16×25m, ~16s swim (C3 realised pace at cs=90) on a send-off."""
    return {'duration': n * send_off,
            'laps': [{'index': i, 'distance': 25.0, 'moving_time': 16,
                      'elapsed_time': send_off} for i in range(n)]}


# §5.4 behaviour check — 30s send-off: dense set (d=0.533) → whole-set
# effective duration → >90s attainment row → Z5 fraction present.
zone_seconds, total = _pace_engine(sprint_set(30), 90)
check('dense set total', total == 480)
check('dense set z5 present', zone_seconds[5] > 0)
check('dense set z4 dominant work', zone_seconds[4] == 256 * 0.75)
check('dense rest at z3 (C work)', abs(zone_seconds[3] - (256 * 0.10 + 224)) < 1e-6)
check('dense sums to elapsed', abs(sum(zone_seconds.values()) - 480) < 1e-6)

# 60s send-off: sparse (d=0.267) → rep duration (16s) → 70/30 Z3/Z4, no Z5.
zone_seconds, total = _pace_engine(sprint_set(60), 90)
check('sparse set no z5', zone_seconds[5] == 0)
check('sparse z4 fraction', abs(zone_seconds[4] - 256 * 0.30) < 1e-6)
check('sparse z3 = work + rest', abs(zone_seconds[3] - (256 * 0.70 + 16 * 44)) < 1e-6)
check('sparse sums to elapsed', abs(sum(zone_seconds.values()) - 960) < 1e-6)

# Long continuous B1 effort: 400m in 354s (B1 pace at cs=90) → all Z3.
continuous = {'duration': 360,
              'laps': [{'index': 0, 'distance': 400.0, 'moving_time': 354,
                        'elapsed_time': 354}]}
zone_seconds, total = _pace_engine(continuous, 90)
check('b1 continuous all z3 + z1 leftover',
      zone_seconds[3] == 354 and zone_seconds[1] == 6 and total == 360)

# Short A/B effort (<30s effective) scores one zone below ceiling.
short_ab = {'duration': 0,
            'laps': [{'index': 0, 'distance': 25.0, 'moving_time': 21,
                      'elapsed_time': 90}]}  # 21/25 = 0.84 s/m ≈ B1 pace @25m
zone_seconds, total = _pace_engine(short_ab, 90)
# Work (21s) drops to Z2; the 69s rest gap scores one below B1 → also Z2.
check('short B1 drops to z2', zone_seconds[2] == 90 and total == 90)

# Zero-distance rest laps (FIT) + leading rest scored Z1.
with_rest_laps = {'duration': 0,
                  'laps': [{'index': 0, 'distance': 0.0, 'moving_time': 60, 'elapsed_time': 60},
                           {'index': 1, 'distance': 400.0, 'moving_time': 354, 'elapsed_time': 354},
                           {'index': 2, 'distance': 0.0, 'moving_time': 30, 'elapsed_time': 30}]}
zone_seconds, total = _pace_engine(with_rest_laps, 90)
check('leading rest z1', zone_seconds[1] == 60)
check('trailing rest one below B1', zone_seconds[2] == 30)
check('rest laps total', total == 444)

# Degenerate inputs.
check('no laps -> None', _pace_engine({'laps': []}, 90) is None)
check('cs None -> None', _pace_engine(sprint_set(30), None) is None)
check('cs 0 -> None', _pace_engine(sprint_set(30), 0) is None)
check('all zero-distance -> None',
      _pace_engine({'laps': [{'distance': 0, 'moving_time': 60, 'elapsed_time': 60}]}, 90) is None)

# ── Orchestrator ─────────────────────────────────────────────────────────────
print('orchestrator')
# No HR → pace engine with the athlete's coefficients.
pace_activity = {**sprint_set(30)}
result = calculate_swimbox_points(pace_activity, {**CTX_HRR, 'critical_speed': 90})
check('pace method flag', result['method'] == 'pace_estimate')
check('pace zone minutes sum', abs(sum(result['zone_minutes'].values()) - 8.0) < 0.2)
# No zones at all → pace engine on fixed Stagno coefficients.
result_nz = calculate_swimbox_points(pace_activity,
                                     {'hr_zones': None, 'max_hr': None,
                                      'resting_hr': None, 'critical_speed': 90})
check('zone-less pace still scores', result_nz is not None and result_nz['points'] > 0)
# HR stream present but zones missing → falls through to pace engine.
hr_no_zones = {**sprint_set(30),
               'heart_rate_stream': {'time': [0, 60], 'heartrate': [120, 130]}}
result = calculate_swimbox_points(hr_no_zones,
                                  {'hr_zones': None, 'max_hr': None,
                                   'resting_hr': None, 'critical_speed': 90})
check('no zones -> pace fallback', result['method'] == 'pace_estimate')
# Avg below Z1 = junk summary data → HR engine declines, pace engine takes
# over (real case: dev pool swim with avg_hr=91 vs Z1 floor 107 + 37 laps).
junk_avg = {**sprint_set(30),
            'heart_rate_stream': {'time': [], 'heartrate': [], 'average_heartrate': 91}}
result = calculate_swimbox_points(junk_avg, {**CTX_HRR, 'critical_speed': 90})
check('junk avg falls to pace engine', result is not None and result['method'] == 'pace_estimate')
check('junk avg no laps -> None',
      calculate_swimbox_points({'duration': 600, 'heart_rate_stream':
                                {'average_heartrate': 91}}, CTX_HRR) is None)
# Neither engine → None.
check('nothing -> None',
      calculate_swimbox_points({'duration': 3600},
                               {'hr_zones': None, 'max_hr': None,
                                'resting_hr': None, 'critical_speed': None}) is None)
# Determinism (stub contract §7.2).
a, b = (calculate_swimbox_points(sprint_set(30), {**CTX_HRR, 'critical_speed': 90})
        for _ in range(2))
check('deterministic', a == b)

print(f'\n{PASS} passed, {FAIL} failed')
sys.exit(1 if FAIL else 0)
