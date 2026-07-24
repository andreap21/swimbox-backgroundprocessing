"""Swimbox Points end-to-end simulation (Trello #490) — READ-ONLY.

Runs the REAL worker code path on a user's actual activities: each activity
goes through services.performance._compute_swimbox_points, which fetches
the athlete and (lazily) the HR zones over the swimboxapis API exactly like
the production celery task — the ONLY thing skipped is the final mark PATCH,
so nothing is ever written.

Usage:
    set -a && source .env && set +a
    venv/bin/python tests/simulate_points.py <email> [detail_rows]

SWIMBOXAPIS_URL + SWIMBOXAPIS_CLIENT_TOKEN from the environment decide which
deployment is exercised (point them at dev to test a release).
"""
import os
import sys
from collections import Counter

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.performance import _compute_swimbox_points, SWIM_SPORT_TYPES

BASE = os.getenv('SWIMBOXAPIS_URL', '')
HEADERS = {'Authorization': f"Bearer {os.getenv('SWIMBOXAPIS_CLIENT_TOKEN', '')}"}
EMAIL = sys.argv[1] if len(sys.argv) > 1 else None
DETAIL_ROWS = int(sys.argv[2]) if len(sys.argv) > 2 else 40

if not EMAIL or not BASE:
    sys.exit('usage: simulate_points.py <email> [detail_rows]  '
             '(SWIMBOXAPIS_URL + SWIMBOXAPIS_CLIENT_TOKEN must be set)')

users = requests.get(f'{BASE}/users/', params={'email': EMAIL},
                     headers=HEADERS, timeout=30).json()
if not isinstance(users, list) or not users:
    sys.exit(f'no user with email {EMAIL} on {BASE} ({users})')
user_id = users[0]['id']
print(f"{BASE}  user {user_id} ({users[0].get('name')})")

activities = requests.get(f'{BASE}/activities/by-user/{user_id}',
                          headers=HEADERS, timeout=60).json()
if not isinstance(activities, list):
    sys.exit(f'activities fetch failed: {activities}')
print(f'{len(activities)} activities — running the REAL worker compute path '
      f'(no PATCH, read-only)\n')

header = (f"{'date':<11} {'type':<8} {'dur':>6} {'dist':>6} {'hr':>5} {'laps':>4} "
          f"{'stored':>6} | {'sim':>4} {'ppm':>5} {'method':<13} zone_minutes")
print(header)
print('-' * len(header))
summary = Counter()
for i, act in enumerate(activities):
    sport = act.get('sport_type') or act.get('type') or '?'
    is_swim = sport in SWIM_SPORT_TYPES
    result = _compute_swimbox_points(act)     # ← the real production function
    summary[('swim' if is_swim else 'non-swim',
             result['method'] if result else 'no engine')] += 1
    if i >= DETAIL_ROWS:
        continue
    stream = act.get('heart_rate_stream') or {}
    n_hr = sum(1 for h in (stream.get('heartrate') or []) if h is not None)
    if result:
        zm = ' '.join(f'{k}:{v}' for k, v in result['zone_minutes'].items() if v)
        sim = (f"{result['points']:>4} {result['points_per_minute']:>5} "
               f"{result['method']:<13} {zm}")
    else:
        sim = f"{'—':>4} {'—':>5} {'no engine':<13}"
    gate = '' if is_swim else '  [non-swim: GATED today]'
    print(f"{str(act.get('starting_date') or '')[:10]:<11} {sport:<8} "
          f"{act.get('duration') or 0:>6} {act.get('distance') or 0:>6.0f} "
          f"{n_hr:>5} {len(act.get('laps') or []):>4} "
          f"{str(act.get('swimbox_points')):>6} | {sim}{gate}")

print('\nSummary (sport bucket, engine):')
for (bucket, method), n in sorted(summary.items()):
    print(f'  {bucket:<9} {method:<14} {n:>4}')
print('\nREAD-ONLY: no PATCH was issued; stored values are untouched.')
