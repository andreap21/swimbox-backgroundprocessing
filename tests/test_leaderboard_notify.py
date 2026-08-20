"""Leaderboard ops-event tests (monitoring bot alert on entry generation).

Self-contained assert-style script (house style: swimboxapis/tests).
Run:  python3 tests/test_leaderboard_notify.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import services.performance as perf

PASS = FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        print(f"FAIL: {name}")


class _Resp:
    def __init__(self, status=200):
        self.status_code = status
        self.text = ''


# ---------------------------------------------------------- notifier unit

calls = []


def _fake_post(url, json=None, headers=None, timeout=None):
    calls.append({'url': url, 'json': json, 'headers': headers, 'timeout': timeout})
    return _Resp()


perf.requests.post = _fake_post
os.environ['SWIMBOXAPIS_URL'] = 'https://api.example'
os.environ['SWIMBOXAPIS_CLIENT_TOKEN'] = 'tok'

perf.notify_leaderboard_entries('auth0|abc', 'Piscina Blu',
                                [{'distance_m': 100, 'grade': 'B'},
                                 {'distance_m': 200, 'grade': 'A'}])
check('one POST fired', len(calls) == 1)
check('targets /ops-events', calls[0]['url'].endswith('/ops-events'))
check('action is leaderboard_entry', calls[0]['json']['action'] == 'leaderboard_entry')
check('carries user_id', calls[0]['json']['user_id'] == 'auth0|abc')
check('meta pool', calls[0]['json']['meta']['pool'] == 'Piscina Blu')
check('meta entries collapse to one line', calls[0]['json']['meta']['entries'] == '100m:B 200m:A')
check('bearer auth', calls[0]['headers']['Authorization'] == 'Bearer tok')

# Missing config: no call, no raise.
calls.clear()
os.environ['SWIMBOXAPIS_URL'] = ''
perf.notify_leaderboard_entries('auth0|abc', 'P', [{'distance_m': 100, 'grade': 'B'}])
check('skips silently when unconfigured', calls == [])
os.environ['SWIMBOXAPIS_URL'] = 'https://api.example'

# Network failure: swallowed.
def _boom(*a, **k):
    raise RuntimeError('down')
perf.requests.post = _boom
try:
    perf.notify_leaderboard_entries('auth0|abc', 'P', [{'distance_m': 100, 'grade': 'B'}])
    check('failure swallowed', True)
except Exception:
    check('failure swallowed', False)
perf.requests.post = _fake_post


# ------------------------------------- save_performances integration (fakes)

class _FakeCollection:
    def __init__(self):
        self.docs = []

    def find_one(self, q):
        return None

    def insert_one(self, doc):
        self.docs.append(dict(doc))


class _FakeDB(dict):
    def __init__(self, coll):
        super().__init__()
        self._coll = coll

    def __getitem__(self, name):
        return self._coll


def _run_save(activity):
    """Run save_performances with everything external faked out."""
    coll = _FakeCollection()
    orig = (perf.get_db, perf.evaluate_performance, perf.mark_activity_calculated,
            perf._ensure_indexes, perf._process_personal_records,
            perf._compute_swimbox_points)
    perf.get_db = lambda: _FakeDB(coll)
    perf.evaluate_performance = lambda a, d, p: 'B'
    perf.mark_activity_calculated = lambda aid, extra=None: None
    perf._ensure_indexes = lambda c: None
    perf._process_personal_records = lambda *a, **k: None
    perf._compute_swimbox_points = lambda a: None
    import services.athlete as ath
    orig_fetch = ath.fetch_user
    ath.fetch_user = lambda uid: {'name': 'Test Swimmer'}
    try:
        perf.save_performances(activity)
    finally:
        (perf.get_db, perf.evaluate_performance, perf.mark_activity_calculated,
         perf._ensure_indexes, perf._process_personal_records,
         perf._compute_swimbox_points) = orig
        ath.fetch_user = orig_fetch
    return coll


ACT = {
    'id': 'ACT1', 'user_id': 'auth0|abc', 'sport_type': 'Swim',
    'pool_id': 'POOL9', 'pool_name': 'Piscina Blu',
    'starting_date': '2026-08-20',
    'peaks': {'swim': {'100': {'duration_s': 90, 'pace_per_100m': 90},
                       '200': {'duration_s': 200, 'pace_per_100m': 100}}},
}

calls.clear()
coll = _run_save(dict(ACT))
check('two leaderboard rows written', len(coll.docs) == 2)
check('exactly ONE ops event per activity', len(calls) == 1)
check('event lists both distances',
      calls[0]['json']['meta']['entries'] in ('100m:B 200m:B', '200m:B 100m:B'))

# No pool_id -> no rows -> no event.
calls.clear()
act2 = dict(ACT); act2['pool_id'] = None
coll = _run_save(act2)
check('no rows without pool_id', len(coll.docs) == 0)
check('no event when nothing inserted', calls == [])

# Non-swim -> early return, no event.
calls.clear()
act3 = dict(ACT); act3['sport_type'] = 'Run'
coll = _run_save(act3)
check('no event for non-swim', calls == [])


print(f"\n{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
