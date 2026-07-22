import os
import logging
import random
import string
import time
import requests
from datetime import datetime, timezone

from services.mongodb import get_db

logger = logging.getLogger(__name__)

VALID_DISTANCES = {100, 200, 400, 1000, 1500, 1800, 2000, 3800, 5000, 10000}
PERFORMANCES_COLLECTION = 'performances'
SWIM_SPORT_TYPES = {'Swim', 'swim', 'SWIM'}
PERSONAL_RECORD_GRADES = {'A', 'B', 'C', 'D'}  # E is excluded from both leaderboard and personal records

# Card 475: on first connect, Strava's 6-month backfill and Garmin's historical
# webhook burst create many old activities at once — each of which would fire a
# PERSONAL_BEST push, spamming the user. A genuinely NEW swim is always recent,
# while backfilled history is old, so we suppress the PUSH (not the record
# update) for activities older than this window. Peaks/leaderboard still update
# so the profile is correct immediately — the athlete just isn't notified about
# months-old activities they're importing.
NOTIFY_RECENCY_DAYS = int(os.getenv('ACTIVITY_NOTIFY_RECENCY_DAYS', '2'))


def _activity_is_recent(activity, days=NOTIFY_RECENCY_DAYS):
    """True if the activity's start date is within `days` of now (so a
    notification about it is wanted). Unparseable/missing date → treat as recent
    (fail-open: never silently drop a real new activity's notification)."""
    raw = (activity or {}).get('starting_date')
    if not raw:
        return True
    try:
        # Both providers store ISO8601 with a trailing 'Z'.
        dt = datetime.fromisoformat(str(raw).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return True
    age = datetime.now(timezone.utc) - dt
    return age.total_seconds() <= days * 86400


def evaluate_performance(activity, distance_m, peak):
    """Grade the performance. Returns 'B' by default — logic to be implemented."""
    return 'B'


def _compute_swimbox_points(activity):
    """Swimbox Points (zones #486): thread the full athlete context into the
    stub calculator NOW so the real algorithm needs no caller changes.
    Returns None on any failure (points simply not stored this pass)."""
    try:
        from services.swimbox_points import calculate_swimbox_points
        from services.athlete import fetch_athlete
        context = {'hr_zones': None, 'max_hr': None, 'resting_hr': None,
                   'critical_speed': None}
        athlete = fetch_athlete(activity.get('user_id')) if activity.get('user_id') else None
        if athlete:
            for sp in (athlete.get('sport_profiles') or []):
                if sp.get('sport_type') == 'SWIMMING':
                    profile = sp.get('profile') or {}
                    context.update({
                        'hr_zones': profile.get('hr_zones'),
                        'max_hr': profile.get('max_hr'),
                        'resting_hr': profile.get('resting_hr'),
                        'critical_speed': profile.get('continuous_pace_for_15'),
                    })
                    break
        points = calculate_swimbox_points(activity, context)
        logger.info(f"[POINTS] Activity {activity.get('id')} -> {points} swimbox points")
        return points
    except Exception as e:
        logger.warning(f"[POINTS] calculation failed for {activity.get('id')}: {e}")
        return None


def save_performances(activity):
    """
    Full performance processing pipeline for a single activity.

    1. Gate: only Swim activities are processed.
    2. Grade is evaluated once per distance and reused for both leaderboard + personal records.
    3. Pool leaderboard: saved to performances collection when pool_id is present (grades A-E stored,
       grade E filtered out on the leaderboard read side).
    4. Personal records: compared against athlete's existing peaks; athlete profile updated and
       push notification fired for each new record (grades A-D only; E excluded).
    5. Activity marked as performance_calculated=True via swimboxapis in all cases.
    """
    activity_id = activity.get('id')

    # Gate: only swimming activities
    sport_type = activity.get('sport_type') or activity.get('type') or ''
    if sport_type not in SWIM_SPORT_TYPES:
        logger.info(f"[PERF] Activity {activity_id} sport_type={sport_type!r} — not a swim, skipping")
        mark_activity_calculated(activity_id)
        return

    # Swimbox Points (zones #486 §7): EVERY swimming activity — including
    # manual and peak-less ones — gets a points value (the sport gate above
    # is the only gate; the calculator is a stub for now). Recomputed on
    # every pass, so activity updates overwrite the stored value (§7.2).
    points_extra = {}
    points = _compute_swimbox_points(activity)
    if points is not None:
        points_extra['swimbox_points'] = points

    # Gate: MANUAL activities never concur for performances — neither pool
    # leaderboard nor personal records. Their laps can be user-typed or
    # LLM-generated, not device-recorded; excluded by design for any reason.
    if activity.get('is_manual') or any(
        (src or {}).get('source_id') == 'smartcoach_manual'
        for src in (activity.get('sources') or [])
    ):
        logger.info(f"[PERF] Activity {activity_id} is MANUAL — excluded from leaderboard/records")
        mark_activity_calculated(activity_id, extra=points_extra)
        return

    swim_peaks = (activity.get('peaks') or {}).get('swim') or {}
    if not swim_peaks:
        logger.info(f"[PERF] Activity {activity_id} has no swim peaks — skipping")
        mark_activity_calculated(activity_id, extra=points_extra)
        return

    pool_id = activity.get('pool_id')
    user_id = activity.get('user_id')

    # Fetch user to get display name — name lives in the user table, linked via user_id
    from services.athlete import fetch_user
    athlete_name = ''
    if user_id:
        user = fetch_user(user_id)
        if user:
            athlete_name = (user.get('name') or '').strip()

    db = get_db()
    collection = db[PERFORMANCES_COLLECTION]
    personal_record_candidates = {}  # dist_str -> candidate dict

    for dist_str, peak in swim_peaks.items():
        try:
            distance_m = int(dist_str)
        except (ValueError, TypeError):
            continue

        if distance_m not in VALID_DISTANCES:
            continue

        # Grade evaluated once — shared by both leaderboard and personal record logic
        grade = evaluate_performance(activity, distance_m, peak)

        # Skip grade E entirely
        if grade == 'E':
            logger.info(f"[PERF] Activity {activity_id} dist={distance_m}m grade=E — excluded")
            continue

        # --- Pool leaderboard (pool_id required; grades A-D stored) ---
        if pool_id:
            if not collection.find_one({'activity_id': activity_id, 'distance_m': distance_m}):
                doc = {
                    'id': 'PRF_' + ''.join(random.choices(string.digits, k=10)),
                    'user_id': user_id,
                    'athlete_name': athlete_name,
                    'activity_id': activity_id,
                    'pool_id': str(pool_id),
                    'pool_name': activity.get('pool_name', ''),
                    'distance_m': distance_m,
                    'time_s': peak.get('duration_s'),
                    'pace_per_100m': peak.get('pace_per_100m'),
                    'grade': grade,
                    'timestamp': activity.get('starting_date'),
                    'created_at': datetime.now(timezone.utc).isoformat(),
                }
                collection.insert_one(doc)
                doc.pop('_id', None)
                logger.info(f"[LEADERBOARD] Saved performance {doc['id']} pool={pool_id} dist={distance_m}m grade={grade}")
            else:
                logger.info(f"[LEADERBOARD] Performance already exists for activity {activity_id} dist={distance_m}m — skipping")

        # --- Personal records (grades A-D) ---
        if grade in PERSONAL_RECORD_GRADES:
            personal_record_candidates[dist_str] = {
                'time_s': peak.get('duration_s'),
                'pace_per_100m': peak.get('pace_per_100m'),
                'activity_id': activity_id,
                'date': activity.get('starting_date'),
                'grade': grade,
            }

    # Process personal records against athlete's existing peaks. On first
    # connect (old backfilled activities) the PR is still RECORDED but the push
    # is suppressed to avoid spamming — gated on activity recency (Card 475).
    if personal_record_candidates and user_id:
        _process_personal_records(user_id, personal_record_candidates,
                                  notify=_activity_is_recent(activity))

    _ensure_indexes(collection)
    mark_activity_calculated(activity_id, extra=points_extra)


def _process_personal_records(user_id, candidates, notify=True):
    """
    Compare candidate performances against the athlete's existing peak_performances.
    Updates the athlete profile and (when `notify`) sends a push notification for
    each new personal record. `notify=False` (old backfilled activity on first
    connect) still records the PR — it only skips the push (Card 475).
    """
    from services.athlete import fetch_athlete, get_peak_performances, update_peak_performances
    from services.notifications import send_personal_record_notification

    athlete = fetch_athlete(user_id)
    if not athlete:
        logger.warning(f'[PERSONAL] Could not fetch athlete for {user_id} — skipping personal records')
        return

    existing_peaks = get_peak_performances(athlete)
    records_to_save = {}

    for dist_str, candidate in candidates.items():
        candidate_time = candidate.get('time_s')
        if candidate_time is None:
            continue
        current = existing_peaks.get(dist_str)
        # New record if no existing entry or new time is faster (lower)
        if current is None or candidate_time < current.get('time_s', float('inf')):
            records_to_save[dist_str] = candidate
            logger.info(f'[PERSONAL] New personal record dist={dist_str}m time={candidate_time}s (was: {current})')

    if records_to_save:
        update_peak_performances(athlete, records_to_save)
        if notify:
            for dist_str, record in records_to_save.items():
                send_personal_record_notification(user_id, int(dist_str), record['time_s'])
        else:
            logger.info(f'[PERSONAL] {len(records_to_save)} record(s) saved for {user_id} '
                        f'without push (old/backfill activity — Card 475)')


def mark_activity_calculated(activity_id, extra=None):
    """PATCH swimboxapis to set performance_calculated=True on the activity.
    `extra` merges additional fields into the same PATCH (e.g.
    swimbox_points — zones #486) so the write stays a single request.

    swimboxapis rate-limits by caller, and a backlog drain can briefly exceed
    it (all our calls share one service token). A 429 only fails the *mark* —
    the leaderboard/personal-record writes already happened — but leaving the
    activity unmarked risks reprocessing, so we retry with backoff, honouring
    Retry-After. Bounded so a persistent failure can't hang the worker.
    """
    url = os.getenv('SWIMBOXAPIS_URL', '')
    token = os.getenv('SWIMBOXAPIS_CLIENT_TOKEN', '')
    if not url or not token:
        logger.warning('[PERF] SWIMBOXAPIS_URL or SWIMBOXAPIS_CLIENT_TOKEN not set — skipping mark')
        return

    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.patch(
                f'{url}/activities/{activity_id}',
                json={'performance_calculated': True, **(extra or {})},
                headers={'Authorization': f'Bearer {token}'},
                timeout=5
            )
            if response.status_code == 429 and attempt < max_attempts:
                retry_after = response.headers.get('Retry-After')
                delay = float(retry_after) if (retry_after and retry_after.isdigit()) else min(2 ** attempt, 15)
                logger.warning(f'[PERF] mark {activity_id} 429 (attempt {attempt}/{max_attempts}) — retrying in {delay}s')
                time.sleep(delay)
                continue
            response.raise_for_status()
            logger.info(f'[PERF] Marked activity {activity_id} as performance_calculated=True')
            return
        except Exception as e:
            if attempt < max_attempts:
                delay = min(2 ** attempt, 15)
                logger.warning(f'[PERF] mark {activity_id} failed (attempt {attempt}/{max_attempts}): {e} — retrying in {delay}s')
                time.sleep(delay)
                continue
            logger.error(f'[PERF] Failed to mark activity {activity_id} after {max_attempts} attempts: {e}')


def _ensure_indexes(collection):
    """Create indexes if they don't already exist."""
    try:
        collection.create_index([('pool_id', 1), ('distance_m', 1), ('time_s', 1)], background=True)
        collection.create_index([('user_id', 1), ('activity_id', 1)], background=True)
    except Exception as e:
        logger.warning(f'[PERF] Index creation warning: {e}')
