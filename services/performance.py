import os
import logging
import random
import string
import requests
from datetime import datetime, timezone

from services.mongodb import get_db

logger = logging.getLogger(__name__)

VALID_DISTANCES = {100, 200, 400, 1000, 1500, 1800, 2000, 3800, 5000, 10000}
PERFORMANCES_COLLECTION = 'performances'
SWIM_SPORT_TYPES = {'Swim', 'swim', 'SWIM'}
PERSONAL_RECORD_GRADES = {'A', 'B', 'C', 'D'}  # E is excluded from both leaderboard and personal records


def evaluate_performance(activity, distance_m, peak):
    """Grade the performance. Returns 'B' by default — logic to be implemented."""
    return 'B'


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

    swim_peaks = (activity.get('peaks') or {}).get('swim') or {}
    if not swim_peaks:
        logger.info(f"[PERF] Activity {activity_id} has no swim peaks — skipping")
        mark_activity_calculated(activity_id)
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

    # Process personal records against athlete's existing peaks
    if personal_record_candidates and user_id:
        _process_personal_records(user_id, personal_record_candidates)

    _ensure_indexes(collection)
    mark_activity_calculated(activity_id)


def _process_personal_records(user_id, candidates):
    """
    Compare candidate performances against the athlete's existing peak_performances.
    Updates the athlete profile and sends a push notification for each new personal record.
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
        for dist_str, record in records_to_save.items():
            send_personal_record_notification(user_id, int(dist_str), record['time_s'])


def mark_activity_calculated(activity_id):
    """PATCH swimboxapis to set performance_calculated=True on the activity."""
    try:
        url = os.getenv('SWIMBOXAPIS_URL', '')
        token = os.getenv('SWIMBOXAPIS_CLIENT_TOKEN', '')
        if not url or not token:
            logger.warning('[PERF] SWIMBOXAPIS_URL or SWIMBOXAPIS_CLIENT_TOKEN not set — skipping mark')
            return
        response = requests.patch(
            f'{url}/activities/{activity_id}',
            json={'performance_calculated': True},
            headers={'Authorization': f'Bearer {token}'},
            timeout=5
        )
        response.raise_for_status()
        logger.info(f'[PERF] Marked activity {activity_id} as performance_calculated=True')
    except Exception as e:
        logger.error(f'[PERF] Failed to mark activity {activity_id}: {e}')


def _ensure_indexes(collection):
    """Create indexes if they don't already exist."""
    try:
        collection.create_index([('pool_id', 1), ('distance_m', 1), ('time_s', 1)], background=True)
        collection.create_index([('user_id', 1), ('activity_id', 1)], background=True)
    except Exception as e:
        logger.warning(f'[PERF] Index creation warning: {e}')
