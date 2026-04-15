import os
import logging
import requests

logger = logging.getLogger(__name__)


def _base_url():
    return os.getenv('SWIMBOXAPIS_URL', '')


def _headers():
    return {'Authorization': f'Bearer {os.getenv("SWIMBOXAPIS_CLIENT_TOKEN", "")}'}


def fetch_athlete(user_id):
    """GET /athletes/?user_id=<user_id> — returns first match or None."""
    try:
        resp = requests.get(
            f'{_base_url()}/athletes/',
            params={'user_id': user_id},
            headers=_headers(),
            timeout=5
        )
        resp.raise_for_status()
        athletes = resp.json()
        if isinstance(athletes, list) and athletes:
            return athletes[0]
        return None
    except Exception as e:
        logger.error(f'[PERSONAL] Failed to fetch athlete for {user_id}: {e}')
        return None


def get_peak_performances(athlete):
    """Extract peak_performances dict from SWIMMING sport profile. Returns {} if missing."""
    for sp in (athlete.get('sport_profiles') or []):
        if sp.get('sport_type') == 'SWIMMING':
            return sp.get('profile', {}).get('peak_performances', {})
    return {}


def update_peak_performances(athlete, new_peaks):
    """
    Merge new_peaks into the athlete's SWIMMING sport_profile.peak_performances
    and PATCH /athletes/<athlete_id> with the updated sport_profiles array.
    new_peaks: dict of { distance_str: { time_s, pace_per_100m, activity_id, date, grade } }
    """
    try:
        sport_profiles = list(athlete.get('sport_profiles') or [])
        swimming_found = False
        for sp in sport_profiles:
            if sp.get('sport_type') == 'SWIMMING':
                profile = sp.setdefault('profile', {})
                existing = dict(profile.get('peak_performances') or {})
                existing.update(new_peaks)
                profile['peak_performances'] = existing
                swimming_found = True
                break
        if not swimming_found:
            sport_profiles.append({
                'sport_type': 'SWIMMING',
                'profile': {'peak_performances': new_peaks}
            })

        athlete_id = athlete.get('id')
        resp = requests.patch(
            f'{_base_url()}/athletes/{athlete_id}',
            json={'sport_profiles': sport_profiles},
            headers=_headers(),
            timeout=5
        )
        resp.raise_for_status()
        logger.info(f'[PERSONAL] Updated peak_performances for athlete {athlete_id}: distances={list(new_peaks.keys())}')
    except Exception as e:
        logger.error(f'[PERSONAL] Failed to update peak_performances for athlete {athlete.get("id")}: {e}')
