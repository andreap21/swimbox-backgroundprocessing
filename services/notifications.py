import os
import logging
import requests

logger = logging.getLogger(__name__)


def send_personal_record_notification(user_id, distance_m, time_s):
    """POST /user-notifications/ to create a personal record notification."""
    try:
        url = os.getenv('SWIMBOXAPIS_URL', '')
        token = os.getenv('SWIMBOXAPIS_CLIENT_TOKEN', '')
        if not url or not token:
            logger.warning('[PERSONAL] SWIMBOXAPIS_URL or CLIENT_TOKEN not set — skipping notification')
            return

        minutes = int(time_s // 60)
        seconds = int(time_s % 60)
        time_str = f"{minutes}'{seconds:02d}\""

        payload = {
            'user_id': user_id,
            'title': 'Nuovo personale! 🏊',
            'message': f'Congratulazioni! Hai un nuovo personale per la distanza {distance_m}m con un tempo di {time_str}',
            'type': 'PERSONAL_BEST',
            'additional_fields': {'distance_m': distance_m, 'time_s': time_s},
        }
        resp = requests.post(
            f'{url}/user-notifications/',
            json=payload,
            headers={'Authorization': f'Bearer {token}'},
            timeout=5
        )
        resp.raise_for_status()
        logger.info(f'[PERSONAL] Notification sent for user {user_id} dist={distance_m}m time={time_str}')
    except Exception as e:
        logger.error(f'[PERSONAL] Failed to send notification for user {user_id} dist={distance_m}m: {e}')
