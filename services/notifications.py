import os
import logging
import requests

logger = logging.getLogger(__name__)


def send_personal_record_notification(user_id, distance_m, time_s):
    """POST /user-notifications/ to create a personal record notification.

    Title and body are localised server-side via title_key/message_key —
    backgroundprocessing doesn't need to know which language the user
    prefers or how to format the strings. swimboxapis resolves both based
    on user.preferences.Language and renders the templates with
    template_vars (see services/cms/user_notifications/apis.py POST).

    The label templates live in Strapi under
    `notification.personal_record.title` and `notification.personal_record.body`.
    """
    try:
        url = os.getenv('SWIMBOXAPIS_URL', '')
        token = os.getenv('SWIMBOXAPIS_CLIENT_TOKEN', '')
        if not url or not token:
            logger.warning('[PERSONAL] SWIMBOXAPIS_URL or CLIENT_TOKEN not set — skipping notification')
            return

        payload = {
            'user_id': user_id,
            'title_key': 'notification.personal_record.title',
            'message_key': 'notification.personal_record.body',
            # Variables interpolated into the template. Distinct from
            # `additional_fields` (which is structured metadata stored on
            # the notification entry); they happen to overlap here but
            # the intent differs.
            'template_vars': {'distance_m': distance_m},
            'type': 'PERSONAL_BEST',
            'additional_fields': {'distance_m': distance_m, 'time_s': time_s},
        }
        # 30 s — swimboxapis can take 10–15 s on a cold labels cache (the
        # first call per locale after a deploy fetches all labels from
        # Strapi, paginated 25 at a time). Once cached the call is fast.
        # Backgroundprocessing's Celery worker can spare the time; the 5 s
        # we used to have surfaced as an alarming "Read timed out" log
        # whenever the worker ran shortly after a deploy, even though the
        # notification itself succeeded.
        resp = requests.post(
            f'{url}/user-notifications/',
            json=payload,
            headers={'Authorization': f'Bearer {token}'},
            timeout=30
        )
        resp.raise_for_status()
        logger.info(f'[PERSONAL] Notification queued for user {user_id} dist={distance_m}m')
    except Exception as e:
        logger.error(f'[PERSONAL] Failed to send notification for user {user_id} dist={distance_m}m: {e}')
