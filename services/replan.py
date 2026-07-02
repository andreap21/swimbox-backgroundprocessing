import os
import logging
import requests

logger = logging.getLogger(__name__)


def run_replan_for_user(user_id, mode='weekly'):
    """Trigger the Veyra methodology replan for one athlete (Trello #441).

    The coaching brain lives in swimbox-chatbot; this worker is a thin consumer.
    We POST the chatbot's /coaching/run-replan with `write: true` so it actually
    materialises the rolling 4-week plan into the calendar (the chatbot in turn
    reads/writes the athlete BY user_id via the admin-token by-user_id endpoints —
    there is no current-user token in this back-end path).

    Env (on swimbox-backgroundprocessing):
      CHATBOT_URL        — base URL of the swimbox-chatbot app
      CHATBOT_API_KEY    — the chatbot's require_api_key bearer

    Fire-and-forget semantics from the caller's POV: any failure is logged and
    swallowed, mirroring services/notifications.py. The replan itself is
    idempotent (clear-then-write the future window), so a retried message is safe.
    """
    url = os.getenv('CHATBOT_URL', '')
    api_key = os.getenv('CHATBOT_API_KEY', '')
    if not url or not api_key:
        logger.warning('[REPLAN] CHATBOT_URL or CHATBOT_API_KEY not set — skipping replan for %s', user_id)
        return
    if not user_id:
        logger.warning('[REPLAN] no user_id — skipping')
        return

    try:
        resp = requests.post(
            f'{url}/coaching/run-replan',
            # mode: 'weekly' (review + append next week) | 'daily' (prose only,
            # reserved). The chatbot route parses it and sets plan_week.
            json={'user_id': user_id, 'write': True, 'mode': mode},
            headers={'Authorization': f'Bearer {api_key}'},
            # Generous: the replan clones + adapts up to ~9 sessions into Strapi +
            # creates events; comfortably within a worker's budget.
            timeout=120,
        )
        resp.raise_for_status()
        body = {}
        try:
            body = resp.json()
        except Exception:  # noqa: BLE001
            pass
        cw = (body or {}).get('calendar_write') or {}
        logger.info(
            '[REPLAN] done for %s: decision=%s phase=%s events_written=%s',
            user_id, body.get('decision'), body.get('phase'), cw.get('events_written'),
        )
    except Exception as e:  # noqa: BLE001
        logger.error('[REPLAN] failed for %s: %s', user_id, e)
