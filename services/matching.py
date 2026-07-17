import os
import logging
import requests

logger = logging.getLogger(__name__)


def run_match_for_activity(activity_id, user_id):
    """Ask swimboxapis to match an ingested activity to the day's planned
    session (automated mark-as-done).

    All orchestration (candidate lookup, distance tolerance, the Haiku
    structural matcher on the chatbot, the mark-as-done side effects) lives
    in swimboxapis — this worker is a thin consumer, mirroring replan.py.

    Env (on swimbox-backgroundprocessing):
      SWIMBOXAPIS_URL           — base URL of swimboxapis
      SWIMBOXAPIS_CLIENT_TOKEN  — admin/client bearer for internal endpoints

    Fire-and-forget semantics: any failure is logged and swallowed. The
    matcher endpoint is idempotent (already-linked / already-deleted-event
    guards), so a retried message is safe.
    """
    url = os.getenv('SWIMBOXAPIS_URL', '')
    token = os.getenv('SWIMBOXAPIS_CLIENT_TOKEN', '')
    if not url or not token:
        logger.warning('[MATCH] SWIMBOXAPIS_URL or SWIMBOXAPIS_CLIENT_TOKEN not set — skipping match for %s', activity_id)
        return
    if not activity_id or not user_id:
        logger.warning('[MATCH] activity_id and user_id are required — skipping')
        return

    try:
        resp = requests.post(
            f'{url}/activities/match-planned-session',
            json={'activity_id': activity_id, 'user_id': user_id},
            headers={'Authorization': f'Bearer {token}'},
            # Covers the Strapi session fetches + one Haiku call on the chatbot.
            timeout=60,
        )
        resp.raise_for_status()
        body = {}
        try:
            body = resp.json()
        except Exception:  # noqa: BLE001
            pass
        logger.info('[MATCH] done for activity %s: matched=%s reason=%s event_id=%s',
                    activity_id, body.get('matched'), body.get('reason'), body.get('event_id'))
    except Exception as e:  # noqa: BLE001
        logger.error('[MATCH] failed for activity %s: %s', activity_id, e)
