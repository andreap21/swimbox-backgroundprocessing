import logging
import sys
import os

# Ensure the project root is on the path regardless of where the worker is launched from
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from celery_app import celery

logger = logging.getLogger(__name__)


@celery.task(name='tasks.hello_task')
def hello_task():
    logger.info('hello to you')


@celery.task(name='tasks.calculate_leaderboard_task')
def calculate_leaderboard_task(activity):
    from services.performance import save_performances
    save_performances(activity)


@celery.task(name='tasks.match_activity_task')
def match_activity_task(activity_id, user_id):
    """Try to auto-match a freshly ingested activity to the planned session
    scheduled on the day it was performed (automated mark-as-done). Enqueued
    by swimboxapis right after activity creation (POST /events/match-activity);
    the matching + mark-as-done logic lives in swimboxapis — this task just
    calls its internal endpoint."""
    from services.matching import run_match_for_activity
    run_match_for_activity(activity_id, user_id)


@celery.task(name='tasks.replan_athlete_task')
def replan_athlete_task(user_id, mode='weekly'):
    """Run the Veyra methodology replan for one athlete (Trello #441).
    Enqueued by swimboxapis (POST /events/replan-athlete) when a pro user with a
    saved training_plan_info upgrades or saves their plan profile — and by the
    Sunday scheduler (swimboxapis scripts/scheduled_weekly_replan.py) for every
    active-plan pro athlete. `mode` is forwarded verbatim to the chatbot:
    'weekly' = review + append next week; 'daily' = prose only (reserved)."""
    from services.replan import run_replan_for_user
    run_replan_for_user(user_id, mode=mode)
