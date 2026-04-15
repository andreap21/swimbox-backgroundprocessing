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
