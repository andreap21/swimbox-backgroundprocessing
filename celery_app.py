import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

CLOUDAMQP_URL = os.getenv('CLOUDAMQP_URL', 'amqp://guest:guest@localhost//')

celery = Celery(
    'swimbox_jobs',
    broker=CLOUDAMQP_URL,
    backend='rpc://',  # lightweight — results go back over AMQP, no extra store needed
    include=['tasks'],
)

celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)
