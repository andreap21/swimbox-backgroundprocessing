import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi

_client = None


def get_db():
    global _client
    if _client is None:
        _client = MongoClient(os.getenv('MONGODB_URL'), server_api=ServerApi('1'))
    return _client[os.getenv('MONGODB_DBNAME', 'swimbox')]
