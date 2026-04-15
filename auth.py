import os
from functools import wraps
from flask import request, jsonify

API_TOKEN = os.getenv('API_TOKEN', '')


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Missing or invalid Authorization header'}), 401
        token = auth_header[len('Bearer '):]
        if not API_TOKEN or token != API_TOKEN:
            return jsonify({'error': 'Unauthorized'}), 403
        return f(*args, **kwargs)
    return decorated
