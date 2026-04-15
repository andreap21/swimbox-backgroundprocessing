from flask import Flask, request, jsonify
from celery_app import celery
from tasks import hello_task, calculate_leaderboard_task
from auth import token_required

app = Flask(__name__)


@app.route('/events/hello-world', methods=['POST'])
@token_required
def hello_world():
    hello_task.delay()
    return jsonify({'message': 'Task enqueued'}), 202


@app.route('/events/calculate-leaderboard', methods=['POST'])
@token_required
def calculate_leaderboard():
    activity = request.get_json(force=True)
    if not activity:
        return jsonify({'error': 'No activity payload'}), 400
    calculate_leaderboard_task.delay(activity)
    return jsonify({'message': 'Task enqueued'}), 202


if __name__ == '__main__':
    app.run(debug=True, port=5003)
