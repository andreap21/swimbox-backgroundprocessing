from flask import Flask, request, jsonify
from celery_app import celery
from tasks import hello_task, calculate_leaderboard_task, replan_athlete_task, match_activity_task
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


@app.route('/events/match-activity', methods=['POST'])
@token_required
def match_activity():
    """Enqueue an activity ↔ planned-session match (automated mark-as-done).
    Body: {"activity_id": "...", "user_id": "..."}. Called by swimboxapis
    right after a swim activity is created from a Strava/Garmin sync."""
    payload = request.get_json(force=True) or {}
    activity_id = payload.get('activity_id')
    user_id = payload.get('user_id')
    if not activity_id or not user_id:
        return jsonify({'error': 'activity_id and user_id are required'}), 400
    match_activity_task.delay(activity_id, user_id)
    return jsonify({'message': 'Task enqueued'}), 202


@app.route('/events/replan-athlete', methods=['POST'])
@token_required
def replan_athlete():
    """Enqueue a methodology replan for one athlete (Trello #441).
    Body: {"user_id": "<athlete user_id>", "mode": "weekly"|"daily"} (mode
    optional, defaults weekly). Called by swimboxapis when a pro user with a
    saved training_plan_info upgrades or saves their plan profile, and by the
    Sunday scheduler for every active-plan pro athlete."""
    payload = request.get_json(force=True) or {}
    user_id = payload.get('user_id')
    mode = payload.get('mode') or 'weekly'
    if not user_id:
        return jsonify({'error': 'user_id is required'}), 400
    if mode not in ('weekly', 'daily'):
        return jsonify({'error': f"unknown mode '{mode}'"}), 400
    replan_athlete_task.delay(user_id, mode=mode)
    return jsonify({'message': 'Task enqueued'}), 202


if __name__ == '__main__':
    app.run(debug=True, port=5003)
