"""AI screening for athlete-uploaded swim video (Trello doifu3bM).

WHY THIS EXISTS: the feature accepts user-uploaded video — potentially of a
minor — stores it, and shows it to staff. Without an automated screen, nothing
checks what arrives before a human opens it. This is the content-moderation
layer, not a nicety.

HOW IT SAMPLES FRAMES WITHOUT ffmpeg:
Bunny generates a seek sprite sheet at {cdn}/{guid}/seek/_0.jpg — a 6x6 grid of
frames sampled every 2 s, 1800 px wide. Uploads are capped at 60 s, so ONE
sprite covers the whole clip: sampling ~30 frames is a single HTTP GET plus one
vision call. No MP4 download, no ffmpeg, no new system dependency in the worker.

1800 px sits well inside the 8000 px image limit, and one image is far under the
per-request cap.
"""
import base64
import logging
import os

import requests

logger = logging.getLogger(__name__)

MODEL = os.getenv('VIDEO_SCREENING_MODEL', 'claude-opus-5')
# The four content verdicts the model may return.
VERDICTS = {'PASS', 'NOT_SWIMMING', 'UNUSABLE_FOOTAGE', 'INAPPROPRIATE_CONTENT'}

# Not a content verdict: we could not look at the frames at all. Kept distinct so
# "we could not check" is never confused with "we checked and it was fine", and
# so an outage's backlog can be found and re-screened.
UNABLE = 'UNABLE_TO_VALIDATE'

# Transient failures (a CDN blip, a rate limit) shouldn't cost an athlete their
# upload, so the task retries before the rejection sticks.
MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 60

_PROMPT = """This image is a sprite sheet: a 6x6 grid of still frames sampled at
even intervals across a short video a user uploaded for swimming technique
analysis. Read the frames left-to-right, top-to-bottom. Some cells may be blank
if the video was shorter than the grid.

Decide which ONE verdict applies to the video as a whole:

PASS
    The frames show a person swimming, or clearly preparing to swim, in a pool
    or open water. Poor lighting or an awkward angle is fine as long as a coach
    could still analyse the stroke.

NOT_SWIMMING
    Nobody is swimming — e.g. a landscape, a pet, a screen recording, a person
    talking to camera, gym footage, or an empty pool.

UNUSABLE_FOOTAGE
    Someone is swimming but a coach could not analyse it: far too dark, wildly
    shaky, filmed from so far away the swimmer is a speck, or almost entirely
    obscured.

INAPPROPRIATE_CONTENT
    Nudity, sexual content, violence, or anything else clearly unsuitable.
    Ordinary swimwear is NOT inappropriate.

Answer with a single line of JSON and nothing else:
{"verdict": "<one of the four>", "reason": "<max 12 words>"}"""


def _fetch_sprite(bunny_video_id, cdn_hostname):
    url = f"https://{cdn_hostname}/{bunny_video_id}/seek/_0.jpg"
    resp = requests.get(url, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"sprite fetch returned {resp.status_code} for {url}")
    return base64.standard_b64encode(resp.content).decode('utf-8')


def analyze_frames(bunny_video_id, cdn_hostname):
    """Screen one video. Returns {'verdict', 'reason', 'model'}.

    This is the ONLY function that talks to the model — tune the prompt here
    without touching the pipeline.

    FAILS CLOSED. If we cannot see the frames, we cannot say the video is
    swimming, so it does not pass. An "unable to validate" outcome is a
    rejection, never a pass — the whole point of this layer is that unscreened
    footage never reaches a coach, and a fail-open would route exactly that to a
    human during our own outage.

    The verdict is UNABLE_TO_VALIDATE (distinct from the four content verdicts)
    so the cause is never confused with "we looked and it was bad", and so these
    can be found and re-screened once the outage clears.
    """
    import json

    try:
        image_b64 = _fetch_sprite(bunny_video_id, cdn_hostname)
    except Exception as e:
        logger.error(f"[VIDEOSCREEN] sprite unavailable for {bunny_video_id}: {e}")
        return {'verdict': 'UNABLE_TO_VALIDATE', 'reason': 'sprite_unavailable', 'model': None}

    try:
        import anthropic
        client = anthropic.Anthropic()
        response = client.messages.create(
            model=MODEL,
            max_tokens=200,
            messages=[{
                'role': 'user',
                'content': [
                    {'type': 'image', 'source': {
                        'type': 'base64', 'media_type': 'image/jpeg', 'data': image_b64}},
                    {'type': 'text', 'text': _PROMPT},
                ],
            }],
        )
    except Exception as e:
        logger.error(f"[VIDEOSCREEN] model call failed for {bunny_video_id}: {e}")
        return {'verdict': 'UNABLE_TO_VALIDATE', 'reason': 'model_unavailable', 'model': MODEL}

    text = ''.join(b.text for b in response.content if getattr(b, 'type', None) == 'text').strip()
    try:
        # Tolerate a fenced block around the JSON.
        if text.startswith('```'):
            text = text.split('```')[1].lstrip('json').strip()
        parsed = json.loads(text)
        verdict = parsed.get('verdict')
        if verdict not in VERDICTS:
            raise ValueError(f"unknown verdict {verdict!r}")
        return {'verdict': verdict, 'reason': parsed.get('reason'), 'model': MODEL}
    except Exception as e:
        logger.error(f"[VIDEOSCREEN] unparseable verdict for {bunny_video_id}: {e} | {text[:120]}")
        return {'verdict': 'UNABLE_TO_VALIDATE', 'reason': 'unparseable_verdict', 'model': MODEL}


def run_validation(submission_id, video_asset_id, bunny_video_id, library_id=None):
    """Screen a video and write the verdict straight back to Mongo.

    Direct DB write (as services/performance.py does) rather than a callback
    endpoint — the worker already holds a Mongo handle, and an internal API for
    one field would be a whole extra auth surface.
    """
    from services.mongodb import get_db

    cdn = os.getenv('BUNNY_NET_UPLOAD_CDN_HOSTNAME', '')
    if not cdn:
        logger.error('[VIDEOSCREEN] BUNNY_NET_UPLOAD_CDN_HOSTNAME not set — cannot screen')
        return UNABLE

    result = analyze_frames(bunny_video_id, cdn)
    logger.info(f"[VIDEOSCREEN] {submission_id}/{video_asset_id} -> {result['verdict']}")

    db = get_db()
    collection = db['video_analysis_submissions']
    doc = collection.find_one({'id': submission_id})
    if not doc:
        logger.warning(f"[VIDEOSCREEN] submission {submission_id} vanished")
        return None

    from datetime import datetime
    videos = doc.get('videos', [])
    failed = False
    for v in videos:
        if v.get('id') != video_asset_id:
            continue
        v['ai_validation'] = {
            'verdict': result['verdict'],
            'reason': result.get('reason'),
            'model': result.get('model'),
            'checked_at': datetime.utcnow().isoformat(),
        }
        # A non-PASS fails THAT video. While the submission is still a DRAFT the
        # athlete simply sees that slot in its error state and can re-shoot —
        # better than nuking a submission they are mid-way through assembling.
        if result['verdict'] != 'PASS':
            v['upload_status'] = 'FAILED'
            v['failure_reason'] = result['verdict']
            failed = True
        break

    updates = {'videos': videos, 'updated_at': datetime.utcnow().isoformat()}

    # RACE: screening can land AFTER the athlete hit submit (Bunny was slow, or
    # the queue backed up). submit_for_review only attaches READY videos, so a
    # rejection that arrives first is already excluded — but one arriving later
    # would leave rejected footage sitting on a SUBMITTED submission, i.e. in
    # front of a coach. That is the exact outcome this layer exists to prevent,
    # so the submission is pulled back out of the review queue.
    if failed and doc.get('status') in ('SUBMITTED', 'IN_REVIEW'):
        remaining = [x for x in videos if x.get('upload_status') == 'READY']
        if remaining:
            # Other angles are still clean — let the review proceed on those.
            logger.warning(
                f"[VIDEOSCREEN] {submission_id} was already {doc.get('status')}; dropped "
                f"the rejected video and left {len(remaining)} for review")
            updates['videos'] = remaining
        else:
            # Nothing survives: the coach must not receive this at all.
            logger.warning(
                f"[VIDEOSCREEN] {submission_id} was already {doc.get('status')}; "
                f"rejecting the whole submission ({result['verdict']})")
            updates['status'] = 'REJECTED'
            updates['rejection_reason'] = (
                result['verdict'] if result['verdict'] in
                ('NOT_SWIMMING', 'UNUSABLE_FOOTAGE', 'INAPPROPRIATE_CONTENT')
                else 'UNUSABLE_FOOTAGE')

    collection.update_one({'id': submission_id}, {'$set': updates})
    return result['verdict']
