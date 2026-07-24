"""Swimbox Points calculator (Trello #490, SwimboxPoints.md v1.1).

TRIMP-like internal training load for one activity: time per HR zone x
zone weighting, summed. Two engines, selected binary (§4.1):

    activity has a usable HR signal AND zones available -> HR engine (§3)
    otherwise, SWIM with critical speed AND laps        -> pace engine (§4-5)
    otherwise                                           -> None (no points)

ALL sports are scored since 2026-07-24 (gate lifted, deviation from spec
§9.12): the HR engine is sport-agnostic. The pace engine stays SWIM-ONLY —
the Ritmi table is derived from swimming critical speed and means nothing
for run/ride laps. Known caveat: non-swim scoring uses the SWIMMING
profile's HR zones (running zones sit a few bpm higher, so run intensity
is slightly overstated until per-sport zones exist).

Non-swims WITHOUT usable HR get a FLAT-RATE estimate (no zone breakdown,
zone_minutes omitted): running from average pace, cycling from average
power (watts stream), and cycling without power from a minimal Haiku call
on the averages only (avg speed + duration — never the streams). Flat
estimates: points = pts/min band × elapsed minutes. The LLM path is
best-effort: constrained to the same five band values to keep recomputes
stable, and skipped entirely (no points) when ANTHROPIC_API_KEY is unset
or the call fails.

Deterministic for a given activity + athlete
state — points are recomputed and overwritten on every activity update, and
zone/CS edits affect only future calculations (past is past, no backfill).

Returns None or:
    {'points': int,                    # §8: integer, round half-away
     'points_per_minute': float,       # §1.1: intensity, 2 decimals
     'method': 'hr'|'pace_estimate',   # §4.3: computation method flag
     'zone_minutes': {'z1'..'z5'},     # §3/§6.7: per-zone distribution, 1dp
     'algo_version': ALGO_VERSION}

athlete_context shape (frozen contract):
    {'hr_zones':       zone container dict or None,
     'max_hr':         int or None,
     'resting_hr':     int or None,
     'critical_speed': seconds-per-100m float or None}

Accepted limitations (spec §9): laps carry no stroke/drill information, so
drill/kick/pull laps are classified by realised pace like everything else
(§9.10); activity.duration is elapsed time for Strava but timer time for
Garmin FIT — no correction applied (§3 uses it as the elapsed span).
"""

import logging
import math
import os

from services.ritmi import reference_paces, DISTANCES as RITMI_DISTANCES

logger = logging.getLogger(__name__)

ALGO_VERSION = 1

# ── Zone coefficients (§2) ───────────────────────────────────────────────────
# Continuous fit of Stagno's modified TRIMP weightings (Stagno, Thatcher &
# van Someren 2007): w(x) = A·e^(B·x), x = fraction of HR reserve at the
# zone midpoint. Reproduces the published table (1.25/1.71/2.54/3.61/5.16)
# within ~4% at the default boundaries and adapts to manually edited zones.
W_COEFF_A = 0.178
W_COEFF_B = 3.545
# Fixed Stagno table, used only when no zone container exists to evaluate
# the continuous function against (pace engine for a zone-less athlete).
STAGNO_COEFFICIENTS = [1.25, 1.71, 2.54, 3.61, 5.16]

# ── Pace-engine tunables ─────────────────────────────────────────────────────
# ⚠️ PLACEHOLDERS — REQUIRE COACHING SIGN-OFF (spec §9.1-9.4). These are the
# spec's values, isolated here so tuning is a one-block change.
CEILING_MAP = {'A1': 1, 'A2': 2, 'B1': 3, 'B2': 4, 'C1': 5, 'C2': 5, 'C3': 5}   # §5.2
REST_ZONE = {'A1': 1, 'A2': 1, 'B1': 2, 'B2': 3, 'C1': 3, 'C2': 3, 'C3': 3}     # §4.4
# §5.3 attainment ladder for C-zone work: (max effective duration s, split).
C_ATTAINMENT = [
    (10, {3: 1.0}),
    (20, {3: 0.70, 4: 0.30}),
    (30, {3: 0.65, 4: 0.35}),
    (60, {3: 0.50, 4: 0.50}),
    (90, {3: 0.25, 4: 0.65, 5: 0.10}),
    (None, {3: 0.10, 4: 0.75, 5: 0.15}),
]
AB_SHORT_EFFORT_S = 30          # §5.3: A/B efforts under this score one zone below ceiling
DENSITY_LOW = 0.33              # §5.4: below → reps independent (rep duration)
DENSITY_HIGH = 0.66             # §5.4: above → set is one continuous effort
REST_GAP_MIN_S = 5              # in-lap elapsed−moving gap counted as rest (noise floor)

# ── Intensity bands (pts/min → session character, defined 2026-07-24) ────────
# Lower bound inclusive: band n covers [threshold(n-1), threshold(n)); the
# top band is strictly above the last threshold (3.10 itself is Intensa).
# ⚠️ Boundaries are coaching-tunable like the constants above.
INTENSITY_THRESHOLDS = [1.60, 2.10, 2.60, 3.10]
INTENSITY_LABELS = ['Recupero', 'Leggera', 'Media', 'Intensa', 'Massimale']


def classify_intensity(points_per_minute):
    """(band 1-5, label) for a points-per-minute value."""
    band = 1 + sum(1 for t in INTENSITY_THRESHOLDS if points_per_minute >= t)
    if band == 5 and points_per_minute <= INTENSITY_THRESHOLDS[-1]:
        band = 4  # exactly 3.10 stays Intensa (top band is strictly above)
    return band, INTENSITY_LABELS[band - 1]


# ── Flat-rate estimates for non-swims without usable HR (2026-07-24) ─────────
# ⚠️ PLACEHOLDER bands (Andrea's tables) — coaching-tunable like everything
# above. Each row: (upper bound EXCLUSIVE, pts/min). "Typical amateur"
# anchoring; ELITE = 3.1.
RUN_PACE_PPM = [          # average pace, minutes per km
    (3.5, 3.1),           # faster than 3'30"/km → elite
    (4.0, 3.0),
    (5.0, 2.5),
    (6.0, 2.0),
    (None, 1.6),          # 6'00"/km and slower
]
RIDE_POWER_PPM = [        # average watts (from the activity's watts stream)
    (150, 1.6),
    (200, 2.0),
    (250, 2.5),
    (300, 3.0),
    (None, 3.1),          # 300W+ → elite
]
FLAT_PPM_VALUES = [1.6, 2.0, 2.5, 3.0, 3.1]
# Plausibility guards — outside these, the averages are junk data.
_RUN_PACE_PLAUSIBLE = (2.0, 20.0)    # min/km
_RIDE_SPEED_PLAUSIBLE = (5.0, 60.0)  # km/h
_MIN_WATTS_SAMPLES = 50
LLM_ESTIMATE_MODEL = 'claude-haiku-4-5'


def _band_lookup(value, table):
    for upper, ppm in table:
        if upper is None or value < upper:
            return ppm
    return table[-1][1]


def _average_watts(activity):
    """Mean of the non-None watts stream samples, or None when the stream
    is absent/too thin to trust."""
    watts = (activity.get('heart_rate_stream') or {}).get('watts') or []
    samples = [w for w in watts if isinstance(w, (int, float)) and not isinstance(w, bool)]
    if len(samples) < _MIN_WATTS_SAMPLES:
        return None
    mean = sum(samples) / len(samples)
    return mean if mean >= 30 else None  # near-zero mean = sensor junk


def _llm_ride_ppm(avg_kmh, duration_min):
    """Haiku estimate for a ride with no HR and no power — ONLY the two
    averages are sent, never streams, to keep the call minimal. The answer
    is constrained to the five flat-rate band values so repeated recomputes
    stay stable. Returns a ppm float or None (no key / failure)."""
    if not os.getenv('ANTHROPIC_API_KEY'):
        return None
    try:
        import json
        import anthropic
        client = anthropic.Anthropic(timeout=15.0, max_retries=1)
        response = client.messages.create(
            model=LLM_ESTIMATE_MODEL,
            max_tokens=256,
            output_config={'format': {'type': 'json_schema', 'schema': {
                'type': 'object',
                'properties': {'points_per_minute': {
                    'type': 'number', 'enum': FLAT_PPM_VALUES}},
                'required': ['points_per_minute'],
                'additionalProperties': False,
            }}},
            messages=[{'role': 'user', 'content': (
                f"A cyclist rode for {duration_min:.0f} minutes at an average of "
                f"{avg_kmh:.1f} km/h. No heart-rate or power data is available. "
                "Classify the training intensity for a TYPICAL AMATEUR cyclist "
                "into one band: 1.6 = recovery (roughly under 18 km/h solo), "
                "2.0 = light endurance (~18-23 km/h), 2.5 = moderate aerobic "
                "(~23-28 km/h), 3.0 = intense (~28-33 km/h), 3.1 = maximal / "
                "race pace (above ~33 km/h). Adjust for duration: sustaining a "
                "high average for hours is harder than for 30 minutes, and very "
                "long easy rides stay easy. Answer with exactly one band value."
            )}],
        )
        text = next(b.text for b in response.content if b.type == 'text')
        ppm = float(json.loads(text)['points_per_minute'])
        return ppm if ppm in FLAT_PPM_VALUES else None
    except Exception as e:  # noqa: BLE001 — points simply not stored
        logger.warning('[POINTS] llm ride estimate failed: %s', e)
        return None


def _flat_estimate(activity):
    """(ppm, method) for a non-swim without usable HR, or None.

    Running → pace table. Cycling → power table when a watts stream
    exists, else the Haiku speed estimate. Other sports → None."""
    sport = (activity.get('sport_type') or activity.get('type') or '').lower()
    duration = activity.get('duration') or 0
    distance = activity.get('distance') or 0
    if duration <= 0:
        return None

    if 'run' in sport:
        if distance <= 0:
            return None
        pace_min_km = (duration / 60.0) / (distance / 1000.0)
        if not (_RUN_PACE_PLAUSIBLE[0] <= pace_min_km <= _RUN_PACE_PLAUSIBLE[1]):
            return None
        return _band_lookup(pace_min_km, RUN_PACE_PPM), 'pace_estimate'

    if 'ride' in sport or 'bike' in sport or 'cycl' in sport:
        avg_watts = _average_watts(activity)
        if avg_watts is not None:
            return _band_lookup(avg_watts, RIDE_POWER_PPM), 'power_estimate'
        if distance <= 0:
            return None
        avg_kmh = (distance / 1000.0) / (duration / 3600.0)
        if not (_RIDE_SPEED_PLAUSIBLE[0] <= avg_kmh <= _RIDE_SPEED_PLAUSIBLE[1]):
            return None
        ppm = _llm_ride_ppm(avg_kmh, duration / 60.0)
        if ppm is not None:
            return ppm, 'llm_estimate'
    return None


def _assemble_flat(ppm, duration_seconds, method):
    """Result dict for a flat-rate estimate — no zone distribution
    (zone_minutes None → omitted from the stored detail)."""
    minutes = duration_seconds / 60.0
    points = ppm * minutes
    band, label = classify_intensity(ppm)
    return {
        'points': _round_half_away(points),
        'points_per_minute': ppm,
        'intensity_band': band,
        'intensity_label': label,
        'method': method,
        'zone_minutes': None,
        'algo_version': ALGO_VERSION,
    }

_C_ZONES = {'C1', 'C2', 'C3'}
SWIM_SPORT_TYPES = {'Swim', 'swim', 'SWIM'}  # single source; performance.py imports it


def _round_half_away(x):
    """Round to nearest integer, half AWAY from zero (same convention as
    swimboxapis hr_zones)."""
    return int(math.floor(x + 0.5)) if x >= 0 else -int(math.floor(-x + 0.5))


# ── Coefficients ─────────────────────────────────────────────────────────────

def zone_coefficients(hr_zones, max_hr=None, resting_hr=None):
    """[c1..c5] from the athlete's zone boundaries AS STORED, via the
    continuous function. None when no valid container is available.

    x is the HRR fraction at the zone's bpm midpoint. When resting HR is
    unknown, rest=0 makes x the %HRmax fraction — the §2.2 approximation —
    which also covers manual containers (their lower_pct/upper_pct are
    nulled on edit, so bpm midpoints are the only trustworthy input)."""
    zones = (hr_zones or {}).get('zones') or []
    if len(zones) != 5:
        return None
    derived_from = (hr_zones or {}).get('derived_from') or {}
    max_hr = max_hr or derived_from.get('max_hr') or zones[4].get('upper_bpm')
    resting_hr = resting_hr if resting_hr is not None else derived_from.get('resting_hr')
    rest = int(resting_hr) if resting_hr else 0
    try:
        max_hr = int(max_hr)
    except (TypeError, ValueError):
        return None
    reserve = max_hr - rest
    if reserve <= 0:
        return None
    coefficients = []
    for z in zones:
        lower, upper = z.get('lower_bpm'), z.get('upper_bpm')
        if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
            return None
        mid = (lower + upper) / 2.0
        x = min(max(mid - rest, 0), reserve) / reserve
        coefficients.append(W_COEFF_A * math.exp(W_COEFF_B * x))
    return coefficients


# ── HR engine (§3) ───────────────────────────────────────────────────────────

def has_hr_signal(activity):
    """True when the HR engine could run: any real HR sample, or a summary
    average to fall back on. Used by the caller to decide whether fetching
    zones is worth a request."""
    stream = activity.get('heart_rate_stream') or {}
    if any(hr is not None for hr in (stream.get('heartrate') or [])):
        return True
    return bool(stream.get('average_heartrate')) and bool(activity.get('duration'))


def _clean_stream(activity):
    """Paired, time-sorted (t, hr) samples with interior None heart rates
    linearly interpolated and edge Nones copied from the nearest value.
    None when fewer than 2 usable samples or zero span."""
    stream = activity.get('heart_rate_stream') or {}
    times, hrs = stream.get('time') or [], stream.get('heartrate') or []
    pairs = [(t, hr) for t, hr in zip(times, hrs)
             if isinstance(t, (int, float)) and not isinstance(t, bool)]
    pairs.sort(key=lambda p: p[0])
    known = [i for i, (_, hr) in enumerate(pairs) if hr is not None]
    if len(known) < 2 or pairs[known[-1]][0] <= pairs[known[0]][0]:
        return None
    filled = []
    for i, (t, hr) in enumerate(pairs):
        if hr is None:
            prev = max((k for k in known if k < i), default=None)
            nxt = min((k for k in known if k > i), default=None)
            if prev is None:
                hr = pairs[nxt][1]
            elif nxt is None:
                hr = pairs[prev][1]
            else:
                t0, h0 = pairs[prev]
                t1, h1 = pairs[nxt]
                hr = h0 if t1 == t0 else h0 + (h1 - h0) * (t - t0) / (t1 - t0)
        filled.append((t, hr))
    return filled


def _classify_bpm(bpm, zones):
    """Zone number 1-5 for a bpm value, or 0 (unclassified) below zone 1.
    Above zone 5's upper clamps to 5 (§3)."""
    bpm = _round_half_away(bpm)
    if bpm > zones[4]['upper_bpm']:
        return 5
    if bpm < zones[0]['lower_bpm']:
        return 0
    for z in zones:
        if z['lower_bpm'] <= bpm <= z['upper_bpm']:
            return z['zone']
    return 0  # unreachable for contiguous containers


def _hr_engine(activity, hr_zones):
    """(zone_seconds, total_seconds) from the HR stream, or None.

    Scores the FULL elapsed span (§3): interior gaps left-Riemann over the
    irregular time axis, the stretch before the first sample at the first
    reading, and any tail beyond the last sample (strap dropout / stream
    shorter than activity duration) at the last reading."""
    zones = (hr_zones or {}).get('zones') or []
    if len(zones) != 5:
        return None
    samples = _clean_stream(activity)
    duration = activity.get('duration') or 0

    zone_seconds = {z: 0.0 for z in range(6)}  # 0 = below zone 1, scores nothing
    if samples is None:
        # Fallback: arrays unusable but a summary average exists — score the
        # whole duration at the average bpm. An average BELOW zone 1 is
        # treated as unusable data (a lone summary value under the easiest
        # band is almost certainly optical-sensor junk, seen on real pool
        # swims): decline so the caller falls through to the pace engine
        # instead of storing a 0-point session.
        avg = (activity.get('heart_rate_stream') or {}).get('average_heartrate')
        if not avg or not duration or duration <= 0:
            return None
        zone = _classify_bpm(avg, zones)
        if zone == 0:
            return None
        zone_seconds[zone] = float(duration)
        return {z: zone_seconds[z] for z in range(1, 6)}, float(duration)

    start = max(samples[0][0], 0)
    end = max(float(duration), samples[-1][0])
    if end <= 0:
        return None
    zone_seconds[_classify_bpm(samples[0][1], zones)] += start          # leading edge
    for (t0, h0), (t1, _) in zip(samples, samples[1:]):                  # left-Riemann
        if t1 > t0:
            zone_seconds[_classify_bpm(h0, zones)] += t1 - t0
    if end > samples[-1][0]:                                             # trailing edge
        zone_seconds[_classify_bpm(samples[-1][1], zones)] += end - samples[-1][0]
    return {z: zone_seconds[z] for z in range(1, 6)}, end


# ── Pace engine (§4-5) ───────────────────────────────────────────────────────

def _classify_lap_pace(distance, moving_time, table):
    """Pace zone label for a work lap: nearest Ritmi target pace-per-meter
    at the nearest table distance. Slower than A1 → A1, faster than C3 →
    the fastest defined zone for that distance (§5 boundaries by midpoint)."""
    row_distance = min(RITMI_DISTANCES, key=lambda d: (abs(d - distance), d))
    row = table[row_distance]
    realised = moving_time / distance
    best_zone, best_delta = None, None
    for zone, seconds in row.items():
        if seconds is None:
            continue
        delta = abs(realised - seconds / row_distance)
        if best_delta is None or delta < best_delta:
            best_zone, best_delta = zone, delta
    return best_zone


def _segments_from_laps(laps, table):
    """Laps → ordered work/rest segments.

    Work lap: positive distance and moving_time, classified into a pace
    zone. Its elapsed−moving gap ≥ REST_GAP_MIN_S becomes a trailing rest
    segment (smaller gaps fold into the work time — timing noise).
    Zero-distance laps (FIT keeps them) are rest."""
    segments = []
    for lap in laps:
        try:
            distance = float(lap.get('distance') or 0)
        except (TypeError, ValueError):
            distance = 0.0
        moving = lap.get('moving_time') or 0
        elapsed = lap.get('elapsed_time') or moving
        if not isinstance(moving, (int, float)) or not isinstance(elapsed, (int, float)):
            continue
        elapsed = max(elapsed, moving)
        if distance > 0 and moving > 0:
            gap = elapsed - moving
            work_seconds = float(moving) + (gap if gap < REST_GAP_MIN_S else 0)
            segments.append({'kind': 'work', 'distance': distance,
                             'moving': float(moving), 'seconds': work_seconds,
                             'zone': _classify_lap_pace(distance, moving, table)})
            if gap >= REST_GAP_MIN_S:
                segments.append({'kind': 'rest', 'seconds': float(gap)})
        elif elapsed > 0:
            segments.append({'kind': 'rest', 'seconds': float(elapsed)})
    return segments


def _group_sets(segments):
    """Segments → sets (§5.4). A set is a maximal run of consecutive
    equal-distance work segments plus the rest between/after them (rest
    attaches to the open set — leading rest becomes a standalone rest set,
    scored Z1)."""
    sets, current = [], None
    for seg in segments:
        if seg['kind'] == 'work':
            if current is None or current['distance'] != seg['distance']:
                if current is not None:
                    sets.append(current)
                current = {'distance': seg['distance'], 'work': [], 'rest': []}
            current['work'].append(seg)
        else:
            if current is None:
                sets.append({'distance': None, 'work': [], 'rest': [seg]})
            else:
                current['rest'].append(seg)
    if current is not None:
        sets.append(current)
    return sets


def _effective_duration(work_segments, set_elapsed):
    """§5.4: lookup duration from set density d = work/elapsed. Sparse sets
    (d≤LOW) → mean rep duration; dense (d≥HIGH) → whole-set elapsed;
    between → linear interpolation."""
    work_total = sum(w['moving'] for w in work_segments)
    if set_elapsed <= 0 or not work_segments:
        return 0.0
    rep = work_total / len(work_segments)
    density = work_total / set_elapsed
    if density <= DENSITY_LOW:
        return rep
    if density >= DENSITY_HIGH:
        return set_elapsed
    fraction = (density - DENSITY_LOW) / (DENSITY_HIGH - DENSITY_LOW)
    return rep + fraction * (set_elapsed - rep)


def _c_attainment_split(effective_duration):
    for max_duration, split in C_ATTAINMENT:
        if max_duration is None or effective_duration <= max_duration:
            return split
    return C_ATTAINMENT[-1][1]


def _pace_engine(activity, critical_speed):
    """(zone_seconds, total_seconds) from laps + the Ritmi table, or None.

    Work minutes get the ceiling/attainment distribution (§5), rest minutes
    score one band below the preceding work (§4.4) — no double counting, so
    the zone minutes sum to the laps' elapsed time. Elapsed time beyond the
    laps (deck time before start/after stop) scores Z1."""
    try:
        critical_speed = float(critical_speed)
    except (TypeError, ValueError):
        return None
    if critical_speed <= 0:
        return None
    laps = activity.get('laps') or []
    if not laps:
        # Manual swims carry only totals (no laps, no HR): treat the whole
        # swim as ONE continuous effort at average pace — the same result
        # the engine produces for a single-lap device recording. Skipped
        # when the implied pace is implausible for swimming (same 45s-6'00
        # per-100m band the manual-activity sanitizer uses).
        duration = activity.get('duration') or 0
        distance = activity.get('distance') or 0
        if duration > 0 and distance > 0:
            pace_100 = duration / (distance / 100.0)
            if 45 <= pace_100 <= 360:
                laps = [{'index': 0, 'distance': float(distance),
                         'moving_time': duration, 'elapsed_time': duration}]
    table = reference_paces(critical_speed)
    segments = _segments_from_laps(laps, table)
    if not any(s['kind'] == 'work' for s in segments):
        return None

    zone_seconds = {z: 0.0 for z in range(1, 6)}
    for group in _group_sets(segments):
        rest_seconds = sum(r['seconds'] for r in group['rest'])
        if not group['work']:
            zone_seconds[1] += rest_seconds  # leading rest before any work
            continue
        set_elapsed = sum(w['seconds'] for w in group['work']) + rest_seconds
        effective = _effective_duration(group['work'], set_elapsed)
        for work in group['work']:
            pace_zone = work['zone']
            if pace_zone in _C_ZONES:
                for hr_zone, fraction in _c_attainment_split(effective).items():
                    zone_seconds[hr_zone] += work['seconds'] * fraction
            else:
                ceiling = CEILING_MAP[pace_zone]
                hr_zone = max(1, ceiling - 1) if effective < AB_SHORT_EFFORT_S else ceiling
                zone_seconds[hr_zone] += work['seconds']
        # §4.4: all of this set's rest one band below the work it follows.
        zone_seconds[REST_ZONE[group['work'][-1]['zone']]] += rest_seconds

    laps_elapsed = sum(zone_seconds.values())
    duration = activity.get('duration') or 0
    if duration > laps_elapsed:
        zone_seconds[1] += duration - laps_elapsed
    total = max(float(duration), laps_elapsed)
    return zone_seconds, total


# ── Orchestrator ─────────────────────────────────────────────────────────────

def _assemble(zone_seconds, total_seconds, coefficients, method):
    if total_seconds <= 0:
        return None
    points = sum((zone_seconds[z] / 60.0) * coefficients[z - 1] for z in range(1, 6))
    ppm = round(points / (total_seconds / 60.0), 2)
    band, label = classify_intensity(ppm)  # from the rounded (displayed) value
    return {
        'points': _round_half_away(points),
        'points_per_minute': ppm,
        'intensity_band': band,
        'intensity_label': label,
        'method': method,
        'zone_minutes': {f'z{z}': round(zone_seconds[z] / 60.0, 1) for z in range(1, 6)},
        'algo_version': ALGO_VERSION,
    }


def calculate_swimbox_points(activity, athlete_context):
    """Swimbox Points for one activity (any sport) — dict (see module
    docstring) or None when neither engine can run."""
    context = athlete_context or {}
    hr_zones = context.get('hr_zones')
    coefficients = zone_coefficients(hr_zones, context.get('max_hr'),
                                     context.get('resting_hr'))

    if coefficients is not None:
        hr_result = _hr_engine(activity, hr_zones)
        if hr_result is not None:
            return _assemble(*hr_result, coefficients, 'hr')

    # Pace engine is SWIM-ONLY: the Ritmi table comes from swimming
    # critical speed — classifying run/ride laps against it would be
    # nonsense. Non-swims without usable HR fall to the flat-rate
    # estimators (run pace / ride power / Haiku on ride speed).
    sport_type = activity.get('sport_type') or activity.get('type') or ''
    if sport_type in SWIM_SPORT_TYPES:
        pace_result = _pace_engine(activity, context.get('critical_speed'))
        if pace_result is not None:
            return _assemble(*pace_result, coefficients or STAGNO_COEFFICIENTS,
                             'pace_estimate')
        return None

    flat = _flat_estimate(activity)
    if flat is not None:
        ppm, method = flat
        return _assemble_flat(ppm, activity.get('duration') or 0, method)
    return None
