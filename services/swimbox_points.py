"""Swimbox Points calculator — STUB (zones #486 §7.3).

Single dedicated entry point so the real algorithm lands as a one-file
change. The sport-type gate (swimming only, no multisport) lives in the
CALLING path (services/performance.py) — this function assumes it is only
ever handed a swimming activity.

The interface contract is already final: the calculator receives the
activity AND the athlete context (HR zones, Max HR, Resting HR, critical
speed) so the real implementation needs no caller refactoring. It MUST be
deterministic for a given activity + athlete state — points are recomputed
and overwritten on every activity update (§7.2), so a non-deterministic
implementation would drift on unrelated edits.

athlete_context shape:
    {
        'hr_zones':       zone container dict or None,
        'max_hr':         int or None,
        'resting_hr':     int or None,
        'critical_speed': seconds-per-100m float or None,
    }
"""


def calculate_swimbox_points(activity, athlete_context):
    """Return the Swimbox Points for one swimming activity.

    STUB: fixed 90 for every qualifying activity."""
    return 90
