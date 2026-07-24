"""Ritmi pace table — PORT of swimboxapis/services/coachingutils/pacecalculator.py
(calculate_reference_paces). KEEP IN SYNC with that file: the cascade
percentages, fractionation deviations and distance caps must match, or the
pace-based Swimbox Points engine (Trello #490) diverges from the Ritmi table
athletes see in the app. Ported (rather than called over HTTP) because the
pace-calculator endpoint is user-JWT only and a per-activity network call
inside the points calculator would fail silently; guarded by golden-value
assertions in tests/test_swimbox_points.py.

Pure python — the original's pandas DataFrame is only a container.
"""

DISTANCES = [10, 12.5, 15, 25, 35, 50, 75, 100, 150, 200, 300, 400, 800, 1500]
ZONES = ['A1', 'A2', 'B1', 'B2', 'C1', 'C2', 'C3']

# Fractionation deviation per distance (percent), index-aligned with DISTANCES.
_FRACTIONATION = [0.0, 0.0, -2.0, -2.0, -2.0, -5.0, -5.0, 0.0, 3.0, 3.0, 3.0, 4.5, 5.0, 6.0]

# Zone distance caps: C3 <= 25m, C2 <= 150m, C1 <= 200m.
_ZONE_MAX_DISTANCE = {'C1': 200, 'C2': 150, 'C3': 25}


def reference_paces(test_time_s, test_distance_m=100):
    """Reference paces (seconds) per distance and zone.

    The API treats critical speed (`continuous_pace_for_15`, seconds per
    100m) as a 100m test time, hence the default test distance.

    Returns {distance_m: {zone: seconds or None}} — None where the zone is
    not defined for that distance (distance caps above).
    """
    pace_per_meter = float(test_time_s) / float(test_distance_m)
    base = [pace_per_meter * d for d in DISTANCES]

    # B1 is the anchor: base + metabolic deviation (-6%) + fractionation.
    b1 = [b + (b * -6.0 / 100) + (b * f / 100) for b, f in zip(base, _FRACTIONATION)]
    # Cascade, each zone relative to the adjacent one.
    a2 = [t + (t * 5.0 / 100) for t in b1]
    a1 = [t + (t * 12.0 / 100) for t in a2]
    b2 = [t + (t * -5.0 / 100) for t in b1]
    c1 = [t + (t * -5.5 / 100) for t in b2]
    c2 = [t + (t * -6.0 / 100) for t in c1]
    c3 = [t + (t * -6.5 / 100) for t in c2]

    columns = {'A1': a1, 'A2': a2, 'B1': b1, 'B2': b2, 'C1': c1, 'C2': c2, 'C3': c3}
    table = {}
    for i, d in enumerate(DISTANCES):
        table[d] = {
            zone: (None if d > _ZONE_MAX_DISTANCE.get(zone, float('inf'))
                   else columns[zone][i])
            for zone in ZONES
        }
    return table
