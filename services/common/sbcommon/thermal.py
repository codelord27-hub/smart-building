"""Simple first-order thermal model of a building zone (1-minute steps).

Used by the building simulator (to produce realistic sensor readings that react to
HVAC commands - closing the feedback loop) and by the Control Optimizer's
model-predictive controller to evaluate candidate commands.
"""

K_OUTSIDE = 0.03           # heat exchange with outside air per minute
GAIN_PER_PERSON = 0.004    # °C per minute contributed by one occupant
HVAC_RATE = 0.50           # °C per minute at 100 % power
MODES = ("OFF", "HEAT", "COOL")


def step(temp_c: float, outside_c: float, occupancy: float, mode: str, power_pct: float, minutes: float = 1.0) -> float:
    hvac = 0.0
    if mode == "HEAT":
        hvac = HVAC_RATE * power_pct / 100.0
    elif mode == "COOL":
        hvac = -HVAC_RATE * power_pct / 100.0
    delta = K_OUTSIDE * (outside_c - temp_c) + GAIN_PER_PERSON * occupancy + hvac
    return temp_c + delta * minutes


def mpc_choose(temp_c: float, target_c: float, band_c: float, occupancy: float, outside_c: float,
               rated_kw: float, horizon_min: int = 60, power_step: int = 5,
               comfort_weight: float = 10.0, energy_weight: float = 0.05) -> dict:
    """Grid-search model-predictive control.

    For every candidate (mode, power) the zone temperature is simulated over the horizon and a cost
    = comfort penalty (squared deviation outside the comfort band) + energy cost is computed.
    Returns the cheapest command. This is intentionally CPU-bound so that the HPA on the
    Control Optimizer reacts to load.
    """
    best = None
    for mode in MODES:
        powers = [0] if mode == "OFF" else range(power_step, 101, power_step)
        for power in powers:
            t = temp_c
            cost = 0.0
            kw = rated_kw * power / 100.0
            for _ in range(horizon_min):
                t = step(t, outside_c, occupancy, mode, power)
                dev = abs(t - target_c) - band_c
                if dev > 0:
                    cost += comfort_weight * dev * dev
                cost += energy_weight * kw
            if best is None or cost < best["cost"]:
                best = {"mode": mode, "power_pct": int(power), "power_kw": round(kw, 3),
                        "cost": round(cost, 4), "predicted_temp_c": round(t, 2)}
    return best
