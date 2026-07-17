"""Deterministic, contract-valid simulated 15-minute electricity readings."""
from datetime import datetime, timedelta, timezone
import hashlib, math, random, uuid
NAMESPACE = uuid.UUID("34d45e32-9867-4fcf-b3eb-c5b7fe8b9c28")

def tariff_for(ts):
    return "PEAK" if 17 <= ts.hour < 22 else "SHOULDER" if 7 <= ts.hour < 17 else "OFF_PEAK"

def build_interval_event(meter_number, interval_start, seed=42):
    if interval_start.tzinfo is None: raise ValueError("interval_start must be timezone-aware")
    start = interval_start.astimezone(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=interval_start.minute % 15)
    meter_id = f"MTR-{meter_number:06d}"
    digest = int(hashlib.sha256(f"{seed}|{meter_id}|{start.isoformat()}".encode()).hexdigest()[:16], 16); rng = random.Random(digest)
    hour_load = .20 + .55 * max(0, math.sin((start.hour - 10) * math.pi / 12)); weekend = .83 if start.weekday() >= 5 else 1
    temp = 28 + 7 * math.sin((start.hour - 13) * math.pi / 12) + rng.uniform(-1.5, 1.5)
    kwh = round(max(.03, (hour_load + .018 * max(0, temp - 30)) * weekend + rng.gauss(0, .045)), 4)
    return {"schema_version":"1.0", "event_id":str(uuid.uuid5(NAMESPACE, f"{meter_id}|{start.isoformat()}")), "event_type":"INTERVAL_READING",
      "event_ts_utc":(start + timedelta(minutes=15, seconds=rng.randint(1,120))).isoformat(), "meter_id":meter_id, "device_id":f"DEV-{meter_number:06d}", "service_point_id":f"SP-{meter_number:06d}", "service_point_type":"ELECTRIC",
      "interval_start_utc":start.isoformat(), "interval_end_utc":(start+timedelta(minutes=15)).isoformat(), "interval_minutes":15, "consumption_kwh":kwh,
      "voltage_v":round(230+rng.gauss(0,3),2), "current_a":round(max(0,kwh*4000/230+rng.gauss(0,.2)),3), "power_factor":round(min(1,max(.75,.96+rng.gauss(0,.015))),3), "temperature_c":round(temp,2), "quality_code":"ACTUAL", "tariff_code":tariff_for(start), "measurement_events":[], "device_events":["TAMPER_ALERT"] if rng.random()<.0005 else []}

def generate_batch(meter_count, interval_start, seed=42):
    return [build_interval_event(i, interval_start, seed) for i in range(1, meter_count+1)]
