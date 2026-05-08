"""
generate_data.py — Generates 2-hour normal telemetry CSV for model training.
Produces 15 devices × 7200 seconds = 108,000 rows with realistic diurnal patterns.

Usage: python generate_data.py
"""
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

DEVICES = [f"dev{i}" for i in range(1, 16)]
DEVICE_ROLES = {
    **{f"dev{i}": "core"   for i in range(1,  4)},
    **{f"dev{i}": "edge"   for i in range(4,  8)},
    **{f"dev{i}": "access" for i in range(8, 16)},
}
DURATION = 7200   # 2 hours at 1 Hz
ROLE_SCALE = {"core": 1.8, "edge": 1.2, "access": 0.8}

os.makedirs("data", exist_ok=True)
rng   = np.random.default_rng(42)
start = datetime(2026, 1, 1, 7, 0, 0)
rows  = []

for t in range(DURATION):
    ts         = start + timedelta(seconds=t)
    hour_wave  = 1 + 0.25 * np.sin(2 * np.pi * t / 3600)
    for dev in DEVICES:
        sc = ROLE_SCALE[DEVICE_ROLES[dev]]
        rows.append({
            "timestamp":           ts.strftime("%Y-%m-%d %H:%M:%S"),
            "device_id":           dev,
            "interface_rx_bytes":  max(0, rng.normal(1000 * sc * hour_wave, 80)),
            "interface_tx_bytes":  max(0, rng.normal(1200 * sc * hour_wave, 100)),
            "packet_loss_pct":     max(0, rng.normal(0.08, 0.04)),
            "latency_ms":          max(0, rng.normal(5.5,  0.8)),
            "cpu_pct":             max(0, rng.normal(22,   4)),
            "memory_pct":          max(0, rng.normal(34,   4)),
            "bgp_neighbors":       int(rng.choice([4, 5], p=[0.3, 0.7])),
            "link_flaps":          int(rng.choice([0, 1], p=[0.97, 0.03])),
        })

df  = pd.DataFrame(rows)
out = "data/normal_telemetry_2hr.csv"
df.to_csv(out, index=False)
print(f"Generated {len(df):,} rows → {out}")
