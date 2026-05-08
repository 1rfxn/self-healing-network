"""
src/detect.py — Anomaly detection: Isolation Forest (point) + LSTM Autoencoder (temporal).

Usage:
  python src/detect.py --train --csv data/normal_telemetry_2hr.csv
  python src/detect.py --infer  (reads from demo queue, prints alerts)
"""
import os, sys, json, time, argparse, threading, queue as Q
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, deque

import numpy as np
import pandas as pd

MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models")
ISO_PATH    = os.path.join(MODEL_DIR, "iso_forest.pkl")
LSTM_PATH   = os.path.join(MODEL_DIR, "lstm_ae.pt")
SEQ_LEN     = 20          # LSTM window (20 seconds per device)
LSTM_THRESH = 0.05        # MSE above this = anomaly
ISO_CONTAM  = 0.05

FEATURES = [
    "interface_rx_bytes", "interface_tx_bytes",
    "packet_loss_pct",    "latency_ms",
    "cpu_pct",            "memory_pct",
    "bgp_neighbors",      "link_flaps",
]

# ── Alert dataclass ──────────────────────────────────────────────────────────
@dataclass
class Alert:
    device_id:   str
    anomaly_type: str          # high_loss | high_latency | high_cpu | link_flap | unknown
    confidence:  float         # 0–1
    model:       str           # iso | lstm | both
    metrics:     dict = field(default_factory=dict)
    timestamp:   str  = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self):
        return self.__dict__.copy()


# ── Feature helpers ──────────────────────────────────────────────────────────
def _classify_anomaly(row: dict) -> str:
    if row.get("packet_loss_pct", 0) > 2.0:   return "high_loss"
    if row.get("latency_ms", 0)      > 50:     return "high_latency"
    if row.get("cpu_pct", 0)         > 70:     return "high_cpu"
    if row.get("link_flaps", 0)      >= 2:     return "link_flap"
    return "unknown"


def _to_vector(row: dict) -> np.ndarray:
    return np.array([float(row.get(f, 0)) for f in FEATURES], dtype=np.float32)


# ── Isolation Forest ─────────────────────────────────────────────────────────
class IsoForestDetector:
    def __init__(self):
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler
        self.scaler = StandardScaler()
        self.model  = IsolationForest(contamination=ISO_CONTAM,
                                      n_estimators=200,
                                      random_state=42,
                                      n_jobs=-1)

    def train(self, df: pd.DataFrame):
        X = df[FEATURES].values
        Xs = self.scaler.fit_transform(X)
        self.model.fit(Xs)
        print(f"[iso] Trained on {len(X):,} samples")

    def predict(self, row: dict) -> tuple[bool, float]:
        """Returns (is_anomaly, confidence 0-1)."""
        x  = self.scaler.transform([_to_vector(row)])
        sc = self.model.decision_function(x)[0]          # negative = more anomalous
        pred = self.model.predict(x)[0]                  # -1 = anomaly
        confidence = float(np.clip((-sc) / 0.3, 0, 1))  # normalise to 0-1
        return (pred == -1), confidence

    def save(self):
        import pickle
        os.makedirs(MODEL_DIR, exist_ok=True)
        with open(ISO_PATH, "wb") as f:
            pickle.dump((self.scaler, self.model), f)
        print(f"[iso] Saved → {ISO_PATH}")

    @classmethod
    def load(cls):
        import pickle
        obj = cls.__new__(cls)
        with open(ISO_PATH, "rb") as f:
            obj.scaler, obj.model = pickle.load(f)
        print(f"[iso] Loaded ← {ISO_PATH}")
        return obj


# ── LSTM Autoencoder ─────────────────────────────────────────────────────────
class LSTMAutoencoder:
    def __init__(self, n_features: int = len(FEATURES), hidden: int = 64):
        import torch, torch.nn as nn
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        class _AE(nn.Module):
            def __init__(self):
                super().__init__()
                self.enc = nn.LSTM(n_features, hidden, batch_first=True)
                self.dec = nn.LSTM(hidden,     n_features, batch_first=True)

            def forward(self, x):
                _, (h, _) = self.enc(x)
                rep = h[-1].unsqueeze(1).repeat(1, x.size(1), 1)
                out, _ = self.dec(rep)
                return out

        self.model = _AE().to(self.device)
        self.scaler_mean: np.ndarray | None = None
        self.scaler_std:  np.ndarray | None = None

    def _scale(self, X: np.ndarray) -> np.ndarray:
        return (X - self.scaler_mean) / (self.scaler_std + 1e-8)

    def train(self, df: pd.DataFrame, epochs: int = 20):
        import torch, torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset

        X = df[FEATURES].values.astype(np.float32)
        self.scaler_mean = X.mean(axis=0)
        self.scaler_std  = X.std(axis=0)
        Xs = self._scale(X)

        # Build sequences: sliding window per (arbitrarily) the full series
        seqs = [Xs[i:i+SEQ_LEN] for i in range(len(Xs) - SEQ_LEN)]
        T = torch.tensor(np.array(seqs), dtype=torch.float32)
        dl = DataLoader(TensorDataset(T), batch_size=256, shuffle=True)

        opt   = torch.optim.Adam(self.model.parameters(), lr=1e-3)
        loss_fn = nn.MSELoss()
        self.model.train()
        for ep in range(epochs):
            total = 0
            for (batch,) in dl:
                batch = batch.to(self.device)
                opt.zero_grad()
                out  = self.model(batch)
                loss = loss_fn(out, batch)
                loss.backward()
                opt.step()
                total += loss.item()
            if ep % 5 == 0 or ep == epochs - 1:
                print(f"[lstm] epoch {ep+1}/{epochs}  loss={total/len(dl):.5f}")
        print("[lstm] Training complete")

    def predict(self, window: list[dict]) -> tuple[bool, float]:
        """window: list of SEQ_LEN raw dicts. Returns (is_anomaly, mse)."""
        if self.scaler_mean is None:
            return False, 0.0
        import torch
        X  = np.array([_to_vector(r) for r in window], dtype=np.float32)
        Xs = self._scale(X)
        t  = torch.tensor(Xs, dtype=torch.float32).unsqueeze(0).to(self.device)
        self.model.eval()
        with torch.no_grad():
            out = self.model(t)
        mse = float(torch.mean((out - t) ** 2).item())
        return mse > LSTM_THRESH, mse

    def save(self):
        import torch
        os.makedirs(MODEL_DIR, exist_ok=True)
        torch.save({
            "model": self.model.state_dict(),
            "mean":  self.scaler_mean,
            "std":   self.scaler_std,
        }, LSTM_PATH)
        print(f"[lstm] Saved → {LSTM_PATH}")

    @classmethod
    def load(cls):
        import torch
        obj = cls()
        ck  = torch.load(LSTM_PATH, map_location=obj.device)
        obj.model.load_state_dict(ck["model"])
        obj.model.eval()
        obj.scaler_mean = ck["mean"]
        obj.scaler_std  = ck["std"]
        print(f"[lstm] Loaded ← {LSTM_PATH}")
        return obj


# ── Detector orchestrator ────────────────────────────────────────────────────
class AnomalyDetector:
    """Wraps both models; maintains per-device sliding windows for LSTM."""

    def __init__(self, iso: IsoForestDetector, lstm: LSTMAutoencoder,
                 alert_queue: Q.Queue):
        self.iso   = iso
        self.lstm  = lstm
        self.aq    = alert_queue
        self._wins: dict[str, deque] = defaultdict(lambda: deque(maxlen=SEQ_LEN))

    def process(self, record: dict):
        dev = record["device_id"]
        self._wins[dev].append(record)

        iso_flag, iso_conf = self.iso.predict(record)
        lstm_flag, lstm_mse = (False, 0.0)
        if len(self._wins[dev]) == SEQ_LEN:
            lstm_flag, lstm_mse = self.lstm.predict(list(self._wins[dev]))

        if iso_flag or lstm_flag:
            model_tag = ("both" if iso_flag and lstm_flag
                         else "iso" if iso_flag else "lstm")
            confidence = max(iso_conf, min(lstm_mse / LSTM_THRESH, 1.0))
            alert = Alert(
                device_id    = dev,
                anomaly_type = _classify_anomaly(record),
                confidence   = round(confidence, 3),
                model        = model_tag,
                metrics      = {k: record.get(k) for k in FEATURES},
            )
            try:
                self.aq.put_nowait(alert)
            except Q.Full:
                pass  # drop if decision engine is backed up


# ── Training entry point ─────────────────────────────────────────────────────
def train(csv_path: str):
    df = pd.read_csv(csv_path)
    print(f"[train] Loaded {len(df):,} rows from {csv_path}")

    iso = IsoForestDetector()
    iso.train(df)
    iso.save()

    lstm = LSTMAutoencoder()
    lstm.train(df)
    lstm.save()
    print("[train] All models saved.")
    return iso, lstm


# ── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--train", action="store_true")
    ap.add_argument("--csv",   default="data/normal_telemetry_2hr.csv")
    args = ap.parse_args()

    if args.train:
        if not os.path.exists(args.csv):
            print(f"CSV not found: {args.csv}. Run generate_data.py first.")
            sys.exit(1)
        train(args.csv)
    else:
        print("Pass --train to train models.")
