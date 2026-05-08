"""
src/main.py — Orchestrator + Flask-SocketIO WebSocket server.

Usage:
  python src/main.py --demo       (no Kafka, auto fault injection every 90s)
  python src/main.py              (requires Kafka + trained models)
"""
import os, sys, json, time, queue as Q, argparse, threading, random
from datetime import datetime
from collections import deque, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import eventlet
eventlet.monkey_patch()

from flask import Flask, jsonify
from flask_socketio import SocketIO
from flask_cors import CORS

from ingest import DeviceSimulator, DEVICES, DEVICE_ROLES, start_ingestion, get_demo_queue
from detect import IsoForestDetector, LSTMAutoencoder, Alert, FEATURES, _classify_anomaly, LSTM_THRESH
from decision import IntentEngine, start_decision_worker
from verify import start_verify_worker

# ── Flask ─────────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app, origins="*")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet",
                    logger=False, engineio_logger=False)

# ── Shared state ──────────────────────────────────────────────────────────────
_state = {
    "devices":         {},          # device_id → latest telemetry dict  (LIVE)
    "device_status":   {},          # device_id → "ok"|"anomaly"|"remediating"|"verifying"|"resolved"|"escalated"
    "events":          deque(maxlen=200),
    "alerts_total":    0,
    "resolved_total":  0,
    "escalated_total": 0,
    "mttr_samples":    deque(maxlen=100),
    "active_faults":   set(),
    "incident_start":  {},          # device_id → epoch when anomaly detected
}

alert_queue  = Q.Queue(maxsize=300)
result_queue = Q.Queue(maxsize=300)
_simulators: dict = {}

# ── Helpers ───────────────────────────────────────────────────────────────────
def _now_str():
    return datetime.utcnow().isoformat()


def _emit_event(etype: str, payload: dict):
    """Append to log and push to all dashboard clients."""
    event = {"type": etype, "ts": _now_str(), **payload}
    _state["events"].appendleft(event)
    socketio.emit("event", event)


def _emit_stats():
    """Push updated KPI numbers to dashboard."""
    samples = list(_state["mttr_samples"])
    avg_mttr = round(sum(samples) / len(samples), 1) if samples else 0
    socketio.emit("state", {
        "devices":         _state["devices"],
        "device_status":   _state["device_status"],
        "alerts_total":    _state["alerts_total"],
        "resolved_total":  _state["resolved_total"],
        "escalated_total": _state["escalated_total"],
        "avg_mttr":        avg_mttr,
        "active_faults":   list(_state["active_faults"]),
    })


def _set_device_status(dev: str, status: str):
    _state["device_status"][dev] = status
    socketio.emit("device_status", {"device_id": dev, "status": status})


# ── Pipeline callbacks ────────────────────────────────────────────────────────
def on_remediation(result):
    """
    Called by IntentEngine after constraint check + NETCONF push attempt.
    Puts result on verify queue and emits a rich event to the dashboard.
    """
    dev = result.alert.device_id

    if result.success:
        _set_device_status(dev, "verifying")
        _emit_event("remediation", {
            "device_id":    dev,
            "anomaly_type": result.alert.anomaly_type,
            "playbook":     result.playbook,
            "action":       result.action,
            "duration_ms":  result.duration_ms,
            "success":      True,
            "message":      f"NETCONF pushed '{result.playbook}' to {dev} in {result.duration_ms}ms — watching for clearance",
        })
    else:
        _set_device_status(dev, "escalated")
        _state["escalated_total"] += 1
        _state["active_faults"].discard(dev)
        _emit_event("blocked", {
            "device_id": dev,
            "playbook":  result.playbook,
            "reason":    result.blocked_reason,
            "message":   f"Remediation blocked for {dev}: {result.blocked_reason}",
        })

    try:
        result_queue.put_nowait(result)
    except Q.Full:
        pass

    _emit_stats()


def on_verify(vresult):
    """
    Called by VerificationEngine when the 60-second window closes.
    Updates counters, device status, MTTR and emits final event.
    """
    dev      = vresult.remediation.alert.device_id
    atype    = vresult.remediation.alert.anomaly_type
    duration = vresult.duration_s

    _state["active_faults"].discard(dev)
    _state["incident_start"].pop(dev, None)

    if vresult.resolved:
        _state["resolved_total"] += 1
        _state["mttr_samples"].append(duration)
        _set_device_status(dev, "resolved")
        _emit_event("resolved", {
            "device_id":   dev,
            "anomaly_type": atype,
            "duration_s":  duration,
            "readings":    vresult.readings,
            "message":     f"HEALED {dev} — {atype} cleared in {duration}s after {vresult.readings} readings",
        })
        # Auto-reset status to ok after 5 seconds
        def _reset():
            time.sleep(5)
            _set_device_status(dev, "ok")
        threading.Thread(target=_reset, daemon=True).start()

    else:
        _state["escalated_total"] += 1
        _set_device_status(dev, "escalated")
        _emit_event("escalated", {
            "device_id":  dev,
            "anomaly_type": atype,
            "duration_s": duration,
            "message":    f"ESCALATED {dev} — {atype} not resolved after {duration}s, manual intervention needed",
        })

    _emit_stats()


# ── Thin ML wrapper ────────────────────────────────────────────────────────────
class _Detector:
    """Wraps IsoForest + LSTM and maintains per-device sliding windows."""
    def __init__(self, iso, lstm):
        self.iso  = iso
        self.lstm = lstm
        self._wins = defaultdict(lambda: deque(maxlen=20))

    def process(self, rec: dict):
        """Returns (is_anomaly, confidence, anomaly_type, model_tag)."""
        dev = rec["device_id"]
        self._wins[dev].append(rec)

        iso_flag, iso_conf = self.iso.predict(rec)
        lstm_flag, lstm_mse = False, 0.0
        if len(self._wins[dev]) == 20:
            lstm_flag, lstm_mse = self.lstm.predict(list(self._wins[dev]))

        is_anom   = iso_flag or lstm_flag
        model_tag = ("both" if iso_flag and lstm_flag
                     else "iso" if iso_flag else "lstm" if lstm_flag else "none")
        conf      = max(iso_conf, min(lstm_mse / LSTM_THRESH, 1.0)) if is_anom else 0.0
        atype     = _classify_anomaly(rec) if is_anom else "none"
        return is_anom, round(conf, 3), atype, model_tag


# ── Telemetry consumer ────────────────────────────────────────────────────────
COOLDOWN_S = 90   # min seconds between alerts for same device

def _consume_telemetry(demo: bool, detector):
    cooldown: dict[str, float] = {}

    source = get_demo_queue() if demo else None
    if demo:
        print("[main] Consuming telemetry from demo queue")
    else:
        from kafka import KafkaConsumer
        source = KafkaConsumer(
            "telemetry",
            bootstrap_servers=["localhost:9092"],
            value_deserializer=lambda m: json.loads(m.decode()),
        )
        print("[main] Consuming telemetry from Kafka")

    def _get_next():
        if demo:
            try:
                return source.get(timeout=1)
            except Q.Empty:
                return None
        else:
            return next(iter(source)).value

    while True:
        rec = _get_next()
        if rec is None:
            continue

        dev = rec["device_id"]

        # Update live state (this is what verify.py now reads)
        _state["devices"][dev] = rec

        # Broadcast telemetry to dashboard (every other second to reduce load)
        if int(time.time()) % 2 == 0:
            socketio.emit("telemetry", rec)

        if detector is None:
            continue

        is_anom, conf, atype, model_tag = detector.process(rec)
        if not is_anom:
            continue

        now = time.time()
        if now - cooldown.get(dev, 0) < COOLDOWN_S:
            continue   # still in cooldown for this device

        cooldown[dev] = now
        _state["alerts_total"]  += 1
        _state["active_faults"].add(dev)
        _state["incident_start"][dev] = now

        _set_device_status(dev, "anomaly")

        alert = Alert(
            device_id    = dev,
            anomaly_type = atype,
            confidence   = conf,
            model        = model_tag,
            metrics      = {k: rec.get(k) for k in [
                "packet_loss_pct", "latency_ms", "cpu_pct",
                "memory_pct", "bgp_neighbors", "link_flaps",
            ]},
        )

        _emit_event("alert", {
            "device_id":    dev,
            "anomaly_type": atype,
            "confidence":   conf,
            "model":        model_tag,
            "metrics":      alert.metrics,
            "message":      f"Anomaly: {atype} on {dev} ({conf*100:.0f}% conf, model: {model_tag})",
        })

        _set_device_status(dev, "remediating")
        _emit_stats()

        try:
            alert_queue.put_nowait(alert)
        except Q.Full:
            pass


# ── Auto fault injection (demo mode) ──────────────────────────────────────────
def _auto_inject(sims: dict):
    fault_types = ["high_loss", "high_latency", "high_cpu", "link_flap"]
    devs = list(sims.keys())
    time.sleep(30)   # quiet opening period
    while True:
        dev   = random.choice(devs)
        fault = random.choice(fault_types)
        print(f"[demo] Auto-injecting '{fault}' → {dev}")
        sims[dev].inject_fault(fault)
        time.sleep(90)


# ── REST endpoints ─────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok", "ts": _now_str()})


@app.route("/api/state")
def api_state():
    samples = list(_state["mttr_samples"])
    return jsonify({
        "devices":         _state["devices"],
        "device_status":   _state["device_status"],
        "events":          list(_state["events"])[:50],
        "alerts_total":    _state["alerts_total"],
        "resolved_total":  _state["resolved_total"],
        "escalated_total": _state["escalated_total"],
        "avg_mttr":        round(sum(samples)/len(samples), 1) if samples else 0,
        "active_faults":   list(_state["active_faults"]),
    })


@app.route("/api/inject/<device_id>/<fault_type>", methods=["POST"])
def inject_fault(device_id, fault_type):
    sim = _simulators.get(device_id)
    if not sim:
        return jsonify({"error": f"device '{device_id}' not found"}), 404
    valid = ["high_loss", "high_latency", "high_cpu", "link_flap"]
    if fault_type not in valid:
        return jsonify({"error": f"fault_type must be one of {valid}"}), 400
    sim.inject_fault(fault_type)
    print(f"[inject] Manual: '{fault_type}' → {device_id}")
    _emit_event("fault_injected", {
        "device_id":  device_id,
        "fault_type": fault_type,
        "message":    f"Manual fault injection: {fault_type} → {device_id}",
    })
    return jsonify({"ok": True, "device": device_id, "fault": fault_type})


@socketio.on("connect")
def on_ws_connect():
    print("[ws] Client connected")
    _emit_stats()
    # Replay last 30 events to newly connected client
    for ev in list(_state["events"])[:30]:
        socketio.emit("event", ev)


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    global _simulators

    ap = argparse.ArgumentParser()
    ap.add_argument("--demo",       action="store_true")
    ap.add_argument("--skip-train", action="store_true",
                    help="Skip model load — pure simulation, no ML detection")
    ap.add_argument("--port",       type=int, default=5000)
    args = ap.parse_args()

    print("=" * 54)
    print("   Self-Healing Network AIOps")
    print(f"   Mode: {'DEMO (no Kafka)' if args.demo else 'FULL (Kafka)'}")
    print("=" * 54)

    # Load ML models
    detector = None
    if not args.skip_train:
        try:
            iso      = IsoForestDetector.load()
            lstm     = LSTMAutoencoder.load()
            detector = _Detector(iso, lstm)
            print("[main] ML models loaded ✓")
        except FileNotFoundError:
            print("[main] WARNING: Models not found. Running without ML detection.")
            print("       Fix: python src/detect.py --train --csv data/normal_telemetry_2hr.csv")

    # Start ingestion
    sims, _ = start_ingestion(demo=args.demo)
    _simulators = sims

    # Start decision worker (alert_queue → on_remediation)
    start_decision_worker(alert_queue, sims, on_remediation)

    # Start verify worker — pass _state["devices"] as live_metrics
    start_verify_worker(result_queue, sims, _state["devices"], on_verify)

    # Start telemetry consumer thread
    threading.Thread(
        target=_consume_telemetry, args=(args.demo, detector), daemon=True
    ).start()

    # Auto fault injection in demo mode
    if args.demo:
        threading.Thread(target=_auto_inject, args=(sims,), daemon=True).start()

    print(f"\n   Dashboard : http://localhost:3000")
    print(f"   Backend   : http://localhost:{args.port}/health")
    print(f"   Inject    : POST http://localhost:{args.port}/api/inject/dev1/high_loss")
    print(f"   State API : http://localhost:{args.port}/api/state\n")

    socketio.run(app, host="0.0.0.0", port=args.port, debug=False)


if __name__ == "__main__":
    main()
