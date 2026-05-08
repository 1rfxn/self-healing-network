"""
src/verify.py — 60-second post-remediation verification window.

The watcher checks the LIVE telemetry state stored in main._state["devices"]
rather than generating a fresh simulator reading (which would still show the
fault signature while injection is active).  We pass a shared `live_metrics`
dict from main so the verifier sees the same data the dashboard sees.
"""
import time, threading, queue as Q
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from decision import RemediationResult

VERIFY_WINDOW = 60   # seconds
CLEAR_THRESH  = 3    # consecutive clean reads before declaring resolved


@dataclass
class VerifyResult:
    remediation: RemediationResult
    resolved:    bool
    readings:    int
    duration_s:  float = 0.0
    timestamp:   str   = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self):
        return {**self.__dict__, "remediation": self.remediation.to_dict()}


# ── Clearance thresholds (per anomaly type) ───────────────────────────────────
def _is_clear(anomaly_type: str, metrics: dict) -> bool:
    if anomaly_type == "high_loss":    return metrics.get("packet_loss_pct", 99) < 1.0
    if anomaly_type == "high_latency": return metrics.get("latency_ms",      999) < 20
    if anomaly_type == "high_cpu":     return metrics.get("cpu_pct",          99) < 60
    if anomaly_type == "link_flap":    return metrics.get("link_flaps",       99) == 0
    return True   # unknown types — assume cleared


# ── Verification engine ───────────────────────────────────────────────────────
class VerificationEngine:
    """
    Watches live_metrics (shared dict from main) for each remediated device.
    Calls result_cb with VerifyResult when the window closes.
    Also clears the fault on the simulator once 3 clean readings are seen.
    """

    def __init__(self, simulators: dict,
                 live_metrics: dict,
                 result_cb: Callable[[VerifyResult], None]):
        self.sims         = simulators
        self.live         = live_metrics   # shared reference to _state["devices"]
        self.result_cb    = result_cb
        self._active_lock = threading.Lock()
        self._active: set = set()

    def enqueue(self, rem: RemediationResult):
        if not rem.success:
            # Blocked remediation — still emit a verify result so counters update
            self.result_cb(VerifyResult(rem, resolved=False, readings=0, duration_s=0))
            return
        dev = rem.alert.device_id
        with self._active_lock:
            if dev in self._active:
                return   # already watching this device
            self._active.add(dev)
        t = threading.Thread(target=self._watch, args=(rem,), daemon=True)
        t.start()

    def _watch(self, rem: RemediationResult):
        dev       = rem.alert.device_id
        atype     = rem.alert.anomaly_type
        t_start   = time.monotonic()
        deadline  = t_start + VERIFY_WINDOW
        clean_run = 0
        readings  = 0
        sim       = self.sims.get(dev)

        print(f"[verify] Watching {dev} for {VERIFY_WINDOW}s (anomaly={atype})")

        while time.monotonic() < deadline:
            time.sleep(3)   # check every 3 seconds
            readings += 1

            # Use the live telemetry the dashboard already receives
            m = self.live.get(dev, {})
            if not m:
                continue

            if _is_clear(atype, m):
                clean_run += 1
                print(f"[verify] {dev} clean reading {clean_run}/{CLEAR_THRESH} "
                      f"(loss={m.get('packet_loss_pct',0):.2f} "
                      f"lat={m.get('latency_ms',0):.1f} "
                      f"cpu={m.get('cpu_pct',0):.1f})")
                if clean_run >= CLEAR_THRESH:
                    # Clear the fault on the simulator so metrics return to normal
                    if sim:
                        sim.clear_fault()
                    duration = round(time.monotonic() - t_start, 1)
                    print(f"[verify] RESOLVED {dev} in {duration}s after {readings} readings")
                    with self._active_lock:
                        self._active.discard(dev)
                    self.result_cb(VerifyResult(rem, resolved=True,
                                                readings=readings, duration_s=duration))
                    return
            else:
                clean_run = 0
                print(f"[verify] {dev} still anomalous "
                      f"(loss={m.get('packet_loss_pct',0):.2f} "
                      f"lat={m.get('latency_ms',0):.1f} "
                      f"cpu={m.get('cpu_pct',0):.1f})")

        # Window expired — escalate
        duration = round(time.monotonic() - t_start, 1)
        print(f"[verify] ESCALATED {dev} after {duration}s, {readings} readings")
        if sim:
            sim.clear_fault()   # clear anyway to avoid permanent fault state
        with self._active_lock:
            self._active.discard(dev)
        self.result_cb(VerifyResult(rem, resolved=False,
                                    readings=readings, duration_s=duration))


# ── Worker launcher ───────────────────────────────────────────────────────────
def start_verify_worker(result_queue: Q.Queue,
                        simulators: dict,
                        live_metrics: dict,
                        verify_cb: Callable) -> tuple["VerificationEngine", threading.Event]:
    engine = VerificationEngine(simulators, live_metrics, verify_cb)
    stop   = threading.Event()

    def _worker():
        print("[verify] Worker started")
        while not stop.is_set():
            try:
                rem = result_queue.get(timeout=1)
                engine.enqueue(rem)
            except Q.Empty:
                pass

    threading.Thread(target=_worker, daemon=True).start()
    return engine, stop
