"""
src/decision.py — Intent engine: maps Alert → playbook → constraint check → NETCONF push.
"""
import os, time, threading, queue as Q, yaml
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from detect import Alert

PLAYBOOK_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "playbooks")

# ── Remediation result ────────────────────────────────────────────────────────
@dataclass
class RemediationResult:
    alert:          Alert
    playbook:       str
    action:         str
    success:        bool
    blocked_reason: str = ""
    netconf_xml:    str = ""
    timestamp:      str = field(default_factory=lambda: datetime.utcnow().isoformat())
    duration_ms:    int = 0

    def to_dict(self):
        return {**self.__dict__, "alert": self.alert.to_dict()}


# ── Constraint model ──────────────────────────────────────────────────────────
class ConstraintModel:
    def __init__(self, simulators: dict):
        self.simulators = simulators

    def _current(self, device_id: str) -> dict | None:
        sim = self.simulators.get(device_id)
        return sim.read() if sim else None

    def check_reroute(self, alert: Alert) -> tuple[bool, str]:
        from ingest import DEVICE_ROLES                       # ← fixed import
        role = DEVICE_ROLES.get(alert.device_id, "access")
        candidates = [d for d, r in DEVICE_ROLES.items()
                      if r == role and d != alert.device_id]
        for alt in candidates:
            m = self._current(alt)
            if m and m["cpu_pct"] < 80 and m["memory_pct"] < 80:
                return True, f"alternate={alt} cpu={m['cpu_pct']:.0f}% mem={m['memory_pct']:.0f}%"
        return False, "No suitable alternate path — all at capacity"

    def check_qos(self, alert: Alert) -> tuple[bool, str]:
        m = self._current(alert.device_id)
        if m and m["cpu_pct"] < 70:
            return True, f"cpu={m['cpu_pct']:.0f}%"
        return False, f"CPU too high ({m['cpu_pct']:.0f}%) — QoS push would worsen load"

    def check_scale(self, alert: Alert) -> tuple[bool, str]:
        m = self._current(alert.device_id)
        if m and m["memory_pct"] < 80:
            return True, f"mem_headroom={100 - m['memory_pct']:.0f}%"
        return False, "Insufficient memory headroom"

    def evaluate(self, alert: Alert, playbook: dict) -> tuple[bool, str]:
        checks = {
            "reroute_traffic": self.check_reroute,
            "adjust_qos":      self.check_qos,
            "scale_resources": self.check_scale,
        }
        fn = checks.get(playbook.get("name", ""))
        return fn(alert) if fn else (True, "no constraint")


# ── NETCONF pusher ────────────────────────────────────────────────────────────
def _build_netconf_xml(playbook: dict, alert: Alert) -> str:
    action = playbook.get("action", "unknown")
    dev    = alert.device_id
    if action == "reroute":
        return f"""<config>
  <interfaces xmlns="urn:ietf:params:xml:ns:yang:ietf-interfaces">
    <interface><name>{dev}-eth0</name><enabled>false</enabled></interface>
    <interface><name>{dev}-eth1</name><enabled>true</enabled></interface>
  </interfaces>
</config>"""
    if action == "qos":
        return f"""<config>
  <qos xmlns="urn:ietf:params:xml:ns:yang:ietf-qos-classifier">
    <policy><name>{dev}-policy</name><priority>high</priority></policy>
  </qos>
</config>"""
    if action == "scale":
        return f"""<config>
  <system xmlns="urn:ietf:params:xml:ns:yang:ietf-system">
    <resource-policy><name>{dev}-scale</name><cpu-limit>60</cpu-limit></resource-policy>
  </system>
</config>"""
    return "<config><noop/></config>"


def push_netconf(xml: str, device_id: str) -> bool:
    host = os.environ.get("NETCONF_HOST")
    if host:
        try:
            from ncclient import manager
            with manager.connect(
                host=host,
                port=int(os.environ.get("NETCONF_PORT", 830)),
                username=os.environ.get("NETCONF_USER", "admin"),
                password=os.environ.get("NETCONF_PASS", ""),
                hostkey_verify=False,
            ) as m:
                m.edit_config(target="candidate", config=xml)
                m.commit()
            print(f"[netconf] REAL push OK → {device_id}")
            return True
        except Exception as e:
            print(f"[netconf] Error for {device_id}: {e}")
            return False
    else:
        print(f"[netconf-sim] Config push → {device_id} (simulation mode)")
        time.sleep(0.4)   # simulate network RTT
        return True


# ── Intent engine ─────────────────────────────────────────────────────────────
ANOMALY_PLAYBOOK_MAP = {
    "high_loss":    "reroute_traffic",
    "high_latency": "adjust_qos",
    "high_cpu":     "scale_resources",
    "link_flap":    "reroute_traffic",
    "unknown":      "adjust_qos",
}


class IntentEngine:
    def __init__(self, simulators: dict,
                 result_cb: Callable[[RemediationResult], None]):
        self.constraints = ConstraintModel(simulators)
        self.result_cb   = result_cb
        self._playbooks: dict[str, dict] = {}
        self._load_playbooks()

    def _load_playbooks(self):
        if not os.path.isdir(PLAYBOOK_DIR):
            print(f"[decision] WARNING: playbooks dir not found: {PLAYBOOK_DIR}")
            return
        for name in os.listdir(PLAYBOOK_DIR):
            if name.endswith(".yaml"):
                path = os.path.join(PLAYBOOK_DIR, name)
                with open(path) as f:
                    pb = yaml.safe_load(f)
                    self._playbooks[pb["name"]] = pb
        print(f"[decision] Loaded {len(self._playbooks)} playbooks from {PLAYBOOK_DIR}")

    def handle(self, alert: Alert):
        t0       = time.monotonic()
        pb_name  = ANOMALY_PLAYBOOK_MAP.get(alert.anomaly_type, "adjust_qos")
        playbook = self._playbooks.get(pb_name)

        if not playbook:
            print(f"[decision] No playbook found for '{pb_name}'")
            return

        ok, reason = self.constraints.evaluate(alert, playbook)
        xml        = _build_netconf_xml(playbook, alert)

        if ok:
            pushed   = push_netconf(xml, alert.device_id)
            duration = int((time.monotonic() - t0) * 1000)
            result   = RemediationResult(
                alert=alert, playbook=pb_name,
                action=playbook.get("action", ""), success=pushed,
                netconf_xml=xml, duration_ms=duration,
            )
        else:
            print(f"[decision] Blocked ({pb_name}): {reason}")
            result = RemediationResult(
                alert=alert, playbook=pb_name,
                action=playbook.get("action", ""), success=False,
                blocked_reason=reason,
                duration_ms=int((time.monotonic() - t0) * 1000),
            )

        self.result_cb(result)


# ── Background worker ──────────────────────────────────────────────────────────
def start_decision_worker(alert_queue: Q.Queue, simulators: dict,
                          result_cb: Callable) -> threading.Event:
    engine = IntentEngine(simulators, result_cb)
    stop   = threading.Event()

    def _worker():
        print("[decision] Worker started")
        while not stop.is_set():
            try:
                alert = alert_queue.get(timeout=1)
                engine.handle(alert)
            except Q.Empty:
                pass

    threading.Thread(target=_worker, daemon=True).start()
    return stop
