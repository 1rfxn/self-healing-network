"""
src/ingest.py — Simulates 15 gNMI/SNMP devices streaming telemetry to Kafka
or an in-memory queue (demo mode).
"""
import time, json, math, threading, queue as Q
from datetime import datetime
import numpy as np

DEVICES = [f"dev{i}" for i in range(1, 16)]
DEVICE_ROLES = {
    **{f"dev{i}": "core"   for i in range(1,  4)},
    **{f"dev{i}": "edge"   for i in range(4,  8)},
    **{f"dev{i}": "access" for i in range(8, 16)},
}
KAFKA_TOPIC = "telemetry"
_demo_queue: Q.Queue = Q.Queue(maxsize=5000)


def get_demo_queue() -> Q.Queue:
    return _demo_queue


class DeviceSimulator:
    """Stateful per-device telemetry emitter with fault injection support."""

    def __init__(self, device_id: str):
        self.device_id = device_id
        self.role      = DEVICE_ROLES.get(device_id, "access")
        self.rng       = np.random.default_rng(abs(hash(device_id)) % (2**31))
        self._fault: dict | None = None
        self._t = 0

    def inject_fault(self, fault_type: str):
        self._fault = {"type": fault_type, "start": self._t}

    def clear_fault(self):
        self._fault = None

    def read(self) -> dict:
        self._t += 1
        wave = 1 + 0.25 * math.sin(2 * math.pi * self._t / 3600)
        scale = {"core": 1.8, "edge": 1.2, "access": 0.8}[self.role]

        rx      = max(0, self.rng.normal(1000 * scale * wave, 80))
        tx      = max(0, self.rng.normal(1200 * scale * wave, 100))
        loss    = max(0, self.rng.normal(0.08, 0.04))
        latency = max(0, self.rng.normal(5.5,  0.8))
        cpu     = max(0, self.rng.normal(22,   4))
        memory  = max(0, self.rng.normal(34,   4))
        bgp     = int(self.rng.choice([4, 5], p=[0.3, 0.7]))
        flaps   = int(self.rng.choice([0, 1], p=[0.97, 0.03]))

        if self._fault:
            age = self._t - self._fault["start"]
            ft  = self._fault["type"]
            if ft == "high_loss":
                loss    = min(30,  0.08 + age * 0.8  + self.rng.normal(0, 0.5))
            elif ft == "high_latency":
                latency = min(500, 5.5  + age * 15   + self.rng.normal(0, 2))
            elif ft == "high_cpu":
                cpu     = min(98,  22   + age * 4    + self.rng.normal(0, 2))
            elif ft == "link_flap":
                flaps   = int(self.rng.choice([2, 3, 4]))

        return {
            "timestamp":           datetime.utcnow().isoformat(),
            "device_id":           self.device_id,
            "role":                self.role,
            "interface_rx_bytes":  round(rx, 2),
            "interface_tx_bytes":  round(tx, 2),
            "packet_loss_pct":     round(loss, 3),
            "latency_ms":          round(latency, 2),
            "cpu_pct":             round(cpu, 1),
            "memory_pct":          round(memory, 1),
            "bgp_neighbors":       bgp,
            "link_flaps":          flaps,
        }


def _demo_producer(simulators: dict, stop: threading.Event):
    print("[ingest] Demo producer started — in-memory queue")
    while not stop.is_set():
        for sim in simulators.values():
            rec = sim.read()
            try:
                _demo_queue.put_nowait(rec)
            except Q.Full:
                _demo_queue.get_nowait()
                _demo_queue.put_nowait(rec)
        time.sleep(1.0)


def _kafka_producer(simulators: dict, stop: threading.Event):
    from kafka import KafkaProducer
    p = KafkaProducer(bootstrap_servers=["localhost:9092"],
                      value_serializer=lambda v: json.dumps(v).encode())
    print("[ingest] Kafka producer started")
    while not stop.is_set():
        for sim in simulators.values():
            p.send(KAFKA_TOPIC, value=sim.read())
        p.flush()
        time.sleep(1.0)
    p.close()


def start_ingestion(demo=False) -> tuple[dict, threading.Event]:
    simulators  = {d: DeviceSimulator(d) for d in DEVICES}
    stop        = threading.Event()
    fn          = _demo_producer if demo else _kafka_producer
    threading.Thread(target=fn, args=(simulators, stop), daemon=True).start()
    return simulators, stop
