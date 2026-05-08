# Self-Healing Network AIOps

> Cognizant Technoverse Hackathon 2026 — CMT Track
> Predicts network faults with ML and auto-remediates via NETCONF, reducing MTTR from hours to seconds.

---

## Architecture

```
gNMI/SNMP Devices ──► Kafka (telemetry) ──► Detection  ──► Decision   ──► NETCONF Push ──► Verify
  (15 devices)              │               IsoForest       Playbooks        ncclient       └─► Fixed
  core / edge / access      │               LSTM AE         Constraints      DevNet sim     └─► Escalate
                            └──────────────────────────────────────────── Dashboard (React + WS)
```

### Layer map

| Layer        | File                 | What it does |
|---|---|---|
| Ingestion    | `src/ingest.py`      | Simulates 15 gNMI devices → Kafka / in-memory queue at 1 Hz |
| Detection    | `src/detect.py`      | Isolation Forest (point spikes) + LSTM Autoencoder (temporal drift) |
| Decision     | `src/decision.py`    | Maps anomaly → YAML playbook, constraint check, NETCONF push |
| Verification | `src/verify.py`      | 60s post-remediation watch; confirms fix or escalates |
| Orchestrator | `src/main.py`        | Flask-SocketIO WebSocket server — ties all layers together |
| Dashboard    | `dashboard/src/App.js` | Dark-theme React UI: KPIs, device grid, live charts, event log |

---

## Quick start

### Option A — One-click demo (recommended)

```bash
bash demo.sh
# Opens: http://localhost:3000
```

### Option B — Manual steps

```bash
# 1. Install Python deps
pip install -r requirements.txt

# 2. Generate training data
python generate_data.py

# 3. Train models (~60s)
python src/detect.py --train --csv data/normal_telemetry_2hr.csv

# 4. Backend (Terminal 1)
python src/main.py --demo

# 5. Dashboard (Terminal 2)
cd dashboard && npm install && npm start
```

### Option C — Full Kafka mode

```bash
docker-compose up -d                                   # Kafka + Zookeeper
python src/detect.py --train --csv data/normal_telemetry_2hr.csv
python src/main.py                                     # reads from Kafka
cd dashboard && npm install && npm start
```

---

## ML models

### Isolation Forest (`src/detect.py`)
- Trained on 2 hrs of normal telemetry (108,000 rows × 15 devices)
- Features: rx/tx bytes, packet_loss_pct, latency_ms, cpu_pct, memory_pct, bgp_neighbors, link_flaps
- `contamination=0.05` — expects ~5% anomaly rate in production
- Saved to `models/iso_forest.pkl`

### LSTM Autoencoder (`src/detect.py`)
- Sequence length: 20 samples (20-second window per device)
- Architecture: Encoder LSTM(64) → Decoder LSTM(64 → n_features)
- Trained 20 epochs on normal sequences; MSE > 0.05 → anomaly alert
- Catches **slow, gradual degradation** that threshold rules miss entirely
- Saved to `models/lstm_ae.pt`

---

## Playbooks

| Anomaly type  | Playbook              | Action                    | Key constraint             |
|---|---|---|---|
| `high_loss`   | `reroute_traffic.yaml`| Failover to alternate path| Alternate path < 80% load  |
| `high_latency`| `adjust_qos.yaml`     | Reprioritize traffic class | Device CPU < 70%           |
| `high_cpu`    | `scale_resources.yaml`| Adjust resource policy    | Memory headroom > 20%      |
| `link_flap`   | `reroute_traffic.yaml`| Failover to alternate path| Alternate path < 80% load  |

---

## NETCONF / Cisco DevNet

Real device push (optional):

```bash
export NETCONF_HOST=sandbox-nxos-1.cisco.com
export NETCONF_PORT=830
export NETCONF_USER=admin
export NETCONF_PASS=your_password
```

Without env vars → simulation mode (config printed to console, no device needed).
Free always-on sandbox: https://devnetsandbox.cisco.com

---

## Fault injection API

```bash
# Inject a fault via REST
curl -X POST http://localhost:5000/api/inject/dev1/high_loss
curl -X POST http://localhost:5000/api/inject/dev4/high_latency
curl -X POST http://localhost:5000/api/inject/dev8/high_cpu

# Available fault types: high_loss | high_latency | high_cpu | link_flap
# Available devices:     dev1–dev15
```

Or use the **device drawer** in the dashboard (click any device card).

---

## Tech stack

| Layer       | Technology |
|---|---|
| Telemetry   | pygnmi (gNMI), kafka-python |
| Streaming   | Apache Kafka + Zookeeper (Docker) |
| ML          | scikit-learn (Isolation Forest), PyTorch (LSTM Autoencoder) |
| Remediation | ncclient (NETCONF), PyYAML |
| Backend     | Flask 3, Flask-SocketIO, eventlet |
| Dashboard   | React 18, Recharts, socket.io-client |
| Infra       | Docker Compose |

---

## Project structure

```
self-healing-network/
├── src/
│   ├── __init__.py
│   ├── ingest.py          # 15-device gNMI simulator → Kafka / queue
│   ├── detect.py          # IsoForest + LSTM training & inference
│   ├── decision.py        # Intent engine + constraint check + NETCONF push
│   ├── verify.py          # 60s post-remediation verification
│   └── main.py            # Orchestrator + Flask-SocketIO server
├── playbooks/
│   ├── reroute_traffic.yaml
│   ├── adjust_qos.yaml
│   └── scale_resources.yaml
├── dashboard/
│   ├── public/index.html
│   ├── package.json
│   └── src/
│       ├── index.js
│       ├── index.css
│       └── App.js         # Full React dashboard
├── data/
│   └── normal_telemetry_2hr.csv   (generated)
├── models/                        (generated after training)
│   ├── iso_forest.pkl
│   └── lstm_ae.pt
├── docker-compose.yml
├── generate_data.py
├── requirements.txt
└── demo.sh
```
