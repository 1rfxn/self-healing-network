#!/usr/bin/env bash
# demo.sh — One-click Self-Healing Network AIOps demo
# No Kafka required — runs entirely in-process.
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

GREEN='\033[0;32m'; BLUE='\033[0;34m'; NC='\033[0m'; BOLD='\033[1m'

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║   Self-Healing Network AIOps — Demo Mode     ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════╝${NC}"
echo ""

# 1. Python deps
echo -e "${BLUE}[1/5]${NC} Installing Python dependencies..."
pip install -r requirements.txt -q --disable-pip-version-check

# 2. Generate training data if missing
if [ ! -f "data/normal_telemetry_2hr.csv" ]; then
  echo -e "${BLUE}[2/5]${NC} Generating training data..."
  python generate_data.py
else
  echo -e "${BLUE}[2/5]${NC} Training data exists — skipping generation."
fi

# 3. Train models if missing
if [ ! -f "models/iso_forest.pkl" ] || [ ! -f "models/lstm_ae.pt" ]; then
  echo -e "${BLUE}[3/5]${NC} Training anomaly detection models (this takes ~60s)..."
  python src/detect.py --train --csv data/normal_telemetry_2hr.csv
else
  echo -e "${BLUE}[3/5]${NC} Models already trained — skipping."
fi

# 4. Start backend
echo -e "${BLUE}[4/5]${NC} Starting AIOps backend on http://localhost:5000 ..."
python src/main.py --demo &
BACKEND_PID=$!
sleep 3

# Check backend came up
if ! curl -sf http://localhost:5000/health > /dev/null; then
  echo "Backend failed to start. Check logs above."
  kill $BACKEND_PID 2>/dev/null
  exit 1
fi
echo -e "      ${GREEN}✔ Backend healthy${NC}"

# 5. Start React dashboard
echo -e "${BLUE}[5/5]${NC} Starting React dashboard on http://localhost:3000 ..."
cd dashboard
npm install -q --no-fund --no-audit 2>/dev/null
BROWSER=none npm start &
FRONTEND_PID=$!

echo ""
echo -e "${GREEN}${BOLD}✔  Demo is running!${NC}"
echo ""
echo -e "   Dashboard  : ${BLUE}http://localhost:3000${NC}"
echo -e "   Backend    : ${BLUE}http://localhost:5000/health${NC}"
echo -e "   API state  : ${BLUE}http://localhost:5000/api/state${NC}"
echo ""
echo -e "   Faults auto-inject every 90s. Or inject manually:"
echo -e "   ${BLUE}curl -X POST http://localhost:5000/api/inject/dev1/high_loss${NC}"
echo ""
echo "Press Ctrl+C to stop all processes."

trap "echo ''; echo 'Stopping...'; kill \$BACKEND_PID \$FRONTEND_PID 2>/dev/null; echo 'Done.'" EXIT
wait
