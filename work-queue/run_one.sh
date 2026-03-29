#!/usr/bin/env bash
set -euo pipefail

WORK_DIR="${WORK_DIR:-${HOME}/work-queue}"

# ===== Thay đổi cho 2 Node =====
# Cấu hình container cho cả 2 node để lấy metrics (CPU/RAM)
CONTAINER_1="${CONTAINER_1:-rabbit-1}"
CONTAINER_2="${CONTAINER_2:-rabbit-2}"

# RABBIT_HOST nên trỏ tới IP của HAProxy (Load Balancer) hoặc Node 1
RABBIT_HOST="${RABBIT_HOST:-127.0.0.1}"
RABBIT_PORT="${RABBIT_PORT:-5672}"  # Thường HAProxy sẽ dùng cổng 5670 hoặc 5672
RABBIT_USER="${RABBIT_USER:-admin}"
RABBIT_PASS="${RABBIT_PASS:-admin123}"

PRODUCER_PY="${PRODUCER_PY:-${WORK_DIR}/producer.py}"
WORKER_RUNNER="${WORKER_RUNNER:-${WORK_DIR}/run_workers_until_drained.sh}"
SUMMARIZER="${SUMMARIZER:-${WORK_DIR}/summarize_run.py}"

# ===== Defaults (override bằng env hoặc args) =====
MODE="${MODE:-A}"              # A/B/C
RATE="${RATE:-500}"            # msg/s
N="${N:-2}"                    # workers
PREFETCH="${PREFETCH:-1}"
SIZE="${SIZE:-1024}"           # bytes
RUN_SECONDS="${RUN_SECONDS:-30}"
SLEEP_MS="${SLEEP_MS:-20}"
QUEUE_BASE="${QUEUE_BASE:-orders_queue}"

CPU_DELAY="${CPU_DELAY:-3}"
POLL_INTERVAL="${POLL_INTERVAL:-1}"
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT:-3}"

usage() {
  cat <<EOF
Usage:
  ./run_one.sh [--mode A|B|C] [--rate 500] [--n 1] [--prefetch 1] [--size 1024] [--seconds 30]

Env override also supported:
  MODE RATE N PREFETCH SIZE RUN_SECONDS SLEEP_MS QUEUE_BASE
EOF
}

# ===== Parse args =====
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2;;
    --rate) RATE="$2"; shift 2;;
    --n|--workers) N="$2"; shift 2;;
    --prefetch) PREFETCH="$2"; shift 2;;
    --size|--payload-bytes) SIZE="$2"; shift 2;;
    --seconds) RUN_SECONDS="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

# ===== Mode config =====
QUEUE_DURABLE="1"
PRODUCER_FLAGS=""
QUEUE_NAME=""

case "${MODE}" in
  A)
    QUEUE_DURABLE="0"
    PRODUCER_FLAGS=""
    QUEUE_NAME="${QUEUE_BASE}_A"
    ;;
  B)
    QUEUE_DURABLE="1"
    PRODUCER_FLAGS="--durable --persistent"
    QUEUE_NAME="${QUEUE_BASE}_B"
    ;;
  C)
    QUEUE_DURABLE="1"
    PRODUCER_FLAGS="--durable --persistent --confirm"
    QUEUE_NAME="${QUEUE_BASE}_C"
    ;;
  *)
    echo "Invalid MODE=${MODE} (use A/B/C)"; exit 1;;
esac

# ===== Output =====
TS="$(TZ=Asia/Ho_Chi_Minh date '+%Y%m%d_%H%M%S')"
messages=$((RATE * RUN_SECONDS))
run_tag="mode${MODE}_rate${RATE}_N${N}_pref${PREFETCH}_sz${SIZE}_t${RUN_SECONDS}"
OUT_ROOT="${OUT_ROOT:-${WORK_DIR}/results/${TS}}"
RUN_DIR="${OUT_ROOT}/${run_tag}"
mkdir -p "${RUN_DIR}"

CPU_LOG="${RUN_DIR}/cpu_rabbit.log"
MEM_LOG="${RUN_DIR}/mem_rabbit.log"

echo "=== ONE RUN (2-NODE CLUSTER) ==="
echo "run_tag:       ${run_tag}"
echo "mode:          ${MODE}"
echo "queue:         ${QUEUE_NAME} (durable=${QUEUE_DURABLE})"
echo "rate:          ${RATE} msg/s"
echo "workers N:     ${N}"
echo "prefetch:      ${PREFETCH}"
echo "payload bytes: ${SIZE}"
echo "duration:      ${RUN_SECONDS}s  -> messages=${messages}"
echo "out:           ${RUN_DIR}"
echo "containers:    ${CONTAINER_1}, ${CONTAINER_2}"
echo

# Purge queue: Dùng docker exec vào Node 1 thay vì sudo trên host để đảm bảo lệnh ăn vào đúng cluster
docker exec "${CONTAINER_1}" rabbitmqctl purge_queue "${QUEUE_NAME}" >/dev/null 2>&1 || true

# Start workers runner (realtime + log file)
# Xuất thêm các biến RABBIT_HOST, CONTAINER_1, CONTAINER_2 cho script runner
OUTPUT_DIR="${RUN_DIR}" \
CPU_LOG="${CPU_LOG}" \
MEM_LOG="${MEM_LOG}" \
QUEUE_NAME="${QUEUE_NAME}" \
QUEUE_DURABLE="${QUEUE_DURABLE}" \
PREFETCH="${PREFETCH}" \
SLEEP_MS="${SLEEP_MS}" \
CONTAINER_1="${CONTAINER_1}" \
CONTAINER_2="${CONTAINER_2}" \
RABBIT_HOST="${RABBIT_HOST}" \
RABBIT_PORT="${RABBIT_PORT}" \
RABBIT_USER="${RABBIT_USER}" \
RABBIT_PASS="${RABBIT_PASS}" \
CPU_DELAY="${CPU_DELAY}" \
POLL_INTERVAL="${POLL_INTERVAL}" \
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT}" \
WORK_DIR="${WORK_DIR}" \
"${WORKER_RUNNER}" "${N}" 2>&1 | tee "${RUN_DIR}/workers_stdout.log" &
workers_pid=$!

sleep 1

# Run producer
set +e
producer_out="$(
  python3 "${PRODUCER_PY}" \
    --host "${RABBIT_HOST}" --port "${RABBIT_PORT}" \
    --user "${RABBIT_USER}" --password "${RABBIT_PASS}" \
    --queue "${QUEUE_NAME}" \
    -n "${messages}" \
    --payload-bytes "${SIZE}" \
    --rate "${RATE}" \
    ${PRODUCER_FLAGS} \
    2>&1
)"
rc=$?
set -e
echo "${producer_out}" | tee "${RUN_DIR}/producer_stdout.log" >/dev/null
if [[ "${rc}" -ne 0 ]]; then
  echo "Producer failed rc=${rc}. See ${RUN_DIR}/producer_stdout.log"
  kill "${workers_pid}" 2>/dev/null || true
  wait "${workers_pid}" 2>/dev/null || true
  exit 1
fi

# Parse confirm_fail
confirm_fail="$(echo "${producer_out}" | awk -F': ' '/^confirm_fail:/ {print $2}' | tail -n1)"
confirm_fail="${confirm_fail:-0}"

# Wait drained
wait "${workers_pid}" || true

# Summarize
metrics="$(
  python3 "${SUMMARIZER}" \
    --run-dir "${RUN_DIR}" \
    --pattern "w*.jsonl" \
    --cpu-log "cpu_rabbit.log" \
    --mem-log "mem_rabbit.log"
)"

SUMMARY_CSV="${OUT_ROOT}/summary.csv"
if [[ ! -f "${SUMMARY_CSV}" ]]; then
  echo "mode,rate,N,throughput_msgps,latency_avg_ms,latency_p95_ms,cpu_avg_pct,cpu_max_pct,mem_avg_mib,mem_max_mib,acked_count,worker_duration_s,prefetch,payload_bytes,confirm_fail,queue,run_tag" \
    > "${SUMMARY_CSV}"
fi

echo "${MODE},${RATE},${N},${metrics},${PREFETCH},${SIZE},${confirm_fail},${QUEUE_NAME},${run_tag}" \
  | tee -a "${SUMMARY_CSV}"

echo
echo "DONE."
echo "Run folder:  ${RUN_DIR}"
echo "Summary CSV: ${SUMMARY_CSV}"
