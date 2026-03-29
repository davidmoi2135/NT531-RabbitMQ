#!/usr/bin/env bash
set -euo pipefail

WORK_DIR="${WORK_DIR:-${HOME}/publish-subscribe}"
CONTAINER_NAME="${CONTAINER_NAME:-mq-lab-rabbitmq-1}"

RABBIT_HOST="${RABBIT_HOST:-127.0.0.1}"
RABBIT_PORT="${RABBIT_PORT:-5672}"
RABBIT_USER="${RABBIT_USER:-admin}"
RABBIT_PASS="${RABBIT_PASS:-admin123}"

PRODUCER="${PRODUCER:-${WORK_DIR}/producer_fanout_benchmark.py}"
SUBSCRIBER="${SUBSCRIBER:-${WORK_DIR}/sub_fanout.py}"
SUMMARIZER="${SUMMARIZER:-${WORK_DIR}/summarize_ps_run.py}"

M="${M:-3}"

MODE="${MODE:-A}"
RATE="${RATE:-500}"
PREFETCH="${PREFETCH:-10}"
SIZE="${SIZE:-1024}"
RUN_SECONDS="${RUN_SECONDS:-30}"
SLEEP_MS="${SLEEP_MS:-20}"

EXCHANGE_BASE="${EXCHANGE_BASE:-broadcast_x}"
QUEUE_BASE="${QUEUE_BASE:-subq}"

CPU_DELAY="${CPU_DELAY:-3}"
POLL_INTERVAL="${POLL_INTERVAL:-1}"
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT:-3}"

usage() {
  cat <<EOF
Usage:
  ./run_ps_one.sh [--mode A|B|C] [--rate 500] [--prefetch 10] [--size 1024] [--seconds 30]

Env override supported:
  MODE RATE PREFETCH SIZE RUN_SECONDS SLEEP_MS
  WORK_DIR CONTAINER_NAME
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2;;
    --rate) RATE="$2"; shift 2;;
    --prefetch) PREFETCH="$2"; shift 2;;
    --size|--payload-bytes) SIZE="$2"; shift 2;;
    --seconds) RUN_SECONDS="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

EX_DUR=1
Q_DUR=1
PRODUCER_FLAGS=""
case "${MODE}" in
  A)
    EX_DUR=0; Q_DUR=0; PRODUCER_FLAGS=""
    EXCHANGE="${EXCHANGE_BASE}_A"
    QBASE="${QUEUE_BASE}_A"
    ;;
  B)
    EX_DUR=1; Q_DUR=1; PRODUCER_FLAGS="--durable --persistent"
    EXCHANGE="${EXCHANGE_BASE}_B"
    QBASE="${QUEUE_BASE}_B"
    ;;
  C)
    EX_DUR=1; Q_DUR=1; PRODUCER_FLAGS="--durable --persistent --confirm"
    EXCHANGE="${EXCHANGE_BASE}_C"
    QBASE="${QUEUE_BASE}_C"
    ;;
  *) echo "Invalid MODE=${MODE}"; exit 1;;
esac

TS="$(TZ=Asia/Ho_Chi_Minh date '+%Y%m%d_%H%M%S')"
OUT_ROOT="${OUT_ROOT:-${WORK_DIR}/results/${TS}}"
messages=$((RATE * RUN_SECONDS))
run_tag="mode${MODE}_rate${RATE}_M${M}_pref${PREFETCH}_sz${SIZE}_t${RUN_SECONDS}"
RUN_DIR="${OUT_ROOT}/${run_tag}"
mkdir -p "${RUN_DIR}"

CPU_LOG="${RUN_DIR}/cpu_rabbit.log"
MEM_LOG="${RUN_DIR}/mem_rabbit.log"
TS_CSV="${RUN_DIR}/queue_ts.csv"

echo "=== PUBSUB ONE RUN ==="
echo "run_tag:   ${run_tag}"
echo "mode:      ${MODE}"
echo "exchange:  ${EXCHANGE} (durable=${EX_DUR})"
echo "queues:    ${QBASE}_1..${QBASE}_${M} (durable=${Q_DUR})"
echo "rate:      ${RATE} msg/s"
echo "prefetch:  ${PREFETCH}"
echo "size:      ${SIZE} bytes"
echo "seconds:   ${RUN_SECONDS} -> messages=${messages}"
echo "out:       ${RUN_DIR}"
echo

purge_queue() {
  docker exec "${CONTAINER_NAME}" rabbitmqctl purge_queue "$1" >/dev/null 2>&1 || true
}

get_ready_unacked() {
  local q="$1"
  docker exec "${CONTAINER_NAME}" rabbitmqctl list_queues -q name messages_ready messages_unacknowledged 2>/dev/null \
    | awk -v q="${q}" '$1==q {print $2, $3; found=1} END{if(!found) print "0 0"}'
}

cleanup() {
  kill "${STAT_PID:-}" 2>/dev/null || true
  kill "${PROD_PID:-}" 2>/dev/null || true
  for pid in "${pids[@]:-}"; do
    kill "${pid}" 2>/dev/null || true
  done
}
trap cleanup EXIT

for i in $(seq 1 "${M}"); do
  purge_queue "${QBASE}_${i}"
done

pids=()
for i in $(seq 1 "${M}"); do
  q="${QBASE}_${i}"
  python3 "${SUBSCRIBER}" \
    --host "${RABBIT_HOST}" --port "${RABBIT_PORT}" \
    --user "${RABBIT_USER}" --password "${RABBIT_PASS}" \
    --exchange "${EXCHANGE}" --queue "${q}" --sub-id "${i}" \
    --prefetch "${PREFETCH}" --sleep-ms "${SLEEP_MS}" \
    --log "${RUN_DIR}/s${i}.jsonl" \
    --queue-durable "${Q_DUR}" --exchange-durable "${EX_DUR}" \
    > "${RUN_DIR}/sub${i}.out.log" 2>&1 &
  pids+=("$!")
done

sleep "${CPU_DELAY}"

(
  while true; do
    docker stats --no-stream --format "{{.Name}} {{.CPUPerc}}" "${CONTAINER_NAME}" >> "${CPU_LOG}" || true
    docker stats --no-stream --format "{{.Name}} {{.MemUsage}}" "${CONTAINER_NAME}" >> "${MEM_LOG}" || true
    sleep 1
  done
) &
STAT_PID=$!

echo "ts_iso,ts_epoch,ready_total,unacked_total,total,producer_alive" > "${TS_CSV}"

set +e
python3 "${PRODUCER}" \
  --host "${RABBIT_HOST}" --port "${RABBIT_PORT}" \
  --user "${RABBIT_USER}" --password "${RABBIT_PASS}" \
  --exchange "${EXCHANGE}" \
  -n "${messages}" --payload-bytes "${SIZE}" --rate "${RATE}" \
  ${PRODUCER_FLAGS} \
  > "${RUN_DIR}/producer_stdout.log" 2>&1 &
PROD_PID=$!
set -e

seen=0
zero_ok=0

while true; do
  all_r=0
  all_u=0
  any=0

  for i in $(seq 1 "${M}"); do
    q="${QBASE}_${i}"
    read -r r u < <(get_ready_unacked "${q}")
    r="${r:-0}"
    u="${u:-0}"
    all_r=$((all_r + r))
    all_u=$((all_u + u))
    if [[ "${r}" != "0" || "${u}" != "0" ]]; then
      any=1
    fi
  done

  total=$((all_r + all_u))
  ts_iso="$(TZ=Asia/Ho_Chi_Minh date '+%Y-%m-%d %H:%M:%S')"
  ts_epoch="$(date +%s)"

  if kill -0 "${PROD_PID}" 2>/dev/null; then
    producer_alive=1
  else
    producer_alive=0
  fi

  echo "[${ts_iso}] producer_alive=${producer_alive} total_ready=${all_r} total_unacked=${all_u} total=${total}"
  echo "${ts_iso},${ts_epoch},${all_r},${all_u},${total},${producer_alive}" >> "${TS_CSV}"

  if [[ "${any}" == "1" ]]; then
    seen=1
  fi

  if [[ "${producer_alive}" == "0" && "${seen}" == "1" && "${all_r}" == "0" && "${all_u}" == "0" ]]; then
    zero_ok=$((zero_ok + 1))
  else
    zero_ok=0
  fi

  if [[ "${producer_alive}" == "0" && "${zero_ok}" -ge "${STABLE_ZERO_COUNT}" ]]; then
    break
  fi

  sleep "${POLL_INTERVAL}"
done

wait "${PROD_PID}" || {
  echo "Producer failed. See ${RUN_DIR}/producer_stdout.log"
  exit 1
}

kill "${STAT_PID}" 2>/dev/null || true
for pid in "${pids[@]}"; do
  kill "${pid}" 2>/dev/null || true
done

metrics="$(
  python3 "${SUMMARIZER}" \
    --run-dir "${RUN_DIR}" \
    --pattern "s*.jsonl" \
    --cpu-log "cpu_rabbit.log" \
    --mem-log "mem_rabbit.log" \
    --payload-bytes "${SIZE}"
)"

RUN_SUM="${RUN_DIR}/run_summary.csv"
echo "mode,rate,M,prefetch,payload_bytes,thr_s1,thr_s2,thr_s3,thr_total,p95_s1,p95_s2,p95_s3,p95_total,goodput_MBps,cpu_avg_pct,cpu_max_pct,mem_avg_mib,mem_max_mib,exchange,queue_base,run_tag" > "${RUN_SUM}"
echo "${MODE},${RATE},${M},${PREFETCH},${SIZE},${metrics},${EXCHANGE},${QBASE},${run_tag}" >> "${RUN_SUM}"

SUMMARY="${OUT_ROOT}/summary.csv"
if [[ ! -f "${SUMMARY}" ]]; then
  cat "${RUN_SUM}" > "${SUMMARY}"
else
  tail -n 1 "${RUN_SUM}" >> "${SUMMARY}"
fi

echo
echo "DONE."
echo "Run folder:    ${RUN_DIR}"
echo "Queue TS CSV:  ${TS_CSV}"
echo "Run summary:   ${RUN_SUM}"
echo "Overall CSV:   ${SUMMARY}"