#!/usr/bin/env bash
set -euo pipefail

N="${1:-1}"
WORK_DIR="${WORK_DIR:-${HOME}/NT531-RabbitMQ/work-queue}"
OUTPUT_DIR="${OUTPUT_DIR:-.}"
CPU_LOG="${CPU_LOG:-cpu_rabbit.log}"
MEM_LOG="${MEM_LOG:-mem_rabbit.log}"

RABBIT_HOST="${RABBIT_HOST:-172.31.47.43}"
RABBIT_PORT="${RABBIT_PORT:-5672}"
RABBIT_USER="${RABBIT_USER:-admin}"
RABBIT_PASS="${RABBIT_PASS:-123456}"
QUEUE_NAME="${QUEUE_NAME:-orders_queue}"
PREFETCH="${PREFETCH:-1}"
SLEEP_MS="${SLEEP_MS:-20}"

POLL_INTERVAL="${POLL_INTERVAL:-1}"
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT:-3}"

echo "[Workers] Khởi động ${N} workers..."
declare -a PIDS

for i in $(seq 1 "$N"); do
  python3 "${WORK_DIR}/worker.py" \
    --host "${RABBIT_HOST}" --port "${RABBIT_PORT}" \
    --user "${RABBIT_USER}" --password "${RABBIT_PASS}" \
    --queue "${QUEUE_NAME}" \
    --queue-durable "${QUEUE_DURABLE:-1}" \
    --worker-id "${i}" \
    --prefetch "${PREFETCH}" \
    --sleep-ms "${SLEEP_MS}" \
    --log "${OUTPUT_DIR}/w${i}.jsonl" &
  PIDS+=($!)
done
echo "[Workers] Đã start workers. Đang theo dõi queue và thu thập metrics (Node 2)..."

zero_count=0
while true; do
  alive_count=0
  for p in "${PIDS[@]}"; do
    if kill -0 "$p" 2>/dev/null; then alive_count=$((alive_count + 1)); fi
  done
  
  if [[ "$alive_count" -eq 0 && "${qlen:-0}" -gt 0 ]]; then
    echo "[Workers] CẢNH BÁO: Workers chết nhưng queue còn $qlen messages! Ép thoát."
    break
  fi
  
  # 1. Thu thập CPU/RAM cho Node 2 (Vì Node 1 ko chạy RabbitMQ nữa)
  cpu2=$(ssh -o StrictHostKeyChecking=no ubuntu@172.31.47.43 "ps -C beam.smp -o %cpu= | awk '{s+\$1} END {print s+0}'" 2>/dev/null || echo "0")
  mem_raw2=$(ssh -o StrictHostKeyChecking=no ubuntu@172.31.47.43 "ps -C beam.smp -o rss= | awk '{s+\$1} END {print s/1024}'" 2>/dev/null || echo "0")

  # Ghi log: Để Node 1 = 0 cho file python khỏi lỗi format
  ts=$(date +%s)
  echo "${ts},0,${cpu2}" >> "${CPU_LOG}"
  echo "${ts},0MiB,${mem_raw2}MiB" >> "${MEM_LOG}"

  # 2. Check số lượng message trong Queue qua SSH sang Node 2
  qlen=$(ssh -o StrictHostKeyChecking=no ubuntu@172.31.47.43 "sudo rabbitmqctl list_queues name messages 2>/dev/null" | awk -v q="${QUEUE_NAME}" '$1==q {print $2}')
  qlen="${qlen:-0}"

  if [[ "$qlen" -eq 0 ]]; then
    zero_count=$((zero_count + 1))
  else
    zero_count=0
  fi

  if [[ "$zero_count" -ge "$STABLE_ZERO_COUNT" ]]; then
    echo "[Workers] Queue đã xử lý xong (empty ${STABLE_ZERO_COUNT} lần check liên tục)."
    break
  fi

  sleep "${POLL_INTERVAL}"
done

echo "[Workers] Dọn dẹp: Đang tắt các workers..."
for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
wait
echo "[Workers] Hoàn tất."
