#!/usr/bin/env bash
set -euo pipefail

N="${1:-1}"
WORK_DIR="${WORK_DIR:-${HOME}/work-queue}"
OUTPUT_DIR="${OUTPUT_DIR:-.}"
CPU_LOG="${CPU_LOG:-cpu_rabbit.log}"
MEM_LOG="${MEM_LOG:-mem_rabbit.log}"

# Kế thừa biến môi trường từ run_one.sh
RABBIT_HOST="${RABBIT_HOST:-127.0.0.1}"
RABBIT_PORT="${RABBIT_PORT:-5672}"
RABBIT_USER="${RABBIT_USER:-admin}"
RABBIT_PASS="${RABBIT_PASS:-admin123}"
QUEUE_NAME="${QUEUE_NAME:-orders_queue}"
PREFETCH="${PREFETCH:-1}"
SLEEP_MS="${SLEEP_MS:-20}"

CONTAINER_1="${CONTAINER_1:-rabbit-1}"
CONTAINER_2="${CONTAINER_2:-rabbit-2}"

POLL_INTERVAL="${POLL_INTERVAL:-1}"
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT:-3}"

echo "[Workers] Khởi động ${N} workers..."
declare -a PIDS

# Chạy ngầm các workers
# Chạy ngầm các workers
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
echo "[Workers] Đã start workers. Đang theo dõi queue và thu thập metrics (2 Nodes)..."

zero_count=0
while true; do
  alive_count=0
  for p in "${PIDS[@]}"; do
    if kill -0 "$p" 2>/dev/null; then
      alive_count=$((alive_count + 1))
    fi
  done
  
  if [[ "$alive_count" -eq 0 && "$qlen" -gt 0 ]]; then
    echo "[Workers] CẢNH BÁO: Tất cả workers đã chết nhưng queue vẫn còn $qlen messages! Ép buộc thoát."
    break
  fi
  # 1. Thu thập CPU cho cả 2 node (Dùng lệnh ps tìm tiến trình beam.smp của RabbitMQ)
  cpu1=$(ps -C beam.smp -o %cpu= | awk '{s+=$1} END {print s+0}')
  cpu2=$(ssh nguyentrinh2135@node2-server "ps -C beam.smp -o %cpu= | awk '{s+\$1} END {print s+0}'" 2>/dev/null || echo "0")
  
  # 2. Thu thập RAM cho cả 2 node (rss là kilobytes, chia 1024 để ra MiB)
  mem_raw1=$(ps -C beam.smp -o rss= | awk '{s+=$1} END {print s/1024}')
  mem_raw2=$(ssh nguyentrinh2135@node2-server "ps -C beam.smp -o rss= | awk '{s+\$1} END {print s/1024}'" 2>/dev/null || echo "0")

  # Ghi vào log file (thêm chữ MiB vào để file python đọc đúng format)
  ts=$(date +%s)
  echo "${ts},${cpu1},${cpu2}" >> "${CPU_LOG}"
  echo "${ts},${mem_raw1}MiB,${mem_raw2}MiB" >> "${MEM_LOG}"

  # 3. Check số lượng message trong Queue bằng lệnh sudo trực tiếp
  qlen=$(sudo rabbitmqctl list_queues name messages 2>/dev/null | awk -v q="${QUEUE_NAME}" '$1==q {print $2}')
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
for p in "${PIDS[@]}"; do
  kill "$p" 2>/dev/null || true
done
wait

echo "[Workers] Hoàn tất."
