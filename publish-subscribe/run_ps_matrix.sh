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

M=3

RATES=(${RATES:-500 1000 2000})
PREFETCH_LIST=(${PREFETCH_LIST:-10 50 200})
SIZES=(${SIZES:-1024 10240})
RUN_SECONDS="${RUN_SECONDS:-20}"
SLEEP_MS="${SLEEP_MS:-20}"
MODES=(${MODES:-A B C})

CPU_DELAY="${CPU_DELAY:-3}"
POLL_INTERVAL="${POLL_INTERVAL:-1}"
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT:-3}"

TS="$(TZ=Asia/Ho_Chi_Minh date '+%Y%m%d_%H%M%S')"
OUT_ROOT="${OUT_ROOT:-${WORK_DIR}/results/${TS}}"
RESULT_CSV="${RESULT_CSV:-${OUT_ROOT}/summary.csv}"
mkdir -p "${OUT_ROOT}"

mode_cfg() {
  local mode="$1"
  local base_ex="${EXCHANGE_BASE:-broadcast_x}"
  local base_q="${QUEUE_BASE:-subq}"
  case "${mode}" in
    A) echo "0|0||${base_ex}_A|${base_q}_A" ;;
    B) echo "1|1|--durable --persistent|${base_ex}_B|${base_q}_B" ;;
    C) echo "1|1|--durable --persistent --confirm|${base_ex}_C|${base_q}_C" ;;
    *) echo "bad mode ${mode}" >&2; exit 1 ;;
  esac
}

purge_queue() {
  docker exec "${CONTAINER_NAME}" rabbitmqctl purge_queue "$1" >/dev/null 2>&1 || true
}

get_ready_unacked() {
  local q="$1"
  docker exec "${CONTAINER_NAME}" rabbitmqctl list_queues -q name messages_ready messages_unacknowledged 2>/dev/null \
    | awk -v q="${q}" '$1==q {print $2, $3; found=1} END{if(!found) print "0 0"}'
}

echo "mode,rate,M,prefetch,payload_bytes,thr_s1,thr_s2,thr_s3,thr_total,p95_s1,p95_s2,p95_s3,p95_total,goodput_MBps,cpu_avg_pct,cpu_max_pct,mem_avg_mib,mem_max_mib,confirm_fail,exchange,queues,run_tag" \
  > "${RESULT_CSV}"

for mode in "${MODES[@]}"; do
  IFS="|" read -r EX_DUR Q_DUR PRODUCER_FLAGS EXCHANGE QBASE < <(mode_cfg "${mode}")

  for rate in "${RATES[@]}"; do
    for prefetch in "${PREFETCH_LIST[@]}"; do
      for size in "${SIZES[@]}"; do

        messages=$((rate * RUN_SECONDS))
        run_tag="mode${mode}_rate${rate}_M${M}_pref${prefetch}_sz${size}_t${RUN_SECONDS}"
        run_dir="${OUT_ROOT}/${run_tag}"
        mkdir -p "${run_dir}"

        cpu_log="${run_dir}/cpu_rabbit.log"
        mem_log="${run_dir}/mem_rabbit.log"
        ts_csv="${run_dir}/queue_ts.csv"
        producer_log="${run_dir}/producer_stdout.log"

        q1="${QBASE}_1"
        q2="${QBASE}_2"
        q3="${QBASE}_3"
        queues="${q1};${q2};${q3}"

        echo
        echo "===== RUN ${run_tag} exchange=${EXCHANGE} queues=${queues} ====="

        purge_queue "${q1}"
        purge_queue "${q2}"
        purge_queue "${q3}"

        pids=()
        stat_pid=""
        prod_pid=""

        cleanup_run() {
          if [[ -n "${stat_pid}" ]]; then
            kill "${stat_pid}" 2>/dev/null || true
          fi
          if [[ -n "${prod_pid}" ]]; then
            kill "${prod_pid}" 2>/dev/null || true
          fi
          for pid in "${pids[@]:-}"; do
            kill "${pid}" 2>/dev/null || true
          done
        }

        for i in 1 2 3; do
          q="${QBASE}_${i}"
          python3 "${SUBSCRIBER}" \
            --host "${RABBIT_HOST}" --port "${RABBIT_PORT}" \
            --user "${RABBIT_USER}" --password "${RABBIT_PASS}" \
            --exchange "${EXCHANGE}" --queue "${q}" --sub-id "${i}" \
            --prefetch "${prefetch}" --sleep-ms "${SLEEP_MS}" \
            --log "${run_dir}/s${i}.jsonl" \
            --queue-durable "${Q_DUR}" --exchange-durable "${EX_DUR}" \
            > "${run_dir}/sub${i}.out.log" 2>&1 &
          pids+=("$!")
        done

        sleep "${CPU_DELAY}"

        (
          while true; do
            docker stats --no-stream --format "{{.Name}} {{.CPUPerc}}" "${CONTAINER_NAME}" >> "${cpu_log}" || true
            docker stats --no-stream --format "{{.Name}} {{.MemUsage}}" "${CONTAINER_NAME}" >> "${mem_log}" || true
            sleep 1
          done
        ) &
        stat_pid=$!

        echo "ts_iso,ts_epoch,ready_total,unacked_total,total,producer_alive" > "${ts_csv}"

        set +e
        python3 "${PRODUCER}" \
          --host "${RABBIT_HOST}" --port "${RABBIT_PORT}" \
          --user "${RABBIT_USER}" --password "${RABBIT_PASS}" \
          --exchange "${EXCHANGE}" \
          -n "${messages}" --payload-bytes "${size}" --rate "${rate}" \
          ${PRODUCER_FLAGS} \
          > "${producer_log}" 2>&1 &
        prod_pid=$!
        set -e

        seen=0
        zero_ok=0

        while true; do
          any=0
          all_r=0
          all_u=0

          for q in "${q1}" "${q2}" "${q3}"; do
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

          if kill -0 "${prod_pid}" 2>/dev/null; then
            producer_alive=1
          else
            producer_alive=0
          fi

          echo "[${ts_iso}] ${run_tag} ready=${all_r} unacked=${all_u} total=${total} producer_alive=${producer_alive}"
          echo "${ts_iso},${ts_epoch},${all_r},${all_u},${total},${producer_alive}" >> "${ts_csv}"

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

        set +e
        wait "${prod_pid}"
        rc=$?
        set -e

        confirm_fail="$(awk -F': ' '/^confirm_fail:/ {print $2}' "${producer_log}" | tail -n1)"
        confirm_fail="${confirm_fail:-0}"

        if [[ "${rc}" -ne 0 ]]; then
          echo "Producer failed rc=${rc}. See ${producer_log}"
          cleanup_run
          continue
        fi

        kill "${stat_pid}" 2>/dev/null || true
        for pid in "${pids[@]}"; do
          kill "${pid}" 2>/dev/null || true
        done

        metrics="$(
          python3 "${SUMMARIZER}" \
            --run-dir "${run_dir}" \
            --pattern "s*.jsonl" \
            --cpu-log "cpu_rabbit.log" \
            --mem-log "mem_rabbit.log" \
            --payload-bytes "${size}"
        )"

        echo "${mode},${rate},${M},${prefetch},${size},${metrics},${confirm_fail},${EXCHANGE},${queues},${run_tag}" \
          >> "${RESULT_CSV}"

      done
    done
  done
done

echo "DONE: ${RESULT_CSV}"