#!/usr/bin/env python3
import argparse, glob, json, math, os, re, statistics as st

def pct(sorted_vals, p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_vals[int(k)])
    return float(sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f))

def parse_worker_jsonl(run_dir: str, pattern: str):
    vals_lat = []
    ts_list = []
    count_acked = 0

    files = sorted(glob.glob(os.path.join(run_dir, pattern)))
    for fn in files:
        with open(fn, "r", encoding="utf-8") as f:
            for line in f:
                if not line or line[0] != "{":
                    continue
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                if d.get("acked") is True:
                    count_acked += 1
                    ts = d.get("ts")
                    if isinstance(ts, (int, float)):
                        ts_list.append(float(ts))
                    v = d.get("net_latency_ms")
                    if isinstance(v, (int, float)) and v >= 0:
                        vals_lat.append(float(v))

    ts_list.sort()
    vals_lat.sort()

    if len(ts_list) >= 2:
        dur = ts_list[-1] - ts_list[0]
    else:
        dur = 0.0

    throughput = (count_acked / dur) if dur > 0 else 0.0
    lat_avg = st.mean(vals_lat) if vals_lat else 0.0
    lat_p95 = pct(vals_lat, 0.95)

    return {
        "acked_count": count_acked,
        "worker_duration_s": dur,
        "throughput_msgps": throughput,
        "lat_avg_ms": lat_avg,
        "lat_p95_ms": lat_p95,
        "files": len(files),
        "lat_count": len(vals_lat),
    }

def parse_cpu_log(cpu_log: str):
    """
    Log format mới: timestamp, cpu_node1, cpu_node2
    (VD: 1711550000, 15.5, 20.1)
    Ta sẽ tính tổng CPU của cả 2 node tại mỗi thời điểm.
    """
    total_cpu_samples = []
    try:
        with open(cpu_log, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 3:
                    try:
                        cpu1 = float(parts[1])
                        cpu2 = float(parts[2])
                        total_cpu_samples.append(cpu1 + cpu2)
                    except ValueError:
                        continue
    except FileNotFoundError:
        pass
    
    if not total_cpu_samples:
        return 0.0, 0.0
    
    return float(st.mean(total_cpu_samples)), float(max(total_cpu_samples))

def parse_mem_log(mem_log: str):
    """
    Log format mới: timestamp, mem_node1, mem_node2
    (VD: 1711550000, 123.4MiB, 150.2MiB)
    Ta sẽ parse, đổi ra MiB và tính tổng RAM của cụm tại mỗi thời điểm.
    """
    def to_mib_from_str(mem_str: str) -> float:
        # Xóa khoảng trắng thừa
        mem_str = mem_str.strip()
        m = re.search(r"([\d.]+)\s*(B|KiB|MiB|GiB|TiB)", mem_str, re.IGNORECASE)
        if not m:
            return 0.0
        
        val = float(m.group(1))
        unit = m.group(2).lower()
        
        if unit == "b":
            return val / (1024 * 1024)
        if unit == "kib":
            return val / 1024
        if unit == "mib":
            return val
        if unit == "gib":
            return val * 1024
        if unit == "tib":
            return val * 1024 * 1024
        return val

    total_mem_samples = []
    try:
        with open(mem_log, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 3:
                    mem1_mib = to_mib_from_str(parts[1])
                    mem2_mib = to_mib_from_str(parts[2])
                    total_mem_samples.append(mem1_mib + mem2_mib)
    except FileNotFoundError:
        pass

    if not total_mem_samples:
        return 0.0, 0.0
    
    return float(st.mean(total_mem_samples)), float(max(total_mem_samples))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--pattern", default="w*.jsonl")
    ap.add_argument("--cpu-log", default="cpu_rabbit.log")
    ap.add_argument("--mem-log", default="mem_rabbit.log")
    args = ap.parse_args()

    w = parse_worker_jsonl(args.run_dir, args.pattern)
    cpu_avg, cpu_max = parse_cpu_log(os.path.join(args.run_dir, args.cpu_log))
    mem_avg, mem_max = parse_mem_log(os.path.join(args.run_dir, args.mem_log))

    print(
        f"{w['throughput_msgps']:.2f},"
        f"{w['lat_avg_ms']:.2f},"
        f"{w['lat_p95_ms']:.2f},"
        f"{cpu_avg:.2f},{cpu_max:.2f},"
        f"{mem_avg:.2f},{mem_max:.2f},"
        f"{w['acked_count']},{w['worker_duration_s']:.2f}"
    )

if __name__ == "__main__":
    main()
