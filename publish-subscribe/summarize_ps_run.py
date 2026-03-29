#!/usr/bin/env python3
import argparse, glob, json, math, os, re, statistics as st

def pct(sorted_vals, p: float) -> float:
    if not sorted_vals: return 0.0
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c: return float(sorted_vals[int(k)])
    return float(sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f))

def parse_jsonl(fn: str):
    lat = []
    ts = []
    cnt = 0
    with open(fn, "r", encoding="utf-8") as f:
        for line in f:
            if not line or line[0] != "{":
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("acked") is True:
                cnt += 1
                t = d.get("ts")
                if isinstance(t, (int, float)):
                    ts.append(float(t))
                v = d.get("net_latency_ms")
                if isinstance(v, (int, float)) and v >= 0:
                    lat.append(float(v))
    ts.sort()
    lat.sort()
    dur = (ts[-1] - ts[0]) if len(ts) >= 2 else 0.0
    thr = (cnt / dur) if dur > 0 else 0.0
    avg = st.mean(lat) if lat else 0.0
    p95 = pct(lat, 0.95)
    return cnt, dur, thr, avg, p95

def parse_cpu(fn: str):
    vals=[]
    try:
        for line in open(fn,"r",encoding="utf-8"):
            m=re.search(r"([\d.]+)%", line)
            if m: vals.append(float(m.group(1)))
    except FileNotFoundError:
        pass
    return (st.mean(vals), max(vals)) if vals else (0.0, 0.0)

def parse_mem(fn: str):
    def to_mib(x, unit):
        unit=unit.lower()
        if unit=="b": return x/(1024*1024)
        if unit=="kib": return x/1024
        if unit=="mib": return x
        if unit=="gib": return x*1024
        return x
    vals=[]
    try:
        for line in open(fn,"r",encoding="utf-8"):
            m=re.search(r"([\d.]+)\s*(B|KiB|MiB|GiB)", line)
            if m:
                vals.append(to_mib(float(m.group(1)), m.group(2)))
    except FileNotFoundError:
        pass
    return (st.mean(vals), max(vals)) if vals else (0.0, 0.0)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--pattern", default="s*.jsonl")
    ap.add_argument("--cpu-log", default="cpu_rabbit.log")
    ap.add_argument("--mem-log", default="mem_rabbit.log")
    ap.add_argument("--payload-bytes", type=int, required=True)
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.run_dir, args.pattern)))

    per = []
    total_cnt = 0
    total_thr = 0.0
    all_lat = []

    for fn in files:
        cnt, dur, thr, avg, p95 = parse_jsonl(fn)
        per.append((os.path.basename(fn), cnt, dur, thr, avg, p95))
        total_cnt += cnt
        total_thr += thr
        lat=[]
        with open(fn,"r",encoding="utf-8") as f:
            for line in f:
                if '"net_latency_ms"' not in line: 
                    continue
                try:
                    d=json.loads(line)
                except Exception:
                    continue
                v=d.get("net_latency_ms")
                if isinstance(v,(int,float)) and v>=0:
                    all_lat.append(float(v))

    all_lat.sort()
    lat_avg_all = st.mean(all_lat) if all_lat else 0.0
    lat_p95_all = pct(all_lat, 0.95)

    cpu_avg, cpu_max = parse_cpu(os.path.join(args.run_dir, args.cpu_log))
    mem_avg, mem_max = parse_mem(os.path.join(args.run_dir, args.mem_log))

    goodput_MBps = (total_thr * args.payload_bytes) / 1_000_000.0

    thr_list = [x[3] for x in per] + [0.0,0.0,0.0]
    p95_list = [x[5] for x in per] + [0.0,0.0,0.0]
    thr_s1,thr_s2,thr_s3 = thr_list[0],thr_list[1],thr_list[2]
    p95_s1,p95_s2,p95_s3 = p95_list[0],p95_list[1],p95_list[2]

    print(
        f"{thr_s1:.2f},{thr_s2:.2f},{thr_s3:.2f},{total_thr:.2f},"
        f"{p95_s1:.2f},{p95_s2:.2f},{p95_s3:.2f},{lat_p95_all:.2f},"
        f"{goodput_MBps:.3f},"
        f"{cpu_avg:.2f},{cpu_max:.2f},"
        f"{mem_avg:.2f},{mem_max:.2f}"
    )

if __name__ == "__main__":
    main()