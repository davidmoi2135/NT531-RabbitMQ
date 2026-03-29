#!/usr/bin/env bash
set -euo pipefail

DIR="${1:-$HOME/publish-subcribe}"
PATTERN="${2:-s*.jsonl}"

python3 - "$DIR" "$PATTERN" <<'PY'
import glob, json, math, os, statistics as st, sys

dir_ = sys.argv[1]
pattern = sys.argv[2]
files = sorted(glob.glob(os.path.join(dir_, pattern)))

def pct(vals, p):
    if not vals: return 0.0
    vals = sorted(vals)
    k = (len(vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c: return float(vals[int(k)])
    return float(vals[f] + (vals[c] - vals[f]) * (k - f))

def parse_file(fn):
    vals=[]
    with open(fn, "r", encoding="utf-8") as f:
        for line in f:
            if '"net_latency_ms"' not in line:
                continue
            try:
                d=json.loads(line)
            except Exception:
                continue
            v=d.get("net_latency_ms")
            if isinstance(v,(int,float)) and v>=0:
                vals.append(float(v))
    return vals

all_vals=[]
rows=[]
for fn in files:
    vals=parse_file(fn)
    all_vals += vals
    avg = st.mean(vals) if vals else 0.0
    p95 = pct(vals, 0.95)
    rows.append((os.path.basename(fn), len(vals), avg, p95))

# Print per-subscriber
print("=== Latency per subscriber (sent_ts -> recv_ts) ===")
for name,cnt,avg,p95 in rows:
    print(f"{name:10s} count={cnt:6d} avg_ms={avg:10.2f} p95_ms={p95:10.2f}  (avg_s={avg/1000:7.2f} p95_s={p95/1000:7.2f})")

# Print total combined
avg_all = st.mean(all_vals) if all_vals else 0.0
p95_all = pct(all_vals, 0.95)
print("=== Total (combined) ===")
print(f"files={len(files)} count={len(all_vals)} avg_ms={avg_all:.2f} p95_ms={p95_all:.2f}  (avg_s={avg_all/1000:.2f} p95_s={p95_all/1000:.2f})")
PY
