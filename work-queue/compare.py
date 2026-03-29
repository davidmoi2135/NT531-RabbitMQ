#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path


def load_summary_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]

    numeric_cols = [
        "rate", "N", "prefetch", "payload_bytes",
        "throughput_msgps", "latency_avg_ms", "latency_p95_ms",
        "cpu_avg_pct", "cpu_max_pct", "mem_avg_mib", "mem_max_mib",
        "acked_count", "worker_duration_s", "confirm_fail",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "mode" in df.columns:
        df["mode"] = df["mode"].astype(str).str.strip()

    return df


def plot_compare_modeA_B_throughput_and_p95(summary_csv: str, out_dir: str):
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    df = load_summary_csv(summary_csv)

    # Chỉ lấy Mode A, B và payload 1KB
    sub = df[
        (df["mode"].isin(["A", "B"])) &
        (df["payload_bytes"] == 1024)
    ].copy()

    # Gộp theo mode-rate-N, lấy trung bình qua các mức prefetch
    agg = (
        sub.groupby(["mode", "rate", "N"], as_index=False)
        .agg(
            throughput_msgps=("throughput_msgps", "mean"),
            latency_p95_ms=("latency_p95_ms", "mean"),
        )
        .sort_values(["mode", "N", "rate"])
    )

    # 1) Throughput vs rate, compare Mode A và B
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)

    for ax, mode in zip(axes, ["A", "B"]):
        mode_df = agg[agg["mode"] == mode]
        for n in sorted(mode_df["N"].dropna().unique()):
            line = mode_df[mode_df["N"] == n].sort_values("rate")
            ax.plot(
                line["rate"],
                line["throughput_msgps"],
                marker="o",
                label=f"N={int(n)}"
            )

        ax.set_title(f"Mode {mode} Work Queue: Throughput vs rate (payload=1KB)")
        ax.set_xlabel("Publish rate (msg/s)")
        ax.set_ylabel("Throughput (msg/s)")
        ax.grid(True, alpha=0.3)
        ax.legend()

    plt.tight_layout()
    plt.savefig(out_path / "compare_modeA_modeB_throughput_vs_rate_payload1KB.png", dpi=150)
    plt.close()

    # 2) p95 vs rate, compare Mode A và B
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)

    for ax, mode in zip(axes, ["A", "B"]):
        mode_df = agg[agg["mode"] == mode]
        for n in sorted(mode_df["N"].dropna().unique()):
            line = mode_df[mode_df["N"] == n].sort_values("rate")
            ax.plot(
                line["rate"],
                line["latency_p95_ms"],
                marker="o",
                label=f"N={int(n)}"
            )

        ax.set_title(f"Mode {mode} Work Queue: p95 vs rate (payload=1KB)")
        ax.set_xlabel("Publish rate (msg/s)")
        ax.set_ylabel("Latency p95 (ms)")
        ax.grid(True, alpha=0.3)
        ax.legend()

    plt.tight_layout()
    plt.savefig(out_path / "compare_modeA_modeB_p95_vs_rate_payload1KB.png", dpi=150)
    plt.close()

    print("[OK] Saved:")
    print(out_path / "compare_modeA_modeB_throughput_vs_rate_payload1KB.png")
    print(out_path / "compare_modeA_modeB_p95_vs_rate_payload1KB.png")


if __name__ == "__main__":
    plot_compare_modeA_B_throughput_and_p95(
        summary_csv="results/20260306_130431/summary.csv",
        out_dir="results/20260306_130431/charts_compare"
    )