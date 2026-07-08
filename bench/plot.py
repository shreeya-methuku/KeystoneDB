#!/usr/bin/env python3
"""Read bench/results.csv and produce throughput + latency bar charts."""

import csv
import os
import sys

def main():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed, skipping plots")
        sys.exit(0)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "results.csv")

    if not os.path.exists(csv_path):
        print(f"No results.csv found at {csv_path}")
        sys.exit(1)

    with open(csv_path) as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("results.csv is empty")
        sys.exit(1)

    # Take the last complete run (last 10 rows = 5 workloads x 2 engines)
    rows = rows[-10:]

    # Group by workload, then engine
    workloads = []
    data = {}  # workload -> {engine -> row}
    for row in rows:
        wl = row["workload"]
        eng = row["engine"]
        if wl not in data:
            workloads.append(wl)
            data[wl] = {}
        data[wl][eng] = row

    engines = ["KeystoneDB", "SQLite"]
    colors = {"KeystoneDB": "#2196F3", "SQLite": "#FF9800"}

    # ── Throughput chart ─────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(10, 6))

    x = range(len(workloads))
    width = 0.35

    for i, eng in enumerate(engines):
        vals = []
        for wl in workloads:
            if eng in data.get(wl, {}):
                vals.append(float(data[wl][eng]["ops_per_s"]))
            else:
                vals.append(0)
        offset = (i - 0.5) * width
        bars = ax.bar([xi + offset for xi in x], vals, width,
                      label=eng, color=colors[eng])
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                    f"{v:,.0f}", ha="center", va="bottom", fontsize=7)

    ax.set_yscale("log")
    ax.set_ylabel("ops/s (log scale)")
    ax.set_title("Throughput: KeystoneDB vs SQLite")
    ax.set_xticks(list(x))
    ax.set_xticklabels(workloads, rotation=20, ha="right")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(script_dir, "throughput.png"), dpi=150)
    print("Wrote throughput.png")

    # ── Latency chart ────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(10, 6))

    width = 0.2
    offsets = {"KeystoneDB p50": -1.5, "KeystoneDB p99": -0.5,
               "SQLite p50": 0.5, "SQLite p99": 1.5}
    bar_colors = {"KeystoneDB p50": "#2196F3", "KeystoneDB p99": "#1565C0",
                  "SQLite p50": "#FF9800", "SQLite p99": "#E65100"}

    for label, off in offsets.items():
        eng = label.split()[0]
        pct = label.split()[1]
        col = "p50_us" if pct == "p50" else "p99_us"
        vals = []
        for wl in workloads:
            if eng in data.get(wl, {}):
                vals.append(float(data[wl][eng][col]))
            else:
                vals.append(0)
        ax.bar([xi + off * width for xi in x], vals, width,
               label=label, color=bar_colors[label])

    ax.set_yscale("log")
    ax.set_ylabel("Latency (us, log scale)")
    ax.set_title("Latency: KeystoneDB vs SQLite (p50 / p99)")
    ax.set_xticks(list(x))
    ax.set_xticklabels(workloads, rotation=20, ha="right")
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(script_dir, "latency.png"), dpi=150)
    print("Wrote latency.png")


if __name__ == "__main__":
    main()
