"""Aggregate bench/metrics.jsonl into the 'where does the time go' report.

  python -m da.bench_report            -> bench/report.md, bench/breakdown.png, bench/scaling.png

For each data-scale label (latest run per scale) it reports, per pipeline stage:
  wall-clock, Spark task-time split (compute / shuffle-wait / shuffle-write / GC), bytes read/written/shuffled,
  scan-only I/O probes, and the GPU stage's tokenise-vs-forward split.
"""
from __future__ import annotations

import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .common import BENCH_FILE, ROOT

CATEGORY = {
    "ingest": "Storage ingest (HDFS write)", "embeddings_import": "Storage ingest (HDFS write)", "marts": "Spark analytics (MLlib + marts)",
    "clean": "Spark ETL", "dedup_exact": "Spark ETL", "dedup_near": "Spark ETL", "embed_input": "Spark ETL",
    "entities_extract": "Spark ETL", "entities_vocab": "Spark ETL", "entities_resolve": "Spark ETL", "entities_finalize": "Spark ETL",
    "embed_gpu": "GPU inference", "cluster_fit": "Spark analytics (MLlib + marts)", "topic_keywords": "Spark analytics (MLlib + marts)",
}
ORDER = list(CATEGORY)
COLORS = {"Storage ingest (HDFS write)": "#4C78A8", "Spark ETL": "#F58518", "GPU inference": "#54A24B", "Spark analytics (MLlib + marts)": "#B279A2"}


def load() -> pd.DataFrame:
    if not BENCH_FILE.exists():
        raise SystemExit("no bench/metrics.jsonl yet - run the pipeline first")
    rows = [json.loads(line) for line in open(BENCH_FILE) if line.strip()]
    return pd.DataFrame(rows)


def latest_per_scale(df: pd.DataFrame) -> pd.DataFrame:
    """Latest record per (scale, stage): stages may have been run in separate invocations or on different hosts (GPU)."""
    df = df.copy()
    df["scale"] = df["scale"].astype(str)
    return df.sort_values("ts").groupby(["scale", "stage"], as_index=False).tail(1)


def main():
    df = latest_per_scale(load())
    out = ROOT / "bench"
    lines = ["# Pipeline benchmark report\n"]
    n_docs = {}
    for s, g in df.groupby("scale"):
        info = g[g.stage == "dedup_summary"]
        cl = g[g.stage == "clean_summary"]
        if len(cl):
            n_docs[s] = int(cl.iloc[-1].get("n_out", 0))
    gpu_rows = df[df.stage == "embed_gpu"]
    wall_rows = []
    for s, g in df.groupby("scale"):
        for r in g[g.kind.isin(["spark", "gpu"])].itertuples():
            st = r.stage
            if st == "embed_gpu":
                wall = getattr(r, "wall_s", None)
            else:
                wall = r.wall_s
            wall_rows.append(dict(scale=s, stage=st, wall_s=wall, category=CATEGORY.get(st, "other"),
                                  task_run_s=getattr(r, "task_run_s", None), shuffle_wait_s=getattr(r, "shuffle_wait_s", None),
                                  shuffle_write_s=getattr(r, "shuffle_write_s", None), compute_s=getattr(r, "compute_s", None),
                                  gc_s=getattr(r, "gc_s", None), input_gb=(getattr(r, "input_bytes", 0) or 0) / 1e9,
                                  output_gb=(getattr(r, "output_bytes", 0) or 0) / 1e9,
                                  shuffle_gb=(getattr(r, "shuffle_write_bytes", 0) or 0) / 1e9))
    W = pd.DataFrame(wall_rows)
    W = W[W.stage.isin(ORDER)]
    W["order"] = W.stage.map({s: i for i, s in enumerate(ORDER)})
    W = W.sort_values(["scale", "order"])
    scales = sorted(W.scale.unique(), key=float)

    for s in scales:
        w = W[W.scale == s]
        total = w.wall_s.sum()
        lines.append(f"\n## Scale {s}" + (f"  ({n_docs.get(s, '?'):,} opinions after cleaning)" if s in n_docs else "") + "\n")
        lines.append("| stage | category | wall s | % | task s | compute s | shuffle-wait s | shuffle-write s | GC s | in GB | out GB | shuffle GB |")
        lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        f = lambda v, d=1: "" if pd.isna(v) else f"{v:.{d}f}"  # noqa: E731
        for r in w.itertuples():
            lines.append(f"| {r.stage} | {r.category} | {f(r.wall_s)} | {100 * r.wall_s / total:.0f}% | {f(r.task_run_s)} | {f(r.compute_s)} | "
                         f"{f(r.shuffle_wait_s)} | {f(r.shuffle_write_s)} | {f(r.gc_s)} | {f(r.input_gb, 3)} | {f(r.output_gb, 3)} | {f(r.shuffle_gb, 3)} |")
        cat = w.groupby("category").wall_s.sum()
        lines.append("\n**Wall-clock by category:** " + ", ".join(f"{k} {100 * v / total:.0f}%" for k, v in cat.sort_values(ascending=False).items()))
        sp = w[w.task_run_s.notna()]
        if len(sp) and sp.task_run_s.sum() > 0:
            lines.append(f"\n**Spark task time:** shuffle-wait {100 * sp.shuffle_wait_s.sum() / sp.task_run_s.sum():.1f}%, "
                         f"shuffle-write {100 * sp.shuffle_write_s.sum() / sp.task_run_s.sum():.1f}%, compute {100 * sp.compute_s.sum() / sp.task_run_s.sum():.1f}% "
                         f"of {sp.task_run_s.sum():.0f} task-seconds")
        g = df[(df.scale == s) & (df.kind == "io_probe")]
        if len(g):
            lines.append("\n**Scan-only reads (storage I/O + decode lower bound):** " + ", ".join(f"{r.stage.split(':')[1]} {r.wall_s:.1f}s" for r in g.itertuples()))
        gg = gpu_rows[gpu_rows.scale.astype(str) == s]
        if len(gg):
            r = gg.iloc[-1]
            lines.append(f"\n**GPU stage** ({r.get('device')}, {r.get('model')}, batch {r.get('batch_size')}): {r.get('segments', 0):,} segments, "
                         f"{r.get('segments_per_s', 0):.0f} seg/s; tokenise {r.get('tokenise_s', 0):.1f}s (overlapped) / forward {r.get('gpu_s', 0):.1f}s / wall {r.get('wall_s', 0):.1f}s")

    # figure 1: stacked wall-clock by stage per scale
    fig, ax = plt.subplots(figsize=(9, 4.5))
    bottoms = pd.Series(0.0, index=scales)
    for st in ORDER:
        vals = pd.Series({s: (W[(W.scale == s) & (W.stage == st)].wall_s.sum()) for s in scales})
        if vals.sum() == 0:
            continue
        ax.bar(scales, vals.values, bottom=bottoms.values, label=st, color=COLORS.get(CATEGORY[st]), edgecolor="white", linewidth=.6,
               alpha=0.55 + 0.45 * (ORDER.index(st) % 3) / 2)
        bottoms += vals
    ax.set_xlabel("data scale (fraction of corpus)")
    ax.set_ylabel("wall-clock seconds")
    ax.set_title("Where the time goes, by stage")
    ax.legend(fontsize=7, ncol=2)
    fig.tight_layout()
    fig.savefig(out / "breakdown.png", dpi=150)

    # figure 2: category share vs scale
    if len(scales) >= 1:
        C = W.groupby(["scale", "category"]).wall_s.sum().unstack(fill_value=0).reindex(scales)
        fig, ax = plt.subplots(figsize=(7, 4))
        (C.div(C.sum(axis=1), axis=0) * 100).plot(kind="bar", stacked=True, ax=ax, color=[COLORS.get(c) for c in C.columns])
        ax.set_ylabel("% of pipeline wall-clock")
        ax.set_xlabel("data scale")
        ax.set_title("Bottleneck shift with scale")
        ax.legend(fontsize=7)
        fig.tight_layout()
        fig.savefig(out / "scaling.png", dpi=150)
    (out / "report.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
