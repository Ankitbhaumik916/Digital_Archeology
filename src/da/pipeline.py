"""Orchestrator.  python -m da.pipeline run --stages ingest,clean,dedup --profile test"""
from __future__ import annotations

import argparse
import importlib
import time

from .common import Metrics, get_spark, load_config

STAGES = ["ingest", "clean", "dedup", "entities", "embed_input", "embed", "cluster", "marts"]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--stages", default="ingest,clean,dedup,entities,embed_input,embed,cluster,marts")
    ap.add_argument("--profile")
    ap.add_argument("--config")
    ap.add_argument("--scale", help="label for benchmark runs, e.g. 0.25 (also set ingest.sample_fraction via DA_SAMPLE)")
    ap.add_argument("--sample", type=float, help="override ingest.sample_fraction")
    a = ap.parse_args(argv)

    cfg = load_config(a.config, a.profile)
    if a.sample is not None:
        cfg["ingest"]["sample_fraction"] = a.sample
    scale = a.scale or str(cfg["ingest"].get("sample_fraction", 1.0))
    stages = [s.strip() for s in a.stages.split(",") if s.strip()]
    spark = get_spark(cfg, "digital-archaeology")
    m = Metrics(cfg, spark, scale=scale)
    print(f"run_id={m.run_id} scale={scale} profile={cfg.get('profile')} stages={stages}")
    t0 = time.perf_counter()
    for s in stages:
        if s not in STAGES:
            raise SystemExit(f"unknown stage {s}")
        mod = importlib.import_module(f"da.{s}")
        mod.run(cfg, spark, m)
    m.record("pipeline_total", "info", wall_s=round(time.perf_counter() - t0, 2))
    spark.stop()


if __name__ == "__main__":
    main()
