"""Stage 5a - build the (small) GPU work queue: K text segments sampled evenly across each surviving opinion.

Writes <base>/embed_input and exports a copy to <exchange>/embed_input (local dir the GPU host can read).
Long opinions are represented by segments spread from start to end, not just the first N tokens.
"""
from __future__ import annotations

import shutil

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .common import P, Metrics, local_path, to_uri


def sample_segments(text: str, k: int, n: int) -> list[str]:
    L = len(text)
    if L <= n:
        return [text]
    if L <= n * k:  # short enough to cover contiguously
        starts = list(range(0, L, n))[:k]
    else:
        starts = [int(x) for x in np.linspace(0, L - n, k)]
    out = []
    for s in starts:
        if s > 0:  # snap forward to a word boundary
            j = text.find(" ", s, s + 60)
            s = j + 1 if j != -1 else s
        out.append(text[s:s + n].strip())
    return [o for o in out if o]


def run(cfg: dict, spark: SparkSession, m: Metrics) -> None:
    c = cfg["embed_input"]
    k, n = int(c["segments"]), int(c["segment_chars"])
    keep = spark.read.parquet(P(cfg, "dedup_keep"))
    clean = spark.read.parquet(P(cfg, "clean")).join(keep, "doc_id")

    @F.pandas_udf("array<string>")
    def seg_udf(text: pd.Series) -> pd.Series:
        return pd.Series([sample_segments(t or "", k, n) for t in text])

    with m.stage("embed_input"):
        df = clean.select("doc_id", "year", seg_udf("text").alias("segments"))
        n_docs = keep.count()
        parts = max(1, int(np.ceil(n_docs / 20000)))
        df.repartition(parts).write.mode("overwrite").parquet(P(cfg, "embed_input"))
        ex = to_uri(cfg["paths"]["exchange"])
        if ex.startswith("file://"):
            shutil.rmtree(local_path(ex) + "/embed_input", ignore_errors=True)
            # new work queue => any previously computed embeddings belong to different documents; never resume from them
            shutil.rmtree(local_path(ex) + "/embeddings", ignore_errors=True)
        spark.read.parquet(P(cfg, "embed_input")).coalesce(parts).write.mode("overwrite").parquet(ex + "/embed_input")
    m.record("embed_input_summary", "info", n_docs=n_docs, segments_per_doc=k, segment_chars=n)
    print(f"embed_input: {n_docs:,} opinions x <= {k} segments -> {ex}/embed_input")
