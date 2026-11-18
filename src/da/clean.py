"""Stage 2 - normalise text, score OCR noise, filter junk, hash for exact-dup detection.

Output <base>/clean : doc_id, year, jurisdiction, case_name, ocr, n_words, alpha_ratio, noise, text_hash, text
(partitioned by year so later stages can prune by time).
"""
from __future__ import annotations

import hashlib

import pandas as pd
from pyspark.sql import SparkSession, types as T
from pyspark.sql import functions as F

from .common import P, Metrics
from .textutil import clean_text, norm_for_hash, text_stats

_SCHEMA = T.StructType([
    T.StructField("text", T.StringType()),
    T.StructField("n_words", T.IntegerType()),
    T.StructField("alpha_ratio", T.DoubleType()),
    T.StructField("noise", T.DoubleType()),
    T.StructField("text_hash", T.StringType()),
])


def _make_udf(max_chars: int):
    @F.pandas_udf(_SCHEMA)
    def clean_udf(raw: pd.Series) -> pd.DataFrame:
        texts, nw, ar, ns, hs = [], [], [], [], []
        for s in raw:
            t = clean_text(s or "", max_chars)
            n, a, z = text_stats(t)
            texts.append(t)
            nw.append(n)
            ar.append(a)
            ns.append(z)
            hs.append(hashlib.md5(norm_for_hash(t).encode()).hexdigest())
        return pd.DataFrame({"text": texts, "n_words": nw, "alpha_ratio": ar, "noise": ns, "text_hash": hs})
    return clean_udf


def run(cfg: dict, spark: SparkSession, m: Metrics) -> None:
    c = cfg["clean"]
    out = P(cfg, "clean")
    with m.stage("clean"):
        raw = spark.read.parquet(P(cfg, "raw_parquet"))
        df = raw.withColumn("c", _make_udf(c["max_chars"])("raw_text")).select(
            "doc_id", "case_name", "court_name", "ocr", "text_src", "raw_chars", "year", "jurisdiction", "c.*")
        n_all = None
        df = df.where((F.col("n_words") >= c["min_words"]) & (F.col("noise") <= c["max_noise"]) & (F.col("alpha_ratio") >= 0.6))
        df.repartition("year").write.mode("overwrite").partitionBy("year").parquet(out)
    n_raw = spark.read.parquet(P(cfg, "raw_parquet")).count()
    n_clean = spark.read.parquet(out).count()
    m.record("clean_summary", "info", n_in=n_raw, n_out=n_clean, dropped=n_raw - n_clean)
    print(f"clean: {n_raw:,} -> {n_clean:,} opinions kept ({n_raw - n_clean:,} dropped as short/noisy)")
    m.io_probe("clean", out, ["doc_id", "text"])
