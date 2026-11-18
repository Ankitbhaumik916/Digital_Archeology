"""Stage 3 - exact + near-duplicate removal (MinHash-LSH with verification, connected components).

exact    : identical normalised text (md5)
near     : 5-word shingles -> 128-perm MinHash -> 16 LSH bands -> candidate pairs -> verify est. Jaccard
           on the full signature -> connected components -> keep best-quality member of each component.
Outputs  <base>/dedup_map (doc_id, canonical_id, reason) and <base>/dedup_keep (doc_id).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Window, types as T
from pyspark.sql import functions as F

from .common import P, Metrics
from .textutil import band_hashes, minhash, norm_for_hash, word_shingles, _params

_SIG_SCHEMA = T.StructType([
    T.StructField("sig", T.ArrayType(T.IntegerType())),
    T.StructField("bands", T.ArrayType(T.LongType())),
])


def _sig_udf(k: int, num_perm: int, bands: int):
    @F.pandas_udf(_SIG_SCHEMA)
    def f(text: pd.Series) -> pd.DataFrame:
        a, b = _params(num_perm)
        sigs, bnds = [], []
        for t in text:
            s = minhash(word_shingles(norm_for_hash(t), k), a, b)
            sigs.append(s.astype(np.int32).tolist())
            bnds.append(band_hashes(s, bands).tolist())
        return pd.DataFrame({"sig": sigs, "bands": bnds})
    return f


def _pairs_udf(max_bucket: int, window: int):
    @F.udf(T.ArrayType(T.StructType([T.StructField("a", T.LongType()), T.StructField("b", T.LongType())])))
    def f(ids):
        ids = sorted(ids)
        n = len(ids)
        out = []
        if n <= max_bucket:
            for i in range(n):
                for j in range(i + 1, n):
                    out.append((ids[i], ids[j]))
        else:  # giant bucket (boilerplate): compare each doc with its next `window` neighbours only
            for i in range(n):
                for j in range(i + 1, min(i + 1 + window, n)):
                    out.append((ids[i], ids[j]))
        return out
    return f


def connected_components(spark: SparkSession, edges, threshold: int, max_iter: int = 50):
    """edges: DataFrame(a,b) -> DataFrame(id, comp) with comp = min id in component.
    Driver-side (scipy) when small enough, else iterative min-label propagation in Spark."""
    n = edges.count()
    if n == 0:
        return spark.createDataFrame([], "id long, comp long"), n
    if n <= threshold:
        from scipy.sparse import coo_matrix
        from scipy.sparse.csgraph import connected_components as cc
        pdf = edges.select("a", "b").toPandas()
        ids = np.unique(np.concatenate([pdf.a.values, pdf.b.values]))
        idx = {v: i for i, v in enumerate(ids)}  # noqa: F841 (kept for readability)
        ai = np.searchsorted(ids, pdf.a.values)
        bi = np.searchsorted(ids, pdf.b.values)
        g = coo_matrix((np.ones(len(ai), dtype=np.int8), (ai, bi)), shape=(len(ids), len(ids)))
        _, lab = cc(g, directed=False)
        comp_min = pd.Series(ids).groupby(lab).transform("min").values
        return spark.createDataFrame(pd.DataFrame({"id": ids, "comp": comp_min})), n
    # --- Spark-native label propagation ---
    e = edges.select("a", "b").union(edges.select(F.col("b").alias("a"), F.col("a").alias("b"))).distinct()
    labels = e.select(F.col("a").alias("id")).distinct().withColumn("comp", F.col("id"))
    for _ in range(max_iter):
        nb = e.join(labels.select(F.col("id").alias("b"), F.col("comp").alias("bl")), "b").groupBy("a").agg(
            F.min("bl").alias("m"))
        new = labels.join(nb, labels.id == nb.a, "left").select(
            "id", F.least("comp", F.coalesce("m", "comp")).alias("comp")).localCheckpoint()
        changed = new.join(labels.select("id", F.col("comp").alias("old")), "id").where(F.col("comp") != F.col("old")).count()
        labels = new
        if changed == 0:
            break
    return labels, n


def run(cfg: dict, spark: SparkSession, m: Metrics) -> None:
    d = cfg["dedup"]
    docs = spark.read.parquet(P(cfg, "clean")).select("doc_id", "text_hash", "noise", "n_words", "text")

    # ---- exact duplicates: keep best (lowest noise, then longest, then lowest id) per hash
    with m.stage("dedup_exact"):
        w = Window.partitionBy("text_hash").orderBy(F.col("noise").asc(), F.col("n_words").desc(), F.col("doc_id").asc())
        ex = (docs.select("doc_id", "text_hash", "noise", "n_words")
              .withColumn("rk", F.row_number().over(w))
              .withColumn("canonical_id", F.first("doc_id").over(w)))
        exact_map = ex.where(F.col("rk") > 1).select("doc_id", "canonical_id", F.lit("exact").alias("reason"))
        exact_map.write.mode("overwrite").parquet(P(cfg, "dedup_exact_map"))
    exact_map = spark.read.parquet(P(cfg, "dedup_exact_map"))
    remaining = docs.join(exact_map.select("doc_id"), "doc_id", "left_anti")

    # ---- near duplicates
    with m.stage("dedup_near"):
        sigs = remaining.withColumn("s", _sig_udf(d["shingle_words"], d["num_perm"], d["bands"])("text")).select(
            "doc_id", "noise", "n_words", F.col("s.sig").alias("sig"), F.col("s.bands").alias("bands"))
        sigs.write.mode("overwrite").parquet(P(cfg, "dedup_sigs"))
        sigs = spark.read.parquet(P(cfg, "dedup_sigs"))
        bnd = sigs.select("doc_id", F.posexplode("bands").alias("band", "bh"))
        buckets = (bnd.groupBy("band", "bh").agg(F.collect_list("doc_id").alias("ids"))
                   .where(F.size("ids") > 1))
        cand = (buckets.select(F.explode(_pairs_udf(d["max_bucket"], d["window"])("ids")).alias("p"))
                .select("p.a", "p.b").distinct())
        sa = sigs.select(F.col("doc_id").alias("a"), F.col("sig").alias("sa"))
        sb = sigs.select(F.col("doc_id").alias("b"), F.col("sig").alias("sb"))
        verified = (cand.join(sa, "a").join(sb, "b")
                    .withColumn("jac", F.expr("aggregate(zip_with(sa, sb, (x, y) -> cast(x = y as int)), 0, (acc, v) -> acc + v)")
                                / F.size("sa"))
                    .where(F.col("jac") >= d["jaccard"]).select("a", "b", "jac"))
        verified.write.mode("overwrite").parquet(P(cfg, "dedup_edges"))
        edges = spark.read.parquet(P(cfg, "dedup_edges"))
        comp, n_edges = connected_components(spark, edges, int(d["cc_spark_threshold"]))
        # canonical = best-quality member of each component
        q = sigs.select("doc_id", "noise", "n_words")
        cw = Window.partitionBy("comp").orderBy(F.col("noise").asc(), F.col("n_words").desc(), F.col("doc_id").asc())
        ranked = (comp.join(q, comp.id == q.doc_id).withColumn("rk", F.row_number().over(cw))
                  .withColumn("canonical_id", F.first("doc_id").over(cw)))
        near_map = ranked.where(F.col("rk") > 1).select("doc_id", "canonical_id", F.lit("near").alias("reason"))
        near_map.write.mode("overwrite").parquet(P(cfg, "dedup_near_map"))

    dmap = exact_map.unionByName(spark.read.parquet(P(cfg, "dedup_near_map")))
    dmap.write.mode("overwrite").parquet(P(cfg, "dedup_map"))
    keep = spark.read.parquet(P(cfg, "clean")).select("doc_id").join(
        spark.read.parquet(P(cfg, "dedup_map")).select("doc_id"), "doc_id", "left_anti")
    keep.write.mode("overwrite").parquet(P(cfg, "dedup_keep"))
    n_in = spark.read.parquet(P(cfg, "clean")).count()
    n_keep = spark.read.parquet(P(cfg, "dedup_keep")).count()
    n_exact = exact_map.count()
    m.record("dedup_summary", "info", n_in=n_in, n_keep=n_keep, n_exact=n_exact, n_near=n_in - n_keep - n_exact,
             n_edges=n_edges)
    print(f"dedup: {n_in:,} -> {n_keep:,} (exact {n_exact:,}, near {n_in - n_keep - n_exact:,}; {n_edges:,} verified edges)")
