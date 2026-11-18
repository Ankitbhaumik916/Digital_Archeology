"""Stage 1 - ingest CourtListener bulk CSVs into HDFS as parquet partitioned by year / jurisdiction.

Inputs (in paths.raw, any of .csv / .csv.bz2 / .csv.gz):
  opinions-*        id, cluster_id, plain_text, html*, xml_harvard, extracted_by_ocr
  opinion-clusters-*  id, docket_id, case_name, date_filed
  dockets-*         id, court_id
  courts-*          id, full_name, jurisdiction
Output: <base>/raw_parquet/year=YYYY/jurisdiction=X/*.parquet
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .common import P, Metrics


def _csv(spark: SparkSession, path: str, cols: list[str]) -> DataFrame:
    df = (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
          .option("quote", '"').option("maxCharsPerColumn", -1).csv(path))
    have = set(df.columns)
    return df.select(*[F.col(c) if c in have else F.lit(None).cast("string").alias(c) for c in cols])


def _strip_html(c):
    x = F.regexp_replace(c, r"(?is)<(script|style)[^>]*>.*?</\1>", " ")
    x = F.regexp_replace(x, r"<[^>]+>", " ")
    return x


def run(cfg: dict, spark: SparkSession, m: Metrics) -> None:
    ing = cfg["ingest"]
    raw = cfg["paths"]["raw"].rstrip("/")
    if "://" not in raw:
        from .common import to_uri
        raw = to_uri(raw)

    out = P(cfg, "raw_parquet")
    with m.stage("ingest"):
        courts = _csv(spark, f"{raw}/courts-*", ["id", "full_name", "jurisdiction"]).select(
            F.col("id").alias("court_id"), F.col("full_name").alias("court_name"),
            F.coalesce(F.col("jurisdiction"), F.lit("U")).alias("jurisdiction"))
        dockets = _csv(spark, f"{raw}/dockets-*", ["id", "court_id"]).select(
            F.col("id").alias("docket_id"), "court_id")
        clusters = _csv(spark, f"{raw}/opinion-clusters-*", ["id", "docket_id", "case_name", "date_filed"]).select(
            F.col("id").alias("cluster_id"), "docket_id", "case_name",
            F.substring("date_filed", 1, 4).cast("int").alias("year"), "date_filed")
        clusters = clusters.where(F.col("year").between(ing["min_year"], ing["max_year"]))
        frac = float(ing.get("sample_fraction", 1.0))
        if frac < 1.0:  # deterministic, nested samples: the 10% subset is contained in the 25% subset
            clusters = clusters.where(F.pmod(F.hash("cluster_id"), F.lit(10000)) < int(frac * 10000))
        clusters = (clusters.join(dockets, "docket_id", "left").join(F.broadcast(courts), "court_id", "left")
                    .withColumn("jurisdiction", F.coalesce("jurisdiction", F.lit("U"))))

        tcols = ing["text_columns"]
        ops = _csv(spark, f"{raw}/opinions-*", ["id", "cluster_id", "extracted_by_ocr", "type"] + tcols)
        exprs = []
        for c in tcols:
            v = F.col(c) if c == "plain_text" else _strip_html(F.col(c))
            exprs.append((c, F.when(F.length(F.trim(F.col(c))) > 0, v)))
        ops = ops.select(
            F.col("id").cast("long").alias("doc_id"), F.col("cluster_id"),
            F.lower(F.coalesce(F.col("extracted_by_ocr"), F.lit("f"))).isin("t", "true", "1").alias("ocr"),
            F.coalesce(*[F.when(e.isNotNull(), F.lit(n)) for n, e in exprs]).alias("text_src"),
            F.coalesce(*[e for _, e in exprs]).alias("raw_text"))
        ops = ops.where(F.col("raw_text").isNotNull())
        df = ops.join(clusters, "cluster_id", "inner").select(
            "doc_id", "cluster_id", "case_name", "date_filed", "court_id", "court_name", "text_src", "ocr",
            F.length("raw_text").alias("raw_chars"), "raw_text", "year", "jurisdiction")
        (df.repartition(F.col("year"), F.col("jurisdiction"))
           .write.mode("overwrite").partitionBy("year", "jurisdiction").parquet(out))
    n = spark.read.parquet(out).count()
    m.record("ingest_summary", "info", n_docs=n)
    print(f"ingested {n:,} opinions -> {out}")
    m.io_probe("raw_parquet", out)
