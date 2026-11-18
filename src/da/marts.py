"""Stage 7 - serving marts for the dashboard: how entities and topics move through time.

All aggregations run in Spark over the full corpus; only the small results are exported (parquet) to
<export>/ for the dashboard. Tables:
  docs_year(year, jurisdiction, n_docs)          topics(topic_id, label, keywords, n_docs)
  topic_year(topic_id, year, jurisdiction, n_docs)
  entities(ent_id, canonical, total_docs, variants)      entity_year(ent_id, year, jurisdiction, n_docs)
  entity_topic_decade(ent_id, decade, topic_id, n_docs)  entity_context_decade(ent_id, decade, term, score, n)
  examples(ent_id, year, jurisdiction, doc_id, case_name, ctx)
"""
from __future__ import annotations

from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from .cluster import LEGAL_STOP
from .common import P, Metrics, export_dir


def run(cfg: dict, spark: SparkSession, m: Metrics) -> None:
    e = cfg["entities"]
    keep = spark.read.parquet(P(cfg, "dedup_keep"))
    clean = spark.read.parquet(P(cfg, "clean")).join(keep, "doc_id")
    assign = spark.read.parquet(P(cfg, "topics/assign"))
    kw = spark.read.parquet(P(cfg, "topics/keywords"))
    dim = spark.read.parquet(P(cfg, "entities/dim"))
    men = spark.read.parquet(P(cfg, "entities/mentions"))
    exp = export_dir(cfg)

    def save(df, name):
        pdf = df.toPandas()
        pdf.to_parquet(exp / f"{name}.parquet", index=False)
        print(f"  marts/{name}: {len(pdf):,} rows")

    with m.stage("marts"):
        docs_year = clean.groupBy("year", "jurisdiction").agg(F.count("*").alias("n_docs"))
        save(docs_year, "docs_year")
        save(kw.select("topic_id", "label", "keywords", "n_docs"), "topics")
        topic_year = (clean.select("doc_id", "year", "jurisdiction").join(assign, "doc_id")
                      .groupBy("topic_id", "year", "jurisdiction").agg(F.count("*").alias("n_docs")))
        save(topic_year, "topic_year")

        top = dim.orderBy(F.col("total_docs").desc()).limit(int(e["dashboard_entities"]))
        top_ids = top.select("ent_id")
        save(top.select("ent_id", "canonical", "total_docs", F.slice("variants", 1, 8).alias("variants")), "entities")
        mt = men.join(F.broadcast(top_ids), "ent_id")
        ent_year = mt.groupBy("ent_id", "year", "jurisdiction").agg(F.countDistinct("doc_id").alias("n_docs"))
        save(ent_year, "entity_year")
        et = (mt.join(assign, "doc_id").withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
              .groupBy("ent_id", "decade", "topic_id").agg(F.countDistinct("doc_id").alias("n_docs")))
        save(et, "entity_topic_decade")

        # ---- framing: context words that are distinctive for an entity in a decade (KL-style lift vs the decade background)
        stop = list(set(StopWordsRemover.loadDefaultStopWords("english")) | set(LEGAL_STOP))
        ctx = (mt.select("ent_id", "year", "ctx").withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
               .select("ent_id", "decade", F.explode(F.split(F.lower(F.regexp_replace("ctx", r"[^A-Za-z\s]", " ")), r"\s+")).alias("term")))
        ctx = ctx.where((F.length("term") >= 4) & (~F.col("term").isin(stop)))
        names = dim.select("ent_id", "root_norm")
        ctx = (ctx.join(F.broadcast(names), "ent_id").where(~F.expr("array_contains(split(root_norm, ' '), term)"))
               .drop("root_norm"))
        ed = ctx.groupBy("ent_id", "decade", "term").agg(F.count("*").alias("n"))
        ed_tot = ed.groupBy("ent_id", "decade").agg(F.sum("n").alias("n_ed"))
        bg = ed.groupBy("decade", "term").agg(F.sum("n").alias("bg_n"))
        bg_tot = bg.groupBy("decade").agg(F.sum("bg_n").alias("bg_tot"))
        scored = (ed.join(ed_tot, ["ent_id", "decade"]).join(bg, ["decade", "term"]).join(bg_tot, "decade")
                  .withColumn("p", F.col("n") / F.col("n_ed")).withColumn("q", F.col("bg_n") / F.col("bg_tot"))
                  .withColumn("score", F.col("p") * F.log2(F.col("p") / F.col("q"))).where("n >= 3 and score > 0"))
        w = Window.partitionBy("ent_id", "decade").orderBy(F.col("score").desc(), "term")
        ctx_top = scored.withColumn("rk", F.row_number().over(w)).where("rk <= 15").select("ent_id", "decade", "term", "score", "n")
        save(ctx_top, "entity_context_decade")

        # ---- concordance examples: <=6 per entity per decade, deterministic
        cn = clean.select("doc_id", "case_name")
        ex = (mt.join(cn, "doc_id").withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int")))
        we = Window.partitionBy("ent_id", "decade").orderBy(F.hash("doc_id", "ent_id"))
        ex = ex.withColumn("rk", F.row_number().over(we)).where("rk <= 6").select(
            "ent_id", "year", "jurisdiction", "doc_id", "case_name", "ctx")
        save(ex, "examples")
    print(f"marts exported to {exp}")
