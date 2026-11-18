"""Stage 6 - distributed clustering of opinion embeddings (Spark MLlib) + topic labelling.

  1. import GPU embeddings from the exchange dir into the data lake (parquet on HDFS)
  2. choose k by silhouette sweep on a sample (or use a fixed k), fit cosine KMeans on ALL opinions
  3. label topics with class-based TF-IDF (c-TF-IDF) over a per-topic text sample
Outputs <base>/embeddings, <base>/topics/{assign,keywords}
"""
from __future__ import annotations

import json
import pathlib

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import Normalizer, StopWordsRemover
from pyspark.ml.functions import array_to_vector
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from .common import P, ROOT, Metrics, local_path, to_uri

LEGAL_STOP = ("court plaintiff plaintiffs defendant defendants appellant appellee appellants appellees petitioner petitioners "
              "respondent respondents opinion case cases also would may held see id cir ct supp app rev stat section sec "
              "judgment judge district appeal affirmed reversed remanded motion order upon thereof therein whether shall "
              "must without within one two not per et al pursuant argues argued argue claim claims v vs states united "
              "matter question issue issues record trial finding conclusion review standard rule act").split()


def import_embeddings(cfg, spark, m):
    ex = to_uri(cfg["paths"]["exchange"]) + "/embeddings"
    stats_f = pathlib.Path(local_path(ex)) / "_embed_stats.json"
    if stats_f.exists():  # GPU benchmark measured on the GPU host (Colab/T4/A100) joins this run's metrics
        st = json.loads(stats_f.read_text())
        m.record("embed_gpu", "gpu", **{k: v for k, v in st.items() if k != "scale"})
    with m.stage("embeddings_import"):
        spark.read.parquet(ex).write.mode("overwrite").parquet(P(cfg, "embeddings"))


def run(cfg: dict, spark: SparkSession, m: Metrics) -> None:
    c = cfg["cluster"]
    import_embeddings(cfg, spark, m)
    emb = spark.read.parquet(P(cfg, "embeddings"))
    keep = spark.read.parquet(P(cfg, "dedup_keep"))
    feats = (emb.join(keep, "doc_id").select("doc_id", array_to_vector("vec").alias("raw"))
             .transform(lambda d: Normalizer(inputCol="raw", outputCol="features", p=2.0).transform(d)).select("doc_id", "features"))
    feats = feats.repartition(int(spark.conf.get("spark.sql.shuffle.partitions"))).cache()
    n = feats.count()
    print(f"cluster: {n:,} opinion embeddings")

    sweep = {}
    with m.stage("cluster_fit"):
        k = c["k"]
        if str(k).lower() == "auto":
            frac = min(1.0, c["silhouette_sample"] / max(n, 1))
            samp = feats.sample(False, frac, seed=c["seed"]).cache()
            ev = ClusteringEvaluator(featuresCol="features", distanceMeasure="cosine", metricName="silhouette")
            for kk in c["k_candidates"]:
                mdl = KMeans(k=int(kk), seed=c["seed"], maxIter=30, distanceMeasure="cosine", featuresCol="features").fit(samp)
                sweep[int(kk)] = float(ev.evaluate(mdl.transform(samp)))
            samp.unpersist()
            k = max(sweep, key=sweep.get)
            print(f"  silhouette sweep {sweep} -> k={k}")
        k = int(k)
        model = KMeans(k=k, seed=c["seed"], maxIter=c["max_iter"], distanceMeasure="cosine", featuresCol="features",
                       predictionCol="topic_id", initMode="k-means||").fit(feats)
        assign = model.transform(feats).select("doc_id", "topic_id")
        assign.write.mode("overwrite").parquet(P(cfg, "topics/assign"))
    feats.unpersist()
    (ROOT / "bench").mkdir(exist_ok=True)
    (ROOT / "bench" / "cluster_sweep.json").write_text(json.dumps({"k": k, "silhouette": sweep}))
    m.record("cluster_summary", "info", n_docs=n, k=k, sweep=sweep)

    # ------------------------------------------------------------ c-TF-IDF topic keywords
    with m.stage("topic_keywords"):
        assign = spark.read.parquet(P(cfg, "topics/assign"))
        counts = {r.topic_id: r["count"] for r in assign.groupBy("topic_id").count().collect()}
        fr = {t: min(1.0, c["topic_sample_per_topic"] / cnt) for t, cnt in counts.items()}
        text = spark.read.parquet(P(cfg, "clean")).select("doc_id", F.substring("text", 1, 20000).alias("text"))
        samp = text.join(assign, "doc_id").sampleBy("topic_id", fr, seed=c["seed"])
        tok = samp.select("topic_id", F.explode(F.split(F.lower(F.regexp_replace("text", r"[^A-Za-z\s]", " ")), r"\s+")).alias("term"))
        stop = set(StopWordsRemover.loadDefaultStopWords("english")) | set(LEGAL_STOP)
        tok = tok.where((F.length("term") >= 4) & (~F.col("term").isin(list(stop))))
        tf = tok.groupBy("topic_id", "term").count().where("count >= 3")
        tot_topic = tf.groupBy("topic_id").agg(F.sum("count").alias("topic_total"))
        avg_words = tf.agg(F.sum("count")).first()[0] / max(len(counts), 1)
        tot_term = tf.groupBy("term").agg(F.sum("count").alias("term_total"))
        sc = (tf.join(tot_topic, "topic_id").join(tot_term, "term")
              .withColumn("score", (F.col("count") / F.col("topic_total")) * F.log(1 + F.lit(avg_words) / F.col("term_total"))))
        w = Window.partitionBy("topic_id").orderBy(F.col("score").desc(), "term")
        kw = (sc.withColumn("rk", F.row_number().over(w)).where(F.col("rk") <= c["topic_keywords"])
              .groupBy("topic_id").agg(F.array_sort(F.collect_list(F.struct("rk", "term"))).alias("t"))
              .select("topic_id", F.expr("transform(t, x -> x.term)").alias("keywords")))
        sizes = assign.groupBy("topic_id").agg(F.count("*").alias("n_docs"))
        kw = kw.join(sizes, "topic_id").withColumn("label", F.concat_ws(" / ", F.slice("keywords", 1, 3)))
        kw.write.mode("overwrite").parquet(P(cfg, "topics/keywords"))
    for r in spark.read.parquet(P(cfg, "topics/keywords")).orderBy(F.col("n_docs").desc()).limit(10).collect():
        print(f"  topic {r.topic_id:>3} ({r.n_docs:>6,}): {', '.join(r.keywords[:8])}")
