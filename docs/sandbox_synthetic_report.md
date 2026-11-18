# Pipeline benchmark report


## Scale 0.1  (2,331 opinions after cleaning)

| stage | category | wall s | % | task s | compute s | shuffle-wait s | shuffle-write s | GC s | in GB | out GB | shuffle GB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ingest | Storage ingest (HDFS write) | 26.8 | 30% | 26.6 | 26.6 | 0.0 | 0.0 | 0.4 | 0.018 | 0.003 | 0.004 |
| embeddings_import | Storage ingest (HDFS write) | 0.7 | 1% | 0.4 | 0.4 | 0.0 | 0.0 | 0.0 | 0.002 | 0.001 | 0.000 |
| marts | Spark analytics (MLlib + marts) | 5.9 | 6% | 4.3 | 4.2 | 0.0 | 0.1 | 0.1 | 0.013 | 0.000 | 0.002 |
| clean | Spark ETL | 13.2 | 15% | 22.7 | 22.6 | 0.0 | 0.1 | 0.3 | 0.004 | 0.002 | 0.004 |
| dedup_exact | Spark ETL | 0.9 | 1% | 0.6 | 0.6 | 0.0 | 0.0 | 0.1 | 0.003 | 0.000 | 0.000 |
| dedup_near | Spark ETL | 6.3 | 7% | 7.0 | 7.0 | 0.0 | 0.0 | 0.1 | 0.006 | 0.001 | 0.001 |
| embed_input | Spark ETL | 1.7 | 2% | 1.8 | 1.7 | 0.0 | 0.0 | 0.2 | 0.005 | 0.003 | 0.003 |
| entities_extract | Spark ETL | 2.8 | 3% | 4.5 | 4.5 | 0.0 | 0.0 | 0.2 | 0.003 | 0.000 | 0.000 |
| entities_vocab | Spark ETL | 0.7 | 1% | 0.5 | 0.5 | 0.0 | 0.0 | 0.0 | 0.001 | 0.000 | 0.000 |
| entities_resolve | Spark ETL | 2.4 | 3% | 2.5 | 2.5 | 0.0 | 0.0 | 0.1 | 0.001 | 0.000 | 0.000 |
| entities_finalize | Spark ETL | 4.3 | 5% | 3.4 | 3.4 | 0.0 | 0.0 | 0.3 | 0.001 | 0.000 | 0.001 |
| embed_gpu | GPU inference | 1.3 | 1% |  |  |  |  |  |  |  |  |
| cluster_fit | Spark analytics (MLlib + marts) | 16.9 | 19% | 13.0 | 12.9 | 0.0 | 0.1 | 0.4 | 0.270 | 0.000 | 0.004 |
| topic_keywords | Spark analytics (MLlib + marts) | 6.9 | 8% | 8.1 | 8.1 | 0.0 | 0.0 | 0.4 | 0.010 | 0.000 | 0.001 |

**Wall-clock by category:** Spark ETL 36%, Spark analytics (MLlib + marts) 33%, Storage ingest (HDFS write) 30%, GPU inference 1%

**Spark task time:** shuffle-wait 0.0%, shuffle-write 0.4%, compute 99.6% of 95 task-seconds

**Scan-only reads (storage I/O + decode lower bound):** raw_parquet 1.2s, clean 0.2s

**GPU stage** (none(hash), hash, batch 64.0): 6,854.0 segments, 5468 seg/s; tokenise 0.0s (overlapped) / forward 1.1s / wall 1.3s

## Scale 0.25  (5,583 opinions after cleaning)

| stage | category | wall s | % | task s | compute s | shuffle-wait s | shuffle-write s | GC s | in GB | out GB | shuffle GB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ingest | Storage ingest (HDFS write) | 28.7 | 27% | 29.2 | 29.1 | 0.0 | 0.0 | 0.7 | 0.018 | 0.006 | 0.010 |
| embeddings_import | Storage ingest (HDFS write) | 0.7 | 1% | 0.7 | 0.7 | 0.0 | 0.0 | 0.0 | 0.004 | 0.003 | 0.000 |
| marts | Spark analytics (MLlib + marts) | 6.7 | 6% | 5.2 | 5.2 | 0.0 | 0.1 | 0.1 | 0.019 | 0.000 | 0.003 |
| clean | Spark ETL | 19.1 | 18% | 34.6 | 34.5 | 0.0 | 0.1 | 0.2 | 0.007 | 0.005 | 0.009 |
| dedup_exact | Spark ETL | 1.0 | 1% | 0.7 | 0.7 | 0.0 | 0.0 | 0.0 | 0.005 | 0.000 | 0.000 |
| dedup_near | Spark ETL | 9.7 | 9% | 13.5 | 13.5 | 0.0 | 0.0 | 0.4 | 0.012 | 0.003 | 0.003 |
| embed_input | Spark ETL | 2.1 | 2% | 2.2 | 2.2 | 0.0 | 0.0 | 0.0 | 0.009 | 0.007 | 0.007 |
| entities_extract | Spark ETL | 4.3 | 4% | 7.6 | 7.6 | 0.0 | 0.0 | 0.0 | 0.006 | 0.001 | 0.000 |
| entities_vocab | Spark ETL | 0.9 | 1% | 0.8 | 0.7 | 0.0 | 0.0 | 0.0 | 0.001 | 0.000 | 0.000 |
| entities_resolve | Spark ETL | 2.7 | 3% | 2.5 | 2.5 | 0.0 | 0.0 | 0.0 | 0.001 | 0.000 | 0.000 |
| entities_finalize | Spark ETL | 3.9 | 4% | 3.0 | 3.0 | 0.0 | 0.0 | 0.0 | 0.002 | 0.001 | 0.001 |
| embed_gpu | GPU inference | 2.9 | 3% |  |  |  |  |  |  |  |  |
| cluster_fit | Spark analytics (MLlib + marts) | 17.5 | 16% | 15.8 | 15.7 | 0.0 | 0.1 | 0.9 | 0.541 | 0.000 | 0.003 |
| topic_keywords | Spark analytics (MLlib + marts) | 6.7 | 6% | 7.3 | 7.3 | 0.0 | 0.0 | 0.5 | 0.018 | 0.000 | 0.001 |

**Wall-clock by category:** Spark ETL 41%, Spark analytics (MLlib + marts) 29%, Storage ingest (HDFS write) 27%, GPU inference 3%

**Spark task time:** shuffle-wait 0.0%, shuffle-write 0.4%, compute 99.6% of 123 task-seconds

**Scan-only reads (storage I/O + decode lower bound):** raw_parquet 1.1s, clean 0.4s

**GPU stage** (none(hash), hash, batch 64.0): 16,172.0 segments, 5535 seg/s; tokenise 0.0s (overlapped) / forward 2.6s / wall 2.9s

## Scale 0.5  (11,157 opinions after cleaning)

| stage | category | wall s | % | task s | compute s | shuffle-wait s | shuffle-write s | GC s | in GB | out GB | shuffle GB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ingest | Storage ingest (HDFS write) | 31.4 | 24% | 31.5 | 31.5 | 0.0 | 0.0 | 0.5 | 0.018 | 0.011 | 0.020 |
| embeddings_import | Storage ingest (HDFS write) | 1.1 | 1% | 1.7 | 1.7 | 0.0 | 0.0 | 0.0 | 0.007 | 0.005 | 0.000 |
| marts | Spark analytics (MLlib + marts) | 7.3 | 6% | 7.0 | 6.9 | 0.0 | 0.1 | 0.5 | 0.025 | 0.000 | 0.006 |
| clean | Spark ETL | 30.1 | 23% | 56.8 | 56.7 | 0.0 | 0.2 | 0.5 | 0.012 | 0.010 | 0.019 |
| dedup_exact | Spark ETL | 0.9 | 1% | 0.7 | 0.7 | 0.0 | 0.0 | 0.0 | 0.006 | 0.000 | 0.000 |
| dedup_near | Spark ETL | 14.3 | 11% | 21.9 | 21.9 | 0.0 | 0.0 | 0.2 | 0.022 | 0.006 | 0.005 |
| embed_input | Spark ETL | 2.4 | 2% | 2.8 | 2.7 | 0.0 | 0.0 | 0.0 | 0.017 | 0.013 | 0.013 |
| entities_extract | Spark ETL | 6.2 | 5% | 11.2 | 11.2 | 0.0 | 0.0 | 0.3 | 0.010 | 0.001 | 0.000 |
| entities_vocab | Spark ETL | 1.1 | 1% | 0.9 | 0.9 | 0.0 | 0.0 | 0.0 | 0.001 | 0.000 | 0.001 |
| entities_resolve | Spark ETL | 3.1 | 2% | 3.2 | 3.2 | 0.0 | 0.0 | 0.0 | 0.001 | 0.000 | 0.000 |
| entities_finalize | Spark ETL | 5.0 | 4% | 6.0 | 6.0 | 0.0 | 0.0 | 0.2 | 0.004 | 0.001 | 0.002 |
| embed_gpu | GPU inference | 5.5 | 4% |  |  |  |  |  |  |  |  |
| cluster_fit | Spark analytics (MLlib + marts) | 16.9 | 13% | 16.3 | 16.2 | 0.0 | 0.1 | 0.9 | 0.612 | 0.000 | 0.003 |
| topic_keywords | Spark analytics (MLlib + marts) | 6.4 | 5% | 7.2 | 7.2 | 0.0 | 0.0 | 0.3 | 0.032 | 0.000 | 0.001 |

**Wall-clock by category:** Spark ETL 48%, Storage ingest (HDFS write) 25%, Spark analytics (MLlib + marts) 23%, GPU inference 4%

**Spark task time:** shuffle-wait 0.0%, shuffle-write 0.3%, compute 99.7% of 167 task-seconds

**Scan-only reads (storage I/O + decode lower bound):** raw_parquet 1.7s, clean 0.3s

**GPU stage** (none(hash), hash, batch 64.0): 31,327.0 segments, 5650 seg/s; tokenise 0.0s (overlapped) / forward 5.0s / wall 5.5s

## Scale 1.0  (22,249 opinions after cleaning)

| stage | category | wall s | % | task s | compute s | shuffle-wait s | shuffle-write s | GC s | in GB | out GB | shuffle GB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ingest | Storage ingest (HDFS write) | 31.7 | 18% | 33.2 | 33.2 | 0.0 | 0.1 | 1.4 | 0.018 | 0.021 | 0.039 |
| embeddings_import | Storage ingest (HDFS write) | 0.9 | 1% | 1.4 | 1.4 | 0.0 | 0.0 | 0.0 | 0.013 | 0.009 | 0.000 |
| marts | Spark analytics (MLlib + marts) | 8.6 | 5% | 9.1 | 8.9 | 0.0 | 0.2 | 0.6 | 0.029 | 0.000 | 0.010 |
| clean | Spark ETL | 47.9 | 27% | 92.3 | 92.2 | 0.0 | 0.2 | 0.5 | 0.022 | 0.018 | 0.037 |
| dedup_exact | Spark ETL | 0.9 | 1% | 0.7 | 0.6 | 0.0 | 0.0 | 0.0 | 0.006 | 0.000 | 0.001 |
| dedup_near | Spark ETL | 21.6 | 12% | 36.1 | 36.1 | 0.0 | 0.0 | 0.6 | 0.032 | 0.012 | 0.010 |
| embed_input | Spark ETL | 3.5 | 2% | 4.5 | 4.4 | 0.0 | 0.1 | 0.3 | 0.032 | 0.025 | 0.024 |
| entities_extract | Spark ETL | 10.8 | 6% | 19.9 | 19.9 | 0.0 | 0.0 | 0.8 | 0.019 | 0.002 | 0.000 |
| entities_vocab | Spark ETL | 1.1 | 1% | 1.1 | 1.1 | 0.0 | 0.0 | 0.0 | 0.001 | 0.000 | 0.001 |
| entities_resolve | Spark ETL | 2.9 | 2% | 3.4 | 3.4 | 0.0 | 0.0 | 0.1 | 0.001 | 0.000 | 0.001 |
| entities_finalize | Spark ETL | 5.4 | 3% | 6.8 | 6.8 | 0.0 | 0.0 | 0.2 | 0.005 | 0.002 | 0.005 |
| embed_gpu | GPU inference | 10.7 | 6% |  |  |  |  |  |  |  |  |
| cluster_fit | Spark analytics (MLlib + marts) | 20.4 | 12% | 20.8 | 20.6 | 0.0 | 0.1 | 1.1 | 0.862 | 0.000 | 0.004 |
| topic_keywords | Spark analytics (MLlib + marts) | 8.0 | 5% | 10.6 | 10.6 | 0.0 | 0.0 | 0.7 | 0.058 | 0.000 | 0.001 |

**Wall-clock by category:** Spark ETL 54%, Spark analytics (MLlib + marts) 21%, Storage ingest (HDFS write) 19%, GPU inference 6%

**Spark task time:** shuffle-wait 0.0%, shuffle-write 0.3%, compute 99.7% of 240 task-seconds

**Scan-only reads (storage I/O + decode lower bound):** raw_parquet 1.5s, clean 0.7s

**GPU stage** (none(hash), hash, batch 64.0): 58,346.0 segments, 5430 seg/s; tokenise 0.0s (overlapped) / forward 9.8s / wall 10.7s
