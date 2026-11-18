# Pipeline benchmark report


## Scale 0.1  (15,446 opinions after cleaning)

| stage | category | wall s | % | task s | compute s | shuffle-wait s | shuffle-write s | GC s | in GB | out GB | shuffle GB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ingest | Storage ingest (HDFS write) | 57.7 | 13% | 508.6 | 488.3 | 0.0 | 20.3 | 8.3 | 0.063 | 0.064 | 0.226 |
| embeddings_import | Storage ingest (HDFS write) | 4.7 | 1% | 3.7 | 3.7 | 0.0 | 0.0 | 0.0 | 0.024 | 0.022 | 0.000 |
| marts | Spark analytics (MLlib + marts) | 6.7 | 1% | 28.6 | 22.8 | 0.0 | 5.9 | 0.5 | 0.081 | 0.000 | 0.024 |
| clean | Spark ETL | 80.7 | 18% | 423.6 | 422.0 | 0.0 | 1.6 | 2.2 | 0.067 | 0.063 | 0.113 |
| dedup_exact | Spark ETL | 0.7 | 0% | 1.4 | 1.2 | 0.0 | 0.3 | 0.1 | 0.013 | 0.000 | 0.001 |
| dedup_near | Spark ETL | 34.0 | 7% | 140.4 | 139.9 | 0.0 | 0.4 | 1.2 | 0.083 | 0.010 | 0.007 |
| embed_input | Spark ETL | 15.8 | 3% | 60.6 | 60.1 | 0.0 | 0.5 | 1.1 | 0.091 | 0.050 | 0.046 |
| entities_extract | Spark ETL | 42.8 | 9% | 201.8 | 201.8 | 0.0 | 0.0 | 1.6 | 0.066 | 0.006 | 0.000 |
| entities_vocab | Spark ETL | 5.4 | 1% | 16.1 | 14.4 | 0.0 | 1.7 | 0.2 | 0.002 | 0.000 | 0.006 |
| entities_resolve | Spark ETL | 16.3 | 4% | 36.9 | 35.3 | 0.0 | 1.6 | 0.3 | 0.003 | 0.000 | 0.005 |
| entities_finalize | Spark ETL | 37.9 | 8% | 125.6 | 116.1 | 0.0 | 9.5 | 1.9 | 0.017 | 0.006 | 0.008 |
| embed_gpu | GPU inference | 79.2 | 17% |  |  |  |  |  |  |  |  |
| cluster_fit | Spark analytics (MLlib + marts) | 24.0 | 5% | 119.2 | 116.6 | 0.0 | 2.6 | 3.2 | 1.328 | 0.000 | 0.024 |
| topic_keywords | Spark analytics (MLlib + marts) | 50.7 | 11% | 321.9 | 319.3 | 0.0 | 2.6 | 3.9 | 0.198 | 0.000 | 0.093 |

**Wall-clock by category:** Spark ETL 51%, Spark analytics (MLlib + marts) 18%, GPU inference 17%, Storage ingest (HDFS write) 14%

**Spark task time:** shuffle-wait 0.0%, shuffle-write 2.4%, compute 97.6% of 1988 task-seconds

**Scan-only reads (storage I/O + decode lower bound):** raw_parquet 4.2s, clean 0.4s

**GPU stage** (NVIDIA GeForce RTX 4060 Laptop GPU, sentence-transformers/all-MiniLM-L6-v2, batch 256.0): 57,869.0 segments, 731 seg/s; tokenise 74.0s (overlapped) / forward 28.3s / wall 79.2s

## Scale 0.25  (38,389 opinions after cleaning)

| stage | category | wall s | % | task s | compute s | shuffle-wait s | shuffle-write s | GC s | in GB | out GB | shuffle GB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ingest | Storage ingest (HDFS write) | 15.2 | 5% | 47.1 | 44.7 | 0.0 | 2.4 | 1.1 | 0.157 | 0.156 | 0.562 |
| embeddings_import | Storage ingest (HDFS write) | 1.1 | 0% | 1.8 | 1.8 | 0.0 | 0.0 | 0.0 | 0.060 | 0.055 | 0.000 |
| marts | Spark analytics (MLlib + marts) | 11.5 | 3% | 62.0 | 49.6 | 0.0 | 12.4 | 1.2 | 0.145 | 0.000 | 0.058 |
| clean | Spark ETL | 69.0 | 21% | 532.1 | 531.0 | 0.0 | 1.1 | 0.9 | 0.159 | 0.155 | 0.280 |
| dedup_exact | Spark ETL | 0.5 | 0% | 1.2 | 0.9 | 0.0 | 0.3 | 0.0 | 0.015 | 0.000 | 0.002 |
| dedup_near | Spark ETL | 46.0 | 14% | 322.8 | 322.0 | 0.0 | 0.8 | 1.3 | 0.200 | 0.024 | 0.037 |
| embed_input | Spark ETL | 10.9 | 3% | 68.0 | 67.7 | 0.0 | 0.3 | 0.5 | 0.222 | 0.126 | 0.114 |
| entities_extract | Spark ETL | 22.1 | 7% | 161.3 | 161.3 | 0.0 | 0.0 | 0.8 | 0.159 | 0.014 | 0.000 |
| entities_vocab | Spark ETL | 1.0 | 0% | 4.5 | 4.2 | 0.0 | 0.4 | 0.0 | 0.004 | 0.000 | 0.014 |
| entities_resolve | Spark ETL | 13.0 | 4% | 80.9 | 80.6 | 0.0 | 0.4 | 0.1 | 0.004 | 0.000 | 0.012 |
| entities_finalize | Spark ETL | 13.8 | 4% | 66.2 | 64.6 | 0.0 | 1.7 | 0.3 | 0.039 | 0.016 | 0.022 |
| embed_gpu | GPU inference | 57.2 | 17% |  |  |  |  |  |  |  |  |
| cluster_fit | Spark analytics (MLlib + marts) | 6.7 | 2% | 37.8 | 36.6 | 0.0 | 1.2 | 1.2 | 4.275 | 0.000 | 0.036 |
| topic_keywords | Spark analytics (MLlib + marts) | 63.3 | 19% | 468.2 | 466.1 | 0.0 | 2.0 | 3.6 | 0.476 | 0.000 | 0.149 |

**Wall-clock by category:** Spark ETL 53%, Spark analytics (MLlib + marts) 25%, GPU inference 17%, Storage ingest (HDFS write) 5%

**Spark task time:** shuffle-wait 0.0%, shuffle-write 1.2%, compute 98.8% of 1854 task-seconds

**Scan-only reads (storage I/O + decode lower bound):** raw_parquet 0.9s, clean 0.6s

**GPU stage** (NVIDIA GeForce RTX 4060 Laptop GPU, sentence-transformers/all-MiniLM-L6-v2, batch 256.0): 143,720.0 segments, 2511 seg/s; tokenise 32.9s (overlapped) / forward 55.8s / wall 57.2s
