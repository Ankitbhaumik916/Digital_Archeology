# Digital Archaeology at Scale

A hybrid **Hadoop/HDFS → Spark ETL → GPU batch embeddings → Spark MLlib** pipeline that tracks how *entities*
(agencies, statutes, companies) and *topics* evolve across decades of a large, messy text archive
(target corpus: US court opinions from CourtListener bulk data). Every stage is instrumented so the
bottleneck split (storage I/O vs Spark shuffle vs compute vs GPU inference) is a first-class output.

```
 raw CSV/bz2 ─▶ HDFS ─▶ Spark ETL ────────────────────────▶ GPU (T4/A100) ─▶ Spark MLlib ─▶ marts ─▶ dashboard
 (opinions,    parquet   clean · dedup · entity extraction     batch MiniLM      cosine KMeans   Spark     JS + live Spark
  clusters,    by year/  + resolution · build work queue       embeddings        + c-TF-IDF      aggregates + live Spark
  dockets)     court     │                                     (exchange dir)    topics                    query
                         └──────── every stage writes timings + Spark task metrics to bench/metrics.jsonl ───────┘
```

## Layout

| path | what |
|---|---|
| `src/da/ingest.py` | CourtListener CSVs → parquet partitioned by `year/jurisdiction`; deterministic hash sampling for scale runs |
| `src/da/clean.py` | Unicode/HTML/hyphenation/page-marker cleanup, OCR-noise score, quality filter, exact-dup hash |
| `src/da/dedup.py` | Exact + near-duplicate removal: MinHash-LSH → verified pairs → connected components (driver or Spark) |
| `src/da/entities.py` | Entity extraction and **resolution** (typos, abbreviations, acronyms via `Long Name (ACR)` definitions) |
| `src/da/embed_input.py`, `embed.py` | Work queue for the GPU; resumable, length-sorted, fp16, overlapped-tokenisation batch encoder |
| `src/da/cluster.py` | MLlib cosine KMeans (k by silhouette sweep), c-TF-IDF topic labels |
| `src/da/marts.py` | Spark aggregates for the dashboard: presence, topic mix, framing terms, concordance examples |
| `src/da/bench_report.py` | "Where does the time go" report + figures from `bench/metrics.jsonl` |
| `src/da/evaluate.py`, `synth.py` | Synthetic CourtListener-format corpus with ground truth, and accuracy evaluation |
| `dashboard/server.py`, `dashboard/web/` | JavaScript dashboard (entity view, topic view, pipeline view, live Spark query); `server.py` serves the marts as JSON. `dashboard/app.py` is the older Streamlit version |
| `docker-compose.yml`, `docker/` | 1 NameNode + 2 DataNodes + Spark master + 2 workers + driver container |
| `scripts/` | `shard_csv.py` (parallelise ingest), `load_raw_to_hdfs.sh`, `run_scaling.sh` |
| `notebooks/colab_embed.ipynb` | Run the GPU stage on a free Colab/Kaggle T4 |

## 1. Try it in 5 minutes (laptop, synthetic data)

```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt   # needs Java 17 or 21
export PYTHONPATH=src
python -m da.synth --docs 3000 --out data/raw                     # CourtListener-format files + ground truth
python -m da.pipeline run --profile test                          # all stages (test profile uses the CPU 'hash' embedder)
python -m da.evaluate all --profile test                          # accuracy vs ground truth
python -m da.bench_report                                         # bench/report.md, breakdown.png
python dashboard/server.py                                        # http://localhost:8600
```

`--profile test` swaps the GPU transformer for a CPU hashing embedder **for smoke tests only**; real runs use the default
`embed.backend: st`.

## 2. Real data: CourtListener bulk files

Download from <https://www.courtlistener.com/help/api/bulk-data/> the four tables `opinions`, `opinion-clusters`,
`dockets`, `courts` (`*.csv.bz2`) into `data/raw/`. Check the headers first (`bzcat file | head -c 2000`), the ingest stage
selects columns by name and tolerates missing ones but the names must match (see the docstring in `ingest.py`).

**Do this once before ingesting — it is the biggest ingest speed-up.** A single multi-line-CSV `.bz2` cannot be split by Spark,
so one core would decompress and parse the whole file. Shard it (parallel decompression, drops redundant text columns):

```bash
pbzip2 -dc data/raw/opinions-*.csv.bz2 | python scripts/shard_csv.py --kind opinions --out data/raw_sharded --n 64
pbzip2 -dc data/raw/opinion-clusters-*.csv.bz2 | python scripts/shard_csv.py --kind clusters --out data/raw_sharded --n 16
pbzip2 -dc data/raw/dockets-*.csv.bz2         | python scripts/shard_csv.py --kind dockets  --out data/raw_sharded --n 16
cp data/raw/courts-*.csv data/raw_sharded/
```

Scale knobs (`config.yaml`): `ingest.min_year/max_year`, `ingest.sample_fraction` (deterministic and *nested*: the 10 % sample
is a subset of the 25 % sample, so scaling curves are comparable). Start with 5–10 % to validate on your hardware.

## 3. Run on the Docker "cluster"

```bash
docker compose up -d --build                                     # HDFS (2 DataNodes) + Spark master + 2 workers + driver
scripts/load_raw_to_hdfs.sh                                      # copies ./data/raw -> hdfs:///da/raw (put the sharded files in ./data/raw)
docker compose exec pipeline python3 -m da.pipeline run --stages ingest,clean,dedup,entities,embed_input
#   -> writes data/exchange/embed_input   (mounted in every container)
#   GPU step (section 4) -> data/exchange/embeddings
docker compose exec pipeline python3 -m da.pipeline run --stages cluster,marts
docker compose exec pipeline python3 dashboard/server.py --host 0.0.0.0
```

UIs: HDFS <http://localhost:9870>, Spark master <http://localhost:8080>, running job <http://localhost:4040>.
Size workers with `WORKER_MEM` / `WORKER_CORES` in `.env` (defaults 4g / 2) and match `spark.executor.*` in the `docker` profile of `config.yaml`.

> **Not verified in the environment this was written in**: no Docker daemon was available, so `docker-compose.yml` and
> `docker/` are unexecuted. Expect to fix small things (image tag in `docker/Dockerfile.spark`, memory sizes). The Spark code itself
> was run end-to-end in local mode.

## 4. GPU stage

The GPU host only needs `data/exchange/embed_input` (~5 KB per opinion) and `requirements-gpu.txt`:

```bash
pip install -r requirements-gpu.txt
python -m da.embed --input data/exchange/embed_input --output data/exchange/embeddings      # PYTHONPATH=src
```

or open `notebooks/colab_embed.ipynb` on a Colab T4. The job is resumable (finished shards are skipped), writes its own timing
(`tokenise_s`, CUDA-synchronised `gpu_s`, `segments_per_s`) and the `cluster` stage imports it into `bench/metrics.jsonl`.
Model: `sentence-transformers/all-MiniLM-L6-v2` (384-d), 4 segments × 1,400 chars sampled evenly across each opinion, mean-pooled.
Swap the model in `config.yaml` (`embed.model`, `embed.pooling: cls` for BGE-style models).

## 5. Benchmarks — the research output

```bash
scripts/run_scaling.sh docker 0.05 0.1 0.25 0.5 1.0     # full pipeline at nested data scales
```

`bench/report.md` lists, per scale and stage: wall-clock, Spark task-time split (**compute / shuffle-wait / shuffle-write / GC**),
bytes read / written / shuffled, scan-only I/O probes, and the GPU stage's tokenise-vs-forward split; `breakdown.png` and `scaling.png`
show how the bottleneck moves with scale. How to read it:

* `shuffle_wait_s` is time tasks were *blocked fetching shuffle blocks*. In `local[*]` it is ≈ 0 (no network); it becomes meaningful on the docker/real cluster. Shuffle *volume* is reported either way.
* Storage I/O cannot be isolated inside a Spark task, so it is bounded from below by **scan-only probes** (`io_probe:*`, Spark's `noop` sink reading the dataset) and from the input/output byte counters.
* The GPU stage is measured on the GPU host; `wall_s` there includes tokenisation that overlaps with the forward pass, so `gpu_s + tokenise_s` can exceed the wall time.
* Only the latest record per (scale, stage) is reported, so stages run on different hosts merge into one report. Use `DA_RUN_ID` to keep runs distinguishable in `metrics.jsonl`.

## 6. Accuracy checks

`python -m da.evaluate all` compares outputs with the ground truth written by `da.synth` (dup groups, entity surface forms,
latent topics, true topic prevalence per year). See “Measured results”. These validate the *algorithms and plumbing*; they are
not a substitute for spot-checking real CourtListener output (sample 100 resolved entities and 100 near-duplicate pairs by hand
and report precision — that is the number reviewers will ask for).

## 6b. Measured results (synthetic corpus, 2-core / 7 GB sandbox, `local[*]`, CPU hash embedder)

**Accuracy** — 20,000 generated opinions (23,058 records incl. exact/near duplicates, 15 % OCR-noised, 2 % junk, 2 % too short; 94 entities with typos/abbreviations/acronyms and 6 deliberately confusable pairs):

| check | result |
|---|---|
| Duplicate removal (pairwise) | precision **1.000**, recall **0.900** (exact dups 1.000, near-dups 0.828; the misses are copies whose true Jaccard is below the 0.6 verification threshold) |
| Entity resolution (pairwise, truth-known forms) | precision **1.000**, recall **1.000**, 0 of 94 entities split, 0 false merges among the confusable pairs |
| Topic clustering vs latent topic | NMI 0.950, ARI 0.908, purity 0.924 (hash embedder; the real transformer should do better on real text) |
| Recovered vs true topic prevalence by year | r = 0.98–0.99 for every topic that actually changes over time (the one flat topic is undefined) |
| Entity first-appearance vs true start year | median error 0 years (mean abs 0.4) |

Two real bugs were found and fixed by these checks: LSH banding tuned for J ≥ 0.8 missed 95 % of OCR re-scans; and typos in the corporate suffix
(“Compny”) plus probabilistic blocking split entities (recall 0.95 → 1.00 after suffix-typo stripping and single-edit deletion-key blocking).
**These numbers validate the algorithms, not real-data performance** — hand-label a sample of real CourtListener output.

**Scaling** (wall-clock seconds, whole pipeline except the GPU host being this same CPU): 0.1 → 91 s, 0.25 → 107 s, 0.5 → 132 s, 1.0 → 175 s for 2.3 k → 22 k opinions.
At this size Spark start-up/`ingest` overhead (~30 s) dominates and per-stage times grow sub-linearly; `clean` (Python UDF) and `dedup_near` (MinHash) are the
stages that scale with data. Shuffle wait is 0 in local mode by construction. **Do not quote these as the paper's results** — they show the harness works;
the numbers that matter come from `scripts/run_scaling.sh docker …` on the real corpus and a T4 for the GPU stage.

## 7. Design decisions worth defending (and their limits)

* **Near-dups**: 125-perm MinHash on 5-word shingles, 25 bands × 5 rows (LSH threshold ≈ 0.53), candidates verified on the full signature at J ≥ 0.6; the best-quality member (lowest OCR-noise, then longest) is kept. OCR re-scans of one opinion measured J ≈ 0.4–0.8 in the synthetic set, which is why the naive 0.8 threshold missed 95 % of them. Giant LSH buckets (boilerplate) fall back to a sliding-window comparison.
* **Entity resolution** is deliberately conservative: token-aligned matching with ≤ 1 typo per long token, so `Department of Health` ≠ `Department of Housing`. Acronyms link only through evidence (`Long Name (ACR)` definitions in ≥ 2 opinions, or initials). Each surface form attaches to its *most frequent* verified neighbour rather than taking a transitive closure, so one bad edge cannot merge two big entities. Limits: word-order variants (`Labor Department`), and entities with `df < min_df` are not resolved.
* **Extraction** is rule-based (institutional head words + acronyms + case-caption parties). It is fast and distributable, but recall on person names and unusual organisations is lower than an NER model. If time allows, add a GPU NER pass over the same work queue.
* **Topics**: cosine KMeans on document embeddings; `k=auto` picks k by silhouette on a sample, which favours coarse k on real data — for the paper report k ∈ {20, 40, 80} explicitly.
* **Framing** = words that are unusually frequent near an entity in a decade relative to that decade's background (KL-style lift). It shows *what the entity is discussed alongside*, not sentiment.
* **Dashboard "real-time"**: charts are served from marts pre-aggregated by Spark over the *entire* corpus; slicing (courts, years, comparison entities) is instant. The *Live Spark query* panel really re-runs a distributed query on the lake. Word it that way in the pitch rather than claiming every click launches a Spark job.
* **Scale limits**: connected components run on the driver up to `dedup.cc_spark_threshold` edges (20 M), then in Spark; entity vocabulary is capped by `entities.max_vocab`; the entity attach step runs on the driver over the (small) vocabulary.

## 8. Tests

```bash
PYTHONPATH=src python -m pytest -q tests
```
