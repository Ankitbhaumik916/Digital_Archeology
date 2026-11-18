"""Print row counts per stage, top entities with merged variants, topic keywords and GPU throughput.

    PYTHONPATH=src python scripts/show_results.py [--run-id s010] [--topics 10] [--entities 15]
"""
import argparse
import json

import pandas as pd
import pyarrow.dataset as ds

ap = argparse.ArgumentParser()
ap.add_argument("--run-id")
ap.add_argument("--topics", type=int, default=10)
ap.add_argument("--entities", type=int, default=15)
a = ap.parse_args()
pd.set_option("display.width", 250, "display.max_colwidth", 140)


def rows(p):
    return ds.dataset(p, format="parquet", partitioning="hive").count_rows()


print("== row counts")
print(f"  raw (ingested)   {rows('data/raw_parquet'):>8,}")
print(f"  clean            {rows('data/clean'):>8,}")
print(f"  deduplicated     {rows('data/dedup_keep'):>8,}")
mp = pd.read_parquet("data/entities/map")
dim = pd.read_parquet("data/entities/dim")
print(f"  surface forms    {len(mp):>8,}")
print(f"  resolved entities{len(dim):>8,}")

print(f"\n== {a.entities} biggest entities (by docs) and their merged spellings")
top = dim.sort_values("total_docs", ascending=False).head(a.entities)
for r in top.itertuples():
    forms = mp[mp.ent_id == r.ent_id].sort_values("df", ascending=False)
    v = ", ".join(f"{n} ({d})" for n, d in zip(forms.norm, forms.df))
    print(f"  {r.total_docs:6,} docs  {r.canonical!s:45.45}  [{r.n_variants} forms] {v[:160]}")

print(f"\n== topic keywords (first {a.topics} of {len(pd.read_parquet('data/topics/keywords'))}, largest first)")
kw = pd.read_parquet("data/topics/keywords").sort_values("n_docs", ascending=False)
for r in kw.head(a.topics).itertuples():
    print(f"  t{r.topic_id:<3} {r.n_docs:5,} docs  {', '.join(r.keywords)}")

print("\n== embedding throughput (bench/metrics.jsonl)")
for line in open("bench/metrics.jsonl", encoding="utf-8"):
    m = json.loads(line)
    if "segments_per_s" in json.dumps(m) and (not a.run_id or m.get("run_id") == a.run_id):
        keep = {k: v for k, v in m.items() if k in ("run_id", "stage", "kind", "n_docs", "n_segments", "segments",
                                                      "wall_s", "gpu_s", "tokenise_s", "segments_per_s", "device",
                                                      "batch_size", "model")}
        print("  ", keep if keep.get("segments_per_s") else m)
