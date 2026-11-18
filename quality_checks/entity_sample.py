"""Task 1: random sample of 100 resolved entities from the 25% run, for hand-labelled precision.

  PYTHONPATH=src python quality_checks/entity_sample.py

Reads data/entities/dim (read only). Sorted by ent_id first so the seed gives the same rows on every run,
then sampled uniformly (not by frequency). Writes quality_checks/entity_sample.csv.
"""
import pathlib

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "quality_checks" / "entity_sample.csv"

dim = pd.read_parquet(ROOT / "data" / "entities" / "dim").sort_values("ent_id").reset_index(drop=True)
sample = dim.sample(n=100, random_state=42)

out = pd.DataFrame({
    "entity_id": sample.ent_id.values,
    "canonical_name": sample.canonical.values,
    "spelling_variants": [" | ".join(list(v)[:5]) for v in sample.variants],
    "n_documents": sample.total_docs.values,
    "correct": "",
    "note": "",
})
if OUT.exists():
    raise SystemExit(f"{OUT} already exists; not overwriting")
out.to_csv(OUT, index=False, encoding="utf-8")
print(f"population {len(dim):,} resolved entities; wrote {len(out)} rows -> {OUT}")
print(f"sampled entities with more than 5 variants (truncated to 5): {(sample.n_variants > 5).sum()}")
print(f"sampled n_documents: min {out.n_documents.min()}, median {out.n_documents.median()}, max {out.n_documents.max()}")
