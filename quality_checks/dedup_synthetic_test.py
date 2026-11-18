"""Task 2: planted near-duplicate test for the MinHash dedup stage.

  . scripts/env.sh && python quality_checks/dedup_synthetic_test.py

1. Read the 25% run's cleaned opinions (data/clean, read only). Sample 500 with seed 42.
2. Make one near copy of each. Edit level by position in the sample (i % 3): light, medium, heavy -> 167/167/166.
     light  : ~5% of words substituted + 5 OCR character swaps
     medium : 1-2 sentences dropped, then ~15% of words substituted
     heavy  : ~30% of words substituted, first 25 words (header) and last 15 words (footer) replaced
   Copies pass through the same clean_text / text_stats / norm_for_hash as the clean stage.
3. Write original cleaned rows + copies to quality_checks/lake/clean and run the UNCHANGED da.dedup.run()
   with paths.base = quality_checks/lake and the dedup parameters from config.yaml. Stage timings go to
   quality_checks/dedup_test_metrics.jsonl, not bench/metrics.jsonl.
4. Score against the planted labels. Nothing under data/ or bench/ is written.
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import re
import shutil
import sys

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from da import dedup  # noqa: E402
from da.common import Metrics, get_spark, load_config  # noqa: E402
from da.textutil import clean_text, norm_for_hash, text_stats  # noqa: E402

QC = ROOT / "quality_checks"
LAKE = QC / "lake"
SEED = 42
N = 500
COPY_ID_BASE = 9_000_000_000_000
LEVELS = ["light", "medium", "heavy"]

# --------------------------------------------------------------------------------------------- 1. sample
clean = ds.dataset(ROOT / "data" / "clean", format="parquet", partitioning="hive").to_table().to_pandas()
clean = clean.sort_values("doc_id").reset_index(drop=True)      # fixed order so the seed is reproducible
sample = clean.sample(n=N, random_state=SEED).reset_index(drop=True)
assert clean.doc_id.max() < COPY_ID_BASE

# --------------------------------------------------------------------------------------------- 2. copies
rng = np.random.default_rng(SEED)
pool = sorted({w for t in sample.text for w in t.split() if w.isalpha() and len(w) >= 3})
OCR = [("rn", "m"), ("m", "rn"), ("l", "1"), ("i", "l"), ("e", "c"), ("o", "0"), ("h", "b"), ("c", "e")]
COUNTIES = ["Sangamon", "McLean", "Winnebago", "Peoria", "Champaign", "Macon", "Kankakee", "Vermilion"]
JUDGES = ["Harold J. Kessler", "Martha R. Quinlan", "Edward T. Brandt", "Lucille M. Ostrowski", "Roy D. Hafner"]
MONTHS = ["January", "March", "April", "June", "September", "October", "November"]


def substitute(text: str, frac: float):
    parts = re.findall(r"\S+|\s+", text)
    widx = [i for i, p in enumerate(parts) if not p.isspace()]
    k = int(round(frac * len(widx)))
    chosen = rng.choice(len(widx), size=k, replace=False)
    for c in chosen:
        i = widx[c]
        new = parts[i]
        while new == parts[i]:
            new = pool[rng.integers(len(pool))]
        parts[i] = new
    return "".join(parts), k, len(widx), {widx[c] for c in chosen}


def ocr_swaps(text: str, n: int, avoid: set):
    parts = re.findall(r"\S+|\s+", text)
    cand = [i for i, p in enumerate(parts) if not p.isspace() and i not in avoid and any(a in p for a, _ in OCR)]
    done = 0
    for i in rng.permutation(cand):
        if done == n:
            break
        opts = [(a, b) for a, b in OCR if a in parts[i]]
        a, b = opts[rng.integers(len(opts))]
        parts[i] = parts[i].replace(a, b, 1)
        done += 1
    return "".join(parts), done


def drop_sentences(text: str):
    sents = re.split(r"(?<=[.!?])\s+", text)
    if len(sents) < 4:
        return text, 0, 0
    k = int(rng.integers(1, 3))                                   # 1 or 2
    drop = set(rng.choice(np.arange(1, len(sents) - 1), size=min(k, len(sents) - 2), replace=False).tolist())
    words = sum(len(sents[i].split()) for i in drop)
    return " ".join(s for i, s in enumerate(sents) if i not in drop), len(drop), words


def header_footer(text: str):
    w = text.split()
    yr = int(rng.integers(1850, 2010))
    head = (f"Appeal from the Circuit Court of {COUNTIES[rng.integers(len(COUNTIES))]} County; the Hon. "
            f"{JUDGES[rng.integers(len(JUDGES))]}, Judge, presiding. Opinion filed {MONTHS[rng.integers(len(MONTHS))]} "
            f"{int(rng.integers(1, 29))}, {yr}.").split()
    foot = (f"Judgment reversed and cause remanded with directions. Rehearing denied "
            f"{MONTHS[rng.integers(len(MONTHS))]} {int(rng.integers(1, 29))}, {yr + 1}.").split()
    return " ".join(head + w[25:-15] + foot), 25 + 15


labels, copies = [], []
for i, r in sample.iterrows():
    level = LEVELS[i % 3]
    t = r.text
    n_orig = len(t.split())
    rec = dict(copy_id=COPY_ID_BASE + i, original_id=int(r.doc_id), edit_level=level, original_words=n_orig,
               words_substituted=0, sentences_dropped=0, words_dropped=0, ocr_swaps=0, header_footer_words_replaced=0)
    if level == "light":
        t, k, _, touched = substitute(t, 0.05)
        t, sw = ocr_swaps(t, 5, touched)
        rec.update(words_substituted=k, ocr_swaps=sw)
    elif level == "medium":
        t, nd, wd = drop_sentences(t)
        t, k, _, _ = substitute(t, 0.15)
        rec.update(words_substituted=k, sentences_dropped=nd, words_dropped=wd)
    else:
        t, k, _, _ = substitute(t, 0.30)
        t, hf = header_footer(t)
        rec.update(words_substituted=k, header_footer_words_replaced=hf)
    t = clean_text(t)
    n, a, z = text_stats(t)
    rec["change_fraction_upper_bound"] = (rec["words_substituted"] + rec["words_dropped"]
                                          + rec["header_footer_words_replaced"]) / n_orig
    rec["copy_passes_clean_filter"] = bool(n >= 150 and z <= 0.25 and a >= 0.6)
    labels.append(rec)
    row = r.to_dict()
    row.update(doc_id=rec["copy_id"], text=t, n_words=n, alpha_ratio=a, noise=z,
               text_hash=hashlib.md5(norm_for_hash(t).encode()).hexdigest())
    copies.append(row)

labels = pd.DataFrame(labels)
combined = pd.concat([clean, pd.DataFrame(copies)[clean.columns]], ignore_index=True)
assert combined.doc_id.is_unique and len(combined) == len(clean) + N

if LAKE.exists():
    raise SystemExit(f"{LAKE} already exists; remove it deliberately before re-running")
(LAKE / "clean").mkdir(parents=True)
combined.to_parquet(LAKE / "clean" / "part-0.parquet", index=False)
labels.to_csv(QC / "planted_copies_labels.csv", index=False)
print(f"combined clean: {len(clean):,} original + {N} copies = {len(combined):,} rows -> {LAKE / 'clean'}")

# --------------------------------------------------------------------------------------------- 3. run the stage
METRICS_FILE = QC / "dedup_test_metrics.jsonl"


class QCMetrics(Metrics):
    def write(self, rec):                       # keep bench/metrics.jsonl untouched
        with open(METRICS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")


cfg = load_config()
cfg["paths"]["base"] = str(LAKE)
d = cfg["dedup"]
print(f"dedup params: num_perm={d['num_perm']} bands={d['bands']} rows={d['num_perm'] // d['bands']} "
      f"jaccard={d['jaccard']} shingle_words={d['shingle_words']}")
spark = get_spark(cfg, "dedup-synthetic-test")
dedup.run(cfg, spark, QCMetrics(cfg, spark, scale="synthetic", run_id="dedup-synthetic-seed42"))
spark.stop()

# --------------------------------------------------------------------------------------------- 4. score
dmap = pd.read_parquet(LAKE / "dedup_map")
parent = dict(zip(dmap.doc_id.astype("int64"), dmap.canonical_id.astype("int64")))
reason = dict(zip(dmap.doc_id.astype("int64"), dmap.reason))


def root(x: int) -> int:                       # exact removals can point at a doc later removed as near dup
    seen = set()
    while x in parent and x not in seen:
        seen.add(x)
        x = parent[x]
    return x


removed = set(parent)
main_removed = set(pd.read_parquet(ROOT / "data" / "dedup_map").doc_id.astype("int64"))
orig_of = dict(zip(labels.copy_id, labels.original_id))
copy_of = {o: c for c, o in orig_of.items()}

labels["grouped_with_original"] = [root(c) == root(o) for c, o in zip(labels.copy_id, labels.original_id)]
labels["copy_removed"] = labels.copy_id.isin(removed)
labels["original_removed"] = labels.original_id.isin(removed)

sigs = pd.read_parquet(LAKE / "dedup_sigs", columns=["doc_id", "sig"]).set_index("doc_id").sig
def est_j(a, b):
    if a not in sigs.index or b not in sigs.index:
        return np.nan
    return float(np.mean(np.asarray(sigs[a]) == np.asarray(sigs[b])))
labels["minhash_jaccard_estimate"] = [est_j(c, o) for c, o in zip(labels.copy_id, labels.original_id)]

cat = {}
for x in removed:
    if x in orig_of:
        cat[x] = "A_copy_removed_grouped_with_its_original" if root(x) == root(orig_of[x]) else "B_copy_removed_not_with_its_original"
    elif x in copy_of and root(x) == root(copy_of[x]):
        cat[x] = "C_original_removed_grouped_with_its_own_copy"
    elif x in main_removed:
        cat[x] = "D_original_removed_also_removed_in_main_run"
    else:
        cat[x] = "E_original_removed_newly_not_with_its_own_copy"
cats = pd.Series(cat, dtype="object").value_counts().reindex(
    ["A_copy_removed_grouped_with_its_original", "B_copy_removed_not_with_its_original",
     "C_original_removed_grouped_with_its_own_copy", "D_original_removed_also_removed_in_main_run",
     "E_original_removed_newly_not_with_its_own_copy"], fill_value=0)

rows = []
def add(metric, level, num, den):
    rows.append(dict(metric=metric, edit_level=level, numerator=int(num), denominator=int(den),
                     value=(num / den) if den else float("nan")))

for lv in LEVELS:
    g = labels[labels.edit_level == lv]
    add("recall_copy_grouped_with_original", lv, g.grouped_with_original.sum(), len(g))
add("recall_copy_grouped_with_original", "all", labels.grouped_with_original.sum(), len(labels))
n_removed = len(removed)
add("precision_removed_doc_is_planted_copy_of_its_group", "all", cats["A_copy_removed_grouped_with_its_original"], n_removed)
add("precision_excluding_main_run_duplicates", "all", cats["A_copy_removed_grouped_with_its_original"],
    n_removed - cats["D_original_removed_also_removed_in_main_run"])
add("originals_wrongly_removed_E", "all", cats["E_original_removed_newly_not_with_its_own_copy"], len(clean))
for k, v in cats.items():
    add(f"removed_category_{k}", "all", v, n_removed)
add("main_run_removals_also_removed_here", "all", len(main_removed & removed), len(main_removed))

res = pd.DataFrame(rows)
res.to_csv(QC / "dedup_synthetic_results.csv", index=False)
labels.to_csv(QC / "planted_copies_outcomes.csv", index=False)

pd.set_option("display.width", 200, "display.max_colwidth", 60)
print("\n== results (exact counts)")
print(res.to_string(index=False))
print("\n== per edit level: measured edits and MinHash Jaccard estimate (copy vs original)")
print(labels.groupby("edit_level").agg(
    n=("copy_id", "size"),
    change_frac_mean=("change_fraction_upper_bound", "mean"),
    change_frac_min=("change_fraction_upper_bound", "min"),
    change_frac_max=("change_fraction_upper_bound", "max"),
    jaccard_median=("minhash_jaccard_estimate", "median"),
    jaccard_max=("minhash_jaccard_estimate", "max"),
    copies_failing_clean_filter=("copy_passes_clean_filter", lambda s: int((~s).sum())),
).reindex(LEVELS).to_string())
print(f"\nremoved total {n_removed}; by reason: {pd.Series(reason).value_counts().to_dict()}")
