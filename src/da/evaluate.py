"""Accuracy evaluation against synthetic ground truth (data/raw_truth).

  python -m da.evaluate dedup|entities|clusters|temporal|all  [--profile test]
"""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict

import numpy as np
import pandas as pd

from .common import ROOT, P, load_config, local_path

TRUTH = ROOT / "data" / "raw_truth"


def _read(cfg, name, **kw):
    return pd.read_parquet(local_path(P(cfg, name)), **kw)


def _pairs(sizes):
    return sum(n * (n - 1) // 2 for n in sizes)


def _pair_prf(a: pd.Series, b: pd.Series):
    """pairwise precision/recall/F1 between two labelings (aligned Series)."""
    ct = pd.crosstab(a.values, b.values).values
    tp = _pairs(ct.ravel())
    pp = _pairs(ct.sum(axis=1))
    tt = _pairs(ct.sum(axis=0))
    p = tp / pp if pp else 1.0
    r = tp / tt if tt else 1.0
    return p, r, (2 * p * r / (p + r) if p + r else 0.0)


def eval_dedup(cfg):
    truth = pd.read_csv(TRUTH / "docs.csv")
    clean = _read(cfg, "clean", columns=["doc_id"])
    dmap = _read(cfg, "dedup_map")
    t = truth[truth.opinion_id.isin(clean.doc_id)].copy()
    m = dict(zip(dmap.doc_id, dmap.canonical_id))
    t["pred"] = t.opinion_id.map(lambda x: m.get(x, x))
    # canonical ids of chained maps are already final (component representative)
    t["tgroup"] = t.group_id.where(t.group_id >= 0, t.opinion_id)
    p, r, f = _pair_prf(t.pred, t.tgroup)
    dup = t[t.kind.isin(["exact", "near"])]
    rec = {k: float((dup[dup.kind == k].opinion_id.map(lambda x: x in m)).mean()) for k in ("exact", "near")}
    print(f"[dedup] pairwise precision={p:.4f} recall={r:.4f} F1={f:.4f}")
    print(f"        removed-duplicate recall: exact={rec['exact']:.3f} near={rec['near']:.3f}")
    return dict(precision=p, recall=r, f1=f, **{f"{k}_recall": v for k, v in rec.items()})


def eval_entities(cfg):
    from .entities import norm_name
    tr = json.load(open(TRUTH / "entities.json"))
    form2canon = {}
    for canon, forms in tr["forms"].items():
        for f in forms:
            form2canon[norm_name(f)] = canon
        form2canon[norm_name(canon)] = canon
    mp = _read(cfg, "entities/map")           # norm, ent_id, df
    known = mp[mp.norm.isin(form2canon)].copy()
    known["truth"] = known.norm.map(form2canon)
    p, r, f = _pair_prf(known.ent_id, known.truth)
    # df-weighted variant coverage: share of truth-known mentions that map to the dominant cluster of their entity
    known["w"] = known["df"]
    dom = known.groupby(["truth", "ent_id"]).w.sum().reset_index()
    best = dom.groupby("truth").w.max().sum() / dom.w.sum()
    n_unknown = len(mp) - len(known)
    print(f"[entities] surface forms in vocab={len(mp)} (truth-known {len(known)}, other {n_unknown})")
    print(f"           pairwise precision={p:.4f} recall={r:.4f} F1={f:.4f}; mention-weighted purity={best:.4f}")
    # false merges / splits detail
    merged = known.groupby("ent_id").truth.nunique()
    bad = merged[merged > 1].index
    for e in bad[:5]:
        print("           FALSE MERGE:", sorted(set(known[known.ent_id == e].truth)))
    split = known.groupby("truth").ent_id.nunique()
    print("           truth entities split across >1 resolved id:", int((split > 1).sum()), "of", len(split))
    return dict(precision=p, recall=r, f1=f, weighted_purity=float(best))


def eval_clusters(cfg):
    from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score
    truth = pd.read_csv(TRUTH / "docs.csv")
    ta = _read(cfg, "topics/assign")
    df = ta.merge(truth[["opinion_id", "topic", "year"]], left_on="doc_id", right_on="opinion_id")
    df = df[df.topic >= 0]
    nmi = normalized_mutual_info_score(df.topic, df.topic_id)
    ari = adjusted_rand_score(df.topic, df.topic_id)
    pur = df.groupby("topic_id").topic.agg(lambda s: s.value_counts().iloc[0]).sum() / len(df)
    print(f"[clusters] docs={len(df)} k={df.topic_id.nunique()} NMI={nmi:.4f} ARI={ari:.4f} purity={pur:.4f}")
    return dict(nmi=nmi, ari=ari, purity=float(pur))


def eval_temporal(cfg):
    """Correlate recovered topic-share-by-year with the known generating prevalence (after best cluster->topic map)."""
    truth = pd.read_csv(TRUTH / "docs.csv")
    tw = json.load(open(TRUTH / "entities.json"))["topic_weights"]
    ta = _read(cfg, "topics/assign").merge(truth[["opinion_id", "topic", "year"]], left_on="doc_id", right_on="opinion_id")
    ta = ta[ta.topic >= 0]
    maj = ta.groupby("topic_id").topic.agg(lambda s: s.value_counts().index[0])
    ta["mapped"] = ta.topic_id.map(maj)
    yr = ta.groupby(["year", "mapped"]).size().unstack(fill_value=0)
    yr = yr.div(yr.sum(axis=1), axis=0)
    # smooth over 5 years to compare trends (per-year sample noise is large at small N)
    yr = yr.rolling(5, center=True, min_periods=1).mean()
    cors = {}
    for t in yr.columns:
        truth_curve = np.array([tw[str(y)][str(int(t))] for y in yr.index])
        if yr[t].std() > 0 and truth_curve.std() > 0:
            cors[int(t)] = float(np.corrcoef(yr[t].values, truth_curve)[0, 1])
    mean = float(np.mean(list(cors.values()))) if cors else float("nan")
    print(f"[temporal] per-topic correlation of recovered vs true prevalence (5y smoothed): "
          + ", ".join(f"T{t}={c:.2f}" for t, c in sorted(cors.items())) + f" | mean={mean:.3f}")
    # entity emergence: first year an entity is seen vs its real start year
    try:
        from .entities import norm_name
        tr = json.load(open(TRUTH / "entities.json"))
        from .common import export_dir
        ey = pd.read_parquet(export_dir(cfg) / "entity_year.parquet").groupby(["ent_id", "year"], as_index=False).n_docs.sum()
        dim = _read(cfg, "entities/dim")
        start = {e["canon"]: e["start"] for e in tr["entities"]}
        errs = []
        for _, r in dim.iterrows():
            for canon, s in start.items():
                if norm_name(canon) == r.root_norm:
                    sub = ey[ey.ent_id == r.ent_id]
                    sub = sub[sub.n_docs >= 3]
                    if len(sub):
                        errs.append(int(sub.year.min()) - max(s, 1950))
        if errs:
            print(f"           entity first-appearance error vs true start (years): median={np.median(errs):.0f} "
                  f"mean|err|={np.mean(np.abs(errs)):.1f} over {len(errs)} entities")
    except Exception as e:  # marts may not exist yet
        print("           (entity emergence check skipped:", e, ")")
    return dict(mean_topic_corr=mean)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["dedup", "entities", "clusters", "temporal", "all"])
    ap.add_argument("--profile")
    a = ap.parse_args()
    cfg = load_config(profile=a.profile)
    todo = ["dedup", "entities", "clusters", "temporal"] if a.what == "all" else [a.what]
    out = {}
    for w in todo:
        try:
            out[w] = globals()[f"eval_{w}"](cfg)
        except FileNotFoundError as e:
            print(f"[{w}] skipped (stage output missing: {e})")
    json.dump(out, open(ROOT / "bench" / "accuracy.json", "w"), indent=1, default=float)


if __name__ == "__main__":
    main()
