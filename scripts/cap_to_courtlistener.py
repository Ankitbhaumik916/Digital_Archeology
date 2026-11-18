#!/usr/bin/env python3
"""Convert Caselaw Access Project (CAP) data -> CourtListener-style CSV shards that the pipeline's `ingest` stage reads.

Handles (auto-detected, recursive):
  *.jsonl / *.jsonl.gz / *.jsonl.xz    CAP bulk export, one case per line (data/data.jsonl.xz)
  *.json                               one case per file (static.case.law/<reporter>/<vol>/cases/*.json), a list, or {"results": [...]}
  *.parquet / *.csv(.gz)               HuggingFace / Kaggle style tables (columns are matched by common names)

  python cap_to_courtlistener.py --in /path/to/illinois --out data/raw --shards 16
  python cap_to_courtlistener.py --in /path/to/illinois --out data/raw --limit 50000     # quick subset

Writes into --out:  opinions-shardNNNN.csv.gz, opinion-clusters-shardNNNN.csv.gz, dockets-shardNNNN.csv.gz, courts-cap.csv
CAP text is OCR output, so extracted_by_ocr is set to 't' for every opinion.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import html
import json
import lzma
import pathlib
import re
import sys

csv.field_size_limit(2**31 - 1)  # sys.maxsize overflows a C long on Windows
TAG = re.compile(r"<[^>]+>")
WS = re.compile(r"[ \t\f\v]+")


# ------------------------------------------------------------------ reading
def _open(p: pathlib.Path):
    s = p.name.lower()
    if s.endswith(".xz"):
        return lzma.open(p, "rt", encoding="utf-8", errors="replace")
    if s.endswith(".gz"):
        return gzip.open(p, "rt", encoding="utf-8", errors="replace", newline="")
    return open(p, "rt", encoding="utf-8", errors="replace", newline="")


def iter_records(root: pathlib.Path):
    files = [root] if root.is_file() else sorted(q for q in root.rglob("*") if q.is_file())
    for p in files:
        n = p.name.lower()
        try:
            if re.search(r"\.jsonl(\.gz|\.xz)?$", n):
                with _open(p) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            yield json.loads(line)
            elif n.endswith(".json") and not n.startswith(("casesmetadata", "volumesmetadata", "reportersmetadata")):
                with _open(p) as f:
                    d = json.load(f)
                if isinstance(d, list):
                    yield from d
                elif isinstance(d, dict) and isinstance(d.get("results"), list):
                    yield from d["results"]
                elif isinstance(d, dict):
                    yield d
            elif n.endswith(".parquet"):
                import pyarrow.parquet as pq
                pf = pq.ParquetFile(p)
                for b in pf.iter_batches(batch_size=2000):
                    yield from b.to_pylist()
            elif re.search(r"\.csv(\.gz)?$", n):
                with _open(p) as f:
                    yield from csv.DictReader(f)
        except Exception as e:  # keep going: one bad file must not kill a multi-hour conversion
            print(f"  skip {p}: {type(e).__name__}: {e}", file=sys.stderr)


# ------------------------------------------------------------------ extraction
def _first(d: dict, *keys):
    for k in keys:
        v = d.get(k)
        if v not in (None, "", [], {}):
            return v
    return None


def _strip(s: str) -> str:
    if "<" in s:
        s = TAG.sub(" ", s)
    return WS.sub(" ", html.unescape(s)).strip()


def extract_text(rec: dict) -> str:
    cb = rec.get("casebody")
    if isinstance(cb, str):
        try:
            cb = json.loads(cb) if cb.lstrip().startswith("{") else {"data": cb}
        except Exception:
            cb = {"data": cb}
    if isinstance(cb, dict) and isinstance(cb.get("data"), dict):
        cb = cb["data"]  # CAP bulk export: casebody = {"status": ..., "data": {"opinions": [...], "head_matter": ...}}
    if isinstance(cb, dict):
        ops = cb.get("opinions")
        if isinstance(ops, list) and ops:
            parts = [(o.get("text") if isinstance(o, dict) else str(o)) or "" for o in ops]
            t = "\n\n".join(p for p in parts if p.strip())
            if t.strip():
                return t
        for k in ("data", "text", "html", "xml"):
            if isinstance(cb.get(k), str) and cb[k].strip():
                return _strip(cb[k])
        hm = cb.get("head_matter")
        if isinstance(hm, str) and hm.strip():
            return hm
    t = _first(rec, "text", "plain_text", "opinion_text", "opinion", "body", "full_text", "content")
    return _strip(t) if isinstance(t, str) else ""


def extract_meta(rec: dict):
    name = _first(rec, "name", "case_name", "name_abbreviation", "title") or ""
    date = str(_first(rec, "decision_date", "date_filed", "date", "decision_date_original", "year") or "")
    m = re.match(r"(\d{4})(?:-(\d{1,2}))?(?:-(\d{1,2}))?", date)
    if m:
        y, mo, d = m.group(1), int(m.group(2) or 1), int(m.group(3) or 1)
        date = f"{y}-{max(1, min(mo, 12)):02d}-{max(1, min(d, 28)):02d}"
    else:
        date = ""
    court = rec.get("court")
    cname = (court.get("name") or court.get("name_abbreviation")) if isinstance(court, dict) else (court or rec.get("court_name") or "")
    return str(name), date, str(cname or "Unknown court"), _first(rec, "id", "case_id", "cap_id")


def jurisdiction_of(court_name: str) -> str:
    c = court_name.lower()
    if "supreme" in c:
        return "S"
    if "appellate" in c or "appeals" in c:
        return "SA"
    if "circuit" in c or "county" in c or "superior" in c or "trial" in c:
        return "ST"
    if "court of claims" in c or "commission" in c:
        return "SS"
    return "U"


# ------------------------------------------------------------------ writing
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--shards", type=int, default=16)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--min-chars", type=int, default=200)
    ap.add_argument("--sample-fraction", type=float,
                    help="keep a case only if (id %% 1000) < fraction*1000: deterministic sample across all years")
    a = ap.parse_args()
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    def shard_writers(prefix, header):
        fs = [gzip.open(out / f"{prefix}-shard{i:04d}.csv.gz", "wt", newline="", encoding="utf-8") for i in range(a.shards)]
        ws = [csv.writer(f) for f in fs]
        for w in ws:
            w.writerow(header)
        return fs, ws

    fo, wo = shard_writers("opinions", ["id", "cluster_id", "extracted_by_ocr", "type", "plain_text"])
    fc, wc = shard_writers("opinion-clusters", ["id", "docket_id", "case_name", "date_filed"])
    fd, wd = shard_writers("dockets", ["id", "court_id"])
    courts: dict[str, str] = {}
    n = skipped_text = skipped_date = skipped_dup = 0
    keys_seen = None
    seen_ids: set[str] = set()  # CAP ships the same cases as text.data and xml.data; keep the first copy
    for rec in iter_records(pathlib.Path(a.inp)):
        if keys_seen is None:
            keys_seen = sorted(rec.keys()) if isinstance(rec, dict) else type(rec).__name__
        if not isinstance(rec, dict):
            continue
        key = str(_first(rec, "id", "case_id", "cap_id") or "")
        if a.sample_fraction is not None and key.isdigit() and int(key) % 1000 >= a.sample_fraction * 1000:
            continue
        if key and key in seen_ids:
            skipped_dup += 1
            continue
        text = extract_text(rec)
        if len(text) < a.min_chars:
            skipped_text += 1
            continue
        name, date, cname, rid = extract_meta(rec)
        if not date:
            skipped_date += 1
            continue
        if key:
            seen_ids.add(key)
        cid = courts.setdefault(cname, re.sub(r"[^a-z0-9]+", "-", cname.lower()).strip("-") or "unknown")
        rid = int(rid) if rid not in (None, "") and str(rid).isdigit() else n + 1
        s = n % a.shards
        wo[s].writerow([rid, rid, "t", "010combined", text])
        wc[s].writerow([rid, rid, name, date])
        wd[s].writerow([rid, cid])
        n += 1
        if n % 50000 == 0:
            print(f"  {n:,} cases", file=sys.stderr, flush=True)
        if a.limit and n >= a.limit:
            break
    for f in fo + fc + fd:
        f.close()
    with open(out / "courts-cap.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "full_name", "jurisdiction"])
        written: set[str] = set()  # "Illinois Appellate Court" and "...Court." share a slug; a duplicate id doubles every case in the join
        for name, cid in courts.items():
            if cid not in written:
                written.add(cid)
                w.writerow([cid, name.rstrip(". "), jurisdiction_of(name)])
    print(f"wrote {n:,} cases -> {out}  (skipped: {skipped_text:,} short/no text, {skipped_date:,} no date, {skipped_dup:,} duplicate ids; {len(courts)} courts)")
    if n == 0:
        print(f"NOTHING CONVERTED. First record's keys: {keys_seen}\nSend that line to Claude to adapt extract_text()/extract_meta().", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
