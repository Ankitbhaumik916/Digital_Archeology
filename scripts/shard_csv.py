#!/usr/bin/env python3
"""Stream a huge CourtListener CSV (from stdin or a file) into N smaller, valid CSV shards.

Why: a single .csv.bz2 with multi-line quoted fields cannot be split by Spark => ONE task decompresses and parses
the whole file. Sharding once (with parallel decompression) lets Spark read N shards on N cores.
For opinions it also drops the redundant text columns: only the best text column per row is kept.

  pbzip2 -dc opinions-2026-01-01.csv.bz2 | python scripts/shard_csv.py --kind opinions --out data/raw --n 64
  python scripts/shard_csv.py --in dockets-2026-01-01.csv.bz2 --kind dockets --out data/raw --n 16
"""
import argparse
import bz2
import csv
import gzip
import pathlib
import sys

csv.field_size_limit(sys.maxsize)
KEEP = {
    "opinions": ["id", "cluster_id", "extracted_by_ocr", "type", "plain_text", "html_with_citations", "html_columbia",
                 "html_lawbox", "xml_harvard", "html"],
    "clusters": ["id", "docket_id", "case_name", "date_filed"],
    "dockets": ["id", "court_id"],
}
TEXT = ["plain_text", "html_with_citations", "html_columbia", "html_lawbox", "xml_harvard", "html"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp")
    ap.add_argument("--kind", required=True, choices=list(KEEP))
    ap.add_argument("--out", required=True)
    ap.add_argument("--n", type=int, default=32)
    ap.add_argument("--prefix")
    a = ap.parse_args()
    if a.inp:
        op = bz2.open if a.inp.endswith(".bz2") else gzip.open if a.inp.endswith(".gz") else open
        src = op(a.inp, "rt", newline="", encoding="utf-8", errors="replace")
    else:
        src = open(sys.stdin.fileno(), "rt", newline="", encoding="utf-8", errors="replace", closefd=False)
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    prefix = a.prefix or {"opinions": "opinions", "clusters": "opinion-clusters", "dockets": "dockets"}[a.kind]
    rdr = csv.reader(src)
    header = next(rdr)
    keep = [c for c in KEEP[a.kind] if c in header]
    idx = [header.index(c) for c in keep]
    tix = [keep.index(c) for c in TEXT if c in keep]
    files = [gzip.open(out / f"{prefix}-shard{i:04d}.csv.gz", "wt", newline="", encoding="utf-8") for i in range(a.n)]
    ws = [csv.writer(f) for f in files]
    for w in ws:
        w.writerow(keep)
    n = 0
    for row in rdr:
        if len(row) < len(header):
            continue
        r = [row[i] for i in idx]
        if a.kind == "opinions":  # keep only the first non-empty text column
            found = False
            for t in tix:
                if r[t].strip() and not found:
                    found = True
                elif found:
                    r[t] = ""
        ws[n % a.n].writerow(r)
        n += 1
        if n % 200000 == 0:
            print(f"  {n:,} rows", file=sys.stderr, flush=True)
    for f in files:
        f.close()
    print(f"{n:,} rows -> {a.n} shards in {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
