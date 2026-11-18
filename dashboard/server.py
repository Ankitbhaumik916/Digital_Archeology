"""Digital Archaeology dashboard: JSON API over the Spark marts + the static JS front end in dashboard/web.

  . scripts/env.sh && python dashboard/server.py [--port 8600]      (reads marts from $DA_EXPORT or data/export)

Endpoints (all GET except /api/live):
  /api/meta              courts, docs per year x court level, entity list, topics
  /api/entity/<id>       per-year counts, topic mix by decade, framing terms by decade, example passages
  /api/topics            topic counts per year x court level, top entities per topic
  /api/bench             latest benchmark record per (scale, stage) from bench/metrics.jsonl
  POST /api/live/<id>    runs a distributed Spark query over entities/mentions right now
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import pathlib
import sys
import threading
import time
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
WEB = pathlib.Path(__file__).resolve().parent / "web"
EXPORT = pathlib.Path(os.environ.get("DA_EXPORT", ROOT / "data" / "export"))
JUR = {"F": "Federal appellate", "FD": "Federal district", "FB": "Federal bankruptcy", "FS": "Federal special",
       "S": "State supreme", "SA": "State appellate", "ST": "State trial", "SS": "State special", "U": "Unknown"}
MAX_EXAMPLES = 400


def _py(v):
    if isinstance(v, np.ndarray):
        return [_py(x) for x in v.tolist()]
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return None if np.isnan(v) else round(float(v), 6)
    if isinstance(v, float) and v != v:
        return None
    return v


def cols(df: pd.DataFrame) -> dict:
    """Column oriented JSON (much smaller than records for long tables)."""
    return {c: [_py(v) for v in df[c].tolist()] for c in df.columns}


@lru_cache(maxsize=None)
def mart(name: str) -> pd.DataFrame:
    return pd.read_parquet(EXPORT / f"{name}.parquet")


# ------------------------------------------------------------------------------------------- api
def api_meta():
    ents = mart("entities").sort_values("total_docs", ascending=False)
    topics = mart("topics").sort_values("topic_id")
    dy = mart("docs_year")
    return {
        "courts": [{"code": j, "name": JUR.get(j, j)} for j in sorted(dy.jurisdiction.unique())],
        "docs_year": cols(dy[["year", "jurisdiction", "n_docs"]]),
        "entities": cols(ents[["ent_id", "canonical", "total_docs", "variants"]]),
        "topics": cols(topics[["topic_id", "label", "keywords", "n_docs"]]),
    }


def api_entity(ent_id: int):
    ey = mart("entity_year")
    et = mart("entity_topic_decade")
    cx = mart("entity_context_decade")
    ex = mart("examples")
    e = ex[ex.ent_id == ent_id].sort_values("year")
    if len(e) > MAX_EXAMPLES:  # keep an even spread over time rather than the first N
        e = e.groupby((e.year // 10) * 10, group_keys=False).apply(lambda g: g.head(MAX_EXAMPLES // 20))
    return {
        "ent_id": ent_id,
        "year": cols(ey.loc[ey.ent_id == ent_id, ["year", "jurisdiction", "n_docs"]]),
        "topic_decade": cols(et.loc[et.ent_id == ent_id, ["decade", "topic_id", "n_docs"]]),
        "context": cols(cx.loc[cx.ent_id == ent_id, ["decade", "term", "score", "n"]]),
        "examples": cols(e[["year", "jurisdiction", "case_name", "ctx"]]),
    }


def api_topics():
    ty = mart("topic_year")
    et = mart("entity_topic_decade")
    names = mart("entities").set_index("ent_id").canonical
    top = (et.groupby(["topic_id", "ent_id"], as_index=False).n_docs.sum()
             .sort_values(["topic_id", "n_docs"], ascending=[True, False]).groupby("topic_id").head(10))
    top["canonical"] = top.ent_id.map(names)
    return {"topic_year": cols(ty[["topic_id", "year", "jurisdiction", "n_docs"]]),
            "top_entities": cols(top[["topic_id", "ent_id", "canonical", "n_docs"]])}


def api_bench():
    mf = ROOT / "bench" / "metrics.jsonl"
    if not mf.exists():
        return {"stages": [], "gpu": []}
    from da.bench_report import CATEGORY, ORDER, latest_per_scale, load as load_metrics
    df = latest_per_scale(load_metrics())
    st = df[df.stage.isin(CATEGORY)].copy()
    st["category"] = st.stage.map(CATEGORY)
    st["order"] = st.stage.map(ORDER.index)
    keep = [c for c in ["scale", "stage", "category", "order", "wall_s", "compute_s", "shuffle_wait_s",
                        "shuffle_write_s", "gc_s"] if c in st.columns]
    gpu = df[df.stage == "embed_gpu"]
    gkeep = [c for c in ["scale", "segments", "segments_per_s", "gpu_s", "tokenise_s", "wall_s", "device",
                         "model", "batch_size"] if c in gpu.columns]
    docs = df[df.stage == "clean_summary"]
    return {"stages": cols(st.sort_values(["scale", "order"])[keep]), "gpu": cols(gpu[gkeep]),
            "docs": cols(docs[["scale", "n_out"]]) if "n_out" in docs.columns else {}}


_spark = None
_spark_lock = threading.Lock()


def api_live(ent_id: int):
    global _spark
    with _spark_lock:  # one Spark session, one query at a time
        from pyspark.sql import functions as F
        from da.common import P, get_spark, load_config
        cfg = load_config()
        t_start = time.perf_counter()
        if _spark is None:
            _spark = get_spark(cfg, "dashboard-live")
        startup = time.perf_counter() - t_start
        t0 = time.perf_counter()
        pdf = (_spark.read.parquet(P(cfg, "entities/mentions")).where(F.col("ent_id") == int(ent_id))
               .groupBy("year").agg(F.countDistinct("doc_id").alias("opinions")).orderBy("year")).toPandas()
        return {"seconds": round(time.perf_counter() - t0, 3), "startup_seconds": round(startup, 3),
                "result": cols(pdf)}


# ------------------------------------------------------------------------------------------- http
class Handler(BaseHTTPRequestHandler):
    server_version = "da-dashboard/1.0"

    def log_message(self, fmt, *args):  # quieter console: only errors
        if args and str(args[1])[:1] in "45":
            super().log_message(fmt, *args)

    def _json(self, obj, status=HTTPStatus.OK):
        body = json.dumps(obj, separators=(",", ":"), allow_nan=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, message):
        self._json({"error": message}, status)

    def _route_api(self, method: str):
        parts = self.path.split("?")[0].strip("/").split("/")[1:]
        try:
            if method == "GET" and parts == ["meta"]:
                if not (EXPORT / "entities.parquet").exists():
                    return self._error(HTTPStatus.SERVICE_UNAVAILABLE,
                                       f"No marts in {EXPORT}. Run python -m da.pipeline run first.")
                return self._json(api_meta())
            if method == "GET" and len(parts) == 2 and parts[0] == "entity" and parts[1].isdigit():
                return self._json(api_entity(int(parts[1])))
            if method == "GET" and parts == ["topics"]:
                return self._json(api_topics())
            if method == "GET" and parts == ["bench"]:
                return self._json(api_bench())
            if method == "POST" and len(parts) == 2 and parts[0] == "live" and parts[1].isdigit():
                return self._json(api_live(int(parts[1])))
            return self._error(HTTPStatus.NOT_FOUND, f"Unknown endpoint {self.path}")
        except Exception as e:  # report the failure to the page instead of dropping the connection
            return self._error(HTTPStatus.INTERNAL_SERVER_ERROR, f"{type(e).__name__}: {e}")

    def _static(self):
        rel = self.path.split("?")[0].lstrip("/") or "index.html"
        p = (WEB / rel).resolve()
        if WEB not in p.parents or not p.is_file():
            p, status = WEB / "404.html", HTTPStatus.NOT_FOUND
        else:
            status = HTTPStatus.OK
        body = p.read_bytes()
        self.send_response(status)
        self.send_header("Content-Type", (mimetypes.guess_type(p.name)[0] or "application/octet-stream")
                         + ("; charset=utf-8" if p.suffix in (".html", ".js", ".css", ".svg") else ""))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/"):
            return self._route_api("GET")
        return self._static()

    def do_POST(self):
        if self.path.startswith("/api/"):
            return self._route_api("POST")
        return self._error(HTTPStatus.METHOD_NOT_ALLOWED, "POST is only accepted on /api/")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8600)
    a = ap.parse_args()
    mimetypes.add_type("text/javascript", ".js")
    httpd = ThreadingHTTPServer((a.host, a.port), Handler)
    print(f"Digital Archaeology dashboard: http://{'localhost' if a.host == '127.0.0.1' else a.host}:{a.port}", flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
