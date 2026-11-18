"""Shared helpers: config, Spark session, paths, per-stage instrumentation."""
from __future__ import annotations

import contextlib
import copy
import json
import os
import pathlib
import time
import urllib.request
import uuid

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
BENCH_FILE = ROOT / "bench" / "metrics.jsonl"


# --------------------------------------------------------------------------- config
def _deep_merge(a: dict, b: dict) -> dict:
    out = copy.deepcopy(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(path: str | None = None, profile: str | None = None) -> dict:
    path = path or os.environ.get("DA_CONFIG") or ROOT / "config.yaml"
    with open(path) as f:
        cfg = yaml.safe_load(f)
    profile = profile or os.environ.get("DA_PROFILE")
    profiles = cfg.pop("profiles", {}) or {}
    if profile:
        if profile not in profiles:
            raise SystemExit(f"unknown profile {profile!r}; have {sorted(profiles)}")
        cfg = _deep_merge(cfg, profiles[profile])
    if os.environ.get("DA_BASE"):
        cfg["paths"]["base"] = os.environ["DA_BASE"]
    if os.environ.get("DA_RAW"):
        cfg["paths"]["raw"] = os.environ["DA_RAW"]
    if os.environ.get("DA_EXPORT"):
        cfg["paths"]["export"] = os.environ["DA_EXPORT"]
    cfg["profile"] = profile
    return cfg


def to_uri(path: str) -> str:
    if "://" in path:
        return path.rstrip("/")
    q = pathlib.Path(path)
    if not q.is_absolute():
        q = ROOT / q
    if os.name == "nt":  # Hadoop on Windows wants file:///D:/x, not file://D:\x
        return "file:///" + q.as_posix()
    return "file://" + str(q)


def P(cfg: dict, *parts: str) -> str:
    """Location of a dataset under the data root, as a Spark-readable URI."""
    return to_uri(cfg["paths"]["base"]) + "/" + "/".join(parts)


def local_path(uri: str) -> str:
    if os.name == "nt" and uri.startswith("file:///") and uri[9:10] == ":":
        return uri[len("file:///"):]
    return uri[len("file://"):] if uri.startswith("file://") else uri


def export_dir(cfg: dict) -> pathlib.Path:
    q = pathlib.Path(local_path(to_uri(cfg["paths"]["export"])))
    q.mkdir(parents=True, exist_ok=True)
    return q


# --------------------------------------------------------------------------- spark
def get_spark(cfg: dict, app: str):
    from pyspark.sql import SparkSession

    s = cfg["spark"]
    b = SparkSession.builder.appName(app).master(os.environ.get("SPARK_MASTER", s["master"]))
    for k, v in (s.get("conf") or {}).items():
        b = b.config(k, str(v).lower() if isinstance(v, bool) else str(v))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# --------------------------------------------------------------------------- instrumentation
class Metrics:
    """Records wall-clock plus Spark task-level breakdown for each pipeline stage.

    Per stage we store:
      wall_s            wall-clock of the stage
      task_run_s        sum of executor task run time (cpu-seconds of work, across all cores)
      shuffle_wait_s    time tasks spent blocked fetching shuffle blocks (network + remote disk)
      shuffle_write_s   time spent writing shuffle files
      compute_s         task_run_s - shuffle_wait_s - shuffle_write_s  (scan/decode/UDF/aggregate work)
      gc_s              JVM GC time
      input_bytes / output_bytes / shuffle_read_bytes / shuffle_write_bytes
    Storage-I/O is reported separately by `io_probe` (scan-only read of a dataset).
    """

    def __init__(self, cfg: dict, spark=None, scale: str | None = None, run_id: str | None = None):
        self.cfg = cfg
        self.spark = spark
        self.scale = scale or os.environ.get("DA_SCALE", "1.0")
        self.run_id = run_id or os.environ.get("DA_RUN_ID") or time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    # -- Spark REST API ---------------------------------------------------
    def _stages(self):
        if self.spark is None:
            return {}
        sc = self.spark.sparkContext
        ui = sc.uiWebUrl
        if not ui:
            return None
        url = f"{ui}/api/v1/applications/{sc.applicationId}/stages?length=100000"
        try:
            with self._opener.open(url, timeout=10) as r:
                data = json.load(r)
        except Exception:
            return None
        return {(s["stageId"], s["attemptId"]): s for s in data if s.get("status") == "COMPLETE"}

    @contextlib.contextmanager
    def stage(self, name: str, **extra):
        before = self._stages()
        t0 = time.perf_counter()
        yield
        wall = time.perf_counter() - t0
        rec = {"run_id": self.run_id, "scale": self.scale, "stage": name, "kind": "spark",
               "wall_s": round(wall, 3), "ts": time.time(), "profile": self.cfg.get("profile")}
        time.sleep(1.0)  # let the listener bus flush task metrics
        after = self._stages()
        if before is not None and after is not None:
            new = [s for k, s in after.items() if k not in before]
            ms = 1e-3
            run = sum(s.get("executorRunTime", 0) for s in new) * ms
            fetch = sum(s.get("shuffleFetchWaitTime", 0) for s in new) * ms
            swrite = sum(s.get("shuffleWriteTime", 0) for s in new) * 1e-9
            rec.update(
                n_stages=len(new),
                n_tasks=sum(s.get("numCompleteTasks", 0) for s in new),
                task_run_s=round(run, 3),
                shuffle_wait_s=round(fetch, 3),
                shuffle_write_s=round(swrite, 3),
                compute_s=round(max(run - fetch - swrite, 0.0), 3),
                gc_s=round(sum(s.get("jvmGcTime", 0) for s in new) * ms, 3),
                input_bytes=sum(s.get("inputBytes", 0) for s in new),
                output_bytes=sum(s.get("outputBytes", 0) for s in new),
                shuffle_read_bytes=sum(s.get("shuffleReadBytes", 0) for s in new),
                shuffle_write_bytes=sum(s.get("shuffleWriteBytes", 0) for s in new),
            )
        rec.update(extra)
        self.write(rec)
        print(f"[stage:{name}] wall={wall:.1f}s " + " ".join(
            f"{k}={rec[k]}" for k in ("task_run_s", "shuffle_wait_s", "compute_s") if k in rec), flush=True)

    def io_probe(self, name: str, path: str, columns: list[str] | None = None):
        """Scan-only read of a dataset (decode, no compute, no write) => storage I/O lower bound."""
        df = self.spark.read.parquet(path)
        if columns:
            df = df.select(*columns)
        t0 = time.perf_counter()
        df.write.format("noop").mode("overwrite").save()
        dt = time.perf_counter() - t0
        rec = {"run_id": self.run_id, "scale": self.scale, "stage": f"io_probe:{name}", "kind": "io_probe",
               "wall_s": round(dt, 3), "ts": time.time(), "profile": self.cfg.get("profile")}
        self.write(rec)
        print(f"[io_probe:{name}] scan-only read {dt:.1f}s", flush=True)
        return dt

    def record(self, name: str, kind: str, **fields):
        rec = {"run_id": self.run_id, "scale": self.scale, "stage": name, "kind": kind,
               "ts": time.time(), "profile": self.cfg.get("profile")}
        rec.update(fields)
        self.write(rec)

    @staticmethod
    def write(rec: dict):
        BENCH_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(BENCH_FILE, "a") as f:
            f.write(json.dumps(rec) + "\n")
