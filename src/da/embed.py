"""Stage 5b - batch transformer embeddings on GPU (T4/A100), resumable, with GPU-side instrumentation.

Runs anywhere with a GPU (Colab/Kaggle T4, an A100 box) - it only needs the exchange dir:
  python -m da.embed --input data/exchange/embed_input --output data/exchange/embeddings
Design for throughput:
  * segments are length-sorted before batching (minimal padding), fp16 autocast, inference_mode
  * tokenisation runs in a background thread and overlaps with the previous batch's GPU forward pass
  * output is sharded parquet; finished shards are skipped on restart (Colab sessions get pre-empted)
  * time is split into tokenise / host->device+forward (CUDA-synchronised) / pooling so bottlenecks are visible
Backends: st (HF transformer - the real one) | hash (deterministic TF-IDF random projection; CPU smoke tests only).
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.parquet as pq


# ------------------------------------------------------------------------------------- encoders
class HFEncoder:
    def __init__(self, model: str, max_len: int, batch_size: int, fp16: bool, pooling: str = "mean"):
        import torch
        from transformers import AutoModel, AutoTokenizer
        self.torch = torch
        self.dev = "cuda" if torch.cuda.is_available() else "cpu"
        self.tok = AutoTokenizer.from_pretrained(model)
        self.model = AutoModel.from_pretrained(model).to(self.dev).eval()
        self.fp16 = fp16 and self.dev == "cuda"
        if self.fp16:
            self.model = self.model.half()
        self.max_len, self.bs, self.pooling = max_len, batch_size, pooling
        self.dim = self.model.config.hidden_size
        self.gpu_name = torch.cuda.get_device_name(0) if self.dev == "cuda" else "cpu"

    def _tokenise(self, texts):
        t0 = time.perf_counter()
        enc = self.tok(texts, padding=True, truncation=True, max_length=self.max_len, return_tensors="pt")
        if self.dev == "cuda":
            enc = {k: v.pin_memory() for k, v in enc.items()}
        return enc, time.perf_counter() - t0

    def encode(self, texts: list[str], stats: dict) -> np.ndarray:
        torch = self.torch
        n = len(texts)
        order = np.argsort([-len(t) for t in texts], kind="stable")
        out = np.zeros((n, self.dim), dtype=np.float32)
        batches = [order[i:i + self.bs] for i in range(0, n, self.bs)]
        pool = ThreadPoolExecutor(max_workers=1)
        nxt = pool.submit(self._tokenise, [texts[j] for j in batches[0]]) if batches else None
        for bi, idx in enumerate(batches):
            enc, t_tok = nxt.result()
            stats["tokenise_s"] += t_tok
            if bi + 1 < len(batches):
                nxt = pool.submit(self._tokenise, [texts[j] for j in batches[bi + 1]])  # overlaps with GPU work below
            t0 = time.perf_counter()
            with torch.inference_mode():
                enc = {k: v.to(self.dev, non_blocking=True) for k, v in enc.items()}
                h = self.model(**enc).last_hidden_state
                if self.pooling == "cls":
                    v = h[:, 0]
                else:
                    mask = enc["attention_mask"].unsqueeze(-1).to(h.dtype)
                    v = (h * mask).sum(1) / mask.sum(1).clamp(min=1)
                v = torch.nn.functional.normalize(v.float(), dim=-1)
                res = v.cpu().numpy()  # implicit sync
            stats["gpu_s"] += time.perf_counter() - t0
            stats["tokens"] += int(enc["attention_mask"].sum().item())
            out[idx] = res
        pool.shutdown()
        return out


class HashEncoder:
    """Smoke-test encoder: hashed TF-IDF + fixed random projection. NOT a semantic model."""
    dim = 128
    gpu_name = "none(hash)"

    def __init__(self, *a, **k):
        self.R = np.random.RandomState(0).randn(4096, self.dim).astype(np.float32) / np.sqrt(self.dim)

    def encode(self, texts, stats):
        t0 = time.perf_counter()
        n = len(texts)
        M = np.zeros((n, 4096), dtype=np.float32)
        for i, t in enumerate(texts):
            for w in re.findall(r"[a-z]{3,}", t.lower()):
                M[i, zlib.crc32(w.encode()) % 4096] += 1
            stats["tokens"] += len(t.split())
        M = np.log1p(M)
        idf = np.log((1 + n) / (1 + (M > 0).sum(0))) + 1
        V = (M * idf) @ self.R
        V /= np.linalg.norm(V, axis=1, keepdims=True).clip(min=1e-9)
        stats["gpu_s"] += time.perf_counter() - t0
        return V


# ------------------------------------------------------------------------------------- driver
def embed_dir(inp: str, out: str, backend="st", model="sentence-transformers/all-MiniLM-L6-v2", max_len=256,
              batch_size=256, fp16=True, shard_docs=20000, limit=None, pooling="mean") -> dict:
    out_p = pathlib.Path(out)
    out_p.mkdir(parents=True, exist_ok=True)
    enc = HashEncoder() if backend == "hash" else HFEncoder(model, max_len, batch_size, fp16, pooling)
    ds = pads.dataset(inp, format="parquet")
    stats = dict(tokenise_s=0.0, gpu_s=0.0, tokens=0, segments=0, docs=0, shards_done=0, shards_skipped=0)
    t_all = time.perf_counter()
    seen = 0
    for si, rb in enumerate(ds.to_batches(columns=["doc_id", "segments"], batch_size=shard_docs, use_threads=False)):
        if limit and seen >= limit:
            break
        f = out_p / f"emb-{si:05d}.parquet"
        seen += rb.num_rows
        if f.exists():
            stats["shards_skipped"] += 1
            stats["docs"] += rb.num_rows
            continue
        doc_ids = rb.column("doc_id").to_numpy(zero_copy_only=False)
        segs = rb.column("segments").to_pylist()
        flat, owner = [], []
        for di, ss in enumerate(segs):
            for s in ss or []:
                if s and s.strip():
                    flat.append(s)
                    owner.append(di)
        vecs = enc.encode(flat, stats)
        owner = np.asarray(owner)
        acc = np.zeros((len(doc_ids), vecs.shape[1]), dtype=np.float32)
        np.add.at(acc, owner, vecs)
        acc /= np.linalg.norm(acc, axis=1, keepdims=True).clip(min=1e-9)
        tbl = pa.table({"doc_id": pa.array(doc_ids, pa.int64()),
                        "vec": pa.FixedSizeListArray.from_arrays(pa.array(acc.ravel(), pa.float32()), acc.shape[1])})
        tmp = f.with_suffix(".tmp")
        pq.write_table(tbl, tmp)
        tmp.rename(f)
        stats["segments"] += len(flat)
        stats["docs"] += len(doc_ids)
        stats["shards_done"] += 1
        print(f"  shard {si}: {len(doc_ids)} docs / {len(flat)} segments  (gpu {stats['gpu_s']:.1f}s, tok {stats['tokenise_s']:.1f}s)", flush=True)
    stats["wall_s"] = time.perf_counter() - t_all
    stats["device"] = enc.gpu_name
    stats["model"] = model if backend != "hash" else "hash"
    stats["batch_size"] = batch_size
    stats["segments_per_s"] = stats["segments"] / max(stats["wall_s"], 1e-9)
    stats["tokens_per_s"] = stats["tokens"] / max(stats["gpu_s"] + 1e-9, 1e-9)
    return stats


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--backend", default="st", choices=["st", "hash"])
    ap.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    ap.add_argument("--pooling", default="mean", choices=["mean", "cls"])
    ap.add_argument("--max-len", type=int, default=256)
    ap.add_argument("--batch-size", type=int, default=256)
    ap.add_argument("--no-fp16", action="store_true")
    ap.add_argument("--shard-docs", type=int, default=20000)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--scale", default=None, help="benchmark scale label")
    a = ap.parse_args(argv)
    st = embed_dir(a.input, a.output, a.backend, a.model, a.max_len, a.batch_size, not a.no_fp16, a.shard_docs, a.limit, a.pooling)
    st["scale"] = a.scale
    print(json.dumps(st, indent=1))
    sf = pathlib.Path(a.output) / "_embed_stats.json"
    if st["shards_done"] > 0 or not sf.exists():  # a no-op resume must not overwrite the real GPU measurement
        sf.write_text(json.dumps(st))
    return st


def run(cfg, spark, m):
    """Pipeline hook: run embeddings here if this host can (GPU or hash backend), else print hand-off instructions."""
    from .common import local_path, to_uri
    e = cfg["embed"]
    ex = local_path(to_uri(cfg["paths"]["exchange"]))
    have_gpu = False
    if e["backend"] == "st":
        try:
            import torch
            have_gpu = torch.cuda.is_available()
        except Exception:
            have_gpu = False
        if not have_gpu:
            print("\n[embed] no GPU / torch on this host. Run the GPU stage on a T4/A100 host (e.g. Colab):\n"
                  f"    python -m da.embed --input {ex}/embed_input --output {ex}/embeddings\n"
                  "  then re-run:  python -m da.pipeline run --stages cluster,marts\n")
            raise SystemExit(0)
    st = embed_dir(f"{ex}/embed_input", f"{ex}/embeddings", e["backend"], e["model"], e["max_len"], e["batch_size"],
                   e["fp16"], e["shard_docs"], pooling=e.get("pooling", "mean"))
    if st["shards_done"] == 0:
        print("[embed] all shards already present - nothing computed, keeping previous GPU benchmark record")
        return
    m.record("embed_gpu", "gpu", **{k: (round(v, 3) if isinstance(v, float) else v) for k, v in st.items()})
