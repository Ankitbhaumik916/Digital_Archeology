"""Pure-python text helpers used inside Spark pandas UDFs (must be importable on every worker)."""
from __future__ import annotations

import html
import re
import unicodedata
import zlib

import numpy as np

# ------------------------------------------------------------------ cleaning
_CTRL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f​-‏  ﻿�]")
_HYPH = re.compile(r"(?<=[a-z])-[ \t]*\n[ \t]*(?=[a-z])")
_PAGE_LINE = re.compile(r"(?im)^[ \t]*(?:page[ \t]+\d+(?:[ \t]+of[ \t]+\d+)?|\[?\*?\d{1,4}\]?|-[ \t]?\d{1,4}[ \t]?-)[ \t]*$")
_STAR_PAGE = re.compile(r"\*\s?\d{1,4}\b")
_URL = re.compile(r"https?://\S+|www\.\S+")
_HWS = re.compile(r"[ \t\f\v ]+")
_MULTI_NL = re.compile(r"\n{3,}")
_SINGLE_NL = re.compile(r"(?<!\n)\n(?!\n)")
# OCR-noise indicators (word level)
_MIXED = re.compile(r"\b(?=[A-Za-z0-9]*\d)(?=[A-Za-z0-9]*[A-Za-z])[A-Za-z0-9]{5,}\b")
_NOVOWEL = re.compile(r"\b[bcdfghjklmnpqrstvwxyz]{6,}\b", re.I)
_RUN = re.compile(r"(.)\1{3,}")
_ALLCAPS_OK = re.compile(r"^[A-Z0-9]+$")
_WORD = re.compile(r"[A-Za-z0-9][A-Za-z0-9'\-]*")


def clean_text(s: str, max_chars: int = 400_000) -> str:
    if not s:
        return ""
    s = s[:max_chars]
    if "&" in s:
        s = html.unescape(s)
    s = unicodedata.normalize("NFKC", s)
    s = _CTRL.sub(" ", s)
    s = _URL.sub(" ", s)
    s = _HYPH.sub("", s)
    s = _PAGE_LINE.sub("", s)
    s = _STAR_PAGE.sub(" ", s)
    s = _HWS.sub(" ", s)
    s = _SINGLE_NL.sub(" ", s)
    s = re.sub(r"[ \t]*\n[ \t]*", "\n", s)
    s = _MULTI_NL.sub("\n\n", s)
    return _HWS.sub(" ", s).strip()


def text_stats(s: str) -> tuple[int, float, float]:
    """(n_words, alpha_ratio, noise_score). noise = fraction of words that look OCR-garbled."""
    if not s:
        return 0, 0.0, 1.0
    non_space = sum(1 for c in s if not c.isspace())
    alpha = sum(1 for c in s if c.isalpha())
    words = _WORD.findall(s)
    n = len(words)
    if n == 0:
        return 0, 0.0, 1.0
    suspect = len(_MIXED.findall(s)) + len(_RUN.findall(s))
    suspect += sum(1 for w in _NOVOWEL.findall(s) if not _ALLCAPS_OK.match(w))
    return n, alpha / max(non_space, 1), min(1.0, suspect / n)


def norm_for_hash(s: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", s.lower()))


# ------------------------------------------------------------------ minhash
P31 = (1 << 31) - 1


def _params(num_perm: int, seed: int = 1234):
    rng = np.random.RandomState(seed)
    a = rng.randint(1, P31, size=num_perm, dtype=np.int64)
    b = rng.randint(0, P31, size=num_perm, dtype=np.int64)
    return a, b


_word_cache: dict[str, int] = {}


def _word_hashes(words: list[str]) -> np.ndarray:
    cache = _word_cache
    if len(cache) > 2_000_000:
        cache.clear()
    out = np.empty(len(words), dtype=np.int64)
    for i, w in enumerate(words):
        h = cache.get(w)
        if h is None:
            h = zlib.crc32(w.encode("utf-8")) & 0x7FFFFFFF  # deterministic across workers (no PYTHONHASHSEED)
            cache[w] = h
        out[i] = h
    return out


_MULT = np.array([1000003, 998244353, 1000033, 999983, 1000037, 1000039, 1000081, 1000099], dtype=np.int64)


def word_shingles(text_norm: str, k: int, max_words: int = 60000) -> np.ndarray:
    words = text_norm.split()[:max_words]
    if not words:
        return np.zeros(1, dtype=np.int64)
    if len(words) < k:
        return np.array([zlib.crc32(" ".join(words).encode()) & 0x7FFFFFFF], dtype=np.int64)
    wh = _word_hashes(words)
    n = len(wh) - k + 1
    acc = np.zeros(n, dtype=np.int64)
    for j in range(k):
        acc = (acc + wh[j:j + n] * _MULT[j % len(_MULT)]) % P31  # each term < 2^31 * 2^30: fits int64
    return np.unique(acc)


def char_shingles(s: str, k: int = 3) -> np.ndarray:
    s = f"  {s} "
    if len(s) < k:
        return np.array([zlib.crc32(s.encode()) & 0x7FFFFFFF], dtype=np.int64)
    return np.unique(np.fromiter((zlib.crc32(s[i:i + k].encode()) & 0x7FFFFFFF for i in range(len(s) - k + 1)),
                                 dtype=np.int64))


def minhash(shingles: np.ndarray, a: np.ndarray, b: np.ndarray, block: int = 8192) -> np.ndarray:
    sig = np.full(len(a), P31, dtype=np.int64)
    for i in range(0, len(shingles), block):
        h = shingles[i:i + block]
        sig = np.minimum(sig, ((a[:, None] * h[None, :] + b[:, None]) % P31).min(axis=1))
    return sig


_BAND_MIX = np.array([0x9E3779B97F4A7C15, 0xC2B2AE3D27D4EB4F, 0x165667B19E3779F9, 0x27D4EB2F165667C5,
                      0xD6E8FEB86659FD93, 0xFF51AFD7ED558CCD, 0xC4CEB9FE1A85EC53, 0x94D049BB133111EB], dtype=np.uint64)


def band_hashes(sig: np.ndarray, bands: int) -> np.ndarray:
    rows = len(sig) // bands
    m = sig[: bands * rows].astype(np.uint64).reshape(bands, rows)
    with np.errstate(over="ignore"):
        h = (m * _BAND_MIX[:rows][None, :]).sum(axis=1)
        h ^= h >> np.uint64(29)
    return h.view(np.int64)
