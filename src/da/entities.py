"""Stage 4 - entity extraction and entity resolution.

extract  : capitalised-phrase patterns anchored on institutional head words (Agency, Commission, Act, Amendment...),
           acronyms, and parties parsed from the case caption; keeps a +-N char context snippet per mention.
resolve  : normalise (case, punctuation, abbreviations, corporate suffixes) -> MinHash-LSH blocking on char 3-grams
           -> pairwise verification (token-aligned, edit-distance-1 typo tolerance) + acronym<->long-form edges
           -> each surface form attaches to its most frequent verified neighbour (no transitive chaining drift).
Outputs  <base>/entities/{mentions,vocab,map,dim}
"""
from __future__ import annotations

import re
import unicodedata

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Window, types as T
from pyspark.sql import functions as F

from .common import P, Metrics
from .textutil import char_shingles, band_hashes, minhash, _params

ORG_WORDS = {
    "agency", "administration", "association", "authority", "bank", "board", "bureau", "commission", "committee",
    "company", "corporation", "corp", "council", "department", "division", "foundation", "group", "institute",
    "insurance", "office", "railroad", "railway", "service", "system", "union", "university", "trust", "church",
    "hospital", "school", "district", "secretary", "commissioner", "inc", "co", "llc", "ltd", "amendment", "act",
    "clause", "partners", "holdings", "industries", "airlines", "labs", "laboratories", "enterprises", "bancorp",
    "dept", "assn", "assoc", "univ", "comm", "admin",
}
STOP_LEAD = {"the", "in", "see", "under", "by", "of", "for", "and", "at", "to", "that", "this", "as", "on", "whether",
             "however", "but", "if", "when", "because", "we", "a", "an", "it", "its", "with", "from", "our", "their",
             "petitioner", "respondent", "plaintiff", "defendant", "appellant", "appellee", "court", "judge", "also",
             "since", "while", "although", "both", "each", "any", "all", "such", "these", "those", "his", "her", "he", "she"}
ACR_STOP = {"OPINION", "COURT", "STATE", "SUPREME", "DISSENT", "JUDGE", "APPEAL", "FACTS", "NOTE", "PAGE", "SYLLABUS", "HELD",
            "UNITED", "STATES", "COUNSEL", "ORDER", "JUDGMENT", "AFFIRMED", "REVERSED", "PER", "CURIAM", "OPINIONS", "THE", "AND",
            "US", "USC", "CFR", "II", "III", "IV", "VI", "VII", "VIII", "IX", "XI", "XII", "ALJ", "NO", "OF", "THE", "AND",
            "SEE", "ID", "CIR", "APP", "SUPP", "INC", "LLC", "LTD", "CO", "FED", "REG", "STAT", "SCT", "ED", "WL", "NE", "NW",
            "SE", "SW", "WD", "ND", "SD", "ED", "MD", "CD", "ORS", "PER", "CURIAM", "ET", "AL", "FOR", "NOT", "ANY", "ALL"}
ABBR = {"dept": "department", "assn": "association", "assoc": "association", "natl": "national", "intl": "international",
        "comm": "commission", "commn": "commission", "admin": "administration", "adm": "administration", "bd": "board",
        "bur": "bureau", "univ": "university", "mfg": "manufacturing", "bros": "brothers", "govt": "government",
        "corp": "corporation", "co": "company", "inc": "incorporated", "ltd": "limited", "svc": "service"}
CORP_SUFFIX = {"inc", "incorporated", "corp", "corporation", "co", "company", "llc", "ltd", "limited", "plc", "lp"}
CONNECT = {"of", "and", "for", "on", "in"}

_TOK = r"[A-Z][\w'’&\-]*"
# connectors: "of/for/on [the]" and a bare "and"/"&" (but NOT "and the": that starts a new noun phrase)
_PHRASE = re.compile(rf"\b{_TOK}(?:(?:\s+(?:(?:of|for|on)(?:\s+the)?|and(?!\s+the\b)|&))?\s+{_TOK}){{0,7}}")
_ACR = re.compile(r"\b[A-Z]{2,6}\b")
_DEF = re.compile(r"\(\s*(?:the\s+)?[\u201c\"']?([A-Z]{2,8})s?[\u201d\"']?\s*\)")
_V = re.compile(r"\s+v(?:s)?\.?\s+", re.I)


_FUZZY_SUFFIX = ("company", "corporation", "incorporated", "limited")


def _is_suffix_typo(t: str) -> bool:
    """OCR/typo variants of a corporate suffix ('compny', 'corpoation') are dropped like the suffix itself."""
    return len(t) >= 5 and any(abs(len(t) - len(w)) <= 1 and _lev(t, w) <= 1 for w in _FUZZY_SUFFIX)


def norm_name(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower().replace("&", " and ")
    s = re.sub(r"[.'’,]", "", s)
    s = re.sub(r"[^a-z0-9\s\-]", " ", s).replace("-", " ")
    toks = [ABBR.get(t, t) for t in s.split() if t != "the"]
    while len(toks) > 1 and (toks[-1] in CORP_SUFFIX or _is_suffix_typo(toks[-1])):
        toks.pop()
    return " ".join(toks)


def _is_org(t: str) -> bool:
    t = t.lower().strip(".,;:'’")
    return t in ORG_WORDS or (t.endswith("s") and t[:-1] in ORG_WORDS)


def _keep_phrases(phrase: str) -> list[str]:
    """Split a raw capitalised run into entity phrases and keep the institutional ones.
    'X Commission and Y Administration' -> two entities; 'Department of Health and Human Services' stays one
    (we only split at 'and' when the word before it is already an institutional head word)."""
    toks = phrase.split()
    parts, cur = [], []
    for i, t in enumerate(toks):
        if t.lower() in ("and", "&") and cur and _is_org(cur[-1]) and i + 1 < len(toks):
            parts.append(cur)
            cur = []
        elif cur and t[:1].isupper() and t.lower() in STOP_LEAD:  # capitalised "The"/"In"/"See" mid-run = new sentence/heading
            parts.append(cur)
            cur = [t]
        else:
            cur.append(t)
    parts.append(cur)
    out = []
    for toks in parts:
        while toks and (toks[0].lower().strip("'’") in STOP_LEAD or (toks[0].isupper() and toks[0] in ACR_STOP)):
            toks = toks[1:]
        while toks and toks[-1].lower() in CONNECT:
            toks = toks[:-1]
        if len(toks) < 2:
            continue
        if any(_is_org(t) for t in toks):
            out.append(" ".join(toks))
    return out


def acr_match(acr: str, words: list[str]) -> bool:
    """Can `acr` be spelled by taking a prefix of each of a subset of `words` (in order), using the first and last word?
    FDIC <- Federal Deposit Insurance Corporation; FINRA <- Financial Industry Regulatory Authority; DOJ <- Department Of Justice."""
    a = acr.lower()
    w = [x.lower() for x in words]
    n = len(w)
    if n < 2 or len(a) < 2:
        return False
    from functools import lru_cache

    @lru_cache(maxsize=None)
    def rec(i: int, j: int) -> bool:  # a[i:] must be spelled by words[j:], word j..n-1 ; last word must be used
        if j == n:
            return i == len(a)
        if i == len(a):
            return False  # last word unused
        for L in range(1, min(len(w[j]), len(a) - i) + 1):
            if w[j][:L] == a[i:i + L] and rec(i + L, j + 1):
                return True
        if j > 0 and j < n - 1 and rec(i, j + 1):  # skip a middle word (connector or non-initial)
            return True
        return False
    return rec(0, 0)


def find_definitions(text: str):
    """'... the Federal Deposit Insurance Corporation (FDIC) ...' -> [('fdic', 'federal deposit insurance corporation')]"""
    out = []
    for m in _DEF.finditer(text):
        acr = m.group(1)
        if acr in ACR_STOP:
            continue
        back = text[max(0, m.start() - 160):m.start()].split()
        for k in range(2, min(len(back), 11) + 1):
            cand = back[-k:]
            core = [c.strip(",;:") for c in cand]
            if not core[0][:1].isupper() or core[0].lower().strip("'’") in STOP_LEAD:
                continue
            if any(not (c[:1].isupper() or c.lower() in CONNECT or c.lower() == "the") for c in core):
                continue
            if acr_match(acr, [c for c in core if c.lower() != "the"]):
                out.append((acr.lower(), norm_name(" ".join(core))))
                break
    return out


def _party_entities(case_name: str):
    out = []
    if not case_name:
        return out
    for side in _V.split(case_name, maxsplit=1):
        side = re.sub(r"\b(et al|et ux|et vir)\.?", "", side).strip(" ,.;")
        toks = side.split()
        if 1 < len(toks) <= 9 and ({t.lower().strip(".,") for t in toks} & ORG_WORDS):
            out.append(side)
    return out


_ENT_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("kind", T.StringType()),
    T.StructField("norm", T.StringType()), T.StructField("raw", T.StringType()),
    T.StructField("cnt", T.IntegerType()), T.StructField("ctx", T.StringType())]))


def extract_mentions(text: str, case_name: str, max_per_doc: int, ctx_chars: int):
    found: dict[str, list] = {}

    def add(raw, ctx):
        n = norm_name(raw)
        if not n or len(n) < 3:
            return
        e = found.get(n)
        if e is None:
            found[n] = [raw, 1, ctx]
        else:
            e[1] += 1

    for m in _PHRASE.finditer(text):
        for ph in _keep_phrases(m.group(0)):
            a = max(0, m.start() - ctx_chars)
            add(ph, re.sub(r"\s+", " ", text[a:m.end() + ctx_chars]))
    for m in _ACR.finditer(text):
        w = m.group(0)
        if w not in ACR_STOP and len(w) >= 3:
            a = max(0, m.start() - ctx_chars)
            add(w, re.sub(r"\s+", " ", text[a:m.end() + ctx_chars]))
    for p in _party_entities(case_name):
        add(p, case_name)
    items = sorted(found.items(), key=lambda kv: -kv[1][1])[:max_per_doc]
    res = [("m", n, v[0], v[1], v[2]) for n, v in items]
    for acr, long_ in set(find_definitions(text)):
        res.append(("d", acr, long_, 1, ""))
    return res


def _extract_udf(max_per_doc, ctx_chars):
    @F.pandas_udf(_ENT_SCHEMA)
    def f(text: pd.Series, case: pd.Series) -> pd.Series:
        return pd.Series([extract_mentions(t or "", c or "", max_per_doc, ctx_chars) for t, c in zip(text, case)])
    return f


# ---------------------------------------------------------------- pairwise verification
try:
    from rapidfuzz.distance import Levenshtein as _Lev

    def _lev(a, b):
        return _Lev.distance(a, b)
except Exception:  # pragma: no cover
    def _lev(a, b):
        prev = list(range(len(b) + 1))
        for i, ca in enumerate(a, 1):
            cur = [i]
            for j, cb in enumerate(b, 1):
                cur.append(min(prev[j] + 1, cur[-1] + 1, prev[j - 1] + (ca != cb)))
            prev = cur
        return prev[-1]

_GENERIC_EXTRA = {"united", "states", "us", "american", "national", "federal", "state", "new"}


def _tok_close(x: str, y: str) -> bool:
    n = max(len(x), len(y))
    if n < 6:
        return False
    d = _lev(x, y)
    return d <= 1 or (n >= 11 and d <= 2)


def same_entity(a: str, b: str) -> bool:
    if a == b:
        return True
    ta = [t for t in a.split() if t not in CONNECT]
    tb = [t for t in b.split() if t not in CONNECT]
    if len(ta) != len(tb):
        short, long_ = (ta, tb) if len(ta) < len(tb) else (tb, ta)
        if 1 <= len(long_) - len(short) <= 2 and len(short) >= 2:
            extra = [t for t in long_ if t not in short]
            if extra and all(t in _GENERIC_EXTRA for t in extra) and all(t in long_ for t in short):
                return True
        return False
    exact = sum(1 for x, y in zip(ta, tb) if x == y)
    if len(ta) >= 2 and exact == 0:
        return False
    return all(x == y or _tok_close(x, y) for x, y in zip(ta, tb))


def deletion_keys(norm: str) -> list[str]:
    """SymSpell-style blocking keys: two names that differ by ONE edit inside one token (>=6 chars) and are otherwise
    identical share at least one key (the name itself, or the name with the same character deleted). Exact recall for
    the single-typo case that same_entity() accepts, with no probabilistic misses and tiny buckets."""
    toks = norm.split()
    keys = {norm}
    for j, t in enumerate(toks):
        if len(t) >= 6:
            for i in range(len(t)):
                keys.add(" ".join(toks[:j] + [t[:i] + t[i + 1:]] + toks[j + 1:]))
    return list(keys)


def _acronym(name: str) -> str | None:
    toks = [t for t in name.split() if t not in CONNECT]
    if len(toks) < 2:
        return None
    return "".join(t[0] for t in toks)


_LSH_SCHEMA = T.ArrayType(T.LongType())


def run(cfg: dict, spark: SparkSession, m: Metrics) -> None:
    e = cfg["entities"]
    keep = spark.read.parquet(P(cfg, "dedup_keep"))
    clean = spark.read.parquet(P(cfg, "clean")).join(keep, "doc_id")
    out = P(cfg, "entities")

    # ------------------------------------------------------------ extract
    with m.stage("entities_extract"):
        ms = clean.select("doc_id", "year", "jurisdiction",
                          _extract_udf(e["max_per_doc"], e["ctx_chars"])("text", "case_name").alias("m"))
        ms = ms.select("doc_id", "year", "jurisdiction", F.explode("m").alias("x")).select(
            "doc_id", "year", "jurisdiction", "x.kind", "x.norm", "x.raw", "x.cnt", "x.ctx")
        ms.write.mode("overwrite").parquet(out + "/mentions_raw")
    allrows = spark.read.parquet(out + "/mentions_raw")
    mentions = allrows.where("kind = 'm'").drop("kind")
    defs = (allrows.where("kind = 'd'").groupBy(F.col("norm").alias("d_acr"), F.col("raw").alias("d_long"))
            .agg(F.countDistinct("doc_id").alias("d_docs")))

    # ------------------------------------------------------------ vocabulary
    with m.stage("entities_vocab"):
        vocab = (mentions.groupBy("norm").agg(F.countDistinct("doc_id").alias("df"), F.max_by("raw", "cnt").alias("surface"))
                 .where(F.col("df") >= e["min_df"]).orderBy(F.col("df").desc(), "norm").limit(e["max_vocab"]))
        vocab.write.mode("overwrite").parquet(out + "/vocab")
    vocab = spark.read.parquet(out + "/vocab")

    # ------------------------------------------------------------ resolution
    with m.stage("entities_resolve"):
        nperm, nbands = e["lsh_perm"], e["lsh_bands"]

        @F.pandas_udf(_LSH_SCHEMA)
        def lsh_udf(names: pd.Series) -> pd.Series:
            a, b = _params(nperm, seed=99)
            return pd.Series([band_hashes(minhash(char_shingles(n, 3), a, b), nbands).tolist() for n in names])

        bnd = vocab.select("norm", "df", F.posexplode(lsh_udf("norm")).alias("band", "bh"))
        buckets = bnd.groupBy("band", "bh").agg(F.collect_list("norm").alias("names")).where(F.size("names") > 1)
        mb, win = e["max_bucket"], e["window"]

        @F.udf(T.ArrayType(T.StructType([T.StructField("a", T.StringType()), T.StructField("b", T.StringType())])))
        def pair_udf(names):
            names = sorted(names)
            n = len(names)
            r = []
            if n <= mb:
                for i in range(n):
                    for j in range(i + 1, n):
                        r.append((names[i], names[j]))
            else:
                for i in range(n):
                    for j in range(i + 1, min(i + 1 + win, n)):
                        r.append((names[i], names[j]))
            return r

        cand_lsh = buckets.select(F.explode(pair_udf("names")).alias("p")).select("p.a", "p.b")

        # exact single-edit blocking (complements the probabilistic LSH, which can miss short names)
        @F.pandas_udf("array<string>")
        def del_udf(names: pd.Series) -> pd.Series:
            return pd.Series([deletion_keys(n) for n in names])

        del_buckets = (vocab.select("norm", F.explode(del_udf("norm")).alias("k")).groupBy("k")
                       .agg(F.collect_set("norm").alias("names")).where(F.size("names") > 1))
        cand_del = del_buckets.select(F.explode(pair_udf("names")).alias("p")).select("p.a", "p.b")
        cand = cand_lsh.unionByName(cand_del).distinct()

        @F.pandas_udf("boolean")
        def verify(a: pd.Series, b: pd.Series) -> pd.Series:
            return pd.Series([same_entity(x, y) for x, y in zip(a, b)])

        lsh_edges = cand.where(verify("a", "b"))

        # acronym <-> long form (acronym must be unique-ish: link to the most frequent long form only)
        @F.udf("array<string>")
        def acr_udf(n):
            toks = n.split()
            k1 = [t[0] for t in toks if t not in CONNECT]
            keys = {"".join(k1), "".join(t[0] for t in toks)}
            return [k for k in keys if len(toks) >= 2 and len(k) >= 3]

        longs = vocab.withColumn("acr", F.explode(acr_udf("norm"))).where(F.col("acr").isNotNull())
        shorts = (vocab.where(~F.col("norm").contains(" ") & (F.length("norm") >= 3) & (F.length("norm") <= 7))
                  .select(F.col("norm").alias("s_norm")))
        w = Window.partitionBy("acr").orderBy(F.col("df").desc(), "norm")
        best_long = (longs.withColumn("rk", F.row_number().over(w)).where("rk = 1")
                     .select(F.col("norm").alias("l_norm"), F.col("acr")))
        acr_edges = shorts.join(best_long, shorts.s_norm == best_long.acr).select(
            F.col("s_norm").alias("a"), F.col("l_norm").alias("b"))
        # definitional evidence: "Long Name (ACR)" seen in >= 2 opinions, both forms in the vocabulary
        vv = vocab.select("norm")
        def_edges = (defs.where(F.col("d_docs") >= 2)
                     .join(vv.select(F.col("norm").alias("v1")), F.col("d_acr") == F.col("v1"))
                     .join(vv.select(F.col("norm").alias("v2")), F.col("d_long") == F.col("v2"))
                     .select(F.col("d_acr").alias("a"), F.col("d_long").alias("b")))
        edges = lsh_edges.unionByName(acr_edges).unionByName(def_edges).distinct()
        edges.write.mode("overwrite").parquet(out + "/edges")

    edges = spark.read.parquet(out + "/edges")
    # attach each surface form to the most frequent verified neighbour (ties -> lexicographic); resolve chains on driver
    df_map = {r.norm: r.df for r in vocab.select("norm", "df").collect()}
    nbrs: dict[str, list[str]] = {}
    for r in edges.select("a", "b").collect():
        if r.a in df_map and r.b in df_map:
            nbrs.setdefault(r.a, []).append(r.b)
            nbrs.setdefault(r.b, []).append(r.a)
    parent: dict[str, str] = {}
    for x, ys in nbrs.items():
        better = [y for y in ys if df_map[y] > df_map[x] or (df_map[y] == df_map[x] and y < x)]
        if better:
            parent[x] = min(better, key=lambda y: (-df_map[y], y))

    def root(x):
        seen = 0
        while x in parent and seen < 100:
            x = parent[x]
            seen += 1
        return x

    roots = {n: root(n) for n in df_map}
    root_ids = {r: i for i, r in enumerate(sorted(set(roots.values()), key=lambda r: (-df_map[r], r)), start=1)}
    mp = pd.DataFrame({"norm": list(roots), "root_norm": list(roots.values())})
    mp["ent_id"] = mp.root_norm.map(root_ids).astype("int64")
    mp["df"] = mp.norm.map(df_map).astype("int64")
    mp_sdf = spark.createDataFrame(mp)
    mp_sdf.write.mode("overwrite").parquet(out + "/map")
    mp_sdf = spark.read.parquet(out + "/map")

    with m.stage("entities_finalize"):
        men = mentions.join(F.broadcast(mp_sdf.select("norm", "ent_id")), "norm")
        men = men.groupBy("doc_id", "ent_id", "year", "jurisdiction").agg(
            F.sum("cnt").alias("cnt"), F.first("ctx").alias("ctx"))
        men.write.mode("overwrite").partitionBy("year").parquet(out + "/mentions")
        men = spark.read.parquet(out + "/mentions")
        surf = vocab.select(F.col("norm").alias("vnorm"), "surface")
        canon = (mp_sdf.join(surf, mp_sdf.root_norm == surf.vnorm).select("ent_id", F.col("surface").alias("canonical")).distinct())
        variants = mp_sdf.groupBy("ent_id").agg(F.count("*").alias("n_variants"),
                                                F.collect_list(F.struct("df", "norm")).alias("v"))
        variants = variants.select("ent_id", "n_variants", F.expr("transform(array_sort(v, (l, r) -> case when l.df < r.df then 1 when l.df > r.df then -1 else 0 end), x -> x.norm)").alias("variants"))
        totals = men.groupBy("ent_id").agg(F.countDistinct("doc_id").alias("total_docs"))
        rootn = mp_sdf.where(F.col("norm") == F.col("root_norm")).select("ent_id", "root_norm")
        dim = totals.join(canon, "ent_id").join(variants, "ent_id").join(rootn, "ent_id")
        dim.write.mode("overwrite").parquet(out + "/dim")
    n_ent = spark.read.parquet(out + "/dim").count()
    m.record("entities_summary", "info", vocab=len(df_map), resolved_entities=n_ent, edges=edges.count())
    print(f"entities: {len(df_map):,} surface forms -> {n_ent:,} resolved entities")
