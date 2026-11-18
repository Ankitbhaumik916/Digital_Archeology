"""Synthetic CourtListener-style corpus with ground truth.

Why: lets us test every stage offline and *measure accuracy* (dedup, entity resolution, clustering,
temporal recovery) against known truth. Files mirror the CourtListener bulk schema so the real
ingest code path is exercised (multi-line quoted CSV, bz2, html vs plain text, OCR flag).

  python -m da.synth --docs 20000 --out data/raw --seed 7
"""
from __future__ import annotations

import argparse
import bz2
import csv
import io
import json
import math
import pathlib
import random
import re

TOPICS = {
    0: ("environmental regulation", "emission pollutant wetlands discharge permit remediation contamination toxic watershed "
        "emissions standard attainment hazardous cleanup habitat endangered ambient air water quality runoff superfund "
        "monitoring compliance pesticide effluent stormwater aquifer carbon ozone landfill".split()),
    1: ("criminal procedure", "warrant search seizure probable cause defendant sentence conviction indictment suppression "
        "confession counsel arraignment plea trial jury verdict acquittal parole probation custody interrogation "
        "miranda felony misdemeanor prosecutor bail habeas evidence".split()),
    2: ("labor and employment", "employee employer union bargaining strike wages grievance arbitration discrimination "
        "overtime workplace retaliation harassment seniority pension collective unfair practice picketing lockout "
        "workers safety hazard termination benefits hourly contract negotiation".split()),
    3: ("securities and finance", "securities investor fraud prospectus disclosure shareholder broker registration "
        "derivative offering merger fiduciary insider trading dividend bond underwriter portfolio liquidity credit "
        "lending mortgage bankruptcy solvency deposit capital equity audit".split()),
    4: ("intellectual property", "patent infringement trademark copyright invention claim licensing prior art "
        "damages injunction software algorithm royalty design counterfeit dilution originality fair use "
        "specification inventor prosecution obviousness validity trade secret".split()),
    5: ("immigration", "alien deportation removal asylum visa naturalization citizenship petitioner border "
        "refugee persecution admission inadmissible detention hearing immigration judge waiver residency "
        "permanent lawful status credible fear expedited appeal".split()),
    6: ("tax", "deduction taxpayer income revenue assessment deficiency refund audit exemption depreciation "
        "capital gain basis estate gift levy lien penalty withholding return liability fiscal corporate "
        "partnership allocation credit amortization".split()),
    7: ("antitrust and contracts", "monopoly competition antitrust restraint trade price fixing merger market "
        "contract breach warranty damages consideration indemnity insurance premium policy coverage claim "
        "exclusion subrogation liability negligence tort supplier distributor exclusive dealing".split()),
}
GENERIC = ("court plaintiff appellant appellee judgment statute provision record whether pursuant "
           "opinion decision argument reasoning holding precedent doctrine standard review discretion "
           "issue matter court district remand affirm reverse motion brief circuit finding conclusion "
           "authority purpose interest effect rule application question").split()
ERA_VERBS = {"early": ["regulated", "investigated", "enforced", "licensed", "supervised", "inspected"],
             "late": ["challenged", "deregulated", "settled", "outsourced", "litigated", "reinterpreted"]}

# canonical, topic, start_year, end_year, variants (spec -> resolvable surface forms)
E = lambda c, t, s, e=2030, acr=None, abbr=(): dict(canon=c, topic=t, start=s, end=e, acr=acr, abbr=list(abbr))  # noqa: E731
CORE = [
    E("Environmental Protection Agency", 0, 1970, acr="EPA", abbr=["U.S. Environmental Protection Agency"]),
    E("Army Corps of Engineers", 0, 1930),
    E("Clean Air Act", 0, 1970),
    E("Clean Water Act", 0, 1972),
    E("National Labor Relations Board", 2, 1935, acr="NLRB"),
    E("Occupational Safety and Health Administration", 2, 1971, acr="OSHA"),
    E("Equal Employment Opportunity Commission", 2, 1965, acr="EEOC"),
    E("Department of Labor", 2, 1950, abbr=["Dept. of Labor"]),
    E("Securities and Exchange Commission", 3, 1934, acr="SEC"),
    E("Federal Reserve Board", 3, 1950),
    E("Federal Deposit Insurance Corporation", 3, 1950, acr="FDIC", abbr=["Federal Deposit Insurance Corp."]),
    E("Financial Industry Regulatory Authority", 3, 2007, acr="FINRA"),
    E("Patent and Trademark Office", 4, 1950, abbr=["Patent & Trademark Office"]),
    E("Copyright Office", 4, 1950),
    E("Federal Trade Commission", 7, 1950, acr="FTC"),
    E("Department of Justice", 1, 1950, acr="DOJ", abbr=["Dept. of Justice"]),
    E("Federal Bureau of Investigation", 1, 1950, acr="FBI"),
    E("Drug Enforcement Administration", 1, 1973, acr="DEA"),
    E("Fourth Amendment", 1, 1950),
    E("Fifth Amendment", 1, 1950),
    E("Immigration and Naturalization Service", 5, 1950, 2003, acr="INS"),
    E("Board of Immigration Appeals", 5, 1950, acr="BIA"),
    E("Department of Homeland Security", 5, 2003, acr="DHS"),
    E("Internal Revenue Service", 6, 1953, acr="IRS"),
    E("Department of the Treasury", 6, 1950),
    E("State Farm Mutual Automobile Insurance Company", 7, 1950, abbr=["State Farm Mutual Automobile Insurance Co."]),
    # hard negatives: near-identical names that are DIFFERENT entities
    E("Department of Health", 2, 1950),
    E("Department of Housing", 6, 1965),
    E("Hartford Fire Insurance Company", 7, 1950),
    E("Hartford Life Insurance Company", 7, 1950),
    E("Northern Pacific Railway Company", 7, 1950),
    E("Northern Pacific Railroad Company", 7, 1950),
    E("Federal Communications Commission", 7, 1950, acr="FCC"),
    E("Federal Aviation Administration", 7, 1958, acr="FAA"),
]
SURN = "Acme Baxter Cordell Dunmore Everton Fairbanks Garrison Halloran Ingram Jameson Kessler Lockhart Marlowe Norwood Ostrander Prescott Quimby Radcliffe Sterling Thornton Underhill Vandermeer Whitmore Yardley Zellner".split()
IND = "Chemical Steel Pharmaceutical Energy Mining Software Airlines Shipping Foods Textile Telecom Motors Petroleum Semiconductor Rail Paper".split()
SUFF = ["Corporation", "Company", "Inc."]


def _fillers(rng, n):
    seen, out = set(), []
    while len(out) < n:
        c = f"{rng.choice(SURN)} {rng.choice(IND)} {rng.choice(SUFF)}"
        core = c.rsplit(" ", 1)[0]
        if core in seen:
            continue
        seen.add(core)
        t = rng.choice([0, 2, 3, 4, 6, 7])
        s = rng.choice([1950, 1960, 1975, 1985, 1995, 2005])
        out.append(E(c, t, s, s + rng.choice([25, 40, 80])))
    return out


def topic_weights(y):
    def sig(x, mid, w):
        return 1 / (1 + math.exp(-(x - mid) / w))
    w = {0: 0.02 + 0.16 * sig(y, 1972, 4), 1: 0.10 + 0.14 * math.exp(-((y - 1985) / 22) ** 2),
         2: 0.05 + 0.20 * math.exp(-((y - 1962) / 18) ** 2), 3: 0.04 + 0.16 * sig(y, 1995, 6),
         4: 0.02 + 0.22 * sig(y, 1988, 7), 5: 0.02 + 0.18 * sig(y, 2004, 4),
         6: 0.08 + 0.05 * math.exp(-((y - 1958) / 10) ** 2), 7: 0.16 - 0.09 * sig(y, 1990, 12)}
    s = sum(w.values())
    return {k: v / s for k, v in w.items()}


def ocr_noise(s: str, rng: random.Random, rate: float) -> str:
    subs = {"l": "1", "O": "0", "o": "0", "e": "c", "i": "l", "m": "rn", "n": "ii", "a": "o", "t": "f", "h": "b"}
    out = []
    for ch in s:
        r = rng.random()
        if r < rate and ch in subs:
            out.append(subs[ch])
        elif r < rate * 1.3 and ch.isalpha():
            continue  # dropped char
        else:
            out.append(ch)
    return "".join(out)


def typo(word: str, rng: random.Random) -> str:
    """One-edit typo on a long token (keeps first letter)."""
    toks = word.split(" ")
    cand = [i for i, t in enumerate(toks) if len(t) >= 7]
    if not cand:
        return word
    i = rng.choice(cand)
    t = toks[i]
    p = rng.randrange(1, len(t) - 1)
    kind = rng.choice(["sub", "del", "ins"])
    if kind == "sub":
        t = t[:p] + rng.choice("aeioulnrst0") + t[p + 1:]
    elif kind == "del":
        t = t[:p] + t[p + 1:]
    else:
        t = t[:p] + rng.choice("aeiou") + t[p:]
    toks[i] = t
    return " ".join(toks)


def make_doc(rng, year, topic, ents, forms_log):
    words = TOPICS[topic][1]
    era = "early" if year < 1980 else "late"
    defined: set = set()
    n_sent = rng.randint(28, 75)
    chosen = []
    if ents:
        k = rng.choice([1, 1, 2, 2, 3])
        pool = [e for e in ents if e["topic"] == topic and e["start"] <= year <= e["end"]]
        others = [e for e in ents if e["start"] <= year <= e["end"]]
        for _ in range(k):
            src = pool if (pool and rng.random() < 0.75) else others
            if src:
                chosen.append(rng.choice(src))
    ment_at = {rng.randrange(n_sent): e for e in chosen for _ in range(rng.randint(1, 4))}
    sents = []
    for i in range(n_sent):
        t1, t2, t3 = (rng.choice(words) for _ in range(3))
        g1, g2 = rng.choice(GENERIC), rng.choice(GENERIC)
        if i in ment_at:
            e = ment_at[i]
            r = rng.random()
            if e["acr"] and r < 0.22:
                form = e["acr"]
            elif e["abbr"] and r < 0.40:
                form = rng.choice(e["abbr"])
            elif r < 0.52:
                form = "the " + e["canon"] if i % 2 else e["canon"]
            elif r < 0.62:
                form = typo(e["canon"], rng)
            else:
                form = e["canon"]
            if e["acr"] and form == e["canon"] and e["canon"] not in defined and rng.random() < 0.6:
                form = f'{e["canon"]} ({e["acr"]})'          # first mention defines the acronym, as real opinions do
                defined.add(e["canon"])
            forms_log.setdefault(e["canon"], set()).add(form.split(" (")[0])
            verb = rng.choice(ERA_VERBS[era])
            s = rng.choice([
                f"The {form} {verb} the {t1} pursuant to the {t2} {g1}.",
                f"Under the {t1} {t2}, the {form} {verb} each {g1} and the {t3}.",
                f"Petitioner argued that the {form} had {verb} the {t1} without a {g1} {t2}.",
            ]).replace("The the ", "The ").replace("the the ", "the ")
        else:
            s = rng.choice([
                f"The {g1} considered whether the {t1} constituted a {t2} under the {t3} {g2}.",
                f"We hold that the {t1} {g1} was consistent with the {t2} and the {g2}.",
                f"On this {g1}, the {t1} and the {t2} require that the {t3} be reviewed.",
                f"The {g1} of the {t1} {g2} is not disputed by the {t2}.",
                f"Because the {t1} {t2} is a {g1} of {t3}, the {g2} must be affirmed.",
            ])
        if rng.random() < 0.03:
            s += f" See {rng.randint(1, 900)} F.{rng.choice(['2d', '3d'])} {rng.randint(1, 1200)} ({rng.randint(1950, year)})."
        sents.append(s)
    # paragraph breaks
    text, para = [], []
    for s in sents:
        para.append(s)
        if len(para) >= rng.randint(3, 6):
            text.append(" ".join(para))
            para = []
    if para:
        text.append(" ".join(para))
    return sents, "\n\n".join(text)


def hyphenate(text: str, rng, p=0.02):
    """Simulate OCR line-break hyphenation: 'government' -> 'govern-\\nment'."""
    def f(m):
        w = m.group(0)
        if rng.random() < p and len(w) > 8:
            k = rng.randrange(3, len(w) - 3)
            return w[:k] + "-\n" + w[k:]
        return w
    return re.sub(r"[a-z]{6,}", f, text)


def to_html(text: str) -> str:
    esc = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    paras = "".join(f'<p id="p{i}">{p.replace(chr(10), " ")}</p>' for i, p in enumerate(esc.split("\n\n")))
    return f'<div class="opinion"><h2>OPINION</h2>{paras}<span class="footnote">*1</span></div>'


def near_dup(text: str, rng) -> str:
    sents = text.split(". ")
    for _ in range(rng.randint(1, 3)):
        if len(sents) > 10:
            sents.pop(rng.randrange(len(sents)))
    t = ". ".join(sents)
    t = f"SUPREME COURT OF THE STATE\n\nReporter page {rng.randint(1, 999)}\n\n" + t + f"\n\n[{rng.randint(1, 500)}]\n"
    return ocr_noise(t, rng, 0.004)


def generate(n_docs: int, out: pathlib.Path, seed: int = 7, bz2_opinions: bool = True,
             y0: int = 1950, y1: int = 2020, ocr_frac: float = 0.15):
    rng = random.Random(seed)
    out.mkdir(parents=True, exist_ok=True)
    truth_dir = out.parent / "raw_truth"
    truth_dir.mkdir(parents=True, exist_ok=True)
    ents = CORE + _fillers(rng, 60)
    years = list(range(y0, y1 + 1))
    ywts = [1 + 2.5 * (y - y0) / (y1 - y0) for y in years]

    courts = [("scotus", "Supreme Court of the United States", "F"), ("ca1", "First Circuit", "F"),
              ("ca5", "Fifth Circuit", "F"), ("ca9", "Ninth Circuit", "F"), ("nysd", "SDNY", "FD"),
              ("txnd", "ND Texas", "FD"), ("cand", "ND California", "FD"), ("ny", "New York Court of Appeals", "S"),
              ("cal", "California Supreme Court", "S"), ("texapp", "Texas Court of Appeals", "SA"),
              ("calctapp", "California Court of Appeal", "SA")]
    with open(out / "courts-2026-01-01.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "pacer_court_id", "pacer_has_rss_feed", "full_name", "short_name", "jurisdiction", "position", "citation_string", "in_use"])
        for i, (cid, name, jur) in enumerate(courts):
            w.writerow([cid, "", "", name, name, jur, i, "", "t"])

    op_fields = ["id", "date_created", "date_modified", "author_str", "per_curiam", "joined_by_str", "type", "sha1", "page_count",
                 "download_url", "local_path", "plain_text", "html", "html_lawbox", "html_columbia", "html_anon_2020",
                 "xml_harvard", "html_with_citations", "extracted_by_ocr", "author_id", "cluster_id"]
    cl_fields = ["id", "date_created", "date_modified", "judges", "date_filed", "date_filed_is_approximate", "slug",
                 "case_name_short", "case_name", "case_name_full", "source", "docket_id", "precedential_status", "citation_count"]
    dk_fields = ["id", "date_created", "date_modified", "court_id", "case_name", "docket_number", "source"]

    op_path = out / ("opinions-2026-01-01.csv.bz2" if bz2_opinions else "opinions-2026-01-01.csv")
    op_raw = bz2.open(op_path, "wt", newline="") if bz2_opinions else open(op_path, "w", newline="")
    fc = open(out / "opinion-clusters-2026-01-01.csv", "w", newline="")
    fd = open(out / "dockets-2026-01-01.csv", "w", newline="")
    wo, wc, wd = csv.writer(op_raw), csv.writer(fc), csv.writer(fd)
    wo.writerow(op_fields); wc.writerow(cl_fields); wd.writerow(dk_fields)

    forms_log: dict[str, set] = {}
    truth = []
    next_id = 1

    def emit(text, year, kind, group, topic, ocr, as_html, parties):
        nonlocal next_id
        oid = cid = did = next_id
        next_id += 1
        court = rng.choice(courts)[0]
        date = f"{year}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"
        case = f"{parties[0]} v. {parties[1]}"
        wd.writerow([did, date, date, court, case, f"{rng.randint(1, 99)}-{rng.randint(100, 9999)}", 1])
        wc.writerow([cid, date, date, "", date, "f", "slug", parties[0], case, case, "C", did, "Published", 0])
        plain, html = ("", to_html(text)) if as_html else (text, "")
        wo.writerow([oid, date, date, "", "f", "", "020lead", "", "", "", "", plain, "", "", "", "", "", html,
                     "t" if ocr else "f", "", cid])
        truth.append((oid, group, kind, topic, year))
        return oid

    prev = None
    for _ in range(n_docs):
        year = rng.choices(years, ywts)[0]
        tw = topic_weights(year)
        topic = rng.choices(list(tw), list(tw.values()))[0]
        sents, text = make_doc(rng, year, topic, ents, forms_log)
        r = rng.random()
        pool = [e for e in ents if e["start"] <= year <= e["end"]]
        p1, p2 = rng.choice(pool)["canon"] if rng.random() < .5 else rng.choice(SURN) + " Holdings", rng.choice(SURN) + " " + rng.choice(IND)
        if r < 0.02:                                     # junk / garbage OCR
            junk = "".join(rng.choice("abcdefghijklmnopqrstuvwxyz0123456789 |~^ ") for _ in range(rng.randint(3000, 6000)))
            oid = emit(junk, year, "junk", -1, -1, True, False, (p1, p2))
            truth[-1] = (oid, oid, "junk", -1, year)
            continue
        if r < 0.04:                                     # too short
            oid = emit(" ".join(text.split()[:40]), year, "short", -1, topic, False, False, (p1, p2))
            truth[-1] = (oid, oid, "short", topic, year)
            continue
        ocr = rng.random() < ocr_frac
        body = hyphenate(ocr_noise(text, rng, rng.choice([0.004, 0.01, 0.02])), rng) if ocr else text
        as_html = (not ocr) and rng.random() < 0.15
        oid = emit(body, year, "orig", -1, topic, ocr, as_html, (p1, p2))
        truth[-1] = (oid, oid, "orig", topic, year)
        d = rng.random()
        if d < 0.06:                                     # exact duplicate (different source record)
            emit(body, year, "exact", oid, topic, ocr, as_html, (p1, p2))
        elif d < 0.16:                                   # near duplicate (re-scan / reformatted)
            emit(near_dup(text, rng), year, "near", oid, topic, True, False, (p1, p2))
    for h in (op_raw, fc, fd):
        h.close()

    with open(truth_dir / "docs.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["opinion_id", "group_id", "kind", "topic", "year"])
        w.writerows(truth)
    with open(truth_dir / "entities.json", "w") as f:
        json.dump({"forms": {k: sorted(v) for k, v in forms_log.items()},
                   "entities": [{k: e[k] for k in ("canon", "topic", "start", "end")} for e in ents],
                   "topics": {k: v[0] for k, v in TOPICS.items()},
                   "topic_weights": {y: topic_weights(y) for y in range(y0, y1 + 1)}}, f)
    print(f"wrote {n_docs} base docs ({next_id - 1} opinions incl. duplicates) to {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=20000)
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--plain", action="store_true", help="do not bz2-compress opinions")
    a = ap.parse_args()
    generate(a.docs, pathlib.Path(a.out), a.seed, not a.plain)
