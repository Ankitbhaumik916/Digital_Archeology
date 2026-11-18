"""Digital Archaeology dashboard: watch an entity or a topic evolve across decades of court opinions.

  streamlit run dashboard/app.py          (reads marts from $DA_EXPORT or data/export)

All numbers were computed by the Spark/MLlib pipeline over the full archive. The "Live Spark query" panel
re-runs a distributed query against the data lake from the browser to show the pipeline is live, not a static chart.
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import sys
import time

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
EXPORT = pathlib.Path(os.environ.get("DA_EXPORT", ROOT / "data" / "export"))
JUR = {"F": "Federal appellate", "FD": "Federal district", "FB": "Federal bankruptcy", "FS": "Federal special",
       "S": "State supreme", "SA": "State appellate", "ST": "State trial", "SS": "State special", "U": "Unknown"}

st.set_page_config(page_title="Digital Archaeology", page_icon="🏛️", layout="wide")


@st.cache_data(show_spinner=False)
def load(name: str) -> pd.DataFrame:
    return pd.read_parquet(EXPORT / f"{name}.parquet")


def have_data() -> bool:
    return (EXPORT / "entities.parquet").exists()


def decade(y):
    return (y // 10) * 10


# ----------------------------------------------------------------------------------------------- sidebar
st.sidebar.title("🏛️ Digital Archaeology")
st.sidebar.caption("Entity & topic evolution in a large text archive. Hadoop/HDFS → Spark ETL → GPU embeddings → Spark MLlib.")
page = st.sidebar.radio("View", ["Entity evolution", "Topic evolution", "Pipeline & benchmarks"], label_visibility="collapsed")

if page != "Pipeline & benchmarks" and not have_data():
    st.error(f"No marts found in `{EXPORT}`. Run the pipeline (`python -m da.pipeline run`) first.")
    st.stop()

if page != "Pipeline & benchmarks":
    docs_year = load("docs_year")
    jur_all = sorted(docs_year.jurisdiction.unique())
    jsel = st.sidebar.multiselect("Court level", jur_all, default=jur_all, format_func=lambda j: f"{JUR.get(j, j)} ({j})")
    ymin, ymax = int(docs_year.year.min()), int(docs_year.year.max())
    yr = st.sidebar.slider("Years", ymin, ymax, (ymin, ymax))
    smooth = st.sidebar.slider("Smoothing (years)", 1, 9, 3, step=2)
    dy = docs_year[docs_year.jurisdiction.isin(jsel) & docs_year.year.between(*yr)].groupby("year", as_index=False).n_docs.sum()
    st.sidebar.metric("Opinions in view", f"{int(dy.n_docs.sum()):,}")


def per10k(counts: pd.DataFrame, label_col="label"):
    """counts: year, n_docs, label -> mentions per 10,000 opinions filed that year (in the selected courts)."""
    m = counts.merge(dy.rename(columns={"n_docs": "total"}), on="year", how="right")
    out = []
    for lab, g in m.groupby(label_col):
        g = g.set_index("year").reindex(dy.year)
        g["n_docs"] = g.n_docs.fillna(0)
        g["rate"] = g.n_docs / dy.set_index("year").n_docs.clip(lower=1) * 10_000
        g["rate"] = g.rate.rolling(smooth, center=True, min_periods=1).mean()
        g["label"] = lab
        out.append(g.reset_index()[["year", "n_docs", "rate", "label"]])
    return pd.concat(out) if out else pd.DataFrame(columns=["year", "n_docs", "rate", "label"])


# ----------------------------------------------------------------------------------------------- entity view
if page == "Entity evolution":
    ents, topics = load("entities"), load("topics").set_index("topic_id")
    ents = ents.sort_values("total_docs", ascending=False)
    opts = {int(r.ent_id): f"{r.canonical}  ·  {int(r.total_docs):,} opinions" for r in ents.itertuples()}
    st.title("How does an entity's presence and framing change over time?")
    c1, c2 = st.columns([3, 2])
    ent_id = c1.selectbox("Entity (agency, statute, company…)", list(opts), format_func=opts.get, index=0, key="ent")
    cmp_ids = c2.multiselect("Compare with", [i for i in opts if i != ent_id], format_func=opts.get, max_selections=4)
    row = ents[ents.ent_id == ent_id].iloc[0]

    ey = load("entity_year")
    ey = ey[ey.jurisdiction.isin(jsel) & ey.year.between(*yr)]
    names = {int(r.ent_id): r.canonical for r in ents.itertuples()}
    sel = ey[ey.ent_id.isin([ent_id] + cmp_ids)].groupby(["ent_id", "year"], as_index=False).n_docs.sum()
    sel["label"] = sel.ent_id.map(names)
    series = per10k(sel[["year", "n_docs", "label"]])

    mine = sel[sel.ent_id == ent_id]
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Opinions mentioning", f"{int(mine.n_docs.sum()):,}")
    k2.metric("First seen", int(mine.year.min()) if len(mine) else "–")
    peak = series[series.label == names[ent_id]]
    k3.metric("Peak year", int(peak.loc[peak.rate.idxmax(), "year"]) if len(peak) and peak.rate.max() > 0 else "–")
    k4.metric("Spelling variants merged", len(row.variants))
    with st.expander("Resolved surface forms (entity resolution output)"):
        st.write(", ".join(f"`{v}`" for v in row.variants))

    fig = px.line(series, x="year", y="rate", color="label", labels={"rate": "mentions per 10,000 opinions", "year": ""})
    fig.update_layout(height=340, margin=dict(t=30, b=10), legend=dict(orientation="h", y=-0.2, title=None))
    st.subheader("Presence")
    st.plotly_chart(fig, width="stretch")

    left, right = st.columns(2)
    et = load("entity_topic_decade")
    et = et[(et.ent_id == ent_id)]
    et = et.assign(share=et.n_docs / et.groupby("decade").n_docs.transform("sum"),
                   topic=et.topic_id.map(lambda t: f"{t}: {topics.label.get(t, t)}"))
    et = et[et.decade.between(decade(yr[0]), decade(yr[1]))]
    with left:
        st.subheader("Which legal areas it appears in")
        if len(et):
            f2 = px.bar(et, x="decade", y="share", color="topic", labels={"share": "share of its opinions", "decade": ""})
            f2.update_layout(height=380, margin=dict(t=10, b=10), legend=dict(orientation="h", y=-0.35, title=None))
            st.plotly_chart(f2, width="stretch")
    with right:
        st.subheader("How it is framed (distinctive nearby words)")
        cx = load("entity_context_decade")
        cx = cx[(cx.ent_id == ent_id) & cx.decade.between(decade(yr[0]), decade(yr[1]))]
        if len(cx):
            keep = cx.sort_values("score", ascending=False).groupby("decade").head(3).term.unique()
            hm = cx[cx.term.isin(keep)].pivot_table(index="term", columns="decade", values="score", fill_value=0)
            hm = hm.loc[hm.idxmax(axis=1).sort_values().index] if len(hm) else hm
            f3 = px.imshow(hm, aspect="auto", color_continuous_scale="Blues", labels=dict(color="distinctiveness"))
            f3.update_layout(height=380, margin=dict(t=10, b=10), coloraxis_showscale=False)
            f3.update_xaxes(type="category")
            st.plotly_chart(f3, width="stretch")
        else:
            st.info("Not enough context text for this entity in the selected range.")

    st.subheader("In their own words")
    ex = load("examples")
    ex = ex[(ex.ent_id == ent_id) & ex.jurisdiction.isin(jsel) & ex.year.between(*yr)].sort_values("year")
    if len(ex):
        dsel = st.select_slider("Decade", sorted(decade(ex.year).unique()), value=sorted(decade(ex.year).unique())[0],
                                format_func=lambda d: f"{d}s")
        pat = re.compile("|".join(re.escape(v) for v in sorted(row.variants, key=len, reverse=True)[:6]), re.I) if len(row.variants) else None
        for r in ex[decade(ex.year) == dsel].head(6).itertuples():
            txt = pat.sub(lambda m: f"**{m.group(0)}**", r.ctx) if pat else r.ctx
            st.markdown(f"> …{txt}…  \n> <sub>{r.case_name} · {r.year} · {JUR.get(r.jurisdiction, r.jurisdiction)}</sub>", unsafe_allow_html=True)

    with st.expander("⚡ Run this as a live Spark job over the full data lake"):
        st.caption("Executes a distributed query on the partitioned parquet lake right now (not a cached chart).")
        if st.button("Run live query"):
            try:
                from pyspark.sql import functions as F
                from da.common import P, get_spark, load_config
                cfg = load_config()

                @st.cache_resource
                def _spark():
                    return get_spark(cfg, "dashboard-live")
                t0 = time.perf_counter()
                df = (_spark().read.parquet(P(cfg, "entities/mentions")).where(F.col("ent_id") == int(ent_id))
                      .groupBy("year").agg(F.countDistinct("doc_id").alias("opinions")).orderBy("year"))
                pdf = df.toPandas()
                dt = time.perf_counter() - t0
                st.success(f"Spark returned {len(pdf)} year-buckets in {dt:.2f}s")
                st.plotly_chart(px.bar(pdf, x="year", y="opinions").update_layout(height=260, margin=dict(t=10)), width="stretch")
            except Exception as e:  # pragma: no cover
                st.warning(f"Live Spark query unavailable here: {e}")

# ----------------------------------------------------------------------------------------------- topic view
elif page == "Topic evolution":
    topics = load("topics").set_index("topic_id")
    ty = load("topic_year")
    ty = ty[ty.jurisdiction.isin(jsel) & ty.year.between(*yr)].groupby(["topic_id", "year"], as_index=False).n_docs.sum()
    ty["label"] = ty.topic_id.map(lambda t: f"{t}: {topics.label[t]}")
    st.title("How do legal topics rise and fall?")
    allr = per10k(ty[["year", "n_docs", "label"]])
    allr["share"] = allr.rate / 100  # per 10k -> percent of opinions
    f = px.area(allr, x="year", y="share", color="label", labels={"share": "% of opinions", "year": ""}, groupnorm=None)
    f.update_layout(height=380, margin=dict(t=10, b=10), legend=dict(orientation="h", y=-0.3, title=None))
    st.plotly_chart(f, width="stretch")
    t_sel = st.selectbox("Topic detail", list(topics.index), key="topic", format_func=lambda t: f"{t}: {topics.label[t]}  ·  {int(topics.n_docs[t]):,} opinions")
    c1, c2 = st.columns([2, 3])
    with c1:
        st.markdown("**Keywords (c-TF-IDF)**")
        st.write(", ".join(topics.keywords[t_sel]))
        ents = load("entities").set_index("ent_id")
        et = load("entity_topic_decade")
        top = et[et.topic_id == t_sel].groupby("ent_id").n_docs.sum().sort_values(ascending=False).head(10)
        st.markdown("**Most associated entities**")
        st.dataframe(pd.DataFrame({"entity": [ents.canonical.get(i, i) for i in top.index], "opinions": top.values}), hide_index=True, width="stretch")
    with c2:
        one = allr[allr.label == f"{t_sel}: {topics.label[t_sel]}"]
        st.plotly_chart(px.line(one, x="year", y="share", labels={"share": "% of opinions", "year": ""}).update_layout(height=300, margin=dict(t=10)),
                        width="stretch")

# ----------------------------------------------------------------------------------------------- pipeline view
else:
    st.title("Pipeline instrumentation")
    st.caption("Every stage is timed; Spark task metrics are split into compute / shuffle / GC; GPU time is CUDA-synchronised.")
    mf = ROOT / "bench" / "metrics.jsonl"
    if not mf.exists():
        st.info("No benchmark data yet.")
        st.stop()
    from da.bench_report import CATEGORY, latest_per_scale, load as load_metrics
    df = latest_per_scale(load_metrics())
    df = df[df.stage.isin(CATEGORY)]
    df["category"] = df.stage.map(CATEGORY)
    scales = sorted(df.scale.unique(), key=float)
    sc = st.select_slider("Data scale", scales, value=scales[-1])
    d = df[df.scale == sc]
    g = d.groupby("category", as_index=False).wall_s.sum()
    a, b = st.columns(2)
    a.plotly_chart(px.bar(d, x="wall_s", y="stage", color="category", orientation="h", labels={"wall_s": "wall-clock s"}).update_layout(height=420, yaxis=dict(autorange="reversed")), width="stretch")
    b.plotly_chart(px.pie(g, names="category", values="wall_s", hole=.5).update_layout(height=420), width="stretch")
    if len(scales) > 1:
        allc = df.groupby(["scale", "category"], as_index=False).wall_s.sum()
        allc["scale"] = allc.scale.astype(float)
        st.plotly_chart(px.line(allc.sort_values("scale"), x="scale", y="wall_s", color="category", markers=True, log_x=True, log_y=True,
                                labels={"wall_s": "wall-clock s", "scale": "data scale"}), width="stretch")
    acc = ROOT / "bench" / "accuracy.json"
    if acc.exists():
        st.subheader("Accuracy vs. synthetic ground truth")
        st.json(json.loads(acc.read_text()))
