"""
Data Center Siting Analysis — Streamlit Dashboard
Run from repo root: streamlit run app/app.py
"""

import json
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path

# ── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="US Data Center Siting Analysis",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded",
)

ROOT = Path(__file__).resolve().parent.parent

# ── Cached loaders ───────────────────────────────────────────────────────────

@st.cache_data
def load_ranking():
    df = pd.read_csv(
        ROOT / "data_revealed/04_tables/county_attractiveness_ranking.csv",
        index_col="rank",
    )
    df["fips_str"] = df["county_fips"].astype(str).str.zfill(5)
    return df


@st.cache_data
def load_preprocessed():
    df = pd.read_csv(ROOT / "data_revealed/04_tables/county_preprocessed.csv")
    ID_COLS = ["county_key", "county_fips", "county", "state", "num_datacenters"]
    feat_cols = [c for c in df.columns if c not in ID_COLS]
    return df, feat_cols


@st.cache_data
def load_shap():
    df = pd.read_csv(ROOT / "data_revealed/04_tables/shap_values.csv")
    return df.set_index("county_key")


@st.cache_data
def load_geojson():
    with open(ROOT / "app/counties.geojson") as f:
        return json.load(f)


# ── SHAP bar chart helper ────────────────────────────────────────────────────

def shap_bar(shap_vals, feature_names, title, top_n=15):
    idx  = np.argsort(np.abs(shap_vals))[-top_n:]
    y    = [feature_names[i] for i in idx]
    x    = [float(shap_vals[i]) for i in idx]
    cols = ["#ef5350" if v > 0 else "#42a5f5" for v in x]

    fig = go.Figure(go.Bar(
        x=x, y=y, orientation="h",
        marker_color=cols,
        hovertemplate="%{y}: %{x:.4f}<extra></extra>",
    ))
    fig.add_vline(x=0, line_width=1, line_color="#444")
    fig.update_layout(
        title=dict(text=title, font=dict(size=13)),
        xaxis_title="SHAP value",
        height=480,
        margin=dict(l=10, r=10, t=45, b=10),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="#eeeeee"),
    )
    return fig


# ── Load data ────────────────────────────────────────────────────────────────

ranking            = load_ranking()
prep_df, FEAT_COLS = load_preprocessed()
shap_df            = load_shap()

# Derive feature name lists from SHAP CSV columns (strips "s1_" / "s2_" prefix)
_s1_cols  = [c for c in shap_df.columns if c.startswith("s1_")]
_s2_cols  = [c for c in shap_df.columns if c.startswith("s2_")]
S1_FEATS  = [c[3:] for c in _s1_cols]
S2_FEATS  = [c[3:] for c in _s2_cols]


# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🏢 DC Siting Analysis")
    st.markdown("County-level predictive model for data center attractiveness across 3,138 US counties.")
    st.markdown("---")
    page = st.radio(
        "Navigate",
        ["📊 Overview", "🗺️ National Map", "🔍 County Explorer", "🌱 Emerging Markets"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown(
        "**Model**: Two-part Hurdle (LightGBM)  \n"
        "**Features**: 37 structural indicators  \n"
        "**Stage 1 AUC**: 0.847  \n"
        "**Median AE**: 0.052"
    )
    st.markdown("---")
    st.markdown(
        "[📄 GitHub](https://github.com/RX67-RX67/data-center-siting-analysis)"
    )


# ════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Overview
# ════════════════════════════════════════════════════════════════════════════

if page == "📊 Overview":
    st.title("US Data Center Siting Analysis")
    st.markdown(
        "A structural analysis of which county-level factors drive data center location "
        "decisions across all 3,138 US counties. The primary model is a **two-part hurdle model** "
        "(LightGBM) that separately estimates the probability of *any* DC presence and the "
        "expected scale given presence — enabling distinct SHAP interpretations for each question."
    )

    # ── KPI cards ──
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Counties Analyzed", "3,138")
    c2.metric("Counties with DCs", "767  (24.4%)")
    c3.metric("Structural Features", "37")
    c4.metric("Top County", "Loudoun, VA  (304 DCs)")

    st.markdown("---")

    # ── Model comparison ──
    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.subheader("Model Performance")
        perf = pd.DataFrame({
            "Metric": [
                "Tweedie Deviance",
                "MAE (log1p scale)",
                "RMSE (log1p scale)",
                "AUC — has any DC",
                "Median AE (raw)",
            ],
            "Option 1 — Tweedie": [1.756, 0.312, 0.474, 0.807, 0.185],
            "Option 2 — Hurdle ✓": [1.965, 0.279, 0.524, 0.833, 0.052],
        }).set_index("Metric")
        st.dataframe(perf, width="stretch")
        st.caption(
            "Option 2 (Hurdle) is the primary model: +2.6 pp AUC and 4× lower "
            "median absolute error, at the cost of slightly higher RMSE."
        )

    with col_right:
        st.subheader("Target Variable Distribution")
        bins   = [0, 0.001, 1, 5, 10, 50, 100, 500]
        labels = ["= 0", "(0, 1)", "[1, 5)", "[5, 10)", "[10, 50)", "[50, 100)", "≥ 100"]
        y = ranking["num_datacenters"]
        counts = pd.cut(y, bins=bins, labels=labels, right=False).value_counts(sort=False)
        fig_dist = px.bar(
            x=counts.index.astype(str),
            y=counts.values,
            labels={"x": "num_datacenters", "y": "County count"},
            color=counts.values,
            color_continuous_scale="Blues",
        )
        fig_dist.update_layout(
            showlegend=False, coloraxis_showscale=False,
            margin=dict(l=10, r=10, t=10, b=10),
            height=280, plot_bgcolor="white", paper_bgcolor="white",
        )
        st.plotly_chart(fig_dist, width="stretch")
        st.caption("76.1% of counties have zero data centers — the defining modeling challenge.")

    st.markdown("---")

    # ── Key findings ──
    st.subheader("Key Findings")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Stage 1 — What drives PRESENCE?**")
        findings_s1 = pd.DataFrame({
            "Rank": ["1", "2", "3", "4", "5–8"],
            "Feature": [
                "clean_energy_jobs",
                "grid_infrastructure_jobs",
                "land_value_1_4_acre_standardized",
                "wage_information",
                "NRI hazard risk indices",
            ],
            "Interpretation": [
                "Power sector as grid capacity proxy",
                "Grid infrastructure prerequisite",
                "Economic density, not just land cost",
                "IT labor market depth",
                "⚠️ Geographic confounders — not causal",
            ],
        }).set_index("Rank")
        st.dataframe(findings_s1, width="stretch")

    with col_b:
        st.markdown("**Stage 2 — What drives SCALE?**")
        findings_s2 = pd.DataFrame({
            "Rank": ["1", "2", "3", "4", "5+"],
            "Feature": [
                "wage_information",
                "grid_infrastructure_jobs",
                "clean_energy_jobs",
                "commercial_price",
                "NRI hazard indices",
            ],
            "Interpretation": [
                "↑ from rank 4 — agglomeration effect",
                "Stable across both stages",
                "Stable across both stages",
                "↑ from rank 9 — cost at scale",
                "Largely disappear (confounding confirmed)",
            ],
        }).set_index("Rank")
        st.dataframe(findings_s2, width="stretch")

    st.info(
        "**Key insight**: Hazard risk indices (lightning, hurricane, tornado) appear as top "
        "presence predictors because the existing DC market happens to be in geographically "
        "risky areas (Virginia, Texas, Georgia). They drop from the top 10 to ranks 20–30 "
        "in Stage 2, confirming this is correlation, not causation."
    )


# ════════════════════════════════════════════════════════════════════════════
# PAGE 2 — National Map
# ════════════════════════════════════════════════════════════════════════════

elif page == "🗺️ National Map":
    st.title("National Attractiveness Map")
    st.markdown(
        "Color each county by its model score. Hover for full profile. "
        "Score = **P(has DC) × E[scale | DC present]**."
    )

    # Controls
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        color_col = st.selectbox(
            "Color by",
            ["attractiveness", "p_presence", "expected_scale", "num_datacenters"],
            format_func=lambda x: {
                "attractiveness":  "Attractiveness score  (P × E[scale])",
                "p_presence":      "P(presence)  — Stage 1",
                "expected_scale":  "Expected scale  — Stage 2",
                "num_datacenters": "Actual data centers (current)",
            }[x],
        )
    with col2:
        show = st.selectbox(
            "Show counties",
            ["All counties", "Zero-DC only", "DC counties only"],
        )
    with col3:
        log_scale = st.checkbox("Log scale", value=True)

    plot_df = ranking.copy()
    if show == "Zero-DC only":
        plot_df = plot_df[plot_df["num_datacenters"] == 0]
    elif show == "DC counties only":
        plot_df = plot_df[plot_df["num_datacenters"] > 0]

    color_vals = np.log1p(plot_df[color_col]) if log_scale else plot_df[color_col]
    color_label = f"log1p({color_col})" if log_scale else color_col

    geojson = load_geojson()
    fig_map = px.choropleth(
        plot_df.assign(_c=color_vals),
        geojson=geojson,
        locations="fips_str",
        color="_c",
        color_continuous_scale="Plasma",
        scope="usa",
        hover_name="county",
        hover_data={
            "state":           True,
            "num_datacenters": ":.1f",
            "p_presence":      ":.3f",
            "expected_scale":  ":.2f",
            "attractiveness":  ":.3f",
            "fips_str":        False,
            "_c":              False,
        },
        labels={"_c": color_label},
    )
    fig_map.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        coloraxis_colorbar=dict(title=color_label, thickness=14, len=0.7),
        geo=dict(bgcolor="rgba(0,0,0,0)"),
        height=580,
    )
    st.plotly_chart(fig_map, width="stretch")
    st.caption(f"Showing {len(plot_df):,} counties.")


# ════════════════════════════════════════════════════════════════════════════
# PAGE 3 — County Explorer
# ════════════════════════════════════════════════════════════════════════════

elif page == "🔍 County Explorer":
    st.title("County Explorer")
    st.markdown(
        "Select any US county to see its structural profile and per-feature SHAP attribution "
        "for both model stages."
    )

    col1, col2 = st.columns(2)
    with col1:
        sel_state = st.selectbox("State", sorted(ranking["state"].unique()))
    with col2:
        county_options = sorted(
            ranking[ranking["state"] == sel_state]["county"].unique()
        )
        sel_county = st.selectbox("County", county_options)

    # Look up the county
    mask = (ranking["state"] == sel_state) & (ranking["county"] == sel_county)
    row  = ranking[mask].iloc[0]
    rank_val = int(row.name)

    # ── Profile cards ──
    st.markdown("---")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Current DCs",      f"{row['num_datacenters']:.1f}")
    c2.metric("P(Presence)",      f"{row['p_presence']:.3f}")
    c3.metric("Expected Scale",   f"{row['expected_scale']:.2f}")
    c4.metric("Attractiveness",   f"{row['attractiveness']:.3f}")
    c5.metric("National Rank",    f"#{rank_val:,} / 3,138")

    # Status badge
    if row["num_datacenters"] > 0:
        st.success(
            f"✅ **{sel_county}, {sel_state}** currently has data center activity "
            f"({row['num_datacenters']:.1f} estimated DCs)."
        )
    elif row["p_presence"] >= 0.5:
        st.warning(
            f"⚡ **{sel_county}, {sel_state}** has **no current DCs** but the model rates it "
            f"as structurally ready — P(presence) = {row['p_presence']:.3f}."
        )
    else:
        st.info(
            f"ℹ️ **{sel_county}, {sel_state}** has no current data centers "
            f"(P(presence) = {row['p_presence']:.3f})."
        )

    # ── SHAP ──
    st.markdown("---")
    st.subheader("SHAP Feature Attribution")
    st.markdown(
        "🔴 Red bars push the prediction **up** &nbsp; | &nbsp; 🔵 Blue bars push the prediction **down**"
    )

    county_key = f"{sel_state}||{sel_county}"

    if county_key not in shap_df.index:
        st.warning("No SHAP data found for this county.")
    else:
        sv1_flat = shap_df.loc[county_key, _s1_cols].values.astype(float)
        sv2_flat = shap_df.loc[county_key, _s2_cols].values.astype(float)

        col_l, col_r = st.columns(2)
        with col_l:
            st.plotly_chart(
                shap_bar(
                    sv1_flat, S1_FEATS,
                    "Stage 1 — Presence drivers<br>"
                    "<sup>Impact on log-odds P(has DC) — top 15 by |SHAP|</sup>",
                ),
                width="stretch",
            )
        with col_r:
            st.plotly_chart(
                shap_bar(
                    sv2_flat, S2_FEATS,
                    "Stage 2 — Scale drivers<br>"
                    "<sup>Impact on log E[count | DC present] — top 15 by |SHAP|</sup>",
                ),
                width="stretch",
            )

        # Feature value table
        with st.expander("📋 Full feature values and SHAP scores"):
            prep_row = prep_df[prep_df["county_key"] == county_key]
            if not prep_row.empty:
                X_row = prep_row[FEAT_COLS].values[0]
                feat_df = pd.DataFrame({
                    "Feature":         S1_FEATS,
                    "Value":           X_row.round(4),
                    "SHAP — Presence": sv1_flat.round(4),
                    "SHAP — Scale":    sv2_flat.round(4),
                })
                feat_df["Abs SHAP (Presence)"] = np.abs(sv1_flat)
                feat_df = feat_df.sort_values("Abs SHAP (Presence)", ascending=False).drop(
                    columns="Abs SHAP (Presence)"
                ).set_index("Feature")
                st.dataframe(feat_df, width="stretch")

    # ── Compare to similar counties ──
    st.markdown("---")
    st.subheader("Similar counties by structural profile")
    st.caption("Nearest neighbors by Euclidean distance in the (log1p-transformed) feature space.")

    X_all   = prep_df[FEAT_COLS].values
    X_this  = prep_df[prep_df["county_key"] == county_key][FEAT_COLS].values
    if len(X_this):
        dists   = np.linalg.norm(X_all - X_this[0], axis=1)
        nearest = np.argsort(dists)[1:6]  # skip self
        sim_keys = prep_df.iloc[nearest]["county_key"].values
        sim_df   = ranking[ranking["county_key"].isin(sim_keys)][
            ["county", "state", "num_datacenters", "p_presence", "attractiveness"]
        ].round(3)
        st.dataframe(sim_df.set_index("county"), width="stretch")


# ════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Emerging Markets
# ════════════════════════════════════════════════════════════════════════════

elif page == "🌱 Emerging Markets":
    st.title("Emerging Markets")
    st.markdown(
        "Counties with **zero current data centers** but high structural attractiveness. "
        "These are first-mover opportunities where the model finds the structural prerequisites "
        "— grid capacity, IT labor, economic density — without existing DC activity."
    )

    zero_dc = ranking[ranking["num_datacenters"] == 0].copy()

    # ── Filters ──
    col1, col2, col3 = st.columns(3)
    with col1:
        min_p = st.slider("Min P(presence)", 0.0, 1.0, 0.25, 0.05)
    with col2:
        states_filter = st.multiselect(
            "Filter by state (optional)", sorted(zero_dc["state"].unique())
        )
    with col3:
        top_n = st.selectbox("Show top N", [25, 50, 100, 250, "All"], index=1)

    filtered = zero_dc[zero_dc["p_presence"] >= min_p]
    if states_filter:
        filtered = filtered[filtered["state"].isin(states_filter)]
    if top_n != "All":
        filtered = filtered.head(int(top_n))

    st.markdown(f"**{len(filtered):,}** counties match the current filters.")

    # ── Bar chart of top 20 ──
    top20 = filtered.head(20)
    fig_em = px.bar(
        top20,
        x="attractiveness",
        y=(top20["county"] + ",  " + top20["state"]),
        orientation="h",
        color="p_presence",
        color_continuous_scale="Blues",
        labels={
            "y":             "",
            "attractiveness": "Attractiveness Score",
            "p_presence":    "P(presence)",
        },
        title=f"Top {min(20, len(top20))} emerging market counties (zero current DCs)",
        text="attractiveness",
    )
    fig_em.update_traces(texttemplate="%{text:.2f}", textposition="outside")
    fig_em.update_layout(
        yaxis=dict(autorange="reversed"),
        height=max(380, min(20, len(top20)) * 28),
        margin=dict(l=10, r=60, t=45, b=10),
        plot_bgcolor="white",
        paper_bgcolor="white",
        coloraxis_colorbar=dict(title="P(presence)", thickness=12, len=0.6),
    )
    st.plotly_chart(fig_em, width="stretch")

    # ── Full table ──
    st.subheader("Full ranking table")
    display = filtered[
        ["county", "state", "p_presence", "expected_scale", "attractiveness"]
    ].copy()
    display.index.name = "rank"
    display = display.rename(columns={
        "p_presence":     "P(presence)",
        "expected_scale": "E[scale]",
        "attractiveness": "Score",
    })

    st.dataframe(display, width="stretch", height=550)

    st.caption(
        "💡 Counties with P(presence) > 0.5 and no current DCs are particularly interesting — "
        "the model finds them structurally ready but they haven't yet attracted investment."
    )
