"""
EU trade explorer — a one-page Streamlit view of the COMEXT pipeline's output.

Reads the aggregated extract in dashboard/extract/ (built from the pipeline's
DuckDB by scripts/build_dashboard_extract.py), so it runs anywhere without the
pipeline or the multi-GB database.

Run:
    uv run --group dashboard streamlit run dashboard/app.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

EXTRACT = Path(__file__).parent / "extract"

EU_TOTAL = "EU27"
SCOPES = {
    "Extra-EU": "Trade with countries outside the EU",
    "Intra-EU": "Trade between EU member states",
    "All partners": "Extra-EU and intra-EU trade together",
}
FLOWS = {"Exports": "EXPORT", "Imports": "IMPORT"}

# Reference palette (dataviz skill): categorical slots 1-2 for the two flows,
# blue <-> red diverging arms with a neutral grey midpoint for the balance map.
PALETTE = {
    "light": {
        "EXPORT": "#2a78d6",
        "IMPORT": "#eb6834",
        "grid": "#e1e0d9",
        "axis": "#c3c2b7",
        "muted": "#898781",
        "ink": "#0b0b0b",
        "neutral": "#f0efec",
        "land": "#f0efec",
        "diverging": ["#a82a2b", "#e34948", "#f3b3b0", "#f0efec", "#b7d3f6", "#2a78d6", "#184f95"],
    },
    "dark": {
        "EXPORT": "#3987e5",
        "IMPORT": "#d95926",
        "grid": "#2c2c2a",
        "axis": "#383835",
        "muted": "#898781",
        "ink": "#f0efec",
        "neutral": "#383835",
        "land": "#2c2c2a",
        "diverging": ["#e66767", "#b33a3a", "#5c2b2a", "#383835", "#1f3d63", "#2a78d6", "#86b6ef"],
    },
}


# ─── Data ──────────────────────────────────────────────────────────────────────


@st.cache_data
def load() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read the extract and attach partner scope and labels."""
    countries = pd.read_csv(EXTRACT / "countries.csv", keep_default_na=False)
    chapter_names = pd.read_csv(EXTRACT / "chapters.csv", dtype={"chapter": str})
    eu = set(countries.loc[countries["is_eu"], "code"])

    partners = pd.read_parquet(EXTRACT / "partners.parquet")
    partners["scope"] = partners["partner"].isin(eu).map({True: "intra", False: "extra"})
    partners["month"] = pd.to_datetime(partners["period"], format="%Y%m")

    chapters = pd.read_parquet(EXTRACT / "chapters.parquet")
    chapters["month"] = pd.to_datetime(chapters["period"], format="%Y%m")
    return partners, chapters, countries, chapter_names


def fmt_eur(value: float) -> str:
    """€ with a readable magnitude suffix."""
    sign = "−" if value < 0 else ""
    value = abs(value)
    for threshold, suffix in ((1e12, "tn"), (1e9, "bn"), (1e6, "m"), (1e3, "k")):
        if value >= threshold:
            return f"{sign}€{value / threshold:,.1f} {suffix}"
    return f"{sign}€{value:,.0f}"


def scope_filter(df: pd.DataFrame, scope: str) -> pd.DataFrame:
    """Keep the rows that belong to the chosen trade scope."""
    if scope == "Extra-EU":
        return df[df["scope"] == "extra"]
    if scope == "Intra-EU":
        return df[df["scope"] == "intra"]
    return df


def theme() -> dict[str, object]:
    """Pick the palette steps for the viewer's light or dark theme."""
    kind = getattr(getattr(st.context, "theme", None), "type", None) or "light"
    return PALETTE["dark" if kind == "dark" else "light"]


def base_layout(fig: go.Figure, colors: dict[str, object], height: int) -> go.Figure:
    """Recessive grid and axes, transparent surface, compact margins."""
    fig.update_layout(
        height=height,
        margin={"l": 8, "r": 8, "t": 8, "b": 8},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "system-ui, -apple-system, Segoe UI, sans-serif", "color": colors["ink"]},
        hoverlabel={"font_size": 13},
        legend={"orientation": "h", "y": 1.08, "x": 0, "title": None},
    )
    fig.update_xaxes(showgrid=False, linecolor=colors["axis"], tickfont_color=colors["muted"])
    fig.update_yaxes(
        gridcolor=colors["grid"],
        zeroline=False,
        linecolor=colors["axis"],
        tickfont_color=colors["muted"],
    )
    return fig


# ─── Charts ────────────────────────────────────────────────────────────────────


def trend_chart(monthly: pd.DataFrame, colors: dict[str, object]) -> go.Figure:
    """Exports and imports per month on one € axis."""
    fig = go.Figure()
    for label, flow in FLOWS.items():
        series = monthly[monthly["flow"] == flow]
        fig.add_trace(
            go.Scatter(
                x=series["month"],
                y=series["value_eur"] / 1e9,
                name=label,
                mode="lines+markers",
                line={"width": 2, "color": colors[flow]},
                marker={"size": 8, "color": colors[flow]},
                hovertemplate="%{y:,.1f} bn €<extra>" + label + "</extra>",
            )
        )
    fig = base_layout(fig, colors, 340)
    fig.update_layout(hovermode="x unified")
    fig.update_xaxes(tickformat="%b %Y", nticks=7)
    fig.update_yaxes(title_text="€ bn", rangemode="tozero", title_font_color=colors["muted"])
    return fig


def ranked_bars(
    df: pd.DataFrame, label_col: str, color: str, colors: dict[str, object]
) -> go.Figure:
    """Horizontal top-N bars, largest at the top, value labels at the bar ends."""
    df = df.sort_values("value_eur")
    fig = go.Figure(
        go.Bar(
            x=df["value_eur"] / 1e9,
            y=df[label_col],
            orientation="h",
            marker={"color": color, "cornerradius": 4},
            text=[fmt_eur(v) for v in df["value_eur"]],
            textposition="outside",
            textfont={"color": colors["muted"], "size": 12},
            cliponaxis=False,
            customdata=df[["share", "detail"]],
            hovertemplate=(
                "<b>%{y}</b><br>%{customdata[1]}%{x:,.2f} bn €<br>"
                "%{customdata[0]:.1%} of total<extra></extra>"
            ),
        )
    )
    fig = base_layout(fig, colors, 380)
    fig.update_layout(bargap=0.35)
    fig.update_xaxes(visible=False, range=[0, df["value_eur"].max() / 1e9 * 1.35])
    fig.update_yaxes(gridcolor="rgba(0,0,0,0)", tickfont_color=colors["ink"], automargin=True)
    return fig


def balance_map(balance: pd.DataFrame, colors: dict[str, object]) -> go.Figure:
    """Diverging choropleth of the trade balance with each partner."""
    # Clip the scale at the 95th percentile so a couple of giants don't wash out the rest.
    limit = float(balance["balance"].abs().quantile(0.95)) or 1.0
    ramp = colors["diverging"]
    scale = [[i / (len(ramp) - 1), c] for i, c in enumerate(ramp)]
    fig = go.Figure(
        go.Choropleth(
            locations=balance["iso3"],
            z=(balance["balance"] / 1e9).clip(-limit / 1e9, limit / 1e9),
            colorscale=scale,
            zmid=0,
            zmin=-limit / 1e9,
            zmax=limit / 1e9,
            marker_line_color=colors["axis"],
            marker_line_width=0.5,
            customdata=balance[["name", "exports_fmt", "imports_fmt", "balance_fmt"]],
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>Exports %{customdata[1]}<br>"
                "Imports %{customdata[2]}<br>Balance <b>%{customdata[3]}</b><extra></extra>"
            ),
            colorbar={
                "title": {"text": "€ bn", "font": {"color": colors["muted"]}},
                "thickness": 10,
                "len": 0.7,
                "tickfont": {"color": colors["muted"]},
            },
        )
    )
    fig = base_layout(fig, colors, 460)
    fig.update_geos(
        projection_type="natural earth",
        showframe=False,
        showcoastlines=False,
        showland=True,
        landcolor=colors["land"],
        bgcolor="rgba(0,0,0,0)",
        showcountries=False,
        lataxis_range=[-58, 85],
    )
    return fig


# ─── Page ──────────────────────────────────────────────────────────────────────


def main() -> None:
    st.set_page_config(page_title="EU trade explorer", page_icon="🌍", layout="wide")
    partners, chapters, countries, chapter_names = load()
    colors = theme()
    names = dict(zip(countries["code"], countries["name"], strict=True))
    months = sorted(partners["month"].unique())

    st.title("EU trade explorer")
    st.caption(
        "What the 27 EU member states buy and sell, month by month. Eurostat COMEXT data, "
        "loaded by a [Dagster pipeline](https://github.com/MedAmine-SUDO/comext-trade-pipeline) "
        "and aggregated from ~5 million product-level rows per month."
    )

    # Filters, in one row above everything they affect.
    reporters = [EU_TOTAL, *sorted(partners["reporter"].unique(), key=lambda c: names.get(c, c))]
    c1, c2, c3 = st.columns([1.2, 1.4, 2])
    reporter = c1.selectbox(
        "Reporter",
        reporters,
        format_func=lambda c: "EU27 (all member states)" if c == EU_TOTAL else names.get(c, c),
    )
    scope = (
        c2.segmented_control(
            "Partners",
            list(SCOPES),
            default="Extra-EU",
            help="\n\n".join(f"**{k}**: {v}" for k, v in SCOPES.items()),
        )
        or "Extra-EU"
    )
    start, end = c3.select_slider(
        "Months",
        options=months,
        value=(months[0], months[-1]),
        format_func=lambda m: pd.Timestamp(m).strftime("%b %Y"),
    )

    in_range = partners[(partners["month"] >= start) & (partners["month"] <= end)]
    if reporter != EU_TOTAL:
        in_range = in_range[in_range["reporter"] == reporter]
    trade = scope_filter(in_range, scope)

    # Member states report intra-EU trade later than extra-EU trade, so the newest
    # months are incomplete until every reporter has delivered.
    if scope != "Extra-EU":
        intra = partners[partners["scope"] == "intra"]
        coverage = intra.groupby("month")["reporter"].nunique()
        coverage = coverage[(coverage.index >= start) & (coverage.index <= end) & (coverage < 27)]
        if reporter != EU_TOTAL:
            reported = intra[intra["reporter"] == reporter]["month"].unique()
            coverage = coverage[~coverage.index.isin(reported)]
        if not coverage.empty:
            if reporter == EU_TOTAL:
                listed = ", ".join(
                    f"{pd.Timestamp(m).strftime('%b %Y')} ({n}/27 countries)"
                    for m, n in coverage.items()
                )
                gap = f"Intra-EU figures are still incomplete for {listed}"
            else:
                listed = ", ".join(pd.Timestamp(m).strftime("%b %Y") for m in coverage.index)
                gap = f"{names.get(reporter, reporter)} has not reported intra-EU trade for {listed} yet"
            st.warning(
                f"{gap}: member states deliver it later than extra-EU trade. "
                "Use Extra-EU for like-for-like months.",
                icon="⚠️",
            )

    # Headline numbers for the last month in range.
    monthly = trade.groupby(["month", "flow"], as_index=False)["value_eur"].sum()
    by_month = (
        monthly.pivot(index="month", columns="flow", values="value_eur")
        .reindex(columns=list(FLOWS.values()), fill_value=0)
        .fillna(0)
    )
    latest = by_month.iloc[-1]
    previous = by_month.iloc[-2] if len(by_month) > 1 else None
    label_month = pd.Timestamp(by_month.index[-1]).strftime("%B %Y")

    def delta(flow: str) -> str | None:
        if previous is None or previous[flow] == 0:
            return None
        return f"{latest[flow] / previous[flow] - 1:+.1%} vs previous month"

    k1, k2, k3, k4 = st.columns(4)
    # Neutral deltas: more exports or imports is neither good nor bad news by itself.
    k1.metric(
        f"Exports · {label_month}", fmt_eur(latest["EXPORT"]), delta("EXPORT"), delta_color="off"
    )
    k2.metric(
        f"Imports · {label_month}", fmt_eur(latest["IMPORT"]), delta("IMPORT"), delta_color="off"
    )
    balance_now = latest["EXPORT"] - latest["IMPORT"]
    k3.metric(
        f"Trade balance · {label_month}",
        fmt_eur(balance_now),
        "surplus" if balance_now >= 0 else "deficit",
        delta_color="off",
        delta_arrow="off",
    )
    total = trade["value_eur"].sum()
    k4.metric(f"Total trade · {len(by_month)} months", fmt_eur(total))

    st.subheader("Exports and imports per month")
    st.plotly_chart(trend_chart(monthly, colors), width="stretch", config={"displayModeBar": False})

    # Partners and products, for one flow at a time.
    flow_label = st.segmented_control("Flow", list(FLOWS), default="Exports") or "Exports"
    flow = FLOWS[flow_label]
    selected = trade[trade["flow"] == flow]
    flow_total = selected["value_eur"].sum()

    left, right = st.columns(2)
    with left:
        st.subheader(f"Top 10 partners · {flow_label.lower()}")
        top = (
            selected.groupby("partner", as_index=False)["value_eur"].sum().nlargest(10, "value_eur")
        )
        top["name"] = top["partner"].map(names).fillna(top["partner"])
        top["share"] = top["value_eur"] / flow_total
        top["detail"] = ""
        st.plotly_chart(
            ranked_bars(top, "name", colors[flow], colors),
            width="stretch",
            config={"displayModeBar": False},
        )

    with right:
        st.subheader(f"Top 10 product chapters · {flow_label.lower()}")
        ch = chapters[(chapters["month"] >= start) & (chapters["month"] <= end)]
        ch = ch[ch["flow"] == flow]
        if reporter != EU_TOTAL:
            ch = ch[ch["reporter"] == reporter]
        if scope != "All partners":
            ch = ch[ch["scope"] == ("extra" if scope == "Extra-EU" else "intra")]
        top_ch = ch.groupby("chapter", as_index=False)["value_eur"].sum().nlargest(10, "value_eur")
        top_ch = top_ch.merge(chapter_names, on="chapter", how="left")
        top_ch["label"] = top_ch["short_name"].fillna("Chapter " + top_ch["chapter"])
        top_ch["share"] = top_ch["value_eur"] / ch["value_eur"].sum()
        top_ch["detail"] = [
            f"Chapter {c}: {wrap(n)}<br>"
            for c, n in zip(top_ch["chapter"], top_ch["name"], strict=True)
        ]
        st.plotly_chart(
            ranked_bars(top_ch, "label", colors[flow], colors),
            width="stretch",
            config={"displayModeBar": False},
        )

    st.subheader("Trade balance with each partner")
    st.caption(
        "Exports minus imports over the selected months. Blue: the EU side sells more than it "
        "buys. Red: it buys more than it sells. The scale is capped at the 95th percentile; hover "
        "for exact values."
    )
    bal = (
        trade.pivot_table(
            index="partner", columns="flow", values="value_eur", aggfunc="sum", fill_value=0
        )
        .reindex(columns=list(FLOWS.values()), fill_value=0)
        .reset_index()
    )
    bal["balance"] = bal["EXPORT"] - bal["IMPORT"]
    bal = bal.merge(countries, left_on="partner", right_on="code", how="left")
    bal = bal[bal["iso3"].fillna("") != ""]
    for col, src in (
        ("exports_fmt", "EXPORT"),
        ("imports_fmt", "IMPORT"),
        ("balance_fmt", "balance"),
    ):
        bal[col] = bal[src].map(fmt_eur)
    if bal.empty:
        st.info("No partner data for this selection.")
    else:
        st.plotly_chart(balance_map(bal, colors), width="stretch", config={"displayModeBar": False})

    with st.expander("Data table"):
        table = (
            bal[["name", "EXPORT", "IMPORT", "balance"]]
            .rename(
                columns={
                    "name": "Partner",
                    "EXPORT": "Exports (€)",
                    "IMPORT": "Imports (€)",
                    "balance": "Balance (€)",
                }
            )
            .sort_values("Exports (€)", ascending=False)
        )
        st.dataframe(
            table,
            hide_index=True,
            width="stretch",
            column_config={
                c: st.column_config.NumberColumn(format="localized") for c in table.columns[1:]
            },
        )
        st.download_button(
            "Download CSV", table.to_csv(index=False), "eu_trade_by_partner.csv", mime="text/csv"
        )

    st.caption(
        "Source: Eurostat COMEXT (full_v2 monthly files), CN8 product level, all statistical "
        "procedures, values in euros. EU27 totals are the sum of the 27 member states, so "
        "intra-EU trade counts each shipment once as an export and once as an import."
    )


def wrap(text: object, width: int = 60) -> str:
    """Break a long official chapter name over several tooltip lines."""
    if not isinstance(text, str):
        return ""
    lines, line = [], ""
    for word in text.split():
        if line and len(line) + len(word) > width:
            lines.append(line)
            line = word
        else:
            line = f"{line} {word}".strip()
    return "<br>".join([*lines, line])


if __name__ == "__main__":
    main()
