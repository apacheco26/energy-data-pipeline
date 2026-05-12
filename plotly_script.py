# standard library and third party imports
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import plotly.colors as pc
from sqlalchemy import create_engine
from scipy import stats
import dash
from dash import dcc, html, Input, Output, State, dash_table, no_update

# shared color and theme constants
SOLAR_COLOR = "#378ADD"
WIND_COLOR = "#639922"
COAL_COLOR = "#8B6F47"
TEMPLATE = "plotly_dark"
BG_COLOR = "#111217"

DATABASE_URL = "postgresql://postgres:OWtkbuZIeqeLtQHwCftsLuhJArTJCoVc@caboose.proxy.rlwy.net:49545/railway"

engine = create_engine(DATABASE_URL)

# GDP Pearson r by year
df_gdp = pd.read_sql("""
    select make_timestamp(year, 1, 1, 0, 0, 0) as time, r
    from gdp_alignment_correlation order by year
""", engine)

# country alignment scores across all international years
df_country_rank_all = pd.read_sql("""
    select country, year,
           round(((solar_rank + wind_rank) / 2)::numeric, 4) as alignment_score
    from international_alignment
    where year between 2005 and 2020
    order by year, alignment_score asc
""", engine)

# per country composite alignment over time
df_intl = pd.read_sql("""
    select year, country,
           round(((solar_rank + wind_rank) / 2)::numeric, 4) as composite_alignment
    from international_alignment
    where year between 2005 and 2020 order by year, country
""", engine)

# solar and wind Pearson r by year
df_corr = pd.read_sql("""
    select make_date(year::int, 1, 1) as time, solar_r, wind_r
    from climate_alignment_correlation order by year
""", engine)

# composite alignment score per state per year
df_composite_all = pd.read_sql("""
    select state, year,
           round(composite_alignment::numeric, 4) as composite_alignment
    from composite_alignment
    order by year, state
""", engine)

# wind speed and per capita wind investment per state per year
df_wind_scatter_all = pd.read_sql("""
    select ca.state, ca.year, c.avg_wind_speed,
           round((e.wind_mw * 1000000.0 / ec.population)::numeric, 4)
               as wind_mw_per_million
    from composite_alignment ca
    join climate c
        on ca.state = c.state and ca.year = c.year
    join energy e
        on ca.state = e.state and ca.year = e.year
    join state st
        on ca.state = st.abbrev
    join economic ec
        on st.fips = ec.state_fip and ca.year = ec.year
    order by ca.year, c.avg_wind_speed
""", engine)

# GHI and per capita solar investment per state per year
df_solar_scatter_all = pd.read_sql("""
    select ca.state, ca.year, c.avg_ghi,
           round((e.solar_mw * 1000000.0 / ec.population)::numeric, 4)
               as solar_mw_per_million
    from composite_alignment ca
    join climate c
        on ca.state = c.state and ca.year = c.year
    join energy e
        on ca.state = e.state and ca.year = e.year
    join state st
        on ca.state = st.abbrev
    join economic ec
        on st.fips = ec.state_fip and ca.year = ec.year
    order by ca.year, c.avg_ghi
""", engine)

# composite alignment time series for the state detail line chart
df_all_states = pd.read_sql("""
    select time, state, composite_alignment
    from composite_alignment_grafana order by time
""", engine)

# 2023 renewable share used in the top 5 comparison panel
df_renewable = pd.read_sql("""
    select state, renewable_share_pct
    from energy where year = 2023
    order by renewable_share_pct desc
""", engine)

# full joined dataset for KPIs radar and exploration panels
df_master = pd.read_sql("""
    select ca.state, ca.year,
           round(ca.composite_alignment::numeric, 4) as composite_alignment,
           e.solar_mw, e.wind_mw, e.coal_consumption_tons,
           e.co2_emission, e.renewable_share_pct, e.avg_price,
           ec.population, ec.gdp_per_capita,
           round(c.avg_ghi::numeric, 4) as avg_ghi,
           round(c.avg_wind_speed::numeric, 4) as avg_wind_speed,
           round((e.solar_mw * 1000000.0 / ec.population)::numeric, 4)
               as solar_mw_per_million,
           round((e.wind_mw * 1000000.0 / ec.population)::numeric, 4)
               as wind_mw_per_million,
           round((e.coal_consumption_tons * 1000000.0 / ec.population)::numeric, 4)
               as coal_per_million,
           round((e.co2_emission / ec.population)::numeric, 6)
               as co2_per_capita
    from composite_alignment ca
    join energy e on ca.state = e.state and ca.year = e.year
    join climate c on ca.state = c.state and ca.year = c.year
    join state st on ca.state = st.abbrev
    join economic ec on st.fips = ec.state_fip and ca.year = ec.year
    where ec.population > 0
    order by ca.year, ca.state
""", engine)

# 2008 vs 2023 renewable shift precomputed view
df_coal_shift = pd.read_sql("""
    select * from coal_to_renewable_shift
    order by renewable_share_change desc
""", engine)

# release DB connections after all queries complete
engine.dispose()

# sorted state list for the state dropdown
all_states = sorted(df_all_states["state"].unique())
available_years = sorted(df_composite_all["year"].unique().tolist())

# consistent per state color map using a 48 color palette
_palette = pc.qualitative.Light24 + pc.qualitative.Dark24
STATE_COLORS = {
    s: _palette[i % len(_palette)]
    for i, s in enumerate(sorted(df_wind_scatter_all["state"].unique()))
}

# year dropdown options shared across all panels
year_options = (
    [{"label": "All Years (Avg)", "value": "overall"}]
    + [{"label": str(y), "value": y} for y in available_years]
)

# radar chart axis column names and display labels
RADAR_AXES = [
    "renewable_share_pct",
    "solar_mw_per_million", "wind_mw_per_million",
    "coal_per_million", "co2_per_capita",
]
RADAR_LABELS = [
    "Renewable %",
    "Solar/M", "Wind/M",
    "Coal (inv.)", "CO\u2082 (inv.)",
]
# axes where lower raw value is better so normalization is flipped
RADAR_INVERT = {"coal_per_million", "co2_per_capita"}

# profile table column order and display names
TABLE_COLS = [
    "state", "composite_alignment", "renewable_share_pct",
    "solar_mw", "wind_mw", "coal_consumption_tons",
    "co2_emission", "avg_price", "gdp_per_capita",
]
TABLE_HEADERS = {
    "state": "State",
    "composite_alignment": "Alignment",
    "renewable_share_pct": "Renew %",
    "solar_mw": "Solar MW",
    "wind_mw": "Wind MW",
    "coal_consumption_tons": "Coal (tons)",
    "co2_emission": "CO\u2082",
    "avg_price": "Price (\u00a2/kWh)",
    "gdp_per_capita": "GDP/Capita",
}


# shared dark theme layout applied to every figure
def base_layout(title, xtitle="", ytitle="", yrange=None, theme="dark"):
    if theme == "light":
        bg, tmpl, grid, plot_bg = "#f5f6fa", "plotly_white", "#e0e0e0", "#ffffff"
    else:
        bg, tmpl, grid, plot_bg = BG_COLOR, TEMPLATE, "#2a2d3a", "#1a1c23"
    layout = dict(
        template=tmpl,
        title=dict(text=title, font=dict(size=15)),
        paper_bgcolor=bg,
        plot_bgcolor=plot_bg,
        margin=dict(l=50, r=30, t=50, b=40),
        xaxis=dict(title=xtitle, gridcolor=grid),
        yaxis=dict(title=ytitle, gridcolor=grid),
    )
    if yrange:
        layout["yaxis"]["range"] = yrange
    return layout


# styled section divider with left border accent
def section_header(title):
    return html.Div([
        html.H2(title, style={
            "fontSize": "16px", "fontWeight": "600", "color": "#ddd"
        })
    ], style={
        "background": "#1a1c23",
        "borderLeft": f"4px solid {SOLAR_COLOR}",
        "padding": "10px 16px",
        "margin": "28px 0 12px",
        "borderRadius": "0 4px 4px 0",
    })


# single KPI card with a label and a reactive value element
def kpi_card(label, value_id):
    return html.Div([
        html.P(label, style={
            "fontSize": "11px", "color": "#888", "margin": "0",
            "textTransform": "uppercase", "letterSpacing": "0.6px",
        }),
        html.P("\u2014", id=value_id, style={
            "fontSize": "26px", "fontWeight": "700",
            "color": "#eee", "margin": "6px 0 0", "lineHeight": "1.1",
        }),
    ], style={
        "background": "#1a1c23",
        "borderRadius": "6px",
        "padding": "16px 20px",
        "flex": "1",
        "borderTop": f"3px solid {SOLAR_COLOR}",
    })


# returns averaged or year filtered slice of df_master
def _filter_master(year):
    if year is None:
        return (
            df_master
            .groupby("state")[RADAR_AXES + [
                "solar_mw", "wind_mw", "coal_consumption_tons",
                "co2_emission", "avg_price", "gdp_per_capita",
            ]]
            .mean()
            .reset_index()
        )
    return df_master[df_master["year"] == year].copy()


# computes the four KPI card values for a given year
def compute_kpis(year=None):
    df = _filter_master(year)
    solar = f"{df['solar_mw'].sum():,.0f} MW"
    wind = f"{df['wind_mw'].sum():,.0f} MW"
    renew = f"{df['renewable_share_pct'].mean():.1f}%"
    co2 = f"{df['co2_emission'].mean():,.0f}"
    return solar, wind, renew, co2


# returns sorted table rows for the selected year
def make_profile_data(year=None):
    yr = year if year is not None else max(available_years)
    df = (
        df_master[df_master["year"] == yr][TABLE_COLS]
        .sort_values("composite_alignment", ascending=False)
        .round(4)
    )
    return df.to_dict("records")


# GDP Pearson r line chart
def make_gdp_fig(theme="dark"):
    line_color = "#555555" if theme == "light" else "#f1f1f8"
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_gdp["time"], y=df_gdp["r"],
        mode="lines+markers", name="Pearson r",
        line=dict(color=line_color, width=2), marker=dict(size=5),
    ))
    fig.update_layout(**base_layout(
        "Sub-Question 1: GDP per Capita vs Composite Alignment"
        " (Pearson r, 2008\u20132023)",
        ytitle="Pearson r", yrange=[0, 1], theme=theme
    ))
    return fig


# horizontal bar chart of country alignment scores
def make_country_rank_fig(year=None, theme="dark"):
    if year is None:
        df = (
            df_country_rank_all
            .groupby("country")["alignment_score"]
            .mean()
            .reset_index()
            .sort_values("alignment_score")
        )
        label = "All Years Avg"
    else:
        df = (
            df_country_rank_all[df_country_rank_all["year"] == year]
            [["country", "alignment_score"]]
            .sort_values("alignment_score")
        )
        label = str(year)
    fig = go.Figure(go.Bar(
        x=df["alignment_score"], y=df["country"],
        orientation="h",
        marker=dict(
            color=df["alignment_score"],
            colorscale=[[0, "#EA6460"], [0.5, "#EAB839"], [1, "#73BF69"]],
            cmin=0, cmax=1, showscale=True,
            colorbar=dict(title="Score", thickness=12),
        ),
        text=df["alignment_score"].round(3),
        textposition="outside",
    ))
    fig.update_layout(**base_layout(
        f"Sub-Question 2: Country Alignment Rankings ({label})",
        xtitle="Alignment Score", theme=theme
    ))
    grid = "#e0e0e0" if theme == "light" else "#2a2d3a"
    fig.update_layout(xaxis=dict(range=[0, 1.1], gridcolor=grid))
    return fig


# line chart of composite alignment per country over time
_INTL_HIGHLIGHT = {"United States", "Germany", "Mexico"}
_INTL_HIGHLIGHT_COLORS = {
    "United States": "#1f77b4",
    "Germany": "#2ca02c",
    "Mexico": "#d62728",
}


def _hex_to_rgba(hex_color, alpha):
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def make_intl_fig(theme="dark", hovered_country=None):
    fig = go.Figure()
    for country in df_intl["country"].unique():
        df_c = df_intl[df_intl["country"] == country]
        highlighted = country in _INTL_HIGHLIGHT
        is_hovered = country == hovered_country
        color = _INTL_HIGHLIGHT_COLORS.get(country, "#888888")
        show_fill = highlighted or is_hovered
        fig.add_trace(go.Scatter(
            x=df_c["year"], y=df_c["composite_alignment"],
            mode="lines+markers", name=country,
            line=dict(width=2.5 if (highlighted or is_hovered) else 1.2,
                      color=color if (highlighted or is_hovered) else "#555555"),
            marker=dict(size=5 if (highlighted or is_hovered) else 3),
            opacity=1.0 if (highlighted or is_hovered) else 0.2,
            fill="tozeroy" if show_fill else "none",
            fillcolor=_hex_to_rgba(color, 0.15) if show_fill else None,
        ))
    fig.update_layout(**base_layout(
        "Sub-Question 2: International Composite Alignment"
        " Over Time (2005\u20132020)",
        xtitle="Year", ytitle="Composite Alignment", theme=theme
    ))
    fig.update_layout(legend=dict(
        orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5
    ))
    return fig


# solar and wind Pearson r dual line chart
def make_corr_fig(theme="dark"):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_corr["time"], y=df_corr["solar_r"],
        mode="lines+markers", name="Solar r",
        line=dict(color=SOLAR_COLOR, width=2), marker=dict(size=5),
    ))
    fig.add_trace(go.Scatter(
        x=df_corr["time"], y=df_corr["wind_r"],
        mode="lines+markers", name="Wind r",
        line=dict(color=WIND_COLOR, width=2), marker=dict(size=5),
    ))
    fig.update_layout(**base_layout(
        "Solar vs Wind Alignment: Pearson r (2008\u20132023)",
        ytitle="Pearson r", yrange=[0, 1], theme=theme
    ))
    return fig


# green and red bar chart for top and bottom 10 states
def make_top_bottom_fig(year=None, theme="dark"):
    if year is None:
        df = (
            df_composite_all
            .groupby("state")["composite_alignment"]
            .mean()
            .reset_index()
        )
        label = "All Years Avg"
    else:
        df = (
            df_composite_all[df_composite_all["year"] == year]
            [["state", "composite_alignment"]]
            .copy()
        )
        label = str(year)
    df = df.sort_values("composite_alignment", ascending=False).reset_index(drop=True)
    top10 = set(df.head(10)["state"])
    bot10 = set(df.tail(10)["state"])
    df = df[df["state"].isin(top10 | bot10)].copy()
    df["category"] = df["state"].apply(
        lambda s: "Top 10" if s in top10 else "Bottom 10"
    )
    df = df.sort_values("composite_alignment", ascending=False)
    colors = [
        "#73BF69" if c == "Top 10" else "#EA6460"
        for c in df["category"]
    ]
    fig = go.Figure(go.Bar(
        x=df["state"], y=df["composite_alignment"].round(3),
        marker_color=colors,
        text=df["composite_alignment"].round(3),
        textposition="outside",
    ))
    fig.update_layout(**base_layout(
        f"Composite Alignment by State: Top 10 & Bottom 10 ({label})",
        xtitle="State", ytitle="Composite Alignment", theme=theme
    ))
    fig.update_layout(showlegend=False)
    return fig


# reusable scatter plot with per state colors and a trendline
def _scatter_fig(df, x_col, y_col, x_label, y_label, title, theme="dark"):
    df = df.sort_values(x_col)
    x, y = df[x_col], df[y_col]
    m, b = np.polyfit(x, y, 1)
    fig = go.Figure(go.Scatter(
        x=x, y=y, mode="markers", text=df["state"],
        showlegend=False,
        hovertemplate=(
            "<b>%{text}</b><br>"
            f"{x_label}: %{{x}}<br>"
            f"{y_label}: %{{y}}<extra></extra>"
        ),
        marker=dict(
            size=9, opacity=0.85,
            color=df["state"].map(STATE_COLORS).tolist(),
            line=dict(width=1, color="#fff"),
        ),
    ))
    # linear trendline overlay
    fig.add_trace(go.Scatter(
        x=x, y=m * x + b, mode="lines", showlegend=False,
        line=dict(color="#ffffff", width=1, dash="dash"),
    ))
    fig.update_layout(**base_layout(title, xtitle=x_label, ytitle=y_label, theme=theme))
    return fig


# wind speed vs per capita wind investment scatter
def make_wind_fig(year=None, theme="dark"):
    if year is None:
        df = (
            df_wind_scatter_all
            .groupby("state")
            .agg(avg_wind_speed=("avg_wind_speed", "mean"),
                 wind_mw_per_million=("wind_mw_per_million", "mean"))
            .reset_index()
        )
        label = "All Years Avg"
    else:
        df = df_wind_scatter_all[df_wind_scatter_all["year"] == year].copy()
        label = str(year)
    return _scatter_fig(
        df, "avg_wind_speed", "wind_mw_per_million",
        "Avg Wind Speed (m/s)", "Wind MW per Million People",
        f"Wind Investment vs. Wind Resource by State ({label})", theme=theme
    )


# GHI vs per capita solar investment scatter
def make_solar_fig(year=None, theme="dark"):
    if year is None:
        df = (
            df_solar_scatter_all
            .groupby("state")
            .agg(avg_ghi=("avg_ghi", "mean"),
                 solar_mw_per_million=("solar_mw_per_million", "mean"))
            .reset_index()
        )
        label = "All Years Avg"
    else:
        df = df_solar_scatter_all[df_solar_scatter_all["year"] == year].copy()
        label = str(year)
    return _scatter_fig(
        df, "avg_ghi", "solar_mw_per_million",
        "Avg GHI (kWh/m\u00b2/day)", "Solar MW per Million People",
        f"Solar Investment vs. Solar Resource by State ({label})", theme=theme
    )


# US choropleth colored by composite alignment score
def make_map_fig(year=None, theme="dark"):
    if year is None:
        df = (
            df_composite_all
            .groupby("state")["composite_alignment"]
            .mean()
            .reset_index()
        )
        label = "All Years Avg"
    else:
        df = df_composite_all[df_composite_all["year"] == year].copy()
        label = str(year)
    fig = go.Figure(go.Choropleth(
        locations=df["state"],
        z=df["composite_alignment"],
        locationmode="USA-states",
        colorscale=[[0, "#EA6460"], [0.5, "#EAB839"], [1, "#73BF69"]],
        zmin=0, zmax=1,
        colorbar=dict(title="Alignment<br>Score", thickness=14),
        hovertemplate=(
            "<b>%{location}</b><br>Alignment: %{z:.4f}<extra></extra>"
        ),
    ))
    bg = "#f5f6fa" if theme == "light" else BG_COLOR
    tmpl = "plotly_white" if theme == "light" else TEMPLATE
    land = "#dde1ea" if theme == "light" else "#1a1c23"
    sub = "#aaa" if theme == "light" else "#444"
    fig.update_layout(
        template=tmpl,
        title=dict(
            text=f"Composite Alignment by State: {label}", font=dict(size=15)
        ),
        paper_bgcolor=bg,
        margin=dict(l=20, r=20, t=50, b=20),
        geo=dict(
            scope="usa", bgcolor=bg,
            showlakes=False, landcolor=land,
            subunitcolor=sub
        ),
    )
    return fig


# bar chart comparing selected states to top 5 renewable leaders
def make_renewable_fig(selected_states=None, theme="dark"):
    top5 = set(df_renewable.head(5)["state"])
    sel = set(selected_states or ["AL"])
    show = df_renewable[df_renewable["state"].isin(top5 | sel)].copy()
    show["category"] = show["state"].apply(
        lambda s: "Selected" if s in sel else "Top 5"
    )
    colors_ren = [
        "#f79e3a" if c == "Selected" else "#73BF69"
        for c in show["category"]
    ]
    fig = go.Figure(go.Bar(
        x=show["state"], y=show["renewable_share_pct"],
        marker_color=colors_ren,
        text=show["renewable_share_pct"].round(1),
        textposition="outside",
    ))
    fig.update_layout(**base_layout(
        "Share of Energy that is Renewable: Top 5 vs Selected (2023)",
        xtitle="State", ytitle="Renewable Share (%)", theme=theme
    ))
    fig.update_layout(showlegend=False)
    return fig


# radar chart of six normalized energy metrics per selected state
def make_radar_fig(selected_states=None, year=None, theme="dark"):
    df = _filter_master(year)
    df_norm = df[["state"] + RADAR_AXES].copy()
    # min max normalize each axis across all states
    for col in RADAR_AXES:
        mn, mx = df[col].min(), df[col].max()
        df_norm[col] = (
            (df[col] - mn) / (mx - mn) if mx > mn else pd.Series(0.5, index=df.index)
        )
        if col in RADAR_INVERT:
            df_norm[col] = 1 - df_norm[col]

    label = "All Years Avg" if year is None else str(year)
    labs = RADAR_LABELS + [RADAR_LABELS[0]]
    fig = go.Figure()

    # fixed national average reference polygon
    avg_vals = [df_norm[c].mean() for c in RADAR_AXES] + [df_norm[RADAR_AXES[0]].mean()]
    fig.add_trace(go.Scatterpolar(
        r=avg_vals, theta=labs, fill="toself",
        name="National Avg",
        line=dict(color="#aaaaaa", width=2, dash="dash"),
        fillcolor="rgba(170,170,170,0.08)",
        opacity=1.0,
    ))

    # limit to 5 selected states
    for state in (selected_states or [])[:5]:
        row = df_norm[df_norm["state"] == state]
        if row.empty:
            continue
        vals = [row[c].values[0] for c in RADAR_AXES] + [row[RADAR_AXES[0]].values[0]]
        fig.add_trace(go.Scatterpolar(
            r=vals, theta=labs, fill="toself", name=state,
            line=dict(color=STATE_COLORS.get(state, "#aaa"), width=2),
            opacity=0.75,
        ))
    r_bg = "#f5f6fa" if theme == "light" else BG_COLOR
    r_tmpl = "plotly_white" if theme == "light" else TEMPLATE
    r_grid = "#cccccc" if theme == "light" else "#2a2d3a"
    r_plot = "#ffffff" if theme == "light" else "#1a1c23"
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 1], gridcolor=r_grid),
            angularaxis=dict(gridcolor=r_grid),
            bgcolor=r_plot,
        ),
        paper_bgcolor=r_bg,
        template=r_tmpl,
        title=dict(
            text=f"State Energy Signature ({label})", font=dict(size=15)
        ),
        legend=dict(orientation="h", y=-0.12),
        margin=dict(l=60, r=60, t=60, b=80),
    )
    return fig


# scatter with optional year animation and solar or wind toggle
def make_scatter_anim_fig(resource="wind", animate=False, year=None, theme="dark"):
    if resource == "wind":
        df_src = df_wind_scatter_all
        x_col, y_col = "avg_wind_speed", "wind_mw_per_million"
        x_lbl = "Avg Wind Speed (m/s)"
        y_lbl = "Wind MW per Million People"
    else:
        df_src = df_solar_scatter_all
        x_col, y_col = "avg_ghi", "solar_mw_per_million"
        x_lbl = "Avg GHI (kWh/m\u00b2/day)"
        y_lbl = "Solar MW per Million People"
    res = "Wind" if resource == "wind" else "Solar"

    if animate:
        # fix axis ranges so scale stays constant across all frames
        x_pad = (df_src[x_col].max() - df_src[x_col].min()) * 0.05
        y_pad = (df_src[y_col].max() - df_src[y_col].min()) * 0.05
        x_range = [df_src[x_col].min() - x_pad, df_src[x_col].max() + x_pad]
        y_range = [max(0, df_src[y_col].min() - y_pad), df_src[y_col].max() + y_pad]
        a_bg = "#f5f6fa" if theme == "light" else BG_COLOR
        a_tmpl = "plotly_white" if theme == "light" else TEMPLATE
        a_plot = "#ffffff" if theme == "light" else "#1a1c23"
        a_ctrl_bg = "#e0e0e0" if theme == "light" else "#2a2d3a"
        a_ctrl_txt = "#333" if theme == "light" else "#eee"
        fig = px.scatter(
            df_src.sort_values("year"),
            x=x_col, y=y_col, animation_frame="year",
            color="state", color_discrete_map=STATE_COLORS,
            hover_name="state",
            labels={x_col: x_lbl, y_col: y_lbl},
            range_x=x_range, range_y=y_range,
            template=a_tmpl,
            title=f"{res} Investment vs Resource: Animated (2008\u20132023)",
        )
        fig.update_layout(
            paper_bgcolor=a_bg, plot_bgcolor=a_plot,
            showlegend=False, margin=dict(l=50, r=30, t=60, b=40),
            sliders=[{
                "font": {"color": a_ctrl_txt},
                "currentvalue": {"font": {"color": a_ctrl_txt}, "prefix": "Year: "},
                "bgcolor": a_ctrl_bg, "bordercolor": "#888",
            }],
            updatemenus=[{
                "bgcolor": a_ctrl_bg, "bordercolor": "#888",
                "font": {"color": a_ctrl_txt},
            }],
        )
        return fig

    # static view uses all years averaged or a specific year
    if year is None:
        df = (
            df_src.groupby("state")[[x_col, y_col]]
            .mean().reset_index()
        )
        label = "All Years Avg"
    else:
        df = df_src[df_src["year"] == year].copy()
        label = str(year)
    return _scatter_fig(
        df, x_col, y_col, x_lbl, y_lbl,
        f"{res} Investment vs Resource ({label})"
    )


# histogram of alignment scores with a scipy KDE density curve
def make_hist_fig(year=None, theme="dark"):
    if year is None:
        data = df_composite_all["composite_alignment"].values
        label = "All Years"
    else:
        data = (
            df_composite_all[df_composite_all["year"] == year]
            ["composite_alignment"].values
        )
        label = str(year)
    if len(data) < 2:
        return go.Figure()

    bin_w = 0.05
    kde = stats.gaussian_kde(data)
    xr = np.linspace(0, 1, 200)
    # scale KDE to match histogram count axis
    kde_y = kde(xr) * len(data) * bin_w

    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=data, xbins=dict(start=0, end=1, size=bin_w),
        marker_color=SOLAR_COLOR, opacity=0.7,
        name="States", showlegend=False,
    ))
    fig.add_trace(go.Scatter(
        x=xr, y=kde_y, mode="lines",
        line=dict(color="#f1f1f8", width=2),
        name="KDE", showlegend=False,
    ))
    fig.update_layout(**base_layout(
        f"Alignment Score Distribution ({label})",
        xtitle="Composite Alignment", ytitle="Count", theme=theme
    ))
    return fig


# before and after bar chart using the precomputed shift view
def make_coal_shift_fig(theme="dark"):
    df = df_coal_shift
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="2008", x=df["state"],
        y=df["renewable_share_2008"],
        marker_color="#EA6460",
    ))
    fig.add_trace(go.Bar(
        name="2023", x=df["state"],
        y=df["renewable_share_2023"],
        marker_color="#73BF69",
    ))
    fig.update_layout(**base_layout(
        "Coal to Renewable Shift: 2008 vs 2023 Renewable Share",
        xtitle="State (sorted by change)", ytitle="Renewable Share (%)", theme=theme
    ))
    fig.update_layout(barmode="group", legend=dict(
        orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5
    ))
    return fig


# CO2 per capita vs renewable share scatter
def make_co2_scatter_fig(year=None, theme="dark"):
    df = _filter_master(year)
    label = "All Years Avg" if year is None else str(year)
    return _scatter_fig(
        df, "renewable_share_pct", "co2_per_capita",
        "Renewable Share (%)", "CO\u2082 per Capita",
        f"CO\u2082 Emissions vs Renewable Share ({label})", theme=theme
    )


# retail price vs renewable share scatter
def make_price_scatter_fig(year=None, theme="dark"):
    df = _filter_master(year)
    label = "All Years Avg" if year is None else str(year)
    return _scatter_fig(
        df, "avg_price", "renewable_share_pct",
        "Avg Retail Price (\u00a2/kWh)", "Renewable Share (%)",
        f"Electricity Price vs Renewable Share ({label})", theme=theme
    )


# computes dynamic footnote text for each reactive panel based on the active year
def compute_footnotes(yr):
    year_label = "all-years average" if yr is None else str(yr)

    if yr is None:
        df_cr = (df_country_rank_all.groupby("country")["alignment_score"]
                 .mean().reset_index())
    else:
        df_cr = df_country_rank_all[df_country_rank_all["year"] == yr][
            ["country", "alignment_score"]].copy()
    df_cr = df_cr.sort_values("alignment_score", ascending=False).reset_index(drop=True)
    top_c = df_cr.iloc[0]["country"]
    top_c_score = round(df_cr.iloc[0]["alignment_score"], 3)
    bot_c = df_cr.iloc[-1]["country"]
    bot_c_score = round(df_cr.iloc[-1]["alignment_score"], 3)
    above_half = int((df_cr["alignment_score"] > 0.5).sum())
    fn_country = (
        f"Showing {year_label}. {top_c} leads with a score of {top_c_score}; "
        f"{bot_c} trails at {bot_c_score}. {above_half} of {len(df_cr)} countries "
        f"score above 0.5, indicating investment that outpaces climate resources alone."
    )

    if yr is None:
        df_tb = (df_composite_all.groupby("state")["composite_alignment"]
                 .mean().reset_index())
    else:
        df_tb = df_composite_all[df_composite_all["year"] == yr][
            ["state", "composite_alignment"]].copy()
    df_tb = df_tb.sort_values("composite_alignment", ascending=False).reset_index(drop=True)
    top_st = df_tb.iloc[0]["state"]
    top_st_score = round(df_tb.iloc[0]["composite_alignment"], 3)
    bot_st = df_tb.iloc[-1]["state"]
    bot_st_score = round(df_tb.iloc[-1]["composite_alignment"], 3)
    avg_st = round(df_tb["composite_alignment"].mean(), 3)
    fn_top = (
        f"Showing {year_label}. {top_st} leads with a composite score of {top_st_score} "
        f"and {bot_st} is lowest at {bot_st_score}. The national average is {avg_st}. "
        f"Bottom performers are concentrated in the Southeast and Appalachian regions."
    )

    if yr is None:
        df_w = (df_wind_scatter_all.groupby("state")
                .agg(avg_wind_speed=("avg_wind_speed", "mean"),
                     wind_mw_per_million=("wind_mw_per_million", "mean"))
                .reset_index())
    else:
        df_w = df_wind_scatter_all[df_wind_scatter_all["year"] == yr].copy()
    r_wind = round(df_w["avg_wind_speed"].corr(df_w["wind_mw_per_million"]), 3)
    top_wind = df_w.loc[df_w["wind_mw_per_million"].idxmax(), "state"]
    fn_wind = (
        f"Showing {year_label}. Pearson r = {r_wind} between average wind speed and "
        f"wind capacity per million residents. {top_wind} leads in per capita wind installation. "
        f"States above the trendline invest more than their wind conditions alone would predict."
    )

    if yr is None:
        df_sv = (df_solar_scatter_all.groupby("state")
                 .agg(avg_ghi=("avg_ghi", "mean"),
                      solar_mw_per_million=("solar_mw_per_million", "mean"))
                 .reset_index())
    else:
        df_sv = df_solar_scatter_all[df_solar_scatter_all["year"] == yr].copy()
    r_solar = round(df_sv["avg_ghi"].corr(df_sv["solar_mw_per_million"]), 3)
    top_solar = df_sv.loc[df_sv["solar_mw_per_million"].idxmax(), "state"]
    fn_solar = (
        f"Showing {year_label}. Pearson r = {r_solar} between average GHI and "
        f"solar capacity per million residents. {top_solar} leads in per capita solar installation. "
        f"State policy and incentives drive deployment well beyond climate conditions alone."
    )

    if yr is None:
        df_m = (df_composite_all.groupby("state")["composite_alignment"]
                .mean().reset_index())
    else:
        df_m = df_composite_all[df_composite_all["year"] == yr][
            ["state", "composite_alignment"]].copy()
    high_n = int((df_m["composite_alignment"] > 0.65).sum())
    med_n = int(((df_m["composite_alignment"] >= 0.35) & (df_m["composite_alignment"] <= 0.65)).sum())
    low_n = int((df_m["composite_alignment"] < 0.35).sum())
    best_st = df_m.loc[df_m["composite_alignment"].idxmax(), "state"]
    worst_st = df_m.loc[df_m["composite_alignment"].idxmin(), "state"]
    fn_map = (
        f"Showing {year_label}. {high_n} states in the High tier (above 0.65), "
        f"{med_n} in Medium (0.35 to 0.65), {low_n} in Low (below 0.35). "
        f"{best_st} has the strongest alignment and {worst_st} the weakest. "
        f"A West to East gradient is visible, with Mountain West and New England "
        f"outperforming the South and Midwest."
    )

    if yr is None:
        hist_data = df_composite_all["composite_alignment"].values
    else:
        hist_data = df_composite_all[df_composite_all["year"] == yr]["composite_alignment"].values
    mean_s = round(float(np.mean(hist_data)), 3)
    med_s = round(float(np.median(hist_data)), 3)
    high_h = int((hist_data > 0.65).sum())
    low_h = int((hist_data < 0.35).sum())
    skew_dir = ("right skewed" if mean_s > med_s else
                "left skewed" if mean_s < med_s else "symmetric")
    fn_hist = (
        f"Showing {year_label}. Mean alignment: {mean_s}, median: {med_s} ({skew_dir}). "
        f"{high_h} states score above 0.65 (High) and {low_h} score below 0.35 (Low). "
        f"A rightward shift over time indicates national improvement in alignment."
    )

    df_co2_f = _filter_master(yr)
    r_co2 = round(df_co2_f["renewable_share_pct"].corr(df_co2_f["co2_per_capita"]), 3)
    co2_interp = (
        "A negative correlation suggests higher renewable penetration is associated "
        "with lower per capita emissions at the state level."
        if r_co2 < 0 else
        "A positive correlation suggests industrial mix or other factors dominate "
        "over renewable share at the state level."
    )
    fn_co2 = (
        f"Showing {year_label}. Pearson r = {r_co2} between renewable share and "
        f"CO\u2082 per capita. {co2_interp}"
    )

    df_pr_f = _filter_master(yr)
    r_price = round(df_pr_f["avg_price"].corr(df_pr_f["renewable_share_pct"]), 3)
    price_interp = (
        "A positive value suggests renewable heavy states pay slightly more, possibly "
        "reflecting grid transition costs or regional market structures."
        if r_price > 0 else
        "A negative value suggests renewable heavy states pay less, consistent with "
        "the falling levelized cost of wind and solar generation."
    )
    fn_price = (
        f"Showing {year_label}. Pearson r = {r_price} between electricity price and "
        f"renewable share. {price_interp}"
    )

    if yr is None:
        fn_table = (
            f"Showing all-years average ({min(available_years)}\u2013{max(available_years)}). "
            f"Click any column header to sort. Use the filter row to search by state or metric."
        )
    else:
        fn_table = (
            f"Showing data for {yr}. "
            f"Click any column header to sort. Use the filter row to search by state or metric."
        )

    return (fn_country, fn_top, fn_wind, fn_solar, fn_map,
            fn_hist, fn_co2, fn_price, fn_table)


# initialize Dash app and expose WSGI server for gunicorn
app = dash.Dash(__name__)
app.title = "Beyond the Grid: U.S. Climate and Energy"
server = app.server

# override HTML template to remove white border gaps
app.index_string = (
    "<!DOCTYPE html><html><head>"
    "{%metas%}<title>{%title%}</title>{%favicon%}{%css%}"
    "<style>"
    "html,body{margin:0;padding:0;background:" + BG_COLOR + ";}"
    "*{box-sizing:border-box;}"
    "</style>"
    "</head><body>"
    "{%app_entry%}"
    "<footer>{%config%}{%scripts%}{%renderer%}</footer>"
    "</body></html>"
)

# footnote text style for full width panels
FN = {
    "fontSize": "11px", "color": "#777",
    "marginTop": "6px", "marginBottom": "20px",
    "lineHeight": "1.6",
}
# footnote style for side by side panels with inner padding
FN_HALF = {**FN, "padding": "0 4px"}

FN_GDP = (
    "Pearson r measures the linear relationship between state GDP per capita "
    "and composite alignment score each year. A value near 1 indicates wealthier "
    "states invest more proportionally to their climate potential; a value near 0 "
    "suggests wealth is not a meaningful predictor. The declining trend after 2017 "
    "coincides with falling renewable energy costs, suggesting market and policy "
    "factors have increasingly displaced economic capacity as the primary driver "
    "of investment."
)
FN_COUNTRY = (
    "Composite alignment score is calculated as the average of each country's "
    "percentile rank for solar and wind investment relative to climate potential. "
    "A score near 1 indicates a country invests well above what its climate "
    "resources alone would predict. Germany and the United Kingdom lead despite "
    "moderate solar resources, reflecting strong policy environments. Mexico and "
    "Canada trail despite favorable conditions, suggesting underinvestment "
    "relative to their potential."
)
FN_INTL = (
    "Tracks how each country's combined solar and wind alignment score has evolved "
    "over 15 years. Diverging trajectories after 2010 reflect differences in "
    "national renewable energy policy, feed in tariff adoption, and grid "
    "infrastructure investment. Data coverage ends at 2020 due to PVGIS "
    "availability constraints for North American countries."
)
FN_CORR = (
    "Annual Pearson r between each climate resource and its corresponding "
    "per capita investment across all 48 contiguous states. Wind has maintained "
    "a consistently stronger relationship with investment than solar throughout "
    "the period, while solar alignment has improved from weak to moderate as "
    "utility scale solar deployment accelerated after 2015."
)
FN_TOP = (
    "States are ranked by their composite alignment score. Top performers "
    "such as New Mexico, Maine, and Texas have invested heavily in renewables "
    "relative to their climate potential. Bottom performers, concentrated in the "
    "Southeast and Appalachian regions, show persistent underinvestment relative "
    "to available solar and wind resources, which may reflect grid infrastructure "
    "constraints, regulatory environments, or historical reliance on coal."
)
FN_WIND = (
    "Each point represents a state plotted by its average wind speed against wind "
    "capacity installed per million residents. A positive trend indicates states "
    "with stronger wind resources tend to have higher per capita wind investment. "
    "Outliers above the trend line represent states that have invested "
    "disproportionately relative to their wind conditions."
)
FN_SOLAR = (
    "Each point represents a state plotted by its average global horizontal "
    "irradiance against solar capacity per million residents. The relationship "
    "is more diffuse than wind, consistent with the lower Pearson r values "
    "observed across the study period. High GHI states in the Southwest show "
    "the strongest investment, though several mid range states also perform well "
    "due to state level solar incentive programs."
)
FN_MAP = (
    "Choropleth map showing each state's composite alignment score. "
    "Green indicates strong alignment between climate suitability and renewable "
    "investment; red indicates underperformance relative to climate potential. "
    "The geographic pattern shows a clear West to East gradient, with Mountain "
    "West and New England states outperforming much of the South and Midwest."
)
FN_STATE = (
    "Tracks the composite alignment score annually for user selected states from "
    "2008 to 2023. Scores near zero in early years for many states reflect "
    "near zero renewable capacity before large scale deployment began. Steep "
    "increases after 2015 in states like New Mexico and Texas correspond to rapid "
    "utility scale wind and solar buildout during that period."
)
FN_RENEW = (
    "Compares the selected state's renewable energy share against the five "
    "highest performing states nationally in 2023. Renewable share is calculated "
    "as the proportion of total electricity generation from solar, wind, and other "
    "renewable sources. This panel contextualizes where the selected state sits "
    "relative to national leaders."
)
FN_SOURCES = (
    "Data sources: U.S. Energy Information Administration (EIA) for energy "
    "capacity, generation, and consumption; National Renewable Energy Laboratory "
    "and National Solar Radiation Database (NSRDB) for state-level solar radiation; "
    "National Oceanic and Atmospheric Administration (NOAA) for climate observations; "
    "U.S. Bureau of Economic Analysis (BEA) and U.S. Census Bureau for GDP and "
    "population; Our World in Data (OWID) for international energy statistics; and "
    "Photovoltaic Geographical Information System (PVGIS) for international solar "
    "radiation data. All data is aggregated to the state year or country year level, "
    "covering 2008 to 2023 for U.S. state analysis and 2005 to 2020 for international "
    "comparisons. Alaska is excluded from all U.S. state analysis due to NSRDB "
    "satellite coverage limitations and grid incomparability with the remaining 49 "
    "states. District of Columbia and Puerto Rico are excluded as nonstate geographies. "
    "International data coverage ends at 2020 due to PVGIS availability constraints "
    "for North American countries."
)

# reusable dropdown and label styles
_dd_style = {"background": "#1a1c23", "color": "#111"}
_lbl_style = {
    "color": "#aaa", "fontSize": "13px",
    "marginBottom": "6px", "display": "block",
}

# precompute initial footnotes for the default all-years view
_init_fn = compute_footnotes(None)

app.layout = html.Div(
    id="main-wrapper",
    style={
        "background": BG_COLOR, "color": "#eee",
        "fontFamily": "Segoe UI, Arial, sans-serif",
        "padding": "24px",
    },
    children=[
        dcc.Store(id="theme-store", data="dark"),

        # dashboard title and description
        html.Div([
            html.H1(
                "Beyond the Grid: U.S. Climate and Energy",
                style={"fontSize": "28px", "letterSpacing": "1px",
                       "textAlign": "center"}
            ),
            html.P(
                "Chris Bell & Julian Pacheco | Data Engineering Final Project",
                style={"color": "#aaa", "textAlign": "center",
                       "margin": "6px 0", "fontSize": "14px"}
            ),
            html.P(
                "This dashboard evaluates whether U.S. states invest in "
                "renewable energy proportional to their climate suitability "
                "(solar radiation and wind resources), and compares "
                "state level patterns to international trends.",
                style={"color": "#888", "textAlign": "center",
                       "fontSize": "13px", "maxWidth": "760px",
                       "margin": "10px auto 0", "lineHeight": "1.6"}
            ),
        ], style={
            "borderBottom": "1px solid #2a2d3a",
            "paddingBottom": "20px", "marginBottom": "28px",
        }),

        # global year dropdown and theme toggle
        html.Div([
            html.Div([
                html.Label("Filter all panels by year:", style=_lbl_style),
                dcc.Dropdown(
                    id="global-year-selector",
                    options=year_options,
                    value="overall",
                    clearable=False,
                    style=_dd_style,
                ),
            ], style={"maxWidth": "300px"}),
            html.Div([
                html.Button(
                    "Light Mode", id="theme-toggle", n_clicks=0,
                    style={
                        "background": "#2a2d3a", "color": "#eee",
                        "border": "1px solid #555", "borderRadius": "4px",
                        "padding": "6px 16px", "cursor": "pointer",
                        "fontSize": "13px", "marginTop": "22px",
                    },
                ),
            ]),
        ], style={"display": "flex", "gap": "24px", "alignItems": "flex-start",
                  "marginBottom": "20px"}),

        # four KPI summary cards
        html.Div([
            kpi_card("Total Solar Capacity", "kpi-solar"),
            kpi_card("Total Wind Capacity", "kpi-wind"),
            kpi_card("Avg Renewable Share", "kpi-renewable"),
            kpi_card("Avg CO\u2082 Emissions", "kpi-co2"),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "32px"}),

        # collapsible research objective and methodology summary
        html.Details([
            html.Summary("Research Objective & Methodology", style={
                "fontSize": "14px", "fontWeight": "600", "color": "#aaa",
                "cursor": "pointer", "padding": "10px 0", "letterSpacing": "0.4px",
            }),
            html.Div([
                html.Div([
                    html.H3("Objective", style={"fontSize": "13px", "color": SOLAR_COLOR,
                                                "margin": "0 0 8px", "fontWeight": "600"}),
                    html.P(
                        "This project evaluates whether U.S. states invest in renewable energy "
                        "proportionally to their climate suitability (solar radiation and wind "
                        "resources), and compares state-level patterns to international trends. "
                        "Data was ingested from six government sources \u2014 EIA, NOAA, NSRDB, BEA, "
                        "U.S. Census, OWID, and PVGIS \u2014 into a PostgreSQL database on Railway via "
                        "a modular Python pipeline, then transformed into six production tables "
                        "(state, year, climate, energy, economic, international) covering "
                        "2008\u20132023 for U.S. states and 2005\u20132020 internationally.",
                        style={"fontSize": "12px", "color": "#bbb", "lineHeight": "1.7", "margin": "0 0 16px"},
                    ),
                    html.H3("Research Questions", style={"fontSize": "13px", "color": SOLAR_COLOR,
                                                         "margin": "0 0 8px", "fontWeight": "600"}),
                    html.Ul([
                        html.Li("Primary: Do states invest in renewable energy proportional to their climate suitability?",
                                style={"fontSize": "12px", "color": "#bbb", "marginBottom": "4px"}),
                        html.Li("Sub-Q1: Does GDP per capita correlate with a state's renewable investment relative to its climate potential?",
                                style={"fontSize": "12px", "color": "#bbb", "marginBottom": "4px"}),
                        html.Li("Sub-Q2: How do U.S. state patterns compare to international trends?",
                                style={"fontSize": "12px", "color": "#bbb", "marginBottom": "4px"}),
                    ], style={"margin": "0 0 16px", "paddingLeft": "20px"}),
                ], style={"flex": "1", "paddingRight": "24px"}),

                html.Div([
                    html.H3("Key Calculations", style={"fontSize": "13px", "color": SOLAR_COLOR,
                                                       "margin": "0 0 8px", "fontWeight": "600"}),
                    html.P("Composite Alignment Score (0\u20131)", style={
                        "fontSize": "12px", "color": "#ddd", "fontWeight": "600", "margin": "0 0 4px"}),
                    html.P(
                        "Per-capita investment ratios (solar MW per million \u00f7 avg GHI; "
                        "wind MW per million \u00f7 avg wind speed) are computed per state per year, "
                        "then each ratio is percentile-ranked within its year using "
                        "PERCENT_RANK() OVER (PARTITION BY year). The two ranks are averaged "
                        "into a single 0\u20131 score, where 1 means investment far exceeds what "
                        "climate resources alone would predict.",
                        style={"fontSize": "12px", "color": "#bbb", "lineHeight": "1.7", "margin": "0 0 12px"},
                    ),
                    html.P("GDP Correlation (Sub-Q1)", style={
                        "fontSize": "12px", "color": "#ddd", "fontWeight": "600", "margin": "0 0 4px"}),
                    html.P(
                        "PostgreSQL's native CORR() function is applied between gdp_per_capita "
                        "and composite_alignment, producing one Pearson r per year (2008\u20132023).",
                        style={"fontSize": "12px", "color": "#bbb", "lineHeight": "1.7", "margin": "0 0 12px"},
                    ),
                    html.P("International Alignment (Sub-Q2)", style={
                        "fontSize": "12px", "color": "#ddd", "fontWeight": "600", "margin": "0 0 4px"}),
                    html.P(
                        "The same PERCENT_RANK() logic is applied to the international table "
                        "(4,865 rows, 12 countries) using OWID renewable shares and PVGIS "
                        "irradiance data, ranking each country's solar and wind investment "
                        "relative to climate potential within each year.",
                        style={"fontSize": "12px", "color": "#bbb", "lineHeight": "1.7", "margin": "0 0 16px"},
                    ),
                    html.H3("Key Findings", style={"fontSize": "13px", "color": SOLAR_COLOR,
                                                   "margin": "0 0 8px", "fontWeight": "600"}),
                    html.Ul([
                        html.Li(
                            "Wind aligned more strongly with climate suitability than solar "
                            "(Pearson r 0.43\u20130.73 vs 0.22\u20130.59), though solar improved steadily after 2015.",
                            style={"fontSize": "12px", "color": "#bbb", "marginBottom": "4px"}),
                        html.Li(
                            "Colorado led all states (score 0.776); Louisiana ranked last (0.036). "
                            "Top performers cluster in the Mountain West and New England.",
                            style={"fontSize": "12px", "color": "#bbb", "marginBottom": "4px"}),
                        html.Li(
                            "GDP correlation was weak and declining (r peaked ~0.32 in 2013, "
                            "fell near zero by 2018), suggesting policy and climate drive "
                            "investment more than economic capacity.",
                            style={"fontSize": "12px", "color": "#bbb", "marginBottom": "4px"}),
                        html.Li(
                            "Germany ranked first internationally (score 0.932) despite below-average "
                            "solar resources \u2014 pure policy effect. Mexico (0.094) and Brazil (0.122) "
                            "chronically underperformed relative to their climate potential.",
                            style={"fontSize": "12px", "color": "#bbb", "marginBottom": "4px"}),
                    ], style={"margin": "0", "paddingLeft": "20px"}),
                ], style={"flex": "1.2"}),
            ], style={"display": "flex", "gap": "12px", "padding": "16px 20px",
                      "background": "#1a1c23", "borderRadius": "6px", "marginTop": "8px"}),
        ], style={"marginBottom": "28px", "borderBottom": "1px solid #2a2d3a",
                  "paddingBottom": "20px"}),

        # sub question 1 GDP correlation
        section_header(
            "Sub-Question 1: Does GDP per capita correlate with a"
            " state\u2019s renewable energy investment relative to its"
            " climate potential?"
        ),
        dcc.Graph(id="gdp-graph", figure=make_gdp_fig(), style={"height": "380px"}),
        html.P(FN_GDP, style=FN),

        # sub question 2 international comparison
        section_header(
            "Sub-Question 2: How do U.S. state patterns compare"
            " to international trends?"
        ),
        dcc.Graph(id="country-rank-graph",
                  figure=make_country_rank_fig(),
                  style={"height": "380px"}),
        html.P(id="fn-country", children=_init_fn[0], style=FN),
        dcc.Graph(id="intl-graph", figure=make_intl_fig(), style={"height": "420px"}),
        html.P(FN_INTL, style=FN),

        # primary research question panels
        section_header(
            "Primary Question: Do states invest in renewable energy"
            " proportional to their climate suitability"
        ),
        html.Div([
            html.Div([
                dcc.Graph(id="corr-graph", figure=make_corr_fig(),
                          style={"height": "380px"}),
                html.P(FN_CORR, style=FN_HALF),
            ], style={"flex": "1"}),
            html.Div([
                dcc.Graph(id="top-bottom-graph",
                          figure=make_top_bottom_fig(),
                          style={"height": "450px"}),
                html.P(id="fn-top", children=_init_fn[1], style=FN_HALF),
            ], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "8px"}),
        html.Div([
            html.Div([
                dcc.Graph(id="wind-graph", figure=make_wind_fig(),
                          style={"height": "380px"}),
                html.P(id="fn-wind", children=_init_fn[2], style=FN_HALF),
            ], style={"flex": "1"}),
            html.Div([
                dcc.Graph(id="solar-graph", figure=make_solar_fig(),
                          style={"height": "380px"}),
                html.P(id="fn-solar", children=_init_fn[3], style=FN_HALF),
            ], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "8px"}),
        dcc.Graph(id="map-graph", figure=make_map_fig(),
                  style={"height": "480px"}),
        html.P(id="fn-map", children=_init_fn[4], style=FN),

        # exploratory panels not tied to research questions
        section_header("Explore the Data: State Energy Profiles"),
        html.P(
            "These panels are for exploration and are not directly tied to "
            "the research questions. Use the year filter above to update all "
            "panels simultaneously.",
            style={"color": "#666", "fontSize": "13px",
                   "marginBottom": "20px", "lineHeight": "1.6"}
        ),

        dcc.Graph(id="hist-graph", figure=make_hist_fig(), style={"height": "380px"}),
        html.P(id="fn-hist", children=_init_fn[5], style=FN),

        # 2008 vs 2023 renewable share comparison bar chart
        dcc.Graph(id="coal-shift-graph", figure=make_coal_shift_fig(), style={"height": "420px"}),
        html.P(
            "Two bars per state comparing renewable share in 2008 vs 2023, sorted "
            "by the size of the change. States with the largest green bars relative "
            "to red have undergone the most dramatic energy transitions.",
            style=FN
        ),

        # CO2 and electricity price scatter panels side by side
        html.Div([
            html.Div([
                dcc.Graph(id="co2-scatter-graph",
                          figure=make_co2_scatter_fig(),
                          style={"height": "380px"}),
                html.P(id="fn-co2", children=_init_fn[6], style=FN_HALF),
            ], style={"flex": "1"}),
            html.Div([
                dcc.Graph(id="price-scatter-graph",
                          figure=make_price_scatter_fig(),
                          style={"height": "380px"}),
                html.P(id="fn-price", children=_init_fn[7], style=FN_HALF),
            ], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "8px"}),

        # sortable DataTable showing all key metrics per state
        html.P(id="fn-table", children=_init_fn[8],
               style={**FN, "marginBottom": "8px"}),
        dash_table.DataTable(
            id="profile-table",
            columns=[{"name": TABLE_HEADERS[c], "id": c} for c in TABLE_COLS],
            data=make_profile_data(),
            sort_action="native",
            filter_action="native",
            page_size=15,
            style_table={"overflowX": "auto", "marginBottom": "32px"},
            style_cell={
                "backgroundColor": "#1a1c23", "color": "#eee",
                "border": "1px solid #2a2d3a",
                "fontSize": "12px", "padding": "8px",
                "textAlign": "center",
            },
            style_header={
                "backgroundColor": "#0f1117", "color": "#ccc",
                "fontWeight": "600", "border": "1px solid #2a2d3a",
            },
            style_data_conditional=[{
                "if": {"row_index": "odd"},
                "backgroundColor": "#161820",
            }],
        ),

        # radar chart and animated scatter with shared state dropdown
        section_header("State Profiles: Multi-Dimensional Energy Signature"),
        html.Div([
            html.Label("Select up to 5 states to compare against the national average:",
                       style=_lbl_style),
            dcc.Dropdown(
                id="state-dropdown",
                options=[{"label": s, "value": s} for s in all_states],
                value=[],
                multi=True,
                style=_dd_style,
            ),
        ], style={"maxWidth": "600px", "marginBottom": "12px"}),
        html.Div([
            html.Div([
                dcc.Graph(id="radar-graph",
                          figure=make_radar_fig([]),
                          style={"height": "440px"}),
                html.P(
                    "Radar chart showing five normalized metrics per state: renewable share "
                    "as a percentage of total generation, solar capacity per million residents, "
                    "wind capacity per million residents, coal consumption per million residents "
                    "(inverted), and CO\u2082 per capita (inverted). Inverted axes mean higher "
                    "always represents better performance. All five metrics are min max normalized "
                    "across all 49 states so values range from 0 to 1 and axes are directly "
                    "comparable. The dashed gray polygon is the national average across all 49 "
                    "states and serves as a fixed reference baseline. A state whose polygon "
                    "extends beyond the national average on a given axis is outperforming the "
                    "national mean on that metric. Select up to 5 states to overlay and compare.",
                    style=FN_HALF
                ),
            ], style={"flex": "1"}),
            html.Div([
                # resource and view mode toggles for the animated scatter
                html.Div([
                    html.Label("Resource:", style={**_lbl_style, "display": "inline", "marginRight": "12px"}),
                    dcc.RadioItems(
                        id="resource-toggle",
                        options=[
                            {"label": " Wind", "value": "wind"},
                            {"label": " Solar", "value": "solar"},
                        ],
                        value="wind", inline=True,
                        style={"color": "#aaa", "fontSize": "13px",
                               "display": "inline"},
                    ),
                    html.Span(" \u00a0\u00a0 "),
                    html.Label("View:", style={**_lbl_style, "display": "inline", "marginRight": "12px"}),
                    dcc.RadioItems(
                        id="animate-toggle",
                        options=[
                            {"label": " Static", "value": "static"},
                            {"label": " Animated", "value": "animated"},
                        ],
                        value="static", inline=True,
                        style={"color": "#aaa", "fontSize": "13px",
                               "display": "inline"},
                    ),
                ], style={"marginBottom": "8px"}),
                dcc.Graph(id="anim-scatter-graph",
                          figure=make_scatter_anim_fig(),
                          style={"height": "400px"}),
                html.P(
                    "Static view shows all years average (or selected year). "
                    "Animated view plays through each year with a play button. "
                    "Toggle between wind and solar with the resource selector.",
                    style=FN_HALF
                ),
            ], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "12px", "marginBottom": "8px"}),

        # state alignment over time and renewable comparison
        section_header("State Detail: Composite Alignment Over Time"),
        html.Div([
            html.Label("Select states:", style=_lbl_style),
            dcc.Dropdown(
                id="detail-state-dropdown",
                options=[{"label": s, "value": s} for s in all_states],
                value=["NM", "TX", "KY"],
                multi=True,
                style=_dd_style,
            ),
        ], style={"maxWidth": "600px", "marginBottom": "12px"}),
        html.Div([
            html.Div([
                dcc.Graph(id="state-align-graph", style={"height": "380px"}),
                html.P(FN_STATE, style=FN_HALF),
            ], style={"flex": "1"}),
            html.Div([
                dcc.Graph(id="renewable-graph", style={"height": "380px"}),
                html.P(FN_RENEW, style=FN_HALF),
            ], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "12px"}),

        # findings and future work summary
        html.Hr(style={"borderColor": "#2a2d3a", "margin": "36px 0 16px"}),
        html.Div([
            html.H2("Findings & Future Work", style={
                "fontSize": "16px", "fontWeight": "600", "color": "#ddd",
                "marginBottom": "20px",
            }),
            html.Div([
                html.Div([
                    html.H3("What We Found", style={
                        "fontSize": "13px", "color": SOLAR_COLOR,
                        "fontWeight": "600", "margin": "0 0 10px",
                    }),
                    html.P(
                        "States do invest in renewable energy in proportion to their climate "
                        "suitability, but the strength of that relationship depends on the "
                        "resource and has changed over time. Wind alignment was consistently "
                        "stronger throughout 2008–2023 (Pearson r ranging from 0.43 to 0.73), "
                        "while solar alignment started weak at r = 0.22 in 2008 and "
                        "improved steadily to 0.59 by 2023 as utility-scale solar costs "
                        "collapsed after 2015. Both trends moving upward over the full period "
                        "indicates states are progressively getting better at deploying where "
                        "their climate resources support it.",
                        style={"fontSize": "12px", "color": "#bbb",
                               "lineHeight": "1.7", "margin": "0 0 12px"},
                    ),
                    html.P(
                        "At the state level, Colorado ranked first with a composite alignment "
                        "score of 0.776 and Louisiana ranked last at 0.036 — a gap of 0.74 "
                        "points reflecting fundamentally different trajectories. Top performers "
                        "are concentrated in the Mountain West and New England; bottom performers "
                        "cluster in the Southeast and Appalachian regions. Wyoming scored 0.693 "
                        "despite being the largest coal-producing state, showing that wind "
                        "investment and fossil fuel dependency can coexist.",
                        style={"fontSize": "12px", "color": "#bbb",
                               "lineHeight": "1.7", "margin": "0 0 12px"},
                    ),
                    html.P(
                        "GDP per capita had only a weak and declining relationship with "
                        "alignment. Pearson r peaked at roughly 0.32 around 2013–2014 before "
                        "dropping to near zero in 2018 and stabilizing at 0.15–0.18 from 2019 "
                        "onward. The 2018 inflection coincides with the period when renewable "
                        "costs fell below conventional energy in most U.S. markets — once that "
                        "happened, investment spread broadly regardless of economic capacity. "
                        "Climate suitability and policy explain far more of the variation "
                        "in alignment than wealth alone.",
                        style={"fontSize": "12px", "color": "#bbb",
                               "lineHeight": "1.7", "margin": "0 0 12px"},
                    ),
                    html.P(
                        "Internationally, Germany ranked first in every year with a composite "
                        "score of 0.932 despite having below-average solar irradiance — a "
                        "result driven entirely by policy rather than climate advantage. Denmark "
                        "ranked second at 0.733 and the United Kingdom third at 0.727. The "
                        "United States tracked in the middle of the pack at 0.545 throughout "
                        "the comparison period. Mexico (0.094) and Brazil (0.122) represent "
                        "the starkest underperformance cases: both countries have exceptional "
                        "solar resources, and Brazil has strong wind potential as well, yet "
                        "neither translated that natural advantage into proportional investment "
                        "over 2005–2020. The gap between Germany and Mexico makes it "
                        "immediately clear that policy is the variable our current pipeline "
                        "is missing.",
                        style={"fontSize": "12px", "color": "#bbb",
                               "lineHeight": "1.7", "margin": "0"},
                    ),
                ], style={"flex": "1", "paddingRight": "28px"}),

                html.Div([
                    html.H3("Building on This Work", style={
                        "fontSize": "13px", "color": SOLAR_COLOR,
                        "fontWeight": "600", "margin": "0 0 10px",
                    }),
                    html.P(
                        "The most important next step is building a quantifiable policy metric. "
                        "The results consistently show that countries like Germany and Denmark "
                        "outperform their climate resources, and that GDP correlation has "
                        "weakened significantly — both pointing to governmental action as the "
                        "primary driver of investment patterns that the current pipeline cannot "
                        "yet explain. A policy dimension incorporating feed-in tariff history, "
                        "renewable portfolio standard stringency, carbon pricing levels, and "
                        "public energy subsidy records would complete a three-dimensional "
                        "framework of climate suitability, economic capacity, and governmental "
                        "policy.",
                        style={"fontSize": "12px", "color": "#bbb",
                               "lineHeight": "1.7", "margin": "0 0 12px"},
                    ),
                    html.P(
                        "The second priority is adding wind direction data with a sub-state "
                        "spatial layer. Wind speed is a reasonable proxy for wind potential, "
                        "but direction plays a significant role in where infrastructure can "
                        "realistically be deployed. Incorporating it would require moving from "
                        "state-level averages to a within-state geographic layer, creating a "
                        "three-tier structure — sub-state regions, full state, international — "
                        "that would make the alignment score considerably more accurate.",
                        style={"fontSize": "12px", "color": "#bbb",
                               "lineHeight": "1.7", "margin": "0 0 12px"},
                    ),
                    html.P("Additional concrete next steps include:", style={
                        "fontSize": "12px", "color": "#ddd",
                        "fontWeight": "600", "margin": "0 0 6px",
                    }),
                    html.Ul([
                        html.Li(
                            "Expand the international dataset beyond the current 12 countries "
                            "to include underrepresented regions such as Sub-Saharan Africa "
                            "and Southeast Asia.",
                            style={"fontSize": "12px", "color": "#bbb",
                                   "marginBottom": "5px", "lineHeight": "1.6"}),
                        html.Li(
                            "Incorporate more recent EIA and PVGIS data as it becomes available "
                            "to extend the analysis beyond the current 2023 and 2020 ceilings.",
                            style={"fontSize": "12px", "color": "#bbb",
                                   "marginBottom": "5px", "lineHeight": "1.6"}),
                        html.Li(
                            "Build an automated refresh schedule on Railway rather than relying "
                            "on manual redeployment triggers.",
                            style={"fontSize": "12px", "color": "#bbb",
                                   "marginBottom": "5px", "lineHeight": "1.6"}),
                        html.Li(
                            "Add a formal data quality monitoring layer that flags anomalies in "
                            "incoming API responses before they reach staging tables.",
                            style={"fontSize": "12px", "color": "#bbb",
                                   "marginBottom": "5px", "lineHeight": "1.6"}),
                        html.Li(
                            "Alaska was excluded from all state-level analysis due to NSRDB "
                            "coverage gaps and grid isolation. A future team could address this "
                            "by sourcing an alternative satellite radiation dataset.",
                            style={"fontSize": "12px", "color": "#bbb",
                                   "lineHeight": "1.6"}),
                    ], style={"margin": "0", "paddingLeft": "18px"}),
                ], style={"flex": "1"}),
            ], style={"display": "flex", "gap": "12px"}),
        ], style={
            "background": "#1a1c23", "borderRadius": "6px",
            "padding": "20px 24px", "marginBottom": "28px",
            "borderLeft": f"4px solid {WIND_COLOR}",
        }),

        # data sources footer
        html.Hr(style={"borderColor": "#2a2d3a", "margin": "36px 0 16px"}),
        html.P(FN_SOURCES, style={
            "fontSize": "11px", "color": "#666", "lineHeight": "1.7"
        }),
    ]
)


# drives KPIs country ranking top bottom wind solar map histogram CO2 price table and footnotes
@app.callback(
    Output("kpi-solar", "children"),
    Output("kpi-wind", "children"),
    Output("kpi-renewable", "children"),
    Output("kpi-co2", "children"),
    Output("country-rank-graph", "figure"),
    Output("top-bottom-graph", "figure"),
    Output("wind-graph", "figure"),
    Output("solar-graph", "figure"),
    Output("map-graph", "figure"),
    Output("hist-graph", "figure"),
    Output("co2-scatter-graph", "figure"),
    Output("price-scatter-graph", "figure"),
    Output("profile-table", "data"),
    Output("fn-country", "children"),
    Output("fn-top", "children"),
    Output("fn-wind", "children"),
    Output("fn-solar", "children"),
    Output("fn-map", "children"),
    Output("fn-hist", "children"),
    Output("fn-co2", "children"),
    Output("fn-price", "children"),
    Output("fn-table", "children"),
    Input("global-year-selector", "value"),
    Input("theme-store", "data"),
)
def update_global_year(year, theme):
    yr = None if year == "overall" else int(year)
    t = theme or "dark"
    solar, wind, renew, co2 = compute_kpis(yr)
    fns = compute_footnotes(yr)
    return (
        solar, wind, renew, co2,
        make_country_rank_fig(yr, theme=t),
        make_top_bottom_fig(yr, theme=t),
        make_wind_fig(yr, theme=t),
        make_solar_fig(yr, theme=t),
        make_map_fig(yr, theme=t),
        make_hist_fig(yr, theme=t),
        make_co2_scatter_fig(yr, theme=t),
        make_price_scatter_fig(yr, theme=t),
        make_profile_data(yr),
        *fns,
    )


# updates radar chart when year or selected states change
@app.callback(
    Output("radar-graph", "figure"),
    Input("global-year-selector", "value"),
    Input("state-dropdown", "value"),
    Input("theme-store", "data"),
)
def update_radar(year, states, theme):
    yr = None if year == "overall" else int(year)
    return make_radar_fig((states or [])[:5], yr, theme=theme or "dark")


# updates animated scatter when year resource or animate mode changes
@app.callback(
    Output("anim-scatter-graph", "figure"),
    Input("global-year-selector", "value"),
    Input("resource-toggle", "value"),
    Input("animate-toggle", "value"),
    Input("theme-store", "data"),
)
def update_anim_scatter(year, resource, animate_val, theme):
    animate = animate_val == "animated"
    yr = None if (year == "overall" or animate) else int(year)
    return make_scatter_anim_fig(resource or "wind", animate, yr, theme=theme or "dark")


# line chart of composite alignment for selected states over time
@app.callback(
    Output("state-align-graph", "figure"),
    Input("detail-state-dropdown", "value"),
    Input("theme-store", "data"),
)
def update_state_graph(selected_states, theme):
    t = theme or "dark"
    fig = go.Figure()
    if not selected_states:
        return fig
    for state in selected_states:
        df_s = df_all_states[df_all_states["state"] == state]
        fig.add_trace(go.Scatter(
            x=df_s["time"], y=df_s["composite_alignment"],
            mode="lines+markers", name=state,
            line=dict(width=2), marker=dict(size=4),
        ))
    fig.update_layout(**base_layout(
        "Composite Alignment Over Time: Selected States",
        xtitle="Year", ytitle="Composite Alignment", yrange=[0, 1], theme=t
    ))
    fig.update_layout(legend=dict(
        orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5
    ))
    return fig


# bar chart comparing selected states to top 5 renewable leaders
@app.callback(
    Output("renewable-graph", "figure"),
    Input("detail-state-dropdown", "value"),
    Input("theme-store", "data"),
)
def update_renewable_graph(selected_states, theme):
    return make_renewable_fig(selected_states, theme=theme or "dark")


# redraws static figures when theme changes
@app.callback(
    Output("gdp-graph", "figure"),
    Output("intl-graph", "figure"),
    Output("corr-graph", "figure"),
    Output("coal-shift-graph", "figure"),
    Input("theme-store", "data"),
)
def update_static_figs(theme):
    t = theme or "dark"
    return (
        make_gdp_fig(theme=t),
        make_intl_fig(theme=t),
        make_corr_fig(theme=t),
        make_coal_shift_fig(theme=t),
    )


# fill the hovered country's area on the intl chart
@app.callback(
    Output("intl-graph", "figure", allow_duplicate=True),
    Input("intl-graph", "hoverData"),
    State("theme-store", "data"),
    prevent_initial_call=True,
)
def intl_hover_fill(hover_data, theme):
    hovered = None
    if hover_data and hover_data.get("points"):
        hovered = hover_data["points"][0].get("fullData", {}).get("name")
    return make_intl_fig(theme=theme or "dark", hovered_country=hovered)


# toggle theme store between dark and light on button click
@app.callback(
    Output("theme-store", "data"),
    Output("theme-toggle", "children"),
    Output("theme-toggle", "style"),
    Input("theme-toggle", "n_clicks"),
)
def toggle_theme(n):
    if n and n % 2 == 1:
        return ("light", "Dark Mode", {
            "background": "#ffffff", "color": "#333",
            "border": "1px solid #bbb", "borderRadius": "4px",
            "padding": "6px 16px", "cursor": "pointer",
            "fontSize": "13px", "marginTop": "22px",
        })
    return ("dark", "Light Mode", {
        "background": "#2a2d3a", "color": "#eee",
        "border": "1px solid #555", "borderRadius": "4px",
        "padding": "6px 16px", "cursor": "pointer",
        "fontSize": "13px", "marginTop": "22px",
    })


# updates the main wrapper background and text color when theme changes
@app.callback(
    Output("main-wrapper", "style"),
    Input("theme-store", "data"),
)
def update_wrapper_style(theme):
    if theme == "light":
        return {
            "background": "#f5f6fa", "color": "#222",
            "fontFamily": "Segoe UI, Arial, sans-serif",
            "padding": "24px",
        }
    return {
        "background": BG_COLOR, "color": "#eee",
        "fontFamily": "Segoe UI, Arial, sans-serif",
        "padding": "24px",
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
