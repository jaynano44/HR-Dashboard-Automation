from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# ============================================================
# Page config
# ============================================================
st.set_page_config(
    page_title="HR Intelligence Dashboard",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded",
)

theme_mode = st.sidebar.radio("🎨 테마", ["Dark", "Light"], index=0)
# ============================================================
# Style
# ============================================================
if theme_mode == "Dark":
    CUSTOM_CSS = """
    <style>
    :root {
      --bg: #0b1020;
      --panel: #121933;
      --text: #f3f6ff;
      --muted: #aab6d6;
      --accent: #6ea8fe;
      --border: rgba(255,255,255,0.08);
    }

    .stApp {
      background:
        radial-gradient(circle at top left, rgba(110,168,254,0.18), transparent 25%),
        linear-gradient(180deg, #08101f 0%, #0d1530 100%);
      color: var(--text);
    }

    .card, .kpi-card {
      background: rgba(255,255,255,0.04);
      border: 1px solid var(--border);
      backdrop-filter: blur(10px);
    }

    .hero {
      background: linear-gradient(135deg, rgba(110,168,254,0.25), rgba(255,255,255,0.05));
    }
    </style>
    """

else:
    CUSTOM_CSS = """
    <style>
    :root {
      --bg: #f8fafc;
      --panel: #ffffff;
      --text: #0f172a;
      --muted: #64748b;
      --accent: #2563eb;
      --border: rgba(0,0,0,0.08);
    }

    .stApp {
      background: linear-gradient(180deg, #f1f5f9 0%, #ffffff 100%);
      color: var(--text);
    }

    .card, .kpi-card {
      background: white;
      border: 1px solid var(--border);
      box-shadow: 0 10px 25px rgba(0,0,0,0.06);
    }

    .hero {
      background: linear-gradient(135deg, rgba(37,99,235,0.15), rgba(255,255,255,0.8));
    }
    </style>
    """
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ============================================================
# Utilities
# ============================================================
def _pick_existing(paths: Iterable[str | Path]) -> Path:
    for p in paths:
        pp = Path(p)
        if pp.exists():
            return pp
    raise FileNotFoundError(f"Missing expected file. Tried: {list(map(str, paths))}")


def _to_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _safe_str(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _choose_col(df: pd.DataFrame, candidates: list[str], fallback_contains: list[str] | None = None) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    if fallback_contains:
        for col in df.columns:
            s = str(col).lower()
            if any(k.lower() in s for k in fallback_contains):
                return col
    return None


def _coalesce_columns(df: pd.DataFrame, candidates: list[str], out_name: str) -> pd.Series:
    s = pd.Series([pd.NA] * len(df), index=df.index, dtype="object")
    for c in candidates:
        if c in df.columns:
            s = s.combine_first(df[c].where(df[c].notna()))
    return s.rename(out_name)


def _build_org_path_from_levels(df: pd.DataFrame) -> pd.Series:
    level_cols = [c for c in ["org_level_1", "org_level_2", "org_level_3", "org_level_4", "org_level_5"] if c in df.columns]
    if level_cols:
        return df[level_cols].apply(
            lambda r: " > ".join([str(x).strip() for x in r if pd.notna(x) and str(x).strip()]), axis=1
        )

    if "org_path" in df.columns:
        return _safe_str(df["org_path"])

    parts = []
    org = _coalesce_columns(df, ["current_org", "org"], "org")
    dept = _coalesce_columns(df, ["current_dept", "dept"], "dept")
    for o, d in zip(org.fillna(""), dept.fillna("")):
        row = [x for x in [str(o).strip(), str(d).strip()] if x]
        parts.append(" > ".join(row))
    return pd.Series(parts, index=df.index, name="org_path")


def _split_org_path(path: str, depth: int = 5) -> list[str]:
    parts = [p.strip() for p in str(path).split(">") if str(p).strip()]
    parts = parts[:depth] + [""] * max(0, depth - len(parts))
    return parts[:depth]


# ============================================================
# Load data
# ============================================================
@st.cache_data(show_spinner=False)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    people_path = _pick_existing([
        "data/silver/people_master.csv",
        "data/processed/silver/people_master.csv",
    ])
    history_path = _pick_existing([
        "data/silver/employment_history.csv",
        "data/processed/silver/employment_history.csv",
    ])
    snapshot_path = _pick_existing([
        "data/processed/reference/org_snapshot.csv",
        "data/reference/org_snapshot.csv",
        "data/gold/org_snapshot.csv",
    ])

    people = pd.read_csv(people_path)
    history = pd.read_csv(history_path)
    snapshot = pd.read_csv(snapshot_path)

    # Normalize people
    people["display_org"] = _coalesce_columns(people, ["current_org", "org"], "display_org")
    people["display_dept"] = _coalesce_columns(people, ["current_dept", "dept"], "display_dept")
    people["first_join_date"] = _to_dt(people.get("first_join_date", pd.Series(dtype="object")))
    people["last_exit_date"] = _to_dt(people.get("last_exit_date", pd.Series(dtype="object")))
    people["employment_status"] = _safe_str(people.get("employment_status", pd.Series(dtype="object")))
    people["name"] = _safe_str(people.get("name", pd.Series(dtype="object")))

    # Normalize history
    if "hire_date" in history.columns and "join_date" not in history.columns:
        history["join_date"] = _to_dt(history["hire_date"])
    else:
        history["join_date"] = _to_dt(history.get("join_date", pd.Series(dtype="object")))

    history["exit_date"] = _to_dt(history.get("exit_date", pd.Series(dtype="object")))
    history["name"] = _safe_str(history.get("name", pd.Series(dtype="object")))
    history["org"] = _safe_str(_coalesce_columns(history, ["org", "current_org"], "org"))
    history["dept"] = _safe_str(_coalesce_columns(history, ["dept", "current_dept"], "dept"))
    history["emp_uid"] = _safe_str(_coalesce_columns(history, ["emp_uid"], "emp_uid"))

    history["join_year"] = history["join_date"].dt.year
    history["join_month"] = history["join_date"].dt.month
    history["exit_year"] = history["exit_date"].dt.year
    history["exit_month"] = history["exit_date"].dt.month

    # Normalize snapshot
    snapshot["name"] = _safe_str(snapshot.get("name", pd.Series(dtype="object")))
    snapshot["org"] = _safe_str(snapshot.get("org", pd.Series(dtype="object")))
    snapshot["dept"] = _safe_str(snapshot.get("dept", pd.Series(dtype="object")))
    snapshot["org_path"] = _build_org_path_from_levels(snapshot)

    levels = snapshot["org_path"].apply(_split_org_path).apply(pd.Series)
    levels.columns = [f"org_level_{i}" for i in range(1, len(levels.columns) + 1)]
    snapshot = pd.concat([snapshot, levels], axis=1)

    return people, history, snapshot


# ============================================================
# Business logic
# ============================================================
def get_years(people: pd.DataFrame, history: pd.DataFrame) -> list[int]:
    year_values = []
    for col in ["first_join_date", "last_exit_date"]:
        if col in people.columns:
            year_values.extend(people[col].dt.year.dropna().astype(int).tolist())
    for col in ["join_date", "exit_date"]:
        if col in history.columns:
            year_values.extend(history[col].dt.year.dropna().astype(int).tolist())

    years = sorted(set([y for y in year_values if y >= 2002]))
    return years if years else [pd.Timestamp.today().year]


def active_as_of(history: pd.DataFrame, as_of_date: pd.Timestamp) -> pd.DataFrame:
    d = history.copy()
    return d[
        d["join_date"].notna()
        & (d["join_date"] <= as_of_date)
        & (d["exit_date"].isna() | (d["exit_date"] > as_of_date))
    ].copy()


def yearly_summary(people: pd.DataFrame, history: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    rows = []
    for y in years:
        start_prev = pd.Timestamp(y - 1, 12, 31)
        end_curr = pd.Timestamp(y, 12, 31)
        joins = history[history["join_date"].dt.year == y]["emp_uid"].replace("", pd.NA).dropna().nunique()
        exits = history[history["exit_date"].dt.year == y]["emp_uid"].replace("", pd.NA).dropna().nunique()

        if joins == 0:
            joins = history[history["join_date"].dt.year == y]["name"].nunique()
        if exits == 0:
            exits = history[history["exit_date"].dt.year == y]["name"].nunique()

        headcount = active_as_of(history, end_curr)
        headcount_n = headcount["emp_uid"].replace("", pd.NA).dropna().nunique()
        if headcount_n == 0:
            headcount_n = headcount["name"].nunique()

        opening = active_as_of(history, start_prev)
        opening_n = opening["emp_uid"].replace("", pd.NA).dropna().nunique()
        if opening_n == 0:
            opening_n = opening["name"].nunique()

        rows.append({
            "year": y,
            "opening_headcount": opening_n,
            "headcount": headcount_n,
            "hires": joins,
            "exits": exits,
        })
    return pd.DataFrame(rows)


def monthly_summary(history: pd.DataFrame, year: int) -> pd.DataFrame:
    rows = []
    for m in range(1, 13):
        month_end = pd.Timestamp(year, m, 1) + pd.offsets.MonthEnd(1)
        active_n = active_as_of(history, month_end)
        hc = active_n["emp_uid"].replace("", pd.NA).dropna().nunique()
        if hc == 0:
            hc = active_n["name"].nunique()

        hires = history[(history["join_date"].dt.year == year) & (history["join_date"].dt.month == m)]
        exits = history[(history["exit_date"].dt.year == year) & (history["exit_date"].dt.month == m)]

        hires_n = hires["emp_uid"].replace("", pd.NA).dropna().nunique()
        exits_n = exits["emp_uid"].replace("", pd.NA).dropna().nunique()
        if hires_n == 0:
            hires_n = hires["name"].nunique()
        if exits_n == 0:
            exits_n = exits["name"].nunique()

        rows.append({
            "month": m,
            "headcount": hc,
            "hires": hires_n,
            "exits": exits_n,
        })
    return pd.DataFrame(rows)


def apply_org_filters(df: pd.DataFrame, levels_selected: list[str]) -> pd.DataFrame:
    d = df.copy()
    for idx, selected in enumerate(levels_selected, start=1):
        col = f"org_level_{idx}"
        if col in d.columns and selected != "전체":
            d = d[d[col] == selected]
    return d


def enrich_people_with_snapshot(people: pd.DataFrame, snapshot: pd.DataFrame) -> pd.DataFrame:
    snap = snapshot[[c for c in snapshot.columns if c in ["name", "org", "dept", "org_path", "org_level_1", "org_level_2", "org_level_3", "org_level_4", "org_level_5"]]].drop_duplicates(subset=["name"], keep="first")
    merged = people.merge(snap, on="name", how="left", suffixes=("", "_snap"))

    for c in ["org", "dept", "org_path", "org_level_1", "org_level_2", "org_level_3", "org_level_4", "org_level_5"]:
        snap_c = f"{c}_snap"
        if snap_c in merged.columns:
            merged[c] = merged[snap_c].combine_first(merged.get(c, pd.Series(index=merged.index, dtype="object")))

    return merged


def render_kpi(label, value, sub):
    bg = "rgba(255,255,255,0.06)" if theme_mode == "Dark" else "rgba(255,255,255,0.92)"
    value_color = "#f8fafc" if theme_mode == "Dark" else "#0f172a"
    sub_color = "#94a3b8" if theme_mode == "Dark" else "#475569"
    border = "rgba(255,255,255,0.08)" if theme_mode == "Dark" else "rgba(15,23,42,0.08)"

    st.markdown(f"""
    <div style="
        padding:16px;
        border-radius:18px;
        background:{bg};
        border:1px solid {border};
        backdrop-filter:blur(6px);
    ">
        <div style="color:{sub_color};font-size:13px">{label}</div>
        <div style="font-size:28px;font-weight:800;color:{value_color}">{value}</div>
        <div style="color:{sub_color};font-size:12px">{sub}</div>
    </div>
    """, unsafe_allow_html=True)


def build_tree_edges(snapshot_filtered: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, row in snapshot_filtered.iterrows():
        parts = [str(row.get(f"org_level_{i}", "")).strip() for i in range(1, 6)]
        parts = [p for p in parts if p]
        if not parts:
            fallback = [str(row.get("org", "")).strip(), str(row.get("dept", "")).strip()]
            parts = [p for p in fallback if p]
        for idx, part in enumerate(parts):
            parent = parts[idx - 1] if idx > 0 else "HR 전체"
            records.append((parent, part))
    if not records:
        return pd.DataFrame(columns=["parent", "child"])
    return pd.DataFrame(records, columns=["parent", "child"]).drop_duplicates()


def render_org_tree(snapshot_filtered: pd.DataFrame):
    edges = build_tree_edges(snapshot_filtered)

    if edges.empty:
        st.info("조직 데이터 없음")
        return None

    org_cols = [c for c in snapshot_filtered.columns if c.startswith("org_level_")]

    if not org_cols:
        st.warning("조직 컬럼 없음 (org_level_x)")
        return None

    org_col = org_cols[-1]   # 가장 깊은 조직 사용

    node_counts = snapshot_filtered.groupby(org_col).size().to_dict()

    def get_count(n):
        return node_counts.get(n, 0)

    nodes = set(edges["parent"]).union(set(edges["child"]))
    labels = ["HR"] + list(nodes)

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=list(range(len(labels))),
        y=[0]*len(labels),
        mode="markers+text",
        text=labels,
        textposition="bottom center",
        marker=dict(
            size=22,
            color="#6ea8fe" if theme_mode == "Dark" else "#2563eb",
            line=dict(width=2, color="#dbeafe" if theme_mode == "Dark" else "#bfdbfe"),
        ),
        customdata=labels,
        hovertemplate="%{customdata}<extra></extra>"
    ))

    fig.update_layout(height=400)

    clicked = st.plotly_chart(
        fig,
        width="stretch",
        key="org_tree",
        on_select="rerun"
    )

    return clicked



# ============================================================
# Data prep
# ============================================================
people, history, snapshot = load_data()
people = enrich_people_with_snapshot(people, snapshot)

# 기준 연도는 snapshot이 아니라 history/people에서 계산
years = get_years(people, history)
default_year = years[-1] if years else pd.Timestamp.today().year


# ============================================================
# Sidebar
# ============================================================
st.sidebar.markdown("## 🎛 탐색 필터")

selected_year = st.sidebar.selectbox("기준 연도", years, index=len(years) - 1) if years else pd.Timestamp.today().year
selected_year = int(selected_year)
selected_month = st.sidebar.selectbox("기준 월", ["연간"] + list(range(1, 13)), index=0)

as_of_date = (
    pd.Timestamp(selected_year, 12, 31)
    if selected_month == "연간"
    else pd.Timestamp(selected_year, int(selected_month), 1) + pd.offsets.MonthEnd(1)
)

# 기준 연도/월 시점 재직자만 먼저 계산
active_people = active_as_of(history, as_of_date)

# 사람 마스터의 조직 정보 붙이기
active_people = active_people.merge(
    people[
        [c for c in [
            "emp_uid", "name", "org", "dept", "org_path",
            "org_level_1", "org_level_2", "org_level_3", "org_level_4", "org_level_5",
            "current_title", "current_grade", "employment_status",
            "first_join_date", "last_exit_date"
        ] if c in people.columns]
    ].drop_duplicates(subset=["emp_uid", "name"]),
    on=[c for c in ["emp_uid", "name"] if c in history.columns or c in people.columns],
    how="left",
)

if "org_path" not in active_people.columns:
    active_people["org_path"] = _build_org_path_from_levels(active_people)
    levels = active_people["org_path"].apply(_split_org_path).apply(pd.Series)
    levels.columns = [f"org_level_{i}" for i in range(1, len(levels.columns) + 1)]
    active_people = pd.concat([active_people, levels], axis=1)

# 왼쪽 조직 필터는 "선택 연도 기준 재직자 데이터"에서만 생성
snapshot_year = active_people.copy()

level_selections = []
current_df = snapshot_year.copy()

for i in range(1, 6):
    col = f"org_level_{i}"

    if col not in current_df.columns:
        level_selections.append("전체")
        continue

    values = sorted([
        v for v in current_df[col].dropna().astype(str).unique().tolist()
        if v.strip()
    ])

    options = ["전체"] + values
    selected = st.sidebar.selectbox(f"조직 Depth {i}", options, index=0)
    level_selections.append(selected)

    if selected != "전체":
        current_df = current_df[current_df[col] == selected]

# ============================================================
# Filtered datasets
# ============================================================
# Sidebar에서 만든 active_people에 조직 필터 적용
active_people = apply_org_filters(active_people, level_selections)

# 조직 트리도 같은 기준 연도 재직자 기준으로 맞춤
snapshot_filtered = active_people.copy()

yearly_df = yearly_summary(people, history, years)
monthly_df = monthly_summary(history, selected_year)

selected_people_count = active_people["emp_uid"].replace("", pd.NA).dropna().nunique()
if selected_people_count == 0:
    selected_people_count = active_people["name"].nunique()

selected_hires = history[(history["join_date"].dt.year == selected_year)].copy()
selected_exits = history[(history["exit_date"].dt.year == selected_year)].copy()

selected_hires = apply_org_filters(
    selected_hires.merge(
        people[[c for c in ["name", "org_level_1", "org_level_2", "org_level_3", "org_level_4", "org_level_5"] if c in people.columns]]
        .drop_duplicates(subset=["name"]),
        on="name",
        how="left",
    ),
    level_selections,
)

selected_exits = apply_org_filters(
    selected_exits.merge(
        people[[c for c in ["name", "org_level_1", "org_level_2", "org_level_3", "org_level_4", "org_level_5"] if c in people.columns]]
        .drop_duplicates(subset=["name"]),
        on="name",
        how="left",
    ),
    level_selections,
)

# ============================================================
# Header
# ============================================================
st.markdown(
    f"""
    <div class=\"hero\">
      <div class=\"hero-title\">HR Intelligence Dashboard</div>
      <div class=\"hero-sub\">기준 연도 <b>{selected_year}</b> · 기준 월 <b>{selected_month}</b> · 선택 조직 범위에 맞는 인력/입사/퇴사/조직 트리를 실시간으로 탐색합니다.</div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# KPI Row
# ============================================================
k1, k2, k3, k4 = st.columns(4)
with k1:
    render_kpi("총 인원", f"{selected_people_count:,}", f"기준일 {as_of_date.date()}")
with k2:
    render_kpi("입사", f"{selected_hires['name'].nunique():,}", f"{selected_year}년")
with k3:
    render_kpi("퇴사", f"{selected_exits['name'].nunique():,}", f"{selected_year}년")
with k4:
    prev_row = yearly_df[yearly_df["year"] == selected_year - 1]
    prev_hc = int(prev_row["headcount"].iloc[0]) if not prev_row.empty else 0
    delta = selected_people_count - prev_hc
    sign = "+" if delta >= 0 else ""
    render_kpi("전년 대비", f"{sign}{delta:,}", "총 인원 증감")


# ============================================================
# Layout
# ============================================================
left, right = st.columns([1.2, 0.8], gap="large")

with left:
    st.markdown('<div class="section-title">연도별 인력 · 입사 · 퇴사 추이</div>', unsafe_allow_html=True)
    fig_year = go.Figure()
    fig_year.add_trace(go.Bar(name="입사", x=yearly_df["year"], y=yearly_df["hires"]))
    fig_year.add_trace(go.Bar(name="퇴사", x=yearly_df["year"], y=yearly_df["exits"]))
    fig_year.add_trace(go.Scatter(name="총 인원", x=yearly_df["year"], y=yearly_df["headcount"], mode="lines+markers", yaxis="y2"))
    common_layout = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e5e7eb" if theme_mode=="Dark" else "#111827"),
    )
    fig_year.update_layout(
        **common_layout,
        height=430,
        barmode="group",
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", y=1.02, x=0),
        yaxis2=dict(overlaying="y", side="right", showgrid=False),
    )
    st.plotly_chart(fig_year, width="stretch")

    st.markdown('<div class="section-title">월별 인원 · 입사 · 퇴사</div>', unsafe_allow_html=True)
    fig_month = go.Figure()
    fig_month.add_trace(go.Bar(name="입사", x=monthly_df["month"], y=monthly_df["hires"]))
    fig_month.add_trace(go.Bar(name="퇴사", x=monthly_df["month"], y=monthly_df["exits"]))
    fig_month.add_trace(go.Scatter(name="총 인원", x=monthly_df["month"], y=monthly_df["headcount"], mode="lines+markers", yaxis="y2"))
    fig_month.update_layout(
        **common_layout,
        height=420,
        barmode="group",
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", y=1.02, x=0),
        yaxis2=dict(overlaying="y", side="right", showgrid=False),
    )
    st.plotly_chart(fig_month, width="stretch")

with right:
    st.markdown('<div class="section-title">기준 연도 조직 트리</div>', unsafe_allow_html=True)

    clicked = render_org_tree(snapshot_filtered)

    if clicked and "selection" in clicked:
        selected_node = clicked["selection"]["points"][0]["customdata"]

        if selected_node != "HR 전체":

            mask = False

            for col in [c for c in active_people.columns if c.startswith("org_level_")]:
                mask = mask | active_people[col].astype(str).str.contains(selected_node)

            active_people = active_people[mask]
    

# ============================================================
# Detail section
# ============================================================
st.markdown('<div class="section-title">선택 조직 소속 인원</div>', unsafe_allow_html=True)
people_view_cols = [c for c in ["name", "org", "dept", "current_title", "current_grade", "employment_status", "first_join_date", "last_exit_date"] if c in people.columns or c in active_people.columns]
base_people_df = active_people.copy()
if "first_join_date" not in base_people_df.columns and "name" in people.columns:
    base_people_df = base_people_df.merge(
        people[[c for c in ["name", "first_join_date", "last_exit_date", "employment_status", "current_title", "current_grade"] if c in people.columns]].drop_duplicates(subset=["name"]),
        on="name", how="left"
    )

show_cols = [c for c in ["name", "org", "dept", "current_title", "current_grade", "employment_status", "first_join_date", "last_exit_date"] if c in base_people_df.columns]
people_display = base_people_df[show_cols].drop_duplicates().sort_values([c for c in ["org", "dept", "name"] if c in base_people_df.columns])
st.dataframe(people_display, width="stretch", hide_index=True, height=420)

st.markdown('<div class="section-title">조직별 인원 분포</div>', unsafe_allow_html=True)
org_chart_df = people_display.copy()
org_cols = [c for c in active_people.columns if c.startswith("org_level_")]

if org_cols:
    org_col = org_cols[-1]
else:
    org_col = None
if org_col and not org_chart_df.empty:
    top_org = org_chart_df.groupby(org_col).size().reset_index(name="headcount").sort_values("headcount", ascending=False).head(15)
    fig_org = px.bar(top_org, x=org_col, y="headcount")
    fig_org.update_layout(
        **common_layout,
        height=360,
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig_org, width="stretch")
else:
    st.info("표시할 조직 분포 데이터가 없습니다.")

st.caption("권장 위치: 프로젝트 루트에서 `streamlit run src/app/app.py` 실행")
