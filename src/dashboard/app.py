from __future__ import annotations

import platform
import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from src.processor.build_dataset import build_datasets, load_config
from src.agent.missing import find_missing_department, apply_department_fix, save_fixed

import plotly.graph_objects as go
import plotly.express as px

# -----------------------------
# UI Helpers
# -----------------------------
def set_korean_font():
    if platform.system() == "Windows":
        plt.rcParams["font.family"] = "Malgun Gothic"
    elif platform.system() == "Darwin":
        plt.rcParams["font.family"] = "AppleGothic"
    else:
        plt.rcParams["font.family"] = "NanumGothic"
    plt.rcParams["axes.unicode_minus"] = False


def inject_css():
    st.markdown(
        """
<style>
/* Layout */
.block-container {padding-top: 1.0rem; padding-bottom: 2.0rem; max-width: 1300px;}
/* Metric cards */
div[data-testid="stMetric"]{
  background: #ffffff;
  border: 1px solid #eee;
  padding: 14px 16px;
  border-radius: 14px;
  box-shadow: 0 4px 14px rgba(0,0,0,0.04);
}
div[data-testid="stMetric"] label {font-size: 0.85rem;}
/* Section headers */
.section-title{
  font-size: 1.05rem;
  font-weight: 700;
  margin: 0.25rem 0 0.4rem 0;
}
.section-sub{
  font-size: 0.90rem;
  color: #666;
  margin-bottom: 0.5rem;
}
hr.soft{
  border: none;
  border-top: 1px solid #eee;
  margin: 0.8rem 0 1.0rem 0;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def kpi_row(kpis: list[tuple[str, str | int | float, str | None]]):
    """
    kpis: [(label, value, delta_text_or_None), ...]
    """
    cols = st.columns(len(kpis))
    for i, (label, value, delta) in enumerate(kpis):
        cols[i].metric(label, value, delta=delta)


def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


# -----------------------------
# Plot Helpers
# -----------------------------
def _auto_xtick_labels(labels: list[str], target_ticks: int = 12):
    """
    For long monthly series, show only ~target_ticks labels.
    Returns (display_labels, fontsize)
    """
    n = len(labels)
    if n <= 12:
        fontsize = 11
        return labels, fontsize

    if n > 60:
        fontsize = 6
    elif n > 36:
        fontsize = 7
    elif n > 24:
        fontsize = 8
    else:
        fontsize = 9

    step = max(1, n // target_ticks)
    display = [labels[i] if i % step == 0 else "" for i in range(n)]
    return display, fontsize


def plotly_monthly_growth(monthly: pd.DataFrame, title: str = "월별 입/퇴사 및 총원(누적)"):
    m = monthly.copy()
    m["period"] = m["period"].astype(str)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=m["period"], y=m["hires"], name="입사",
        hovertemplate="월=%{x}<br>입사=%{y}<extra></extra>"
    ))
    fig.add_trace(go.Bar(
        x=m["period"], y=m["exits"], name="퇴사",
        hovertemplate="월=%{x}<br>퇴사=%{y}<extra></extra>"
    ))
    fig.add_trace(go.Scatter(
        x=m["period"], y=m["headcount"],
        name="총원(누적)", mode="lines+markers",
        yaxis="y2",
        hovertemplate="월=%{x}<br>총원(누적)=%{y}<extra></extra>"
    ))

    fig.update_layout(
        title=title,
        barmode="group",
        height=520,
        margin=dict(l=40, r=40, t=60, b=60),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis=dict(
            title="년월",
            type="category",
            tickangle=-45,
            rangeslider=dict(visible=True)  # ✅ 하단 슬라이더로 확대/축소
        ),
        yaxis=dict(title="입/퇴사(명)"),
        yaxis2=dict(title="총원(누적)", overlaying="y", side="right"),
    )

    # ✅ 기본 줌/드래그 활성화
    fig.update_layout(dragmode="pan")
    fig.update_xaxes(fixedrange=False)
    fig.update_yaxes(fixedrange=False)

    return fig


def plotly_bar_top(df: pd.DataFrame, x_col: str, y_col: str, title: str, top_n: int = 15):
    d = df.copy().head(top_n)
    fig = px.bar(
        d, x=x_col, y=y_col,
        title=title,
        height=480
    )
    fig.update_layout(
        margin=dict(l=40, r=40, t=60, b=80),
        xaxis_title="조직",
        yaxis_title="인원",
    )
    fig.update_xaxes(tickangle=-45)
    return fig


# -----------------------------
# Filters
# -----------------------------
def sidebar_filters(build) -> dict:
    """
    Returns dict:
      - year: '전체' or 'YYYY'
      - period_range: (start_period, end_period) or None
      - top_n: int
    """
    st.sidebar.header("필터")

    filters = {"year": "전체", "period_range": None, "top_n": 15}

    if getattr(build, "monthly", None) is not None and build.monthly is not None and not build.monthly.empty:
        m = build.monthly.copy()
        m["year"] = m["period"].astype(str).str.slice(0, 4)
        years = sorted(m["year"].dropna().unique().tolist())
        filters["year"] = st.sidebar.selectbox("연도 선택", options=["전체"] + years, index=0)

        # optional range within selected year/all
        m2 = m if filters["year"] == "전체" else m[m["year"] == filters["year"]]
        periods = m2["period"].astype(str).tolist()
        if len(periods) >= 2:
            start = st.sidebar.selectbox("시작 월", options=periods, index=0)
            end = st.sidebar.selectbox("종료 월", options=periods, index=len(periods) - 1)
            filters["period_range"] = (start, end)

    filters["top_n"] = st.sidebar.slider("Top N", min_value=5, max_value=30, value=15, step=1)
    st.sidebar.caption("※ 월별/바로퇴사 지표는 '입사일/퇴사일' 데이터가 있어야 계산됩니다.")
    return filters


def apply_monthly_filters(monthly: pd.DataFrame, filters: dict) -> pd.DataFrame:
    m = monthly.copy()
    if m.empty:
        return m

    if filters.get("year") and filters["year"] != "전체":
        m = m[m["period"].astype(str).str.startswith(filters["year"])]

    pr = filters.get("period_range")
    if pr:
        s, e = pr
        m = m[(m["period"] >= s) & (m["period"] <= e)]

    return m.reset_index(drop=True)


# -----------------------------
# Main App
# -----------------------------
def run():
    set_korean_font()
    st.set_page_config(page_title="HR Dashboard (UI Frame)", layout="wide")
    inject_css()

    st.title("HR 자동화 대시보드")
    st.caption("UI 프레임 고정 버전 — 탭을 계속 늘려도 깨지지 않게 구성했습니다.")

    # Load
    cfg = load_config("config.yaml")
    build = build_datasets("config.yaml")

    # Filters
    filters = sidebar_filters(build)

    # Tabs (확장형)
    tabs = st.tabs(
        [
            "🏠 요약",
            "📈 월별 추세",
            "🏢 조직/바로퇴사",
            "📊 부서별 지표(추가예정)",
            "🧪 채용 퍼널(추가예정)",
            "🧩 설문/등급(추가예정)",
            "⚙️ 데이터/설정",
        ]
    )
    tab_home, tab_monthly, tab_org, tab_dept, tab_funnel, tab_survey, tab_settings = tabs

    # -----------------------------
    # TAB: HOME
    # -----------------------------
    with tab_home:
        st.markdown('<div class="section-title">핵심 요약</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">현재 로드된 데이터 기준으로 주요 KPI를 요약합니다.</div>', unsafe_allow_html=True)

        # KPI from monthly if exists
        total_hires = total_exits = net = 0
        last_headcount = None

        if getattr(build, "monthly", None) is not None and build.monthly is not None and not build.monthly.empty:
            m = apply_monthly_filters(build.monthly, filters)
            total_hires = safe_int(m["hires"].sum())
            total_exits = safe_int(m["exits"].sum())
            net = total_hires - total_exits
            if len(m) > 0:
                last_headcount = safe_int(m["headcount"].iloc[-1])
        else:
            # fallback to turnover table counts
            tf = build.turnover
            total_hires = safe_int((tf["type"] == "hire").sum())
            total_exits = safe_int((tf["type"] == "exit").sum())
            net = total_hires - total_exits

        kpis = [
            ("총 입사자", total_hires, None),
            ("총 퇴사자", total_exits, None),
            ("순증감", net, None),
        ]
        if last_headcount is not None:
            kpis.append(("총원(누적)", last_headcount, None))

        kpi_row(kpis)

        st.markdown('<hr class="soft">', unsafe_allow_html=True)

        # Quick narrative
        c1, c2 = st.columns([1.5, 1])
        with c1:
            st.markdown('<div class="section-title">요약 메모</div>', unsafe_allow_html=True)
            st.write(
                "- 월별 데이터가 있으면 **입/퇴사 + 총원(누적)**을 우선 표시합니다.\n"
                "- 데이터가 없으면 기존 **입/퇴사 명단(turnover)** 기반으로 요약만 제공합니다.\n"
                "- 다음 단계에서 ‘부서별 입/퇴사율’, ‘채용 퍼널’, ‘설문/등급’을 탭으로 확장합니다."
            )
        with c2:
            st.markdown('<div class="section-title">데이터 상태</div>', unsafe_allow_html=True)
            has_monthly = getattr(build, "monthly", None) is not None and build.monthly is not None and not build.monthly.empty
            has_early = getattr(build, "early_exit_30d", None) is not None and build.early_exit_30d is not None and not build.early_exit_30d.empty
            st.write(f"- 월별 집계: {'✅' if has_monthly else '❌'}")
            st.write(f"- 30일 이내 퇴사: {'✅' if has_early else '❌'}")
            st.write("- (추가예정) 채용 퍼널/설문/보상: ⏳")

    # -----------------------------
    # TAB: MONTHLY
    # -----------------------------
    with tab_monthly:
        st.markdown('<div class="section-title">월별 입/퇴사 및 총원(누적)</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">연도/기간 필터에 따라 차트를 자동 축약/확대합니다.</div>', unsafe_allow_html=True)

        if getattr(build, "monthly", None) is None or build.monthly is None or build.monthly.empty:
            st.warning("월별 추세를 만들 수 있는 '입사일/퇴사일' 데이터가 아직 로드되지 않았습니다.")
        else:
            m = apply_monthly_filters(build.monthly, filters)

            st.plotly_chart(plotly_monthly_growth(m), use_container_width=True)

            with st.expander("월별 집계 테이블 보기", expanded=False):
                st.dataframe(m, use_container_width=True)

    # -----------------------------
    # TAB: ORG / EARLY EXIT
    # -----------------------------
    with tab_org:
        st.markdown('<div class="section-title">조직/바로퇴사(입사 후 30일 이내)</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">조직별 리스크 신호를 빠르게 확인합니다.</div>', unsafe_allow_html=True)

        if getattr(build, "early_exit_30d", None) is None or build.early_exit_30d is None or build.early_exit_30d.empty:
            st.info("30일 이내 퇴사자를 계산할 데이터가 없습니다. (퇴사자 시트의 입사일/퇴사일 필요)")
        else:
            top_n = filters.get("top_n", 15)
            st.plotly_chart(
                plotly_bar_top(build.early_exit_30d, "group", "early_exit_30d", f"조직별 30일 이내 퇴사자수 (Top {top_n})", top_n=top_n),
                use_container_width=True
            )
            with st.expander("조직별 30일 이내 퇴사자 테이블", expanded=False):
                st.dataframe(build.early_exit_30d, use_container_width=True)

        st.markdown('<hr class="soft">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">누락 데이터 보정(대화형)</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">부서/직무 누락을 탐지하고 즉시 반영합니다.</div>', unsafe_allow_html=True)

        grade = build.grade
        dept_col = cfg["columns"]["grade"]["dept"]
        name_col = cfg["columns"]["grade"]["name"]

        missing = find_missing_department(grade, dept_col=dept_col)
        st.caption(f"부서({dept_col}) 누락 인원: {len(missing)}명")

        if len(missing) > 0:
            colA, colB = st.columns([1, 2])
            with colA:
                pick_name = st.selectbox("부서가 비어있는 이름 선택", options=missing[name_col].astype(str).tolist())
            with colB:
                new_dept = st.text_input("부서 입력", value="")

            if st.button("부서 저장 → processed 반영"):
                fixed = apply_department_fix(
                    grade.copy(),
                    name=pick_name,
                    dept=new_dept,
                    name_col=name_col,
                    dept_col=dept_col,
                )
                out_path = f"{cfg['paths']['processed_dir']}/grade_fixed.csv"
                save_fixed(fixed, out_path)
                st.success(f"저장 완료: {out_path}")

    # -----------------------------
    # PLACEHOLDER TABS
    # -----------------------------
    with tab_dept:
        st.info("여기에 ‘부서별 입사율 vs 퇴사율’, ‘부서별 입/퇴사자수’ 등 차트를 추가합니다.")
        st.write(
            "- 입력: (processed) 월별 집계 + 조직 매핑\n"
            "- 출력: 바 차트/비율 차트/Top N\n"
            "- 구현 방식: processor에 KPI 함수 추가 → dashboard 탭에 연결"
        )

    with tab_funnel:
        st.info("여기에 ‘면접 → 합격 → 정규직/프리’ 채용 퍼널(월별) 차트를 추가합니다.")
        st.write(
            "- 입력: 채용 데이터(지원일/면접일/확정일) 또는 별도 엑셀\n"
            "- 출력: Funnel + 월별 추세\n"
            "- 구현 방식: processor에 funnel 집계 함수 추가 → dashboard 탭에 연결"
        )

    with tab_survey:
        st.info("여기에 ‘설문/등급(몰입, eNPS, 만족도)’ 분석 탭을 추가합니다.")
        st.write(
            "- 입력: 설문 시트(없으면 placeholder 유지)\n"
            "- 출력: Heatmap / Radar / 분포\n"
            "- 구현 방식: headcount_2024_loader에서 survey 로드 → processor에서 집계"
        )

    # -----------------------------
    # TAB: SETTINGS / DATA
    # -----------------------------
    with tab_settings:
        st.markdown('<div class="section-title">설정/데이터 확인</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">config.yaml / processed 결과를 빠르게 확인합니다.</div>', unsafe_allow_html=True)

        c1, c2 = st.columns(2)
        with c1:
            st.write("**config.yaml 요약**")
            st.json(cfg)

        with c2:
            st.write("**빌드 결과(데이터프레임 상태)**")
            st.write(f"- grade: {None if build.grade is None else build.grade.shape}")
            st.write(f"- turnover: {None if build.turnover is None else build.turnover.shape}")
            st.write(f"- turnover_yearly: {None if build.turnover_yearly is None else build.turnover_yearly.shape}")
            st.write(f"- monthly: {None if build.monthly is None else build.monthly.shape}")
            st.write(f"- early_exit_30d: {None if build.early_exit_30d is None else build.early_exit_30d.shape}")

        st.markdown('<hr class="soft">', unsafe_allow_html=True)
        st.caption("실행: streamlit run main.py")


if __name__ == "__main__":
    run()