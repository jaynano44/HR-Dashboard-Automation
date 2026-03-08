from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.processor.build_dataset import build_datasets, load_config


def _df_info(df: pd.DataFrame | None) -> str:
    if df is None:
        return "❌ (None)"
    if isinstance(df, pd.DataFrame) and df.empty:
        return "⚠️ (empty)"
    return f"✅ ({len(df):,} rows)"


def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()


def _plot_line(df: pd.DataFrame, x: str, ys: list[str], title: str):
    if df is None or df.empty or x not in df.columns:
        st.caption(f"{title} (표시할 데이터 없음)")
        return
    tmp = df.copy()
    try:
        tmp[x] = pd.to_datetime(tmp[x], errors="coerce")
        tmp = tmp.set_index(x)
        keep = [y for y in ys if y in tmp.columns]
        if keep:
            st.line_chart(tmp[keep], use_container_width=True)
            st.caption(title)
        else:
            st.dataframe(df, use_container_width=True)
    except Exception:
        st.dataframe(df, use_container_width=True)


def run() -> None:
    st.set_page_config(page_title="HR Dashboard Automation MVP", layout="wide")

    cfg = load_config("config.yaml")

    @st.cache_data(show_spinner=True)
    def _build_cached():
        return build_datasets("config.yaml")

    build = _build_cached()

    if build is None:
        st.error("build_datasets()가 None을 반환했습니다. build_dataset.py를 확인하세요.")
        st.stop()

    processed_root = Path(cfg["paths"]["processed_dir"])
    bronze_dir = processed_root / "bronze"
    silver_dir = processed_root / "silver"
    gold_dir = processed_root / "gold"
    qa_dir = processed_root / "qa"
    addons_dir = processed_root / "addons"

    master = build.master_auto.copy() if isinstance(build.master_auto, pd.DataFrame) else pd.DataFrame()

    st.title("HR Dashboard Automation MVP / v2.1")

    # -------------------
    # Sidebar
    # -------------------
    st.sidebar.header("📦 Ingest 상태")
    rep = build.ingest_report or {}
    st.sidebar.metric("raw 파일 수", rep.get("raw_files", 0))
    st.sidebar.metric("읽기 성공", rep.get("read_ok", 0))
    st.sidebar.metric("읽기 실패", rep.get("read_fail", 0))
    st.sidebar.metric("master_auto rows", rep.get("master_auto_rows", 0))

    with st.sidebar.expander("읽기 실패 파일 리스트", expanded=False):
        fails = rep.get("read_fail_list", [])
        if fails:
            st.write(fails)
        else:
            st.caption("실패 없음")

    with st.sidebar.expander("분류 스킵(부가데이터) 리스트", expanded=False):
        cs = rep.get("classified_skips", {}) or {}
        shown = False
        for k in ["aux_pricing", "aux_recruit", "aux_skill", "aux_personalcard", "aux_reference_roster", "org_chart", "legacy_xls", "non_excel", "unmatched_excel"]:
            items = cs.get(k, [])
            if items:
                shown = True
                st.markdown(f"**{k}**")
                st.write(items[:30])
                if len(items) > 30:
                    st.caption(f"... and {len(items)-30} more")
        if not shown:
            st.caption("분류 스킵 없음")

    st.sidebar.divider()
    st.sidebar.header("🔎 필터")

    years = []
    if "snapshot_year" in master.columns:
        years = sorted([int(x) for x in pd.to_numeric(master["snapshot_year"], errors="coerce").dropna().unique().tolist()])
    year_sel = st.sidebar.multiselect("기준연도(snapshot_year)", years, default=years[-1:] if years else [])

    orgs = sorted([x for x in master.get("org", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()]) if not master.empty else []
    org_sel = st.sidebar.multiselect("본부/소속(org)", orgs, default=[])

    depts = []
    if not master.empty and "dept" in master.columns:
        dtmp = master.copy()
        if org_sel and "org" in dtmp.columns:
            dtmp = dtmp[dtmp["org"].astype(str).isin(org_sel)]
        depts = sorted([x for x in dtmp["dept"].dropna().astype(str).unique().tolist()])
    dept_sel = st.sidebar.multiselect("부서/팀(dept)", depts, default=[])

    headcount_mode = st.sidebar.radio(
        "총인원 기준",
        options=["추정(스냅샷+퇴사일)", "GroundTruth(2024 재직자 시트)"],
        index=0,
    )

    def _apply_filters(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        out = df.copy()
        if year_sel and "snapshot_year" in out.columns:
            out["snapshot_year"] = pd.to_numeric(out["snapshot_year"], errors="coerce").astype("Int64")
            out = out[out["snapshot_year"].isin([int(y) for y in year_sel])]
        if org_sel and "org" in out.columns:
            out = out[out["org"].astype(str).isin(org_sel)]
        if dept_sel and "dept" in out.columns:
            out = out[out["dept"].astype(str).isin(dept_sel)]
        return out

    master_f = _apply_filters(master)

    total_hc = build.total_headcount_groundtruth if headcount_mode.startswith("GroundTruth") and build.total_headcount_groundtruth > 0 else build.total_headcount_latest_est
    hc_note = "GroundTruth(재직자 시트)" if headcount_mode.startswith("GroundTruth") and build.total_headcount_groundtruth > 0 else "추정(스냅샷+퇴사일)"

    # -------------------
    # Tabs
    # -------------------
    tab_overview, tab_yearly, tab_monthly, tab_dept, tab_early, tab_qa, tab_addons, tab_settings = st.tabs(
        ["요약", "총인력변동(연도)", "월별 추세", "부서(팀) 지표", "30일 이내 퇴사", "QA", "Addons", "설정/진단"]
    )

    with tab_overview:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("총인력", f"{total_hc:,}", help=hc_note)
        c2.metric("master_auto(전체)", f"{len(master):,}")
        c3.metric("master_auto(필터)", f"{len(master_f):,}")
        c4.metric("30일 이내 퇴사", f"{len(build.early_exit_30d):,}" if isinstance(build.early_exit_30d, pd.DataFrame) else "0")

        st.subheader("master_auto(전체)")
        st.caption(_df_info(master))
        st.dataframe(master.head(100), use_container_width=True)

        st.subheader("master_auto(필터)")
        st.caption(_df_info(master_f))
        st.dataframe(master_f.head(100), use_container_width=True)

    with tab_yearly:
        st.subheader("총인력변동(연도)")
        yearly = build.turnover_yearly.copy() if isinstance(build.turnover_yearly, pd.DataFrame) else _read_csv_safe(gold_dir / "turnover_yearly.csv")
        st.caption(_df_info(yearly))
        st.dataframe(yearly, use_container_width=True)
        if not yearly.empty:
            _plot_line(yearly.rename(columns={"year": "x"}), "x", ["hires", "exits", "net"], "연도별 입/퇴사/순증감")

    with tab_monthly:
        st.subheader("월별 추세")
        monthly = build.monthly.copy() if isinstance(build.monthly, pd.DataFrame) else _read_csv_safe(gold_dir / "monthly.csv")
        st.caption(_df_info(monthly))
        st.dataframe(monthly, use_container_width=True)
        if not monthly.empty:
            _plot_line(monthly, "month", ["hires", "exits", "net"], "월별 입/퇴사/순증감")

    with tab_dept:
        st.subheader("부서(팀) 지표")
        dept_monthly = build.dept_monthly.copy() if isinstance(build.dept_monthly, pd.DataFrame) else _read_csv_safe(gold_dir / "dept_monthly.csv")
        st.caption(_df_info(dept_monthly))
        st.dataframe(dept_monthly.head(300), use_container_width=True)

    with tab_early:
        st.subheader("30일 이내 퇴사")
        early = build.early_exit_30d.copy() if isinstance(build.early_exit_30d, pd.DataFrame) else _read_csv_safe(gold_dir / "early_exit_30d.csv")
        st.caption(_df_info(early))
        st.dataframe(early.head(300), use_container_width=True)

    with tab_qa:
        st.header("QA")
        qa_report = build.qa_report or {}
        st.subheader("qa_report.json")
        st.json(qa_report, expanded=False)

        st.subheader("mismatch_ref_only (reference에만 있고 master에 없는 이름)")
        ref_only = build.qa_mismatch_ref_only.copy() if isinstance(build.qa_mismatch_ref_only, pd.DataFrame) else _read_csv_safe(qa_dir / "mismatch_ref_only.csv")
        st.caption(_df_info(ref_only))
        st.dataframe(ref_only.head(300), use_container_width=True)

        st.subheader("mismatch_master_only (master에만 있고 reference에 없는 이름)")
        master_only = build.qa_mismatch_master_only.copy() if isinstance(build.qa_mismatch_master_only, pd.DataFrame) else _read_csv_safe(qa_dir / "mismatch_master_only.csv")
        st.caption(_df_info(master_only))
        st.dataframe(master_only.head(300), use_container_width=True)

    with tab_addons:
        st.header("Addons (v2 플러그인 1차)")
        st.caption("현재는 최소 컬럼 정형화 수준. 추후 emp_id 매핑/정합성 연결 예정.")

        st.subheader("aux_skill.csv")
        aux_skill = build.aux_skill.copy() if isinstance(build.aux_skill, pd.DataFrame) else _read_csv_safe(addons_dir / "aux_skill.csv")
        st.caption(_df_info(aux_skill))
        st.dataframe(aux_skill.head(300), use_container_width=True)

        st.subheader("aux_recruit.csv")
        aux_recruit = build.aux_recruit.copy() if isinstance(build.aux_recruit, pd.DataFrame) else _read_csv_safe(addons_dir / "aux_recruit.csv")
        st.caption(_df_info(aux_recruit))
        st.dataframe(aux_recruit.head(300), use_container_width=True)

        st.subheader("reference_roster_2021_2024.csv (검증용)")
        ref = build.reference_roster.copy() if isinstance(build.reference_roster, pd.DataFrame) else _read_csv_safe(addons_dir / "reference_roster_2021_2024.csv")
        st.caption(_df_info(ref))
        st.dataframe(ref.head(300), use_container_width=True)

    with tab_settings:
        st.subheader("설정/진단")
        st.json(rep, expanded=False)
        st.write(
            {
                "bronze": str(bronze_dir).replace("\\", "/"),
                "silver": str(silver_dir).replace("\\", "/"),
                "gold": str(gold_dir).replace("\\", "/"),
                "qa": str(qa_dir).replace("\\", "/"),
                "addons": str(addons_dir).replace("\\", "/"),
            }
        )