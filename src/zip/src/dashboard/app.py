# src/dashboard/app.py
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


def run() -> None:
    st.set_page_config(page_title="HR Dashboard Automation MVP", layout="wide")

    cfg = load_config("config.yaml")

    @st.cache_data(show_spinner=True)
    def _build_cached():
        return build_datasets("config.yaml")

    build = _build_cached()

    processed_root = Path(cfg["paths"]["processed_dir"])
    bronze_dir = processed_root / "bronze"
    silver_dir = processed_root / "silver"
    gold_dir = processed_root / "gold"
    qa_dir = processed_root / "qa"
    aux_dir = processed_root / "aux"

    # -------------------
    # Sidebar (filters + ingest summary)
    # -------------------
    st.sidebar.header("필터")
    master = build.master_auto.copy() if isinstance(build.master_auto, pd.DataFrame) else pd.DataFrame()

    # year filter
    years = []
    if "snapshot_year" in master.columns:
        years = sorted([int(x) for x in pd.to_numeric(master["snapshot_year"], errors="coerce").dropna().unique().tolist()])
    year_sel = st.sidebar.multiselect("기준연도(snapshot_year)", years, default=years[-1:] if years else [])

    # org/dept filter
    orgs = sorted([x for x in master.get("org", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()]) if not master.empty else []
    org_sel = st.sidebar.multiselect("본부/소속(org)", orgs, default=[])

    depts = []
    if not master.empty and "dept" in master.columns:
        dtmp = master.copy()
        if org_sel and "org" in dtmp.columns:
            dtmp = dtmp[dtmp["org"].astype(str).isin(org_sel)]
        depts = sorted([x for x in dtmp["dept"].dropna().astype(str).unique().tolist()])
    dept_sel = st.sidebar.multiselect("부서/팀(dept)", depts, default=[])

    st.sidebar.divider()
    st.sidebar.subheader("Ingest 요약")
    rep = build.ingest_report or {}
    st.sidebar.json(
        {
            "raw_files": rep.get("raw_files"),
            "read_ok": rep.get("read_ok"),
            "read_fail": rep.get("read_fail"),
            "master_auto_rows": rep.get("master_auto_rows"),
            "fast_path": rep.get("fast_path"),
        },
        expanded=False,
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

    # -------------------
    # Tabs
    # -------------------
    tab_dash, tab_qa, tab_aux = st.tabs(["Dashboard", "QA", "AUX"])

    with tab_dash:
        st.title("HR Dashboard (MVP v1 + v2.1 Layer/QA/Aux)")

        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("총인력(최신연도 추정)", f"{build.total_headcount_latest_est:,}")
        with c2:
            st.metric("총인력(GroundTruth: 2024 재직자시트)", f"{build.total_headcount_groundtruth:,}")
        with c3:
            st.metric("마스터 rows(필터 적용)", f"{len(master_f):,}")

        st.subheader("master_auto(전체)")
        st.caption(_df_info(master))
        st.dataframe(master.head(50), use_container_width=True)

        st.subheader("master_auto(필터)")
        st.caption(_df_info(master_f))
        st.dataframe(master_f.head(200), use_container_width=True)

        st.subheader("월별 입/퇴사 (gold/monthly)")
        monthly = build.monthly.copy() if isinstance(build.monthly, pd.DataFrame) else _read_csv_safe(gold_dir / "monthly.csv")
        st.caption(_df_info(monthly))
        if not monthly.empty and "month" in monthly.columns:
            try:
                monthly["month"] = pd.to_datetime(monthly["month"], errors="coerce")
                st.line_chart(monthly.set_index("month")[["hires", "exits", "net"]])
            except Exception:
                st.dataframe(monthly, use_container_width=True)
        else:
            st.dataframe(monthly, use_container_width=True)

        st.subheader("30일 이내 퇴사 (gold/early_exit_30d)")
        early = build.early_exit_30d.copy() if isinstance(build.early_exit_30d, pd.DataFrame) else _read_csv_safe(gold_dir / "early_exit_30d.csv")
        st.caption(_df_info(early))
        st.dataframe(early.head(200), use_container_width=True)

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

        st.subheader("processed 레이어 폴더 확인")
        st.write(
            {
                "bronze": str(bronze_dir).replace("\\", "/"),
                "silver": str(silver_dir).replace("\\", "/"),
                "gold": str(gold_dir).replace("\\", "/"),
                "qa": str(qa_dir).replace("\\", "/"),
                "aux": str(aux_dir).replace("\\", "/"),
            }
        )

    with tab_aux:
        st.header("AUX (v2 플러그인 1차)")
        st.caption("현재는 최소 컬럼으로만 정형화(추후 v2에서 본격 매핑/정합성/키 연결).")

        st.subheader("aux_skill.csv")
        aux_skill = build.aux_skill.copy() if isinstance(build.aux_skill, pd.DataFrame) else _read_csv_safe(aux_dir / "aux_skill.csv")
        st.caption(_df_info(aux_skill))
        st.dataframe(aux_skill.head(300), use_container_width=True)

        st.subheader("aux_recruit.csv")
        aux_recruit = build.aux_recruit.copy() if isinstance(build.aux_recruit, pd.DataFrame) else _read_csv_safe(aux_dir / "aux_recruit.csv")
        st.caption(_df_info(aux_recruit))
        st.dataframe(aux_recruit.head(300), use_container_width=True)

        st.subheader("reference_roster_2021_2024.csv (검증용)")
        ref = build.reference_roster.copy() if isinstance(build.reference_roster, pd.DataFrame) else _read_csv_safe(aux_dir / "reference_roster_2021_2024.csv")
        st.caption(_df_info(ref))
        st.dataframe(ref.head(300), use_container_width=True)
