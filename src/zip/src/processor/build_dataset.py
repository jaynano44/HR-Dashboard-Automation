# src/processor/build_dataset.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import yaml

from src.processor.auto_ingest_multi import auto_ingest_engine
from src.processor.headcount_2024_loader import load_headcount_2024
from src.processor.aux_plugins import ingest_aux_recruit, ingest_aux_skill


def load_config(path: str | Path) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"config not found: {p}")
    return yaml.safe_load(p.read_text(encoding="utf-8"))


_SENSITIVE_COLS = [
    "주민등록번호", "resident_id",
    "집주소", "address",
    "핸드폰", "phone",
    "e-mail", "email",
    "내선번호",
    "birth", "생일",
]


def drop_sensitive(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None or not isinstance(df, pd.DataFrame):
        return df
    d = df.copy()
    for c in list(d.columns):
        if str(c).strip() in _SENSITIVE_COLS:
            d = d.drop(columns=[c], errors="ignore")
    return d


def _coerce_datetime(s: pd.Series) -> pd.Series:
    if s is None:
        return s
    if pd.api.types.is_datetime64_any_dtype(s):
        out = s.copy()
    else:
        out = pd.to_datetime(s, errors="coerce")

    # Excel serial
    sn = pd.to_numeric(s, errors="coerce")
    mask = sn.between(20000, 60000)
    if mask.any():
        conv = pd.to_datetime("1899-12-30") + pd.to_timedelta(sn[mask], unit="D")
        out.loc[mask] = conv

    # YYYYMMDD
    ss = s.astype(str).str.strip()
    mask2 = ss.str.fullmatch(r"\d{8}", na=False)
    if mask2.any():
        out.loc[mask2] = pd.to_datetime(ss[mask2], format="%Y%m%d", errors="coerce")

    # epoch placeholder 제거
    out = out.mask(out == pd.Timestamp("1970-01-01"))
    return out


def _month_floor(dt: pd.Series) -> pd.Series:
    d = _coerce_datetime(dt)
    return d.dt.to_period("M").dt.to_timestamp()


def build_monthly(master: pd.DataFrame) -> pd.DataFrame:
    if master is None or master.empty:
        return pd.DataFrame()
    if "join_date" not in master.columns or "exit_date" not in master.columns:
        return pd.DataFrame()

    d = master.copy()
    d["join_date"] = _coerce_datetime(d["join_date"])
    d["exit_date"] = _coerce_datetime(d["exit_date"])

    hires = d.dropna(subset=["join_date"]).copy()
    hires["month"] = _month_floor(hires["join_date"])
    hires_g = hires.groupby(["month"], dropna=False).size().reset_index(name="hires")

    exits = d.dropna(subset=["exit_date"]).copy()
    exits["month"] = _month_floor(exits["exit_date"])
    exits_g = exits.groupby(["month"], dropna=False).size().reset_index(name="exits")

    out = hires_g.merge(exits_g, on="month", how="outer").fillna(0)
    out["hires"] = out["hires"].astype(int)
    out["exits"] = out["exits"].astype(int)
    out["net"] = out["hires"] - out["exits"]
    return out.sort_values("month")


def build_dept_monthly(master: pd.DataFrame) -> pd.DataFrame:
    if master is None or master.empty:
        return pd.DataFrame()

    for c in ["org", "dept", "join_date", "exit_date"]:
        if c not in master.columns:
            return pd.DataFrame()

    d = master.copy()
    d["join_date"] = _coerce_datetime(d["join_date"])
    d["exit_date"] = _coerce_datetime(d["exit_date"])
    d["org"] = d["org"].astype(str).str.strip()
    d["dept"] = d["dept"].astype(str).str.strip()

    hires = d.dropna(subset=["join_date"]).copy()
    hires["month"] = _month_floor(hires["join_date"])
    hires_g = hires.groupby(["org", "dept", "month"]).size().reset_index(name="hires")

    exits = d.dropna(subset=["exit_date"]).copy()
    exits["month"] = _month_floor(exits["exit_date"])
    exits_g = exits.groupby(["org", "dept", "month"]).size().reset_index(name="exits")

    out = hires_g.merge(exits_g, on=["org", "dept", "month"], how="outer").fillna(0)
    out["hires"] = out["hires"].astype(int)
    out["exits"] = out["exits"].astype(int)

    # ✅ 신규 부서가 과거부터 0으로 깔리는 문제 방지: 첫 등장월 이전 제거
    first = (
        out[(out["hires"] > 0) | (out["exits"] > 0)]
        .groupby(["org", "dept"])["month"]
        .min()
        .reset_index(name="first_month")
    )
    out = out.merge(first, on=["org", "dept"], how="left")
    out = out[out["month"] >= out["first_month"]].drop(columns=["first_month"], errors="ignore")
    return out.sort_values(["org", "dept", "month"])


def build_turnover_yearly(master: pd.DataFrame) -> pd.DataFrame:
    if master is None or master.empty:
        return pd.DataFrame(columns=["year", "hires", "exits", "net"])

    d = master.copy()
    d["join_date"] = _coerce_datetime(d.get("join_date"))
    d["exit_date"] = _coerce_datetime(d.get("exit_date"))

    hires = d.dropna(subset=["join_date"]).copy()
    hires["year"] = hires["join_date"].dt.year
    hires_g = hires.groupby("year").size().reset_index(name="hires")

    exits = d.dropna(subset=["exit_date"]).copy()
    exits["year"] = exits["exit_date"].dt.year
    exits_g = exits.groupby("year").size().reset_index(name="exits")

    out = hires_g.merge(exits_g, on="year", how="outer").fillna(0)
    out["hires"] = out["hires"].astype(int)
    out["exits"] = out["exits"].astype(int)
    out["net"] = out["hires"] - out["exits"]
    return out.sort_values("year")


def build_early_exit_30d(master: pd.DataFrame) -> pd.DataFrame:
    if master is None or master.empty:
        return pd.DataFrame()
    if "join_date" not in master.columns or "exit_date" not in master.columns:
        return pd.DataFrame()

    d = master.copy()
    d["join_date"] = _coerce_datetime(d["join_date"])
    d["exit_date"] = _coerce_datetime(d["exit_date"])
    d = d.dropna(subset=["join_date", "exit_date"])
    if d.empty:
        return pd.DataFrame()

    d["tenure_days"] = (d["exit_date"] - d["join_date"]).dt.days
    out = d[(d["tenure_days"] >= 0) & (d["tenure_days"] <= 30)].copy()
    return out.sort_values(["exit_date", "join_date"])


def _norm_name(x: object) -> str:
    return str(x).replace("\u3000", " ").strip()


def build_qa_outputs(
    master: pd.DataFrame,
    reference_roster: pd.DataFrame,
    qa_dir: Path,
) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    qa_dir.mkdir(parents=True, exist_ok=True)

    if master is None:
        master = pd.DataFrame()
    if reference_roster is None:
        reference_roster = pd.DataFrame(columns=["year", "type", "name", "source_file", "sheet"])

    m = master.copy()
    if "name" in m.columns:
        m["__name__"] = m["name"].map(_norm_name)
    else:
        m["__name__"] = ""

    if "snapshot_year" in m.columns:
        m["snapshot_year"] = pd.to_numeric(m["snapshot_year"], errors="coerce").astype("Int64")

    r = reference_roster.copy()
    if not r.empty:
        r["name"] = r["name"].map(_norm_name)
        r["year"] = pd.to_numeric(r["year"], errors="coerce").astype("Int64")

    ref_only_rows = []
    master_only_rows = []

    years = sorted([int(y) for y in r["year"].dropna().unique().tolist()]) if not r.empty else []
    if not years:
        years = sorted([int(y) for y in m["snapshot_year"].dropna().unique().tolist()]) if "snapshot_year" in m.columns else []

    for y in years:
        rm = r[r["year"] == y] if not r.empty and "year" in r.columns else pd.DataFrame()
        mm = m[m["snapshot_year"] == y] if "snapshot_year" in m.columns else m

        master_names = set(mm["__name__"].dropna().astype(str)) if not mm.empty else set()
        ref_names = set(rm["name"].dropna().astype(str)) if not rm.empty else set()

        if not rm.empty:
            ro = rm[~rm["name"].astype(str).isin(master_names)].copy()
            if not ro.empty:
                ref_only_rows.append(ro)

        if not mm.empty and ref_names:
            mo = mm[~mm["__name__"].astype(str).isin(ref_names)].copy()
            if not mo.empty:
                keep = [c for c in ["snapshot_year", "emp_id", "name", "org", "dept", "data_source", "sheet_name"] if c in mo.columns]
                master_only_rows.append(mo[keep])

    ref_only = pd.concat(ref_only_rows, ignore_index=True) if ref_only_rows else pd.DataFrame(columns=["year", "type", "name", "source_file", "sheet"])
    master_only = pd.concat(master_only_rows, ignore_index=True) if master_only_rows else pd.DataFrame(columns=["snapshot_year", "emp_id", "name", "org", "dept", "data_source", "sheet_name"])

    ref_only_path = qa_dir / "mismatch_ref_only.csv"
    master_only_path = qa_dir / "mismatch_master_only.csv"
    ref_only.to_csv(ref_only_path, index=False, encoding="utf-8-sig")
    master_only.to_csv(master_only_path, index=False, encoding="utf-8-sig")

    report = {
        "reference_roster_rows": int(len(reference_roster)) if isinstance(reference_roster, pd.DataFrame) else 0,
        "master_rows": int(len(master)) if isinstance(master, pd.DataFrame) else 0,
        "mismatch_ref_only_rows": int(len(ref_only)),
        "mismatch_master_only_rows": int(len(master_only)),
        "years_compared": years,
        "paths": {
            "mismatch_ref_only": str(ref_only_path).replace("\\", "/"),
            "mismatch_master_only": str(master_only_path).replace("\\", "/"),
        },
    }
    (qa_dir / "qa_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return ref_only, master_only, report


@dataclass
class BuildResult:
    master_auto: pd.DataFrame
    ingest_report: dict

    turnover_yearly: pd.DataFrame
    monthly: pd.DataFrame
    dept_monthly: pd.DataFrame
    early_exit_30d: pd.DataFrame

    reference_roster: pd.DataFrame
    qa_report: dict
    qa_mismatch_ref_only: pd.DataFrame
    qa_mismatch_master_only: pd.DataFrame

    aux_skill: pd.DataFrame
    aux_recruit: pd.DataFrame

    total_headcount_latest_est: int = 0
    total_headcount_groundtruth: int = 0


def build_datasets(config_path: str | Path) -> BuildResult:
    cfg = load_config(config_path)

    raw_dir = Path(cfg["paths"]["raw_dir"])
    processed_root = Path(cfg["paths"]["processed_dir"])
    processed_root.mkdir(parents=True, exist_ok=True)

    # ✅ v2 processed layer structure
    bronze_dir = processed_root / "bronze"
    silver_dir = processed_root / "silver"
    gold_dir = processed_root / "gold"
    qa_dir = processed_root / "qa"
    aux_dir = processed_root / "aux"
    for d in [bronze_dir, silver_dir, gold_dir, qa_dir, aux_dir]:
        d.mkdir(parents=True, exist_ok=True)

    master_auto, ingest_report = auto_ingest_engine(raw_dir, processed_root, secrets=cfg.get("secrets"))

    # --- Save ingest report/manifest into bronze (and keep compatibility at root) ---
    try:
        (bronze_dir / "ingest_report.json").write_text(json.dumps(ingest_report, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

    # ---- reference roster (verification table) ----
    # auto_ingest_multi에서 aux_reference_roster로 분류된 파일들을 기준으로 생성됨
    from src.processor.reference_roster import build_reference_roster
    reference_roster = build_reference_roster(ingest_report, raw_dir, aux_dir)

    # ---- aux plugins (skill/recruit) ----
    aux_skill_df = ingest_aux_skill(ingest_report, raw_dir, aux_dir)
    aux_recruit_df = ingest_aux_recruit(ingest_report, raw_dir, aux_dir)

    tmp = drop_sensitive(master_auto)
    master_auto = tmp if tmp is not None else master_auto

    # ---- Gold metrics ----
    turnover_yearly = build_turnover_yearly(master_auto)
    monthly = build_monthly(master_auto)
    dept_monthly = build_dept_monthly(master_auto)
    early_exit_30d = build_early_exit_30d(master_auto)

    # ---- totals ----
    total_est = 0
    if master_auto is not None and not master_auto.empty:
        d = master_auto.copy()
        d["exit_date"] = _coerce_datetime(d.get("exit_date"))
        d["snapshot_year"] = pd.to_numeric(d.get("snapshot_year"), errors="coerce").astype("Int64")
        latest_year = int(d["snapshot_year"].dropna().max()) if d["snapshot_year"].notna().any() else None
        if latest_year is not None:
            cut = pd.Timestamp(year=latest_year, month=12, day=31)
            active = d[(d["exit_date"].isna()) | (d["exit_date"] > cut)]
            total_est = int(active["name"].notna().sum()) if "name" in active.columns else int(len(active))

    total_gt = 0
    hc_file = (cfg.get("files") or {}).get("headcount_2024_xlsx")
    if hc_file:
        try:
            hc_path = raw_dir / hc_file if not Path(hc_file).is_absolute() else Path(hc_file)
            hc = load_headcount_2024(hc_path)
            active_df = hc.get("active") if isinstance(hc, dict) else None
            if isinstance(active_df, pd.DataFrame) and not active_df.empty:
                total_gt = int(len(active_df))
        except Exception:
            total_gt = 0

    # ---- QA outputs ----
    qa_ref_only, qa_master_only, qa_report = build_qa_outputs(master_auto, reference_roster, qa_dir)

    # ---- Save layered outputs ----
    # Silver
    if master_auto is not None:
        master_auto.to_csv(silver_dir / "master_auto.csv", index=False, encoding="utf-8-sig")

    # Gold
    turnover_yearly.to_csv(gold_dir / "turnover_yearly.csv", index=False, encoding="utf-8-sig")
    monthly.to_csv(gold_dir / "monthly.csv", index=False, encoding="utf-8-sig")
    dept_monthly.to_csv(gold_dir / "dept_monthly.csv", index=False, encoding="utf-8-sig")
    early_exit_30d.to_csv(gold_dir / "early_exit_30d.csv", index=False, encoding="utf-8-sig")

    # Compatibility: also drop top-level files expected by v1 UI/scripts
    try:
        master_auto.to_csv(processed_root / "master_auto.csv", index=False, encoding="utf-8-sig")
        turnover_yearly.to_csv(processed_root / "turnover_yearly.csv", index=False, encoding="utf-8-sig")
        monthly.to_csv(processed_root / "monthly.csv", index=False, encoding="utf-8-sig")
        dept_monthly.to_csv(processed_root / "dept_monthly.csv", index=False, encoding="utf-8-sig")
        early_exit_30d.to_csv(processed_root / "early_exit_30d.csv", index=False, encoding="utf-8-sig")
        (processed_root / "ingest_report.json").write_text(json.dumps(ingest_report, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

    return BuildResult(
        master_auto=master_auto,
        ingest_report=ingest_report,
        turnover_yearly=turnover_yearly,
        monthly=monthly,
        dept_monthly=dept_monthly,
        early_exit_30d=early_exit_30d,
        reference_roster=reference_roster,
        qa_report=qa_report,
        qa_mismatch_ref_only=qa_ref_only,
        qa_mismatch_master_only=qa_master_only,
        aux_skill=aux_skill_df,
        aux_recruit=aux_recruit_df,
        total_headcount_latest_est=total_est,
        total_headcount_groundtruth=total_gt,
    )
