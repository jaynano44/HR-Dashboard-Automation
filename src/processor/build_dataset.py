from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pandas as pd
import yaml

from src.loaders.grade_loader import load_grade_table
from src.loaders.turnover_loader import load_turnover_table
from src.loaders.headcount_2024_loader import load_headcount_2024
from src.processor.metrics import yearly_turnover_summary
from src.processor.metrics_headcount import monthly_hires_exits, early_exit_by_dept


@dataclass
class BuildResult:
    grade: pd.DataFrame
    turnover: pd.DataFrame
    turnover_yearly: pd.DataFrame
    headcount_active: pd.DataFrame | None = None
    headcount_exited: pd.DataFrame | None = None
    monthly: pd.DataFrame | None = None
    early_exit_30d: pd.DataFrame | None = None


def load_config(config_path: str = "config.yaml") -> Dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_datasets(config_path: str = "config.yaml") -> BuildResult:
    cfg = load_config(config_path)

    raw_dir = Path(cfg["paths"]["raw_dir"])
    processed_dir = Path(cfg["paths"]["processed_dir"])
    processed_dir.mkdir(parents=True, exist_ok=True)

    grade_path = raw_dir / cfg["files"]["grade_xlsx"]
    turnover_path = raw_dir / cfg["files"]["turnover_xls"]

    grade = load_grade_table(grade_path)
    turnover = load_turnover_table(turnover_path, password=str(cfg["secrets"]["turnover_password"]))

    # minimal standardization
    name_col = cfg["columns"]["grade"]["name"]
    org_col = cfg["columns"]["grade"]["org"]

    for c in [name_col, org_col]:
        if c in grade.columns:
            grade[c] = grade[c].astype(str).str.strip()

    turnover["name"] = turnover["name"].astype(str).str.strip()

    # best-effort join by name to attach org info for hires/exits
    if name_col in grade.columns and org_col in grade.columns:
        name_to_org = grade[[name_col, org_col]].dropna().drop_duplicates()
        turnover = turnover.merge(name_to_org, how="left", left_on="name", right_on=name_col)
        turnover = turnover.drop(columns=[name_col]).rename(columns={org_col: "org"})
    else:
        turnover["org"] = None

    turnover_yearly = yearly_turnover_summary(turnover)

    # persist processed CSVs (MVP)
    grade.to_csv(processed_dir / "grade_clean.csv", index=False, encoding="utf-8-sig")
    turnover.to_csv(processed_dir / "turnover_clean.csv", index=False, encoding="utf-8-sig")
    turnover_yearly.to_csv(processed_dir / "turnover_yearly.csv", index=False, encoding="utf-8-sig")

    # optional: new headcount file with join/exit dates
    head_active = head_exited = monthly = early30 = None
    if "headcount_2024_xlsx" in cfg.get("files", {}):
        hc_path = raw_dir / cfg["files"]["headcount_2024_xlsx"]
        hc = load_headcount_2024(hc_path)
        head_active = hc.get("active")
        head_exited = hc.get("exited")

        # Ensure common cols exist and save
        if head_active is not None:
            head_active.to_csv(processed_dir / "headcount_active.csv", index=False, encoding="utf-8-sig")
        if head_exited is not None:
            head_exited.to_csv(processed_dir / "headcount_exited.csv", index=False, encoding="utf-8-sig")

        if head_active is not None and head_exited is not None:
            join_col = cfg["columns"]["headcount_2024"]["join_date"]
            exit_col = cfg["columns"]["headcount_2024"]["exit_date"]
            dept_col = cfg["columns"]["headcount_2024"]["dept"]
            org_col2 = cfg["columns"]["headcount_2024"]["org"]

            monthly = monthly_hires_exits(head_active, head_exited, join_col=join_col, exit_col=exit_col)
            monthly.to_csv(processed_dir / "headcount_monthly.csv", index=False, encoding="utf-8-sig")

            early30 = early_exit_by_dept(head_exited, days=30, join_col=join_col, exit_col=exit_col, dept_col=dept_col, org_col=org_col2)
            early30.to_csv(processed_dir / "early_exit_30d.csv", index=False, encoding="utf-8-sig")

    return BuildResult(
        grade=grade,
        turnover=turnover,
        turnover_yearly=turnover_yearly,
        headcount_active=head_active,
        headcount_exited=head_exited,
        monthly=monthly,
        early_exit_30d=early30,
    )