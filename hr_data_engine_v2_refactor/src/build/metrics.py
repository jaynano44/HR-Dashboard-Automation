from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class MetricsResult:
    yearly: pd.DataFrame
    org_level: pd.DataFrame
    headcount: pd.DataFrame


def turnover_rate(exits: int, avg_headcount: float) -> float:
    if avg_headcount <= 0:
        return 0.0
    return round((exits / avg_headcount) * 100.0, 2)


def build_metrics(history_df: pd.DataFrame, snapshot_df: pd.DataFrame) -> MetricsResult:
    history = history_df.copy() if history_df is not None else pd.DataFrame()
    snapshot = snapshot_df.copy() if snapshot_df is not None else pd.DataFrame()

    if not history.empty:
        history["join_date"] = pd.to_datetime(history.get("join_date"), errors="coerce")
        history["exit_date"] = pd.to_datetime(history.get("exit_date"), errors="coerce")

    yearly = build_yearly_metrics(history)
    org_level = build_org_level_metrics(snapshot)
    headcount = build_headcount_metrics(snapshot)
    return MetricsResult(yearly=yearly, org_level=org_level, headcount=headcount)


def build_yearly_metrics(history: pd.DataFrame) -> pd.DataFrame:
    if history is None or history.empty:
        return pd.DataFrame(columns=["year", "headcount", "hires", "exits", "turnover_rate"])

    years = sorted(set(history["join_date"].dt.year.dropna().tolist() + history["exit_date"].dt.year.dropna().tolist()))
    rows = []
    for year in years:
        year = int(year)
        end_of_year = pd.Timestamp(year, 12, 31)
        start_of_year = pd.Timestamp(year - 1, 12, 31)

        active_end = _active_as_of(history, end_of_year)
        active_start = _active_as_of(history, start_of_year)

        hires = _unique_people(history[history["join_date"].dt.year == year])
        exits = _unique_people(history[history["exit_date"].dt.year == year])
        headcount = _unique_people(active_end)
        opening = _unique_people(active_start)
        avg_headcount = (opening + headcount) / 2 if (opening + headcount) > 0 else headcount

        rows.append({
            "year": year,
            "opening_headcount": opening,
            "headcount": headcount,
            "hires": hires,
            "exits": exits,
            "turnover_rate": turnover_rate(exits, avg_headcount),
        })

    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


def build_org_level_metrics(snapshot: pd.DataFrame) -> pd.DataFrame:
    if snapshot is None or snapshot.empty:
        return pd.DataFrame(columns=["org_level", "org_name", "headcount"])

    frames = []
    for col in [c for c in snapshot.columns if c.startswith("org_level_")]:
        temp = snapshot[snapshot[col].astype("string").fillna("").str.strip() != ""].copy()
        if temp.empty:
            continue
        g = temp.groupby(col).size().reset_index(name="headcount")
        g = g.rename(columns={col: "org_name"})
        g["org_level"] = col
        frames.append(g)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["org_level", "org_name", "headcount"])


def build_headcount_metrics(snapshot: pd.DataFrame) -> pd.DataFrame:
    if snapshot is None or snapshot.empty:
        return pd.DataFrame(columns=["org", "dept", "headcount"])

    org_col = "org" if "org" in snapshot.columns else None
    dept_col = "dept" if "dept" in snapshot.columns else None

    if org_col and dept_col:
        return snapshot.groupby([org_col, dept_col]).size().reset_index(name="headcount")
    if org_col:
        return snapshot.groupby(org_col).size().reset_index(name="headcount")
    return pd.DataFrame(columns=["org", "dept", "headcount"])


def _active_as_of(history: pd.DataFrame, as_of_date: pd.Timestamp) -> pd.DataFrame:
    return history[
        history["join_date"].notna()
        & (history["join_date"] <= as_of_date)
        & (history["exit_date"].isna() | (history["exit_date"] > as_of_date))
    ].copy()


def _unique_people(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    if "emp_uid" in df.columns and df["emp_uid"].astype("string").str.strip().ne("").any():
        return df["emp_uid"].astype("string").replace("", pd.NA).dropna().nunique()
    if "name" in df.columns:
        return df["name"].astype("string").replace("", pd.NA).dropna().nunique()
    return len(df)
