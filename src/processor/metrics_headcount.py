from __future__ import annotations

import pandas as pd


def _to_dt(s: pd.Series) -> pd.Series:
    # 점(.) 날짜 지원 + 공백 제거
    s2 = s.astype(str).str.strip().str.replace(".", "-", regex=False)
    dt = pd.to_datetime(s2, errors="coerce")

    # 창립일 이전(또는 2000년 이전) 잘못된 값 제거
    dt = dt[dt >= "2002-01-22"]
    return dt


def monthly_hires_exits(active_df: pd.DataFrame, exited_df: pd.DataFrame,
                        join_col: str = "입사일", exit_col: str = "퇴사일") -> pd.DataFrame:
    """
    Create monthly series of hires/exits and cumulative headcount.
    Returns: DataFrame with columns ['period', 'hires', 'exits', 'net', 'headcount']
    period format: 'YYYY-MM'
    """
    parts = []
    if active_df is not None and join_col in active_df.columns:
        parts.append(active_df[join_col])
    if exited_df is not None and join_col in exited_df.columns:
        parts.append(exited_df[join_col])

    hires_series = pd.concat(parts, ignore_index=True) if parts else pd.Series(dtype="object")
    hires = _to_dt(hires_series).dropna()
    hires_m = hires.dt.to_period("M").astype(str).value_counts().sort_index()

    exits = pd.Series(dtype="object")
    if exited_df is not None and exit_col in exited_df.columns:
        exits = _to_dt(exited_df[exit_col]).dropna()
    exits_m = exits.dt.to_period("M").astype(str).value_counts().sort_index()

    idx = sorted(set(hires_m.index).union(set(exits_m.index)))
    out = pd.DataFrame({"period": idx})
    out["hires"] = out["period"].map(hires_m).fillna(0).astype(int)
    out["exits"] = out["period"].map(exits_m).fillna(0).astype(int)
    out["net"] = out["hires"] - out["exits"]
    out["headcount"] = out["net"].cumsum()
    return out


def early_exit_by_dept(exited_df: pd.DataFrame, days: int = 30,
                       join_col: str = "입사일", exit_col: str = "퇴사일",
                       dept_col: str = "부서", org_col: str = "소속") -> pd.DataFrame:
    """
    Count exits within `days` days of join, grouped by dept/org.
    Returns DataFrame columns ['group', f'early_exit_{days}d'] sorted desc.
    """
    if join_col not in exited_df.columns or exit_col not in exited_df.columns:
        return pd.DataFrame(columns=["group", f"early_exit_{days}d"])
    if exited_df is None:
        return pd.DataFrame(columns=["group", f"early_exit_{days}d"])

    d = exited_df.copy()
    d["_join"] = _to_dt(d.get(join_col))
    d["_exit"] = _to_dt(d.get(exit_col))
    d["tenure_days"] = (d["_exit"] - d["_join"]).dt.days
    d = d[(d["tenure_days"].notna()) & (d["tenure_days"] >= 0) & (d["tenure_days"] <= days)]

    key = None
    if dept_col in d.columns:
        key = dept_col
    elif org_col in d.columns:
        key = org_col

    if key is None:
        return pd.DataFrame(columns=["group", f"early_exit_{days}d"])

    out = d.groupby(key).size().reset_index(name=f"early_exit_{days}d")
    out = out.rename(columns={key: "group"}).sort_values(f"early_exit_{days}d", ascending=False)
    return out