import pandas as pd


def dept_monthly_counts(
    active_df: pd.DataFrame,
    exited_df: pd.DataFrame,
    org_col: str,
    dept_col: str,
    join_col: str,
    exit_col: str,
) -> pd.DataFrame:
    """
    소속(org) + 팀(dept) 기준 월별 입/퇴사 집계
    반환 컬럼: [org_col, dept_col, period, hires, exits]
    """
    hires_parts = []
    for df in [active_df, exited_df]:
        if df is not None and join_col in df.columns:
            t = df[[c for c in [org_col, dept_col, join_col] if c in df.columns]].copy()
            if org_col not in t.columns:
                t[org_col] = "(미상)"
            if dept_col not in t.columns:
                t[dept_col] = "(미상)"
            t[join_col] = pd.to_datetime(t[join_col], errors="coerce")
            hires_parts.append(t.dropna(subset=[join_col]))

    hires_base = (
        pd.concat(hires_parts, ignore_index=True)
        if hires_parts
        else pd.DataFrame(columns=[org_col, dept_col, join_col])
    )

    if hires_base.empty:
        hires = pd.DataFrame(columns=[org_col, dept_col, "period", "hires"])
    else:
        hires = (
            hires_base
            .groupby([org_col, dept_col, hires_base[join_col].dt.to_period("M")])
            .size()
            .reset_index(name="hires")
        )
        hires = hires.rename(columns={hires.columns[-1]: "period"})  # period 컬럼명 보정

    # 퇴사(Exited의 exit_date)
    if exited_df is None or exit_col not in exited_df.columns:
        exits = pd.DataFrame(columns=[org_col, dept_col, "period", "exits"])
    else:
        e = exited_df[[c for c in [org_col, dept_col, exit_col] if c in exited_df.columns]].copy()
        if org_col not in e.columns:
            e[org_col] = "(미상)"
        if dept_col not in e.columns:
            e[dept_col] = "(미상)"

        e[exit_col] = pd.to_datetime(e[exit_col], errors="coerce")
        e = e.dropna(subset=[exit_col])

        if e.empty:
            exits = pd.DataFrame(columns=[org_col, dept_col, "period", "exits"])
        else:
            exits = (
                e.groupby([org_col, dept_col, e[exit_col].dt.to_period("M")])
                .size()
                .reset_index(name="exits")
            )
            exits = exits.rename(columns={exits.columns[-1]: "period"})

    merged = pd.merge(hires, exits, on=[org_col, dept_col, "period"], how="outer").fillna(0)
    merged["period"] = merged["period"].astype(str)
    merged["hires"] = merged["hires"].astype(int)
    merged["exits"] = merged["exits"].astype(int)
    return merged


def trim_before_first_seen(dept_monthly: pd.DataFrame, master_auto: pd.DataFrame) -> pd.DataFrame:
    """
    (org,dept)별 첫 등장 월(first_seen_month) 이전 구간을 dept_monthly에서 제거한다.

    first_seen_month 계산 우선순위:
      1) join_date (있으면 그 달)
      2) snapshot_year (없으면 해당 연도 1월로 근사)
    """
    if dept_monthly is None or dept_monthly.empty:
        return dept_monthly
    if master_auto is None or master_auto.empty:
        return dept_monthly

    dm = dept_monthly.copy()
    dm["month"] = pd.to_datetime(dm["month"], errors="coerce")

    ma = master_auto.copy()
    if "join_date" in ma.columns:
        ma["join_date"] = pd.to_datetime(ma["join_date"], errors="coerce")

    # org/dept 정리
    for c in ["org", "dept"]:
        if c in ma.columns:
            ma[c] = ma[c].astype(str).str.strip()
        if c in dm.columns:
            dm[c] = dm[c].astype(str).str.strip()

    base = ma.dropna(subset=["org", "dept"]).copy()

    # 1) join_date 기반 first_seen
    first_list = []
    if "join_date" in base.columns:
        j = base.dropna(subset=["join_date"]).copy()
        if not j.empty:
            j["first_seen_month"] = j["join_date"].dt.to_period("M").dt.to_timestamp()
            first_list.append(j.groupby(["org", "dept"], as_index=False)["first_seen_month"].min())

    # 2) snapshot_year 기반 first_seen
    if "snapshot_year" in base.columns:
        s = base.copy()
        s["snapshot_year"] = pd.to_numeric(s["snapshot_year"], errors="coerce")
        s = s.dropna(subset=["snapshot_year"])
        if not s.empty:
            s["first_seen_month"] = pd.to_datetime(s["snapshot_year"].astype(int).astype(str) + "-01-01")
            first_list.append(s.groupby(["org", "dept"], as_index=False)["first_seen_month"].min())

    if not first_list:
        return dm

    first = pd.concat(first_list, ignore_index=True)
    first = first.groupby(["org", "dept"], as_index=False)["first_seen_month"].min()

    dm = dm.merge(first, on=["org", "dept"], how="left")
    dm = dm[(dm["first_seen_month"].isna()) | (dm["month"] >= dm["first_seen_month"])]
    return dm.drop(columns=["first_seen_month"])


def dept_rates(
    dept_counts: pd.DataFrame,
    headcount_df: pd.DataFrame,
    org_col: str,
    dept_col: str,
) -> pd.DataFrame:
    """
    dept_counts에 (org,dept)별 headcount를 붙여 입/퇴사율 계산
    반환 컬럼: + headcount, hire_rate, exit_rate
    """
    if dept_counts is None or dept_counts.empty:
        return pd.DataFrame()

    if headcount_df is None or headcount_df.empty:
        out = dept_counts.copy()
        out["headcount"] = 0
        out["hire_rate"] = 0.0
        out["exit_rate"] = 0.0
        return out

    base = (
        headcount_df
        .groupby([org_col, dept_col])
        .size()
        .reset_index(name="headcount")
    )

    out = dept_counts.merge(base, on=[org_col, dept_col], how="left").fillna({"headcount": 0})
    out["hire_rate"] = out.apply(lambda r: (r["hires"] / r["headcount"] * 100) if r["headcount"] else 0, axis=1)
    out["exit_rate"] = out.apply(lambda r: (r["exits"] / r["headcount"] * 100) if r["headcount"] else 0, axis=1)
    return out