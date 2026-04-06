from __future__ import annotations

from pathlib import Path

import pandas as pd


def active_as_of(history: pd.DataFrame, as_of_date: pd.Timestamp) -> pd.DataFrame:
    if history is None or history.empty:
        return pd.DataFrame()

    d = history.copy()
    d["join_date"] = pd.to_datetime(d.get("join_date"), errors="coerce")
    d["exit_date"] = pd.to_datetime(d.get("exit_date"), errors="coerce")

    return d[
        d["join_date"].notna()
        & (d["join_date"] <= as_of_date)
        & (d["exit_date"].isna() | (d["exit_date"] > as_of_date))
    ].copy()


def build_org_snapshot(
    history_df: pd.DataFrame,
    people_df: pd.DataFrame | None = None,
    as_of_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    if as_of_date is None:
        as_of_date = pd.Timestamp.today().normalize()

    active = active_as_of(history_df, as_of_date)
    if active.empty:
        return pd.DataFrame()

    if people_df is not None and not people_df.empty:
        people_cols = [
            c for c in [
                "emp_uid", "name", "current_org", "current_dept", "current_title", "current_grade"
            ] if c in people_df.columns
        ]
        if people_cols:
            active = active.merge(
                people_df[people_cols].drop_duplicates(subset=[c for c in ["emp_uid", "name"] if c in people_cols]),
                on=[c for c in ["emp_uid", "name"] if c in active.columns and c in people_df.columns],
                how="left",
                suffixes=("", "_people"),
            )

    active["org"] = active.get("org").combine_first(active.get("current_org"))
    active["dept"] = active.get("dept").combine_first(active.get("current_dept"))

    active["org"] = active["org"].astype("string").fillna("").str.strip()
    active["dept"] = active["dept"].astype("string").fillna("").str.strip()

    active["org_path"] = active.apply(
        lambda r: " > ".join([x for x in [str(r.get("org", "")).strip(), str(r.get("dept", "")).strip()] if x]),
        axis=1,
    )

    levels = active["org_path"].apply(_split_org_path).apply(pd.Series)
    levels.columns = [f"org_level_{i}" for i in range(1, len(levels.columns) + 1)]
    active = pd.concat([active, levels], axis=1)

    out_cols = [
        c for c in [
            "emp_uid", "name", "org", "dept", "title", "grade", "org_path",
            "org_level_1", "org_level_2", "org_level_3", "org_level_4", "org_level_5",
            "join_date", "exit_date"
        ] if c in active.columns
    ]
    result = active[out_cols].drop_duplicates(subset=[c for c in ["emp_uid", "name"] if c in out_cols]).reset_index(drop=True)
    result["snapshot_date"] = as_of_date
    return result


def build_org_snapshot_from_csv(
    history_csv: str | Path,
    people_csv: str | Path | None = None,
    out_csv: str | Path | None = None,
    as_of_date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    history = pd.read_csv(history_csv)
    people = pd.read_csv(people_csv) if people_csv else None
    dt = pd.Timestamp(as_of_date) if as_of_date is not None else pd.Timestamp.today().normalize()
    snapshot = build_org_snapshot(history_df=history, people_df=people, as_of_date=dt)

    if out_csv is not None:
        out = Path(out_csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        snapshot.to_csv(out, index=False, encoding="utf-8-sig")

    return snapshot


def _split_org_path(path: str, depth: int = 5) -> list[str]:
    parts = [p.strip() for p in str(path).split(">") if str(p).strip()]
    parts = parts[:depth] + [""] * max(0, depth - len(parts))
    return parts[:depth]
