\
from __future__ import annotations

import pandas as pd


def turnover_rate(exits: int, avg_headcount: float) -> float:
    if avg_headcount <= 0:
        return 0.0
    return (exits / avg_headcount) * 100.0


def yearly_turnover_summary(turnover_df: pd.DataFrame) -> pd.DataFrame:
    """
    turnover_df schema: year, type(hire/exit), name
    Returns: year, hires, exits, net
    """
    g = turnover_df.groupby(["year", "type"]).size().unstack(fill_value=0)
    if "hire" not in g.columns:
        g["hire"] = 0
    if "exit" not in g.columns:
        g["exit"] = 0
    out = g.rename(columns={"hire": "hires", "exit": "exits"}).reset_index()
    out["net"] = out["hires"] - out["exits"]
    return out.sort_values("year")

def build_metrics(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    """
    snapshot 기반 기본 metrics 생성
    """

    df = snapshot_df.copy()

    # 기본 정리
    df = df.dropna(subset=["name"])

    # headcount
    org_cols = [c for c in df.columns if c.startswith("org_level_")]

    if org_cols:
        org_col = org_cols[-1]
    else:
        org_col = "org"

    headcount = df.groupby(org_col).size().reset_index(name="headcount")

    return headcount
