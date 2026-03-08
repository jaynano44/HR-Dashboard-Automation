from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


def _norm(x: object) -> str:
    if x is None:
        return ""
    s = str(x)
    s = s.replace("\n", " ").replace("\r", " ")
    s = " ".join(s.split())
    return s.strip()


@dataclass
class AliasMap:
    df: pd.DataFrame


def load_alias_map(path: str | Path) -> Optional[AliasMap]:
    p = Path(path)
    if not p.exists():
        return None

    df = pd.read_csv(p, encoding="utf-8-sig").fillna("")

    required = {"raw_org", "raw_dept", "std_org", "std_dept"}
    if not required.issubset(df.columns):
        raise ValueError(f"alias map must contain columns: {required}")

    for c in required:
        df[c] = df[c].map(_norm)

    df["__key__"] = df["raw_org"] + "||" + df["raw_dept"]

    return AliasMap(df=df)


def apply_alias_map(
    df: pd.DataFrame,
    org_col: str,
    dept_col: str,
    alias: Optional[AliasMap],
    out_org_col: str = "std_org",
    out_dept_col: str = "std_dept",
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    d = df.copy()

    if org_col in d.columns:
        d[org_col] = d[org_col].map(_norm)
    else:
        d[org_col] = ""

    if dept_col in d.columns:
        d[dept_col] = d[dept_col].map(_norm)
    else:
        d[dept_col] = ""

    if alias is None:
        d[out_org_col] = d[org_col]
        d[out_dept_col] = d[dept_col]
        return d, pd.DataFrame()

    amap = alias.df[["__key__", "std_org", "std_dept"]].drop_duplicates()
    d["__key__"] = d[org_col] + "||" + d[dept_col]

    d = d.merge(amap, on="__key__", how="left")

    d[out_org_col] = d["std_org"].fillna(d[org_col])
    d[out_dept_col] = d["std_dept"].fillna(d[dept_col])

    missing = d[d["std_org"].isna()][[org_col, dept_col]].drop_duplicates()

    d = d.drop(columns=["std_org", "std_dept", "__key__"], errors="ignore")

    return d, missing