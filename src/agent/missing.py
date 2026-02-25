\
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def find_missing_department(df: pd.DataFrame, dept_col: str = "부서") -> pd.DataFrame:
    if dept_col not in df.columns:
        return df.iloc[0:0].copy()
    missing = df[df[dept_col].isna() | (df[dept_col].astype(str).str.strip() == "")]
    return missing


def apply_department_fix(df: pd.DataFrame, name: str, dept: str, name_col: str = "이름", dept_col: str = "부서") -> pd.DataFrame:
    if name_col not in df.columns or dept_col not in df.columns:
        return df
    mask = df[name_col].astype(str).str.strip() == str(name).strip()
    df.loc[mask, dept_col] = dept
    return df


def save_fixed(df: pd.DataFrame, out_path: str) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
