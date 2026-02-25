from __future__ import annotations

from pathlib import Path
from typing import Dict, Union

import pandas as pd


def _find_header_row(df: pd.DataFrame, needle: str = "사번", max_scan: int = 20) -> int | None:
    for i in range(min(max_scan, len(df))):
        row = df.iloc[i].astype(str)
        if row.str.contains(needle).any():
            return i
    return None


def _normalize_header(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Detect header row (common in exported Excel with merged header rows),
    assign clean column names and return cleaned DataFrame.
    """
    hdr_idx = _find_header_row(df_raw, needle="사번")
    if hdr_idx is None:
        hdr_idx = _find_header_row(df_raw, needle="이름")
    if hdr_idx is None:
        hdr_idx = 0

    header = df_raw.iloc[hdr_idx].tolist()
    cols = []
    seen = {}
    for j, v in enumerate(header):
        name = str(v).strip() if pd.notna(v) else f"col_{j}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        cols.append(name)

    df = df_raw.iloc[hdr_idx + 1 :].copy()
    df.columns = cols
    df = df.dropna(how="all").reset_index(drop=True)
    return df


def load_headcount_2024(xlsx_path: Union[str, Path]) -> Dict[str, pd.DataFrame]:
    """
    Expects an Excel with (at least) sheets:
      - '재직자' (active employees)
      - '퇴사자' (exited employees)
      - optional survey sheet (Sheet1 or others)

    Returns a dict:
      { "active": DataFrame, "exited": DataFrame, "survey": DataFrame(opt) }
    """
    xlsx_path = Path(xlsx_path)
    out: Dict[str, pd.DataFrame] = {}

    # Active (재직자)
    try:
        raw_active = pd.read_excel(xlsx_path, sheet_name="재직자", header=None, engine="openpyxl")
        out["active"] = _normalize_header(raw_active)
    except Exception:
        out["active"] = None

    # Exited (퇴사자)
    try:
        raw_exited = pd.read_excel(xlsx_path, sheet_name="퇴사자", header=None, engine="openpyxl")
        out["exited"] = _normalize_header(raw_exited)
    except Exception:
        out["exited"] = None

    # Survey (optional) — try common names
    for sname in ["설문", "Survey", "Sheet1", "설문지"]:
        try:
            raw_survey = pd.read_excel(xlsx_path, sheet_name=sname, header=None, engine="openpyxl")
            out["survey"] = _normalize_header(raw_survey)
            break
        except Exception:
            continue

    return out