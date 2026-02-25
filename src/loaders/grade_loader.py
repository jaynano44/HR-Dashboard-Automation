\
from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd


def load_grade_table(xlsx_path: Union[str, Path]) -> pd.DataFrame:
    """
    Load '인력등급' sheet where the true header is on the 2nd row due to merged cells.
    Returns a cleaned DataFrame with meaningful column names.
    """
    xlsx_path = Path(xlsx_path)
    raw = pd.read_excel(xlsx_path, header=None)

    header_row_idx = None
    for i in range(min(10, len(raw))):
        if raw.iloc[i].astype(str).str.contains("이름").any():
            header_row_idx = i
            break

    if header_row_idx is None:
        # fallback: assume row 0 is header
        return pd.read_excel(xlsx_path)

    hdr = raw.iloc[header_row_idx].tolist()
    cols = []
    seen = {}
    for j, v in enumerate(hdr):
        name = str(v).strip() if pd.notna(v) else f"col_{j}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        cols.append(name)

    df = raw.iloc[header_row_idx + 1 :].copy()
    df.columns = cols
    df = df.reset_index(drop=True)

    # drop fully empty rows
    df = df.dropna(how="all")
    return df
