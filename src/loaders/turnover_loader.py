\
from __future__ import annotations

import re
from pathlib import Path
from typing import Union

import pandas as pd

from .excel_loader import decrypt_office_file


def load_turnover_table(xls_path: Union[str, Path], password: str) -> pd.DataFrame:
    """
    Load turnover list from password-protected .xls with multiple sheets (e.g., 2021년~2024년).
    Output schema:
      year: int
      type: "hire" | "exit"
      name: str
    """
    xls_path = Path(xls_path)
    bio = decrypt_office_file(xls_path, password=password)

    xls = pd.ExcelFile(bio, engine="xlrd")
    records = []

    for sheet in xls.sheet_names:
        d = pd.read_excel(xls, sheet_name=sheet, engine="xlrd", header=None)

        header_idx = None
        for idx, row in d.iterrows():
            if row.astype(str).str.contains("입사자").any():
                header_idx = idx
                break
        if header_idx is None:
            continue

        header_row = d.iloc[header_idx]
        hire_col = None
        exit_col = None
        for c, val in header_row.items():
            if str(val).strip() == "입사자":
                hire_col = c
            if str(val).strip() == "퇴사자":
                exit_col = c

        if hire_col is None or exit_col is None:
            continue

        data = d.iloc[header_idx + 1 :][[hire_col, exit_col]].copy()

        year = int(re.sub(r"\D", "", str(sheet)))
        for typ, col in [("hire", hire_col), ("exit", exit_col)]:
            ser = data[col].dropna()
            for name in ser.astype(str).str.strip():
                if name and name.lower() != "nan":
                    records.append({"year": year, "type": typ, "name": name})

    return pd.DataFrame(records)
