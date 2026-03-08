# src/processor/reference_roster.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def build_reference_roster(ingest_report: dict, raw_dir: Path, out_dir: Path) -> pd.DataFrame:
    """
    aux_reference_roster(예: '2021~2024년 입사자, 퇴사자 명단.xlsx')를 검증용 테이블로 저장.
    스키마: year,type,name,source_file,sheet
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    cs = (ingest_report or {}).get("classified_skips", {}) or {}
    files = cs.get("aux_reference_roster", []) or []
    rows = []

    import re

    def _pick_year(sheet: str, fname: str) -> Optional[int]:
        m = re.search(r"(19|20)\d{2}", str(sheet))
        if m:
            y = int(m.group(0))
            return y if 1900 <= y <= 2100 else None
        m = re.search(r"(19|20)\d{2}", str(fname))
        if m:
            y = int(m.group(0))
            return y if 1900 <= y <= 2100 else None
        return None

    def _read_sheet_header_scan(xlsx: Path, sheet_name: str, max_scan: int = 200) -> Optional[pd.DataFrame]:
        try:
            raw = pd.read_excel(xlsx, sheet_name=sheet_name, header=None, engine="openpyxl")
        except Exception:
            return None

        raw = raw.dropna(how="all")
        if raw.empty:
            return None

        header_row = None
        scan_n = min(max_scan, len(raw))
        for i in range(scan_n):
            row = raw.iloc[i].astype(str).str.replace(r"\s+", "", regex=True)
            if row.str.contains("입사자").any() or row.str.contains("퇴사자").any() or row.str.contains("퇴직자").any():
                header_row = i
                break
        if header_row is None:
            header_row = 0

        header = raw.iloc[header_row].tolist()
        cols = []
        seen = {}
        for j, v in enumerate(header):
            name = str(v).strip().replace("\n", " ").replace("\r", " ")
            name = name if name not in ["", "nan", "None"] else f"col_{j}"
            if name in seen:
                seen[name] += 1
                name = f"{name}_{seen[name]}"
            else:
                seen[name] = 0
            cols.append(name)

        df = raw.iloc[header_row + 1 :].copy()
        df.columns = cols
        df = df.dropna(how="all").reset_index(drop=True)
        return df if not df.empty else None

    for fname in files:
        fpath = (raw_dir / fname) if not Path(fname).is_absolute() else Path(fname)
        if not fpath.exists():
            continue

        try:
            xls = pd.ExcelFile(fpath, engine="openpyxl")
        except Exception:
            continue

        for sh in xls.sheet_names:
            df = _read_sheet_header_scan(fpath, sh, max_scan=200)
            if df is None or df.empty:
                continue

            cols = [str(c).strip().replace("\n", " ") for c in df.columns]
            df.columns = cols

            join_col = None
            exit_col = None
            for c in cols:
                cc = c.replace(" ", "")
                if join_col is None and ("입사" in cc):
                    join_col = c
                if exit_col is None and ("퇴사" in cc or "퇴직" in cc):
                    exit_col = c

            if join_col is None and len(cols) >= 1:
                join_col = cols[0]
            if exit_col is None and len(cols) >= 2:
                exit_col = cols[1]

            year = _pick_year(sh, fname)

            def _emit(col: Optional[str], typ: str):
                if col is None or col not in df.columns:
                    return
                s = (
                    df[col]
                    .dropna()
                    .astype(str)
                    .map(lambda x: x.strip())
                    .replace({"": None, "nan": None, "None": None})
                    .dropna()
                )
                for name in s.tolist():
                    rows.append(
                        {
                            "year": year,
                            "type": typ,
                            "name": name,
                            "source_file": Path(fname).name,
                            "sheet": str(sh),
                        }
                    )

            _emit(join_col, "join")
            _emit(exit_col, "exit")

    out = pd.DataFrame(rows, columns=["year", "type", "name", "source_file", "sheet"])
    if not out.empty:
        out = out.drop_duplicates(subset=["year", "type", "name", "source_file", "sheet"]).reset_index(drop=True)

    out_path = out_dir / "reference_roster_2021_2024.csv"
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out
