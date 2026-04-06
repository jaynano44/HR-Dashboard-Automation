from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    from src.schema_registry import scan_file  # type: ignore
except Exception:
    scan_file = None


EXCEL_EXTS = {".xlsx", ".xlsm", ".xls"}
CSV_EXTS = {".csv"}


def discover_files(raw_dir: str | Path) -> list[Path]:
    root = Path(raw_dir)
    files = [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in EXCEL_EXTS.union(CSV_EXTS)]
    return sorted(files)


def infer_file_kind(path: Path) -> str:
    name = path.name.lower()

    if scan_file is not None and path.suffix.lower() in {".xlsx", ".xlsm"}:
        try:
            results = scan_file(path)
            kinds = [r.get("kind") for r in results if r.get("kind") not in {"skip", "read_error", None}]
            if kinds:
                priority = ["master", "reference_roster", "sensitive_drop", "org_chart", "unknown"]
                for p in priority:
                    if p in kinds:
                        return p
                return kinds[0]
        except Exception:
            pass

    if "연락망" in name or "주소록" in name:
        return "contact"
    if "master" in name or "재직" in name or "퇴사" in name or "입사" in name:
        return "master"
    if "조직" in name:
        return "org_chart"
    if path.suffix.lower() == ".csv":
        return "master"
    return "unknown"


def read_file_to_records(path: str | Path, file_kind: str | None = None) -> pd.DataFrame:
    p = Path(path)
    kind = file_kind or infer_file_kind(p)

    if p.suffix.lower() in CSV_EXTS:
        return _read_csv(p, kind)
    if p.suffix.lower() in EXCEL_EXTS:
        return _read_excel(p, kind)
    return pd.DataFrame()


def read_all_records(raw_dir: str | Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    files = discover_files(raw_dir)
    frames: list[pd.DataFrame] = []
    manifest_rows: list[dict] = []

    for path in files:
        file_kind = infer_file_kind(path)
        try:
            df = read_file_to_records(path, file_kind=file_kind)
            if df is not None and not df.empty:
                frames.append(df)
            manifest_rows.append({
                "source_file": path.name,
                "file_kind": file_kind,
                "row_count": 0 if df is None else len(df),
                "status": "ok",
                "error": None,
            })
        except Exception as e:
            manifest_rows.append({
                "source_file": path.name,
                "file_kind": file_kind,
                "row_count": 0,
                "status": "error",
                "error": str(e),
            })

    records = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    manifest = pd.DataFrame(manifest_rows)
    return records, manifest


def _read_csv(path: Path, file_kind: str) -> pd.DataFrame:
    encodings = ["utf-8-sig", "cp949", "utf-8"]
    last_error = None
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc, low_memory=False)
            df["source_file"] = path.name
            df["source_sheet"] = "csv"
            df["file_kind"] = file_kind
            return df
        except Exception as e:
            last_error = e
    raise last_error  # type: ignore[misc]


def _read_excel(path: Path, file_kind: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    frames: list[pd.DataFrame] = []

    for sheet_name in xls.sheet_names:
        sheet_df = _read_excel_sheet_with_header_guess(path, sheet_name)
        if sheet_df is None or sheet_df.empty:
            continue
        sheet_df["source_file"] = path.name
        sheet_df["source_sheet"] = sheet_name
        sheet_df["file_kind"] = file_kind
        frames.append(sheet_df)

    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _read_excel_sheet_with_header_guess(path: Path, sheet_name: str) -> pd.DataFrame:
    best_df = None
    best_score = -1

    for header in range(0, 8):
        try:
            df = pd.read_excel(path, sheet_name=sheet_name, header=header, engine="openpyxl")
        except Exception:
            continue

        if df is None or df.empty:
            continue

        cleaned_cols = [str(c).strip() for c in df.columns]
        score = 0
        for c in cleaned_cols:
            if any(k in c for k in ["이름", "성명", "사번", "소속", "본부", "부서", "팀", "입사", "퇴사", "e-mail", "휴대폰"]):
                score += 1

        unnamed = sum(1 for c in cleaned_cols if str(c).lower().startswith("unnamed"))
        score -= unnamed * 0.2

        if score > best_score:
            best_score = score
            best_df = df.copy()

    if best_df is None:
        return pd.DataFrame()

    best_df = best_df.dropna(how="all")
    best_df = best_df.dropna(axis=1, how="all")
    return best_df
