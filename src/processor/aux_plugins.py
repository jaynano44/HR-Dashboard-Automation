# src/processor/aux_plugins.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


def _norm(x: object) -> str:
    return str(x).replace("\n", " ").replace("\r", " ").strip()


def _find_header_row(df: pd.DataFrame, max_scan: int = 50) -> int:
    needles = ["성명", "이름", "사원명", "name", "employee", "skill", "스킬", "기술"]
    scan_n = min(max_scan, len(df))
    for i in range(scan_n):
        row = df.iloc[i].astype(str).str.replace(r"\s+", "", regex=True).str.lower()
        if any(row.str.contains(n.replace(" ", "").lower()).any() for n in needles):
            return i
    return 0


def _read_all_sheets_any(path: Path) -> List[Tuple[str, pd.DataFrame]]:
    out: List[Tuple[str, pd.DataFrame]] = []
    suffix = path.suffix.lower()

    # xlsx/xlsm
    if suffix in [".xlsx", ".xlsm"]:
        try:
            xls = pd.ExcelFile(path, engine="openpyxl")
        except Exception:
            return out
        for sh in xls.sheet_names:
            try:
                raw = pd.read_excel(path, sheet_name=sh, header=None, engine="openpyxl").dropna(how="all")
            except Exception:
                continue
            if raw is None or raw.empty:
                continue
            hdr = _find_header_row(raw, max_scan=80)
            header = raw.iloc[hdr].tolist()
            df = raw.iloc[hdr + 1 :].copy()
            df.columns = [str(c).strip() if str(c).strip() not in ["", "nan", "None"] else f"col_{i}" for i, c in enumerate(header)]
            df = df.dropna(how="all").reset_index(drop=True)
            if not df.empty:
                out.append((sh, df))
        return out

    # xls (환경에 xlrd가 없으면 실패 가능)
    if suffix == ".xls":
        try:
            xls = pd.ExcelFile(path)  # engine auto
        except Exception:
            return out
        for sh in xls.sheet_names:
            try:
                raw = pd.read_excel(path, sheet_name=sh, header=None).dropna(how="all")
            except Exception:
                continue
            if raw is None or raw.empty:
                continue
            hdr = _find_header_row(raw, max_scan=80)
            header = raw.iloc[hdr].tolist()
            df = raw.iloc[hdr + 1 :].copy()
            df.columns = [str(c).strip() if str(c).strip() not in ["", "nan", "None"] else f"col_{i}" for i, c in enumerate(header)]
            df = df.dropna(how="all").reset_index(drop=True)
            if not df.empty:
                out.append((sh, df))
    return out


def _pick_col(df: pd.DataFrame, keys: List[str]) -> Optional[str]:
    cols = [str(c) for c in df.columns]
    def nk(x: str) -> str:
        return x.replace(" ", "").lower()
    cols_n = [nk(c) for c in cols]
    for k in keys:
        kn = nk(k)
        for c, cn in zip(cols, cols_n):
            if kn == cn or kn in cn:
                return c
    return None


def ingest_aux_skill(ingest_report: dict, raw_dir: Path, aux_dir: Path) -> pd.DataFrame:
    """aux_skill 파일을 v2용 테이블로 1차 정형화 (최소 컬럼)."""
    aux_dir.mkdir(parents=True, exist_ok=True)
    cs = (ingest_report or {}).get("classified_skips", {}) or {}
    files = cs.get("aux_skill", []) or []

    rows = []
    for fname in files:
        fpath = raw_dir / fname if not Path(fname).is_absolute() else Path(fname)
        if not fpath.exists():
            continue
        for sh, df in _read_all_sheets_any(fpath):
            if df is None or df.empty:
                continue

            name_col = _pick_col(df, ["성명", "이름", "사원명", "name"])
            emp_col = _pick_col(df, ["사번", "사원번호", "사원코드", "emp_id", "employeeid", "id"])
            skill_col = _pick_col(df, ["기술", "기술스택", "스킬", "skill", "stack"])
            exp_col = _pick_col(df, ["경험여부", "경험", "experience"])

            out = pd.DataFrame()
            out["name"] = df[name_col].astype(str).map(_norm) if name_col else None
            out["emp_id"] = df[emp_col].astype(str).map(_norm) if emp_col else None
            out["skill_text"] = df[skill_col].astype(str).map(_norm) if skill_col else None
            out["experience"] = df[exp_col].astype(str).map(_norm) if exp_col else None
            out["source_file"] = Path(fname).name
            out["sheet"] = str(sh)

            # 너무 많은 공백행 제거
            out = out.dropna(subset=["name", "emp_id"], how="all")
            out = out[out["name"].astype(str).str.strip().ne("") | out["emp_id"].astype(str).str.strip().ne("")]
            if not out.empty:
                rows.append(out)

    final = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["name", "emp_id", "skill_text", "experience", "source_file", "sheet"])
    out_path = aux_dir / "aux_skill.csv"
    final.to_csv(out_path, index=False, encoding="utf-8-sig")
    return final


def ingest_aux_recruit(ingest_report: dict, raw_dir: Path, aux_dir: Path) -> pd.DataFrame:
    """aux_recruit(채용 skill inventory) 파일을 v2용 테이블로 1차 정형화 (최소 컬럼)."""
    aux_dir.mkdir(parents=True, exist_ok=True)
    cs = (ingest_report or {}).get("classified_skips", {}) or {}
    files = cs.get("aux_recruit", []) or []

    rows = []
    for fname in files:
        fpath = raw_dir / fname if not Path(fname).is_absolute() else Path(fname)
        if not fpath.exists():
            continue
        for sh, df in _read_all_sheets_any(fpath):
            if df is None or df.empty:
                continue

            name_col = _pick_col(df, ["성명", "이름", "지원자", "name"])
            skill_col = _pick_col(df, ["skill", "스킬", "기술", "기술스택", "stack"])
            level_col = _pick_col(df, ["레벨", "등급", "숙련", "level"])
            result_col = _pick_col(df, ["결과", "합격", "불합격", "result", "status"])

            out = pd.DataFrame()
            out["name"] = df[name_col].astype(str).map(_norm) if name_col else None
            out["skill_text"] = df[skill_col].astype(str).map(_norm) if skill_col else None
            out["level"] = df[level_col].astype(str).map(_norm) if level_col else None
            out["result"] = df[result_col].astype(str).map(_norm) if result_col else None
            out["source_file"] = Path(fname).name
            out["sheet"] = str(sh)

            out = out.dropna(subset=["name"], how="all")
            out = out[out["name"].astype(str).str.strip().ne("")]
            if not out.empty:
                rows.append(out)

    final = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["name", "skill_text", "level", "result", "source_file", "sheet"])
    out_path = aux_dir / "aux_recruit.csv"
    final.to_csv(out_path, index=False, encoding="utf-8-sig")
    return final
