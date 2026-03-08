# src/processor/headcount_2024_loader.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Tuple, Union

import pandas as pd


def _norm(s: object) -> str:
    return str(s).replace("\n", " ").replace("\r", " ").strip()


def _find_header_row(df: pd.DataFrame, max_scan: int = 200) -> Optional[int]:
    needles = ["사번", "사원코드", "사원번호", "이름", "성명", "사원명", "name", "employee"]
    scan_n = min(max_scan, len(df))
    for i in range(scan_n):
        row = df.iloc[i].astype(str).str.replace(r"\s+", "", regex=True).str.lower()
        if any(row.str.contains(n.replace(" ", "")).any() for n in needles):
            return i
    return None


def _normalize_header(df_raw: pd.DataFrame) -> pd.DataFrame:
    hdr_idx = _find_header_row(df_raw)
    if hdr_idx is None:
        hdr_idx = 0

    header = df_raw.iloc[hdr_idx].tolist()
    cols = []
    seen = {}
    for j, v in enumerate(header):
        name = _norm(v)
        name = name if name not in ["", "nan", "None"] else f"col_{j}"
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


def _score_sheet(sheet_name: str, df: pd.DataFrame) -> Tuple[int, int]:
    sn = sheet_name.lower().replace(" ", "")
    type_score = 0
    if ("재직" in sn) or ("현원" in sn) or ("인원" in sn) or ("조직" in sn):
        type_score += 5
    if ("퇴사" in sn) or ("퇴직" in sn):
        type_score -= 5
    return type_score, int(len(df))


def load_headcount_2024(xlsx_path: Union[str, Path]) -> Dict[str, pd.DataFrame]:
    """
    반환:
      { "active": DataFrame|None, "exited": DataFrame|None, "survey": DataFrame|None }
    - 시트명이 고정('재직자'/'퇴사자')이 아니어도 자동 탐지
    - 헤더 위치가 위쪽에 붙어있지 않아도 스캔해서 복원
    """
    p = Path(xlsx_path)
    out: Dict[str, pd.DataFrame] = {"active": None, "exited": None, "survey": None}

    if not p.exists():
        return out

    try:
        xls = pd.ExcelFile(p, engine="openpyxl")
    except Exception:
        return out

    candidates = []
    for sname in xls.sheet_names:
        try:
            raw = pd.read_excel(p, sheet_name=sname, header=None, engine="openpyxl").dropna(how="all")
            if raw.empty:
                continue
            df = _normalize_header(raw)
            if df.empty:
                continue
            candidates.append((sname, df))
        except Exception:
            continue

    if not candidates:
        return out

    # exited 후보(시트명에 퇴사/퇴직)
    exited_best = None
    exited_best_score = (-10**9, -10**9)
    for sname, df in candidates:
        sn = sname.lower().replace(" ", "")
        if ("퇴사" in sn) or ("퇴직" in sn):
            sc = _score_sheet(sname, df)
            if sc > exited_best_score:
                exited_best_score = sc
                exited_best = (sname, df)

    # active 후보(재직/현원/인원/조직) + exited 제외
    active_best = None
    active_best_score = (-10**9, -10**9)
    for sname, df in candidates:
        if exited_best and sname == exited_best[0]:
            continue
        sn = sname.lower().replace(" ", "")
        if ("재직" in sn) or ("현원" in sn) or ("인원" in sn) or ("조직" in sn):
            sc = _score_sheet(sname, df)
            if sc > active_best_score:
                active_best_score = sc
                active_best = (sname, df)

    # 그래도 없으면: 가장 큰 시트를 active로
    if active_best is None:
        active_best = max(candidates, key=lambda x: len(x[1]))

    out["active"] = active_best[1] if active_best else None
    out["exited"] = exited_best[1] if exited_best else None

    # survey: '설문' 키워드 우선, 없으면 None
    for sname, df in candidates:
        sn = sname.lower().replace(" ", "")
        if "설문" in sn or "survey" in sn:
            out["survey"] = df
            break

    return out
