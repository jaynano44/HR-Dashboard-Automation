# src/processor/career_rank_loader.py
"""
아이엔소프트_구성원_경력연차별_인원_수-YYYYMMDD.xlsx 파서

재직자 시트 컬럼 구조:
  col0=번호, col1=구분(SE/본부장/개발...), col2=이름, col3=직급,
  col4=소속, col5=주민등록번호(민감→제거), col6=생년(민감→제거),
  col7=생년(중복→제거), col8=입사일, col9=학위, col10=학위취득일,
  col11=자격증1, col12=합격일1, col13=자격증2, col14=합격일2,
  col15=자격증3, col16=합격일3, col17=경력기준등급,
  col20=경력증기준등급, col21=전직장경력(개월), col22=전직장경력년수,
  col23=근속합계(개월), col24=근속년수, col25=총경력합(개월), col26=총경력년수
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

_SENSITIVE = {5, 6, 7}  # 주민등록번호, 생년 컬럼 인덱스

_COL_MAP = {
    0:  "seq",
    1:  "role_type",
    2:  "name",
    3:  "grade",
    4:  "org",
    8:  "join_date",
    9:  "education",
    17: "career_grade",
    21: "prev_career_months",
    23: "tenure_months",
    25: "total_career_months",
}


def _parse_sheet(df_raw: pd.DataFrame, snapshot_date: Optional[str] = None) -> pd.DataFrame:
    """header row 탐지 후 컬럼 매핑 + 민감정보 제거"""
    # header row 탐지 (이름/이름 컬럼이 있는 행)
    hdr_idx = None
    for i in range(min(5, len(df_raw))):
        row = df_raw.iloc[i].astype(str)
        if row.str.contains("이름|name", case=False, na=False).any():
            hdr_idx = i
            break
    if hdr_idx is None:
        hdr_idx = 1  # fallback

    data = df_raw.iloc[hdr_idx + 1:].copy().reset_index(drop=True)

    # 민감 컬럼 제거
    drop_cols = [c for i, c in enumerate(data.columns) if i in _SENSITIVE]
    data = data.drop(columns=drop_cols, errors="ignore")

    # 컬럼명 재매핑 (원래 인덱스 기준)
    orig_cols = list(df_raw.columns)
    rename = {}
    for col_idx, std_name in _COL_MAP.items():
        if col_idx < len(orig_cols):
            rename[orig_cols[col_idx]] = std_name

    # 민감 제거 후 남은 컬럼에만 적용
    rename = {k: v for k, v in rename.items() if k in data.columns}
    data = data.rename(columns=rename)

    # 날짜 변환
    if "join_date" in data.columns:
        data["join_date"] = pd.to_datetime(data["join_date"], errors="coerce")

    # 빈 행 제거 (이름 없는 행)
    if "name" in data.columns:
        data = data[data["name"].notna() & ~data["name"].astype(str).isin(["nan","None","NaN",""])]

    # org에서 소속 정리
    if "org" in data.columns:
        data["org"] = data["org"].astype(str).str.strip()
        # 상위 본부 추출 (예: "서비스사업본부-MSA Dev팀" → org="서비스사업본부", dept="MSA Dev팀")
        split = data["org"].str.split(r"[-·]", n=1, expand=True)
        data["org_full"] = data["org"]
        data["org"]  = split[0].str.strip()
        data["dept"] = split[1].str.strip() if 1 in split.columns else None

    # 숫자형 변환
    for col in ["prev_career_months","tenure_months","total_career_months"]:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    if snapshot_date:
        data["snapshot_date"] = snapshot_date

    return data.reset_index(drop=True)


def load_career_rank_files(raw_dir: Path) -> pd.DataFrame:
    """
    raw/ 루트의 경력연차 엑셀 파일들을 모두 로드.
    가장 최신 파일을 기준으로 반환 (중복 이름은 최신 파일 우선).
    """
    pattern_dirs = [raw_dir, raw_dir / "headcount"]
    files = []
    for d in pattern_dirs:
        if d.exists():
            files += sorted(d.glob("*경력연차*.xlsx")) + sorted(d.glob("*경력연차*.xls"))

    if not files:
        return pd.DataFrame()

    frames = []
    for fpath in sorted(files):
        try:
            xl = pd.ExcelFile(fpath, engine="openpyxl")
            # 재직자 시트 우선, 없으면 가장 큰 시트
            sheet = next((s for s in xl.sheet_names if "재직자" in s), None)
            if sheet is None:
                sheet = max(xl.sheet_names,
                            key=lambda s: len(pd.read_excel(fpath, sheet_name=s,
                                                             header=None, engine="openpyxl")))

            df_raw = pd.read_excel(fpath, sheet_name=sheet,
                                   header=None, engine="openpyxl").dropna(how="all")

            # snapshot 날짜 추출 (파일명에서)
            import re
            m = re.search(r"(\d{8})", fpath.stem)
            snap = m.group(1) if m else None

            df = _parse_sheet(df_raw, snapshot_date=snap)
            if not df.empty:
                frames.append(df)
                print(f"  ✅ {fpath.name}  [{sheet}]  {len(df)}명")
        except Exception as e:
            print(f"  ⚠️  {fpath.name} 실패: {e}")

    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True)

    # 중복 제거: 같은 이름이면 가장 최신 snapshot_date 유지
    if "name" in merged.columns and "snapshot_date" in merged.columns:
        merged = (merged
                  .sort_values("snapshot_date", ascending=False)
                  .drop_duplicates(subset=["name"], keep="first")
                  .reset_index(drop=True))

    return merged
