from __future__ import annotations

from typing import Any

import pandas as pd


STANDARD_COLUMN_ALIASES: dict[str, list[str]] = {
    "emp_id": ["emp_id", "사번", "사원번호", "사원코드", "직원번호", "employee_id", "id"],
    "name": ["name", "이름", "성명", "사원명", "직원명"],
    "org": ["org", "소속", "조직", "본부", "사업부", "사업본부", "부문", "센터"],
    "dept": ["dept", "부서", "팀", "그룹", "파트", "실", "랩", "lab"],
    "title": ["title", "직책", "직급", "직위", "호칭"],
    "grade": ["grade", "등급", "밴드", "경력등급", "직급레벨"],
    "join_date": ["join_date", "입사일", "입사일자", "hire_date", "joindate"],
    "exit_date": ["exit_date", "퇴사일", "퇴사일자", "terminationdate", "exitdate"],
    "employment_type": ["employment_type", "고용형태", "채용형태", "계약형태"],
    "phone": ["phone", "휴대폰", "핸드폰", "전화", "연락처", "내선번호", "내선"],
    "email": ["email", "e-mail", "이메일", "메일"],
}

ORG_KEYWORDS = ["본부", "사업부", "사업본부", "부문", "센터", "실", "그룹", "팀", "파트", "랩", "lab"]
DROP_SENSITIVE_COLUMNS = ["주민등록번호", "주민번호", "집주소", "주소", "생년월일"]


def normalize_records(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out.columns = [_clean_text(c) for c in out.columns]
    out = out.drop(columns=[c for c in out.columns if _clean_text(c) in [_clean_text(x) for x in DROP_SENSITIVE_COLUMNS]], errors="ignore")

    out = standardize_columns(out)
    out = normalize_dates(out)
    out = normalize_org_fields(out)
    out = add_review_flags(out)
    return out


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    col_map: dict[str, str] = {}

    for original in out.columns:
        original_clean = _clean_text(original)
        for std_col, aliases in STANDARD_COLUMN_ALIASES.items():
            if original_clean == _clean_text(std_col) or any(original_clean == _clean_text(a) for a in aliases):
                col_map[original] = std_col
                break

    out = out.rename(columns=col_map)

    required_cols = [
        "emp_id", "name", "org", "dept", "title", "grade",
        "join_date", "exit_date", "employment_type", "phone", "email",
    ]
    for col in required_cols:
        if col not in out.columns:
            out[col] = pd.NA

    for col in ["emp_id", "name", "org", "dept", "title", "grade", "employment_type", "phone", "email"]:
        out[col] = out[col].astype("string").str.strip()

    return out


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for col in ["join_date", "exit_date"]:
        parsed = pd.to_datetime(out[col], errors="coerce")

        numeric = pd.to_numeric(out[col], errors="coerce")
        excel_mask = numeric.between(20000, 60000)
        if excel_mask.any():
            parsed.loc[excel_mask] = pd.to_datetime("1899-12-30") + pd.to_timedelta(numeric[excel_mask], unit="D")

        text = out[col].astype("string").fillna("").str.strip()
        yyyymmdd_mask = text.str.fullmatch(r"\d{8}", na=False)
        if yyyymmdd_mask.any():
            parsed.loc[yyyymmdd_mask] = pd.to_datetime(text[yyyymmdd_mask], format="%Y%m%d", errors="coerce")

        out[col] = parsed

    return out


def normalize_org_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    org_like_cols = [c for c in out.columns if _looks_like_org_col(c)]
    if not org_like_cols:
        org_like_cols = [c for c in ["org", "dept"] if c in out.columns]

    path_values: list[Any] = []
    level_values: dict[str, list[Any]] = {f"org_level_{i}": [] for i in range(1, 6)}

    for _, row in out.iterrows():
        parts: list[str] = []
        for c in org_like_cols[:5]:
            val = row.get(c)
            if pd.notna(val) and str(val).strip():
                parts.append(str(val).strip())

        deduped = []
        for p in parts:
            if not deduped or deduped[-1] != p:
                deduped.append(p)

        path_values.append(" > ".join(deduped) if deduped else pd.NA)

        for i in range(1, 6):
            level_values[f"org_level_{i}"].append(deduped[i - 1] if len(deduped) >= i else pd.NA)

    out["org_path"] = pd.Series(path_values, index=out.index, dtype="object")
    for col, values in level_values.items():
        out[col] = pd.Series(values, index=out.index, dtype="object")

    return out


def add_review_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    reasons: list[Any] = []

    for _, row in out.iterrows():
        row_reasons: list[str] = []

        if pd.isna(row.get("name")) or str(row.get("name")).strip() == "":
            row_reasons.append("missing_name")

        if pd.isna(row.get("join_date")) and pd.isna(row.get("exit_date")):
            row_reasons.append("missing_dates")

        if pd.notna(row.get("join_date")) and pd.notna(row.get("exit_date")):
            if row["exit_date"] < row["join_date"]:
                row_reasons.append("exit_before_join")

        if pd.isna(row.get("org_path")) and (pd.notna(row.get("org")) or pd.notna(row.get("dept"))):
            row_reasons.append("org_path_incomplete")

        reasons.append(", ".join(row_reasons) if row_reasons else pd.NA)

    out["review_reason"] = pd.Series(reasons, index=out.index, dtype="object")
    out["needs_review"] = out["review_reason"].notna()
    return out


def _looks_like_org_col(col_name: str) -> bool:
    c = _clean_text(col_name)
    return any(_clean_text(k) in c for k in ORG_KEYWORDS)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\n", " ").replace("\r", " ").strip().lower()
