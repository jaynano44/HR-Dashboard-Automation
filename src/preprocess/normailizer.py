from __future__ import annotations

from typing import Any

import pandas as pd


STANDARD_COLUMN_ALIASES: dict[str, list[str]] = {
    "emp_id": ["emp_id", "사번", "사원번호", "직원번호", "id", "employee_id"],
    "name": ["name", "이름", "성명", "직원명"],
    "org": ["org", "조직", "본부", "사업부", "사업본부", "부문"],
    "dept": ["dept", "부서", "팀", "그룹", "파트"],
    "title": ["title", "직급", "직책", "직위", "호칭"],
    "grade": ["grade", "등급", "직급레벨", "밴드"],
    "join_date": ["join_date", "입사일", "입사일자"],
    "exit_date": ["exit_date", "퇴사일", "퇴사일자"],
    "employment_type": ["employment_type", "고용형태", "계약형태", "채용형태"],
    "skill_name": ["skill_name", "스킬", "기술", "기술스택", "보유기술", "skill"],
    "years_experience": ["years_experience", "경력", "총경력", "경력년수", "years"],
    "project_name": ["project_name", "프로젝트", "프로젝트명"],
}

ORG_KEYWORDS = ["본부", "사업부", "사업본부", "부문", "그룹", "팀", "파트", "센터", "실", "랩", "Lab"]
SKILL_SYNONYMS = {
    "python": ["python", "파이썬"],
    "sql": ["sql", "mysql", "postgresql", "sqlite", "oracle"],
    "java": ["java"],
    "javascript": ["javascript", "js"],
    "react": ["react"],
    "django": ["django"],
    "flask": ["flask"],
    "aws": ["aws", "amazon web services"],
    "docker": ["docker"],
    "kubernetes": ["kubernetes", "k8s"],
    "machine_learning": ["machine learning", "ml", "머신러닝"],
    "deep_learning": ["deep learning", "dl", "딥러닝"],
    "llm": ["llm", "gpt", "rag", "langchain", "transformers"],
}


def normalize_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    범용 HR 데이터 레코드 표준화.
    - 컬럼명 표준화
    - 날짜 표준화
    - 조직 경로 컬럼 생성
    - 스킬 표준화
    - review 필요 여부 플래그 생성
    """
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out.columns = [_clean_text(c) for c in out.columns]

    out = standardize_columns(out)
    out = normalize_dates(out)
    out = normalize_org_fields(out)
    out = normalize_skill_fields(out)
    out = add_review_flags(out)

    return out


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    col_map: dict[str, str] = {}
    existing = list(out.columns)

    for std_col, aliases in STANDARD_COLUMN_ALIASES.items():
        for col in existing:
            col_clean = _clean_text(col)
            if col_clean == _clean_text(std_col):
                col_map[col] = std_col
                break

            if any(col_clean == _clean_text(alias) for alias in aliases):
                col_map[col] = std_col
                break

    out = out.rename(columns=col_map)

    expected_cols = [
        "emp_id",
        "name",
        "org",
        "dept",
        "title",
        "grade",
        "join_date",
        "exit_date",
        "employment_type",
        "skill_name",
        "years_experience",
        "project_name",
    ]
    for col in expected_cols:
        if col not in out.columns:
            out[col] = pd.NA

    str_cols = ["emp_id", "name", "org", "dept", "title", "grade", "employment_type", "skill_name", "project_name"]
    for col in str_cols:
        out[col] = out[col].astype("string").str.strip()

    return out


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for col in ["join_date", "exit_date"]:
        if col not in out.columns:
            continue

        parsed = pd.to_datetime(out[col], errors="coerce")

        numeric = pd.to_numeric(out[col], errors="coerce")
        mask_excel = numeric.between(20000, 60000)
        if mask_excel.any():
            parsed.loc[mask_excel] = pd.to_datetime("1899-12-30") + pd.to_timedelta(numeric[mask_excel], unit="D")

        text = out[col].astype(str).str.strip()
        mask_yyyymmdd = text.str.fullmatch(r"\d{8}", na=False)
        if mask_yyyymmdd.any():
            parsed.loc[mask_yyyymmdd] = pd.to_datetime(text[mask_yyyymmdd], format="%Y%m%d", errors="coerce")

        out[col] = parsed

    return out


def normalize_org_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for i in range(1, 6):
        level_col = f"org_level_{i}"
        if level_col not in out.columns:
            out[level_col] = pd.NA

    if "org_path" not in out.columns:
        out["org_path"] = pd.NA

    # 이미 org/dept가 있다면 기본 path 생성
    if "org" in out.columns or "dept" in out.columns:
        org_vals = out["org"].fillna("").astype(str).str.strip() if "org" in out.columns else ""
        dept_vals = out["dept"].fillna("").astype(str).str.strip() if "dept" in out.columns else ""

        path_vals = []
        for org, dept in zip(org_vals, dept_vals):
            parts = [p for p in [org, dept] if p]
            path_vals.append(" > ".join(parts) if parts else None)

        out["org_path"] = pd.Series(path_vals, index=out.index, dtype="object")

        split_parts = out["org_path"].fillna("").astype(str).str.split(">")
        for idx in out.index:
            parts = [p.strip() for p in split_parts.loc[idx] if p.strip()]
            for i in range(min(5, len(parts))):
                out.at[idx, f"org_level_{i+1}"] = parts[i]

    # 조직 컬럼이 여러 개 이미 있으면 path 보강
    org_like_cols = [c for c in out.columns if _looks_like_org_col(c)]
    if len(org_like_cols) >= 2:
        for idx in out.index:
            parts = []
            for c in org_like_cols[:5]:
                val = out.at[idx, c]
                if pd.notna(val) and str(val).strip():
                    parts.append(str(val).strip())

            if parts:
                out.at[idx, "org_path"] = " > ".join(parts)
                for i, p in enumerate(parts[:5], start=1):
                    out.at[idx, f"org_level_{i}"] = p

    return out


def normalize_skill_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "skill_name" not in out.columns:
        out["skill_name"] = pd.NA

    out["skill_raw"] = out["skill_name"].astype("string").str.strip()
    out["skill_norm"] = out["skill_raw"].apply(_normalize_skill_value)

    return out


def add_review_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "needs_review" not in out.columns:
        out["needs_review"] = False
    if "review_reason" not in out.columns:
        out["review_reason"] = pd.NA

    reasons = []

    for _, row in out.iterrows():
        row_reasons: list[str] = []

        if pd.isna(row.get("name")) or str(row.get("name")).strip() == "":
            row_reasons.append("missing_name")

        if pd.isna(row.get("join_date")) and pd.isna(row.get("exit_date")):
            row_reasons.append("missing_dates")

        if pd.notna(row.get("join_date")) and pd.notna(row.get("exit_date")):
            if row["exit_date"] < row["join_date"]:
                row_reasons.append("exit_before_join")

        if pd.notna(row.get("skill_raw")) and pd.isna(row.get("skill_norm")):
            row_reasons.append("unknown_skill")

        org_path = row.get("org_path")
        if pd.notna(org_path) and str(org_path).strip():
            pass
        elif pd.notna(row.get("org")) or pd.notna(row.get("dept")):
            row_reasons.append("org_path_incomplete")

        reasons.append(", ".join(row_reasons) if row_reasons else None)

    out["review_reason"] = reasons
    out["needs_review"] = out["review_reason"].notna()

    return out


def _normalize_skill_value(value: Any) -> Any:
    if pd.isna(value):
        return pd.NA

    text = _clean_text(value)
    if not text:
        return pd.NA

    for skill_std, aliases in SKILL_SYNONYMS.items():
        alias_clean = [_clean_text(a) for a in aliases]
        if text in alias_clean:
            return skill_std

    return pd.NA


def _looks_like_org_col(col_name: str) -> bool:
    c = _clean_text(col_name)
    return any(k in c for k in [_clean_text(x) for x in ORG_KEYWORDS])


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


if __name__ == "__main__":
    sample = pd.DataFrame(
        [
            {
                "사번": "1001",
                "이름": "홍길동",
                "사업본부": "AI사업본부",
                "팀": "ML팀",
                "입사일": "20230105",
                "퇴사일": None,
                "기술스택": "Python",
            },
            {
                "사원번호": "1002",
                "성명": "김영희",
                "본부": "경영지원본부",
                "부서": "인사팀",
                "입사일자": 45200,
                "퇴사일자": None,
                "기술": "LangChain",
            },
        ]
    )

    result = normalize_records(sample)
    print(result)