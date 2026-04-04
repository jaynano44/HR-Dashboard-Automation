#entity_resolver.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class ResolutionResult:
    employee_master: pd.DataFrame
    employment_history: pd.DataFrame
    match_log: pd.DataFrame


def resolve_entities(records_df: pd.DataFrame) -> ResolutionResult:
    """
    HR 레코드를 받아 동일 인물(emp_uid) 기준으로 정리한다.

    현재 버전(v0):
    1) emp_id exact match
    2) name + join_date exact match
    3) 둘 다 없으면 new person
    4) employment session은 row 단위 기본 생성

    Parameters
    ----------
    records_df : pd.DataFrame
        최소 기대 컬럼:
        - emp_id (optional)
        - name
        - join_date (optional)
        - exit_date (optional)
        - org / dept / title / grade (optional)

    Returns
    -------
    ResolutionResult
    """
    if records_df is None or records_df.empty:
        empty = pd.DataFrame()
        return ResolutionResult(empty, empty, empty)

    df = records_df.copy()

    df = _prepare_columns(df)
    df = _build_match_keys(df)
    df = assign_emp_uid(df)
    employee_master = build_employee_master(df)
    employment_history = build_employment_history(df)
    match_log = build_match_log(df)

    return ResolutionResult(
        employee_master=employee_master,
        employment_history=employment_history,
        match_log=match_log,
    )


def _prepare_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    기본 컬럼 정리.
    """
    out = df.copy()

    expected_cols = [
        "emp_id",
        "name",
        "join_date",
        "exit_date",
        "org",
        "dept",
        "title",
        "grade",
        "employment_type",
        "source_file",
        "source_sheet",
    ]
    for col in expected_cols:
        if col not in out.columns:
            out[col] = pd.NA

    # 문자열 표준화
    str_cols = ["emp_id", "name", "org", "dept", "title", "grade", "employment_type"]
    for col in str_cols:
        out[col] = out[col].astype("string").str.strip()

    # 날짜 표준화
    for col in ["join_date", "exit_date"]:
        out[col] = pd.to_datetime(out[col], errors="coerce")

    # 이름 없는 행 제거
    out = out[out["name"].notna() & (out["name"].astype(str).str.strip() != "")]
    out = out.reset_index(drop=True)

    return out


def _build_match_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    deterministic matching용 key 생성
    """
    out = df.copy()

    out["emp_id_key"] = out["emp_id"].fillna("").astype(str).str.strip()

    out["join_date_key"] = out["join_date"].dt.strftime("%Y-%m-%d")
    out["join_date_key"] = out["join_date_key"].fillna("")

    out["name_join_key"] = (
        out["name"].fillna("").astype(str).str.strip()
        + "||"
        + out["join_date_key"].fillna("").astype(str)
    )

    return out


def assign_emp_uid(df: pd.DataFrame) -> pd.DataFrame:
    """
    동일 인물 기준 emp_uid 부여.

    우선순위
    1. emp_id exact match
    2. name + join_date
    3. new person
    """
    out = df.copy()
    out["emp_uid"] = pd.NA
    out["match_method"] = pd.NA
    out["match_key"] = pd.NA

    uid_counter = 1

    # ------------------------------------------------------------------
    # 1) emp_id exact match
    # ------------------------------------------------------------------
    emp_id_groups = (
        out[out["emp_id_key"] != ""]
        .groupby("emp_id_key", dropna=False)
        .groups
    )

    for emp_id_key, idx_list in emp_id_groups.items():
        emp_uid = f"P{uid_counter:07d}"
        uid_counter += 1

        out.loc[list(idx_list), "emp_uid"] = emp_uid
        out.loc[list(idx_list), "match_method"] = "emp_id_exact"
        out.loc[list(idx_list), "match_key"] = emp_id_key

    # ------------------------------------------------------------------
    # 2) name + join_date
    # emp_uid 없는 행만 처리
    # ------------------------------------------------------------------
    unmatched = out["emp_uid"].isna()
    name_join_groups = (
        out[unmatched & (out["name_join_key"] != "||")]
        .groupby("name_join_key", dropna=False)
        .groups
    )

    for name_join_key, idx_list in name_join_groups.items():
        emp_uid = f"P{uid_counter:07d}"
        uid_counter += 1

        out.loc[list(idx_list), "emp_uid"] = emp_uid
        out.loc[list(idx_list), "match_method"] = "name_join_exact"
        out.loc[list(idx_list), "match_key"] = name_join_key

    # ------------------------------------------------------------------
    # 3) still unmatched → one row one person
    # ------------------------------------------------------------------
    still_unmatched = out["emp_uid"].isna()
    for idx in out[still_unmatched].index:
        emp_uid = f"P{uid_counter:07d}"
        uid_counter += 1

        out.loc[idx, "emp_uid"] = emp_uid
        out.loc[idx, "match_method"] = "new_person"
        out.loc[idx, "match_key"] = None

    return out


def build_employee_master(df: pd.DataFrame) -> pd.DataFrame:
    """
    emp_uid 기준 1인 1행 employee_master 생성
    """
    records: list[dict[str, Any]] = []

    for emp_uid, g in df.groupby("emp_uid", dropna=False):
        g = g.sort_values(["join_date", "exit_date"], na_position="last")

        current_row = _pick_current_row(g)

        records.append(
            {
                "emp_uid": emp_uid,
                "emp_id": _first_notna(g["emp_id"]),
                "name": _first_notna(g["name"]),
                "current_org": current_row.get("org"),
                "current_dept": current_row.get("dept"),
                "current_title": current_row.get("title"),
                "current_grade": current_row.get("grade"),
                "employment_status": _derive_employment_status(g),
                "first_join_date": g["join_date"].min(),
                "last_exit_date": g["exit_date"].max(),
                "record_count": len(g),
            }
        )

    result = pd.DataFrame(records)
    if not result.empty:
        result = result.sort_values(["name", "emp_uid"]).reset_index(drop=True)

    return result


def build_employment_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    현재 버전은 row 단위로 employment_history 생성.
    추후 identity_graph.py에서 session split 고도화 가능.
    """
    out = df.copy().reset_index(drop=True)

    out["session_seq"] = out.groupby("emp_uid").cumcount() + 1
    out["session_id"] = out["emp_uid"].astype(str) + "-S" + out["session_seq"].astype(str).str.zfill(3)

    history_cols = [
        "session_id",
        "emp_uid",
        "emp_id",
        "name",
        "join_date",
        "exit_date",
        "org",
        "dept",
        "title",
        "grade",
        "employment_type",
        "source_file",
        "source_sheet",
        "match_method",
        "match_key",
    ]

    return out[history_cols].copy()


def build_match_log(df: pd.DataFrame) -> pd.DataFrame:
    """
    매칭 근거 로그
    """
    cols = [
        "emp_uid",
        "emp_id",
        "name",
        "join_date",
        "exit_date",
        "match_method",
        "match_key",
        "source_file",
        "source_sheet",
    ]
    return df[cols].copy().reset_index(drop=True)


def _pick_current_row(g: pd.DataFrame) -> dict[str, Any]:
    """
    현재 상태를 대표하는 row 선택
    우선순위:
    1. exit_date 없는 row
    2. 가장 최근 join_date
    """
    active = g[g["exit_date"].isna()]
    if not active.empty:
        row = active.sort_values("join_date", na_position="last").iloc[-1]
        return row.to_dict()

    row = g.sort_values("exit_date", na_position="last").iloc[-1]
    return row.to_dict()


def _derive_employment_status(g: pd.DataFrame) -> str:
    """
    간단한 재직 상태 계산
    """
    if g["exit_date"].isna().any():
        return "active"
    return "exited"


def _first_notna(series: pd.Series) -> Any:
    s = series.dropna()
    if s.empty:
        return None
    return s.iloc[0]


if __name__ == "__main__":
    sample = pd.DataFrame(
        [
            {
                "emp_id": "1001",
                "name": "홍길동",
                "join_date": "2022-01-10",
                "exit_date": None,
                "org": "AI본부",
                "dept": "ML팀",
                "title": "대리",
                "grade": "선임",
            },
            {
                "emp_id": "1001",
                "name": "홍길동",
                "join_date": "2022-01-10",
                "exit_date": None,
                "org": "AI본부",
                "dept": "ML팀",
                "title": "대리",
                "grade": "선임",
            },
            {
                "emp_id": None,
                "name": "김영희",
                "join_date": "2023-03-01",
                "exit_date": None,
                "org": "경영지원",
                "dept": "인사팀",
                "title": "사원",
                "grade": "주니어",
            },
        ]
    )

    result = resolve_entities(sample)
    print("\n[employee_master]")
    print(result.employee_master)
    print("\n[employment_history]")
    print(result.employment_history)
    print("\n[match_log]")
    print(result.match_log)