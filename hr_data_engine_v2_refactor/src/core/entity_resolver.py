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
    if records_df is None or records_df.empty:
        empty = pd.DataFrame()
        return ResolutionResult(empty, empty, empty)

    df = _prepare_columns(records_df)
    df = _build_match_keys(df)
    df = assign_emp_uid(df)

    employee_master = build_employee_master(df)
    employment_history = build_employment_history(df)
    match_log = build_match_log(df)
    return ResolutionResult(employee_master, employment_history, match_log)


def _prepare_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    expected = [
        "emp_id", "name", "org", "dept", "title", "grade", "phone", "email",
        "join_date", "exit_date", "employment_type", "source_file", "source_sheet",
    ]
    for col in expected:
        if col not in out.columns:
            out[col] = pd.NA

    for col in ["emp_id", "name", "org", "dept", "title", "grade", "phone", "email", "employment_type"]:
        out[col] = out[col].astype("string").str.strip()

    for col in ["join_date", "exit_date"]:
        out[col] = pd.to_datetime(out[col], errors="coerce")

    out = out[out["name"].notna() & (out["name"].astype(str).str.strip() != "")].copy()
    out = out.reset_index(drop=True)
    return out


def _build_match_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["emp_id_key"] = out["emp_id"].fillna("").astype(str).str.strip()
    out["join_date_key"] = out["join_date"].dt.strftime("%Y-%m-%d").fillna("")
    out["name_key"] = out["name"].fillna("").astype(str).str.strip()
    out["org_key"] = out["org"].fillna("").astype(str).str.strip()
    out["phone_key"] = out["phone"].fillna("").astype(str).str.replace(r"[^0-9]", "", regex=True)
    out["email_key"] = out["email"].fillna("").astype(str).str.strip().str.lower()
    out["name_join_key"] = out["name_key"] + "||" + out["join_date_key"]
    out["name_org_key"] = out["name_key"] + "||" + out["org_key"]
    return out


def assign_emp_uid(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["emp_uid"] = pd.NA
    out["match_method"] = pd.NA
    out["match_key"] = pd.NA
    uid_counter = 1

    def _assign(groups: dict, method: str):
        nonlocal uid_counter
        for key, idx_list in groups.items():
            if key in {"", "||"}:
                continue
            idxs = list(idx_list)
            unassigned = [idx for idx in idxs if pd.isna(out.at[idx, "emp_uid"])]
            if not unassigned:
                continue
            emp_uid = f"P{uid_counter:07d}"
            uid_counter += 1
            out.loc[unassigned, "emp_uid"] = emp_uid
            out.loc[unassigned, "match_method"] = method
            out.loc[unassigned, "match_key"] = key

    _assign(out[out["emp_id_key"] != ""].groupby("emp_id_key").groups, "emp_id_exact")
    _assign(out[out["name_join_key"] != "||"].groupby("name_join_key").groups, "name_join_exact")
    _assign(out[out["email_key"] != ""].groupby("email_key").groups, "email_exact")
    _assign(out[out["phone_key"] != ""].groupby("phone_key").groups, "phone_exact")
    _assign(out[out["name_org_key"] != "||"].groupby("name_org_key").groups, "name_org_exact")

    for idx in out[out["emp_uid"].isna()].index:
        emp_uid = f"P{uid_counter:07d}"
        uid_counter += 1
        out.loc[idx, "emp_uid"] = emp_uid
        out.loc[idx, "match_method"] = "new_person"
        out.loc[idx, "match_key"] = None

    return out


def build_employee_master(df: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []

    for emp_uid, g in df.groupby("emp_uid", dropna=False):
        g = g.sort_values(["join_date", "exit_date"], na_position="last")
        current = _pick_current_row(g)
        records.append({
            "emp_uid": emp_uid,
            "emp_id": _first_notna(g["emp_id"]),
            "name": _first_notna(g["name"]),
            "current_org": current.get("org"),
            "current_dept": current.get("dept"),
            "current_title": current.get("title"),
            "current_grade": current.get("grade"),
            "phone": _first_notna(g["phone"]),
            "email": _first_notna(g["email"]),
            "employment_status": _derive_employment_status(g),
            "first_join_date": g["join_date"].min(),
            "last_exit_date": g["exit_date"].max(),
            "record_count": len(g),
        })

    result = pd.DataFrame(records)
    if not result.empty:
        result = result.sort_values(["name", "emp_uid"]).reset_index(drop=True)
    return result


def build_employment_history(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().reset_index(drop=True)
    out = out.sort_values(["emp_uid", "join_date", "exit_date"], na_position="last").reset_index(drop=True)
    out["session_seq"] = out.groupby("emp_uid").cumcount() + 1
    out["session_id"] = out["emp_uid"].astype(str) + "-S" + out["session_seq"].astype(str).str.zfill(3)

    cols = [
        "session_id", "emp_uid", "emp_id", "name", "join_date", "exit_date",
        "org", "dept", "title", "grade", "phone", "email", "employment_type",
        "source_file", "source_sheet", "match_method", "match_key",
    ]
    return out[cols].copy()


def build_match_log(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "emp_uid", "emp_id", "name", "org", "dept", "join_date", "exit_date",
        "phone", "email", "match_method", "match_key", "source_file", "source_sheet",
    ]
    return df[cols].copy().reset_index(drop=True)


def _pick_current_row(g: pd.DataFrame) -> dict[str, Any]:
    active = g[g["exit_date"].isna()]
    if not active.empty:
        return active.sort_values("join_date", na_position="last").iloc[-1].to_dict()
    return g.sort_values("exit_date", na_position="last").iloc[-1].to_dict()


def _derive_employment_status(g: pd.DataFrame) -> str:
    return "active" if g["exit_date"].isna().any() else "exited"


def _first_notna(series: pd.Series) -> Any:
    s = series.dropna()
    return None if s.empty else s.iloc[0]
