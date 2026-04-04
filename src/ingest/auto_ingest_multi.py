from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

_COL_ALIASES: Dict[str, List[str]] = {
    "emp_id": ["emp_id", "사번", "사원번호", "사원 번호", "사원ID", "사원코드", "직원코드", "No", "번호"],
    "name": ["name", "이름", "성명", "사원명", "직원명", "구성원명", "퇴사자명", "입사자명"],
    "org": ["org", "본부", "사업본부", "조직", "소속", "부문", "센터", "실", "그룹"],
    "dept": ["dept", "부서", "팀", "파트", "스쿼드", "unit", "소속팀", "팀명", "부서명"],
    "title": ["title", "직무", "직책", "직무명", "직책명"],
    "grade": ["grade", "등급", "직급", "직위", "평가등급"],
    "snapshot_year": ["snapshot_year", "년도", "연도", "기준연도", "스냅샷연도"],
    "join_date": ["join_date", "입사일", "입사일자", "입사년월일", "입사"],
    "exit_date": ["exit_date", "퇴사일", "퇴사일자", "퇴사년월일", "퇴사", "퇴직일", "퇴직일자"],
}
_DATE_COLS = ["join_date", "exit_date"]

_SKIP_KEYWORDS = [
    "조직도", "orgchart", "연락망", "주소록", "채용", "recruit", "지원자",
    "기술스택", "스킬", "skill", "개발스텍", "단가", "연봉", "simu", "실적관리",
    "견적", "제안서", "인사기록카드", "개인이력카드", "경력기술서"
]


def _norm(x: object) -> str:
    if x is None:
        return ""
    s = str(x).replace("\n", " ").replace("\r", " ").strip()
    return re.sub(r"\s+", " ", s)


def _norm_key(x: object) -> str:
    return re.sub(r"[\s\(\)\[\]\{\}_:\-]", "", _norm(x)).lower()


def _extract_year_from_filename(name: str) -> Optional[int]:
    m = re.search(r"(19|20)\d{2}", name)
    if not m:
        return None
    y = int(m.group(0))
    return y if 1900 <= y <= 2100 else None


def _coerce_datetime(s: pd.Series) -> pd.Series:
    out = pd.to_datetime(s, errors="coerce")
    sn = pd.to_numeric(s, errors="coerce")
    m = sn.between(20000, 60000)
    if m.any():
        out.loc[m] = pd.to_datetime("1899-12-30") + pd.to_timedelta(sn[m], unit="D")
    ss = s.astype(str).str.strip()
    m2 = ss.str.fullmatch(r"\d{8}", na=False)
    if m2.any():
        out.loc[m2] = pd.to_datetime(ss[m2], format="%Y%m%d", errors="coerce")
    return out.mask(out == pd.Timestamp("1970-01-01"))


def _find_col_soft(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    cols = list(df.columns)
    lut = {_norm_key(c): c for c in cols}
    for a in aliases:
        k = _norm_key(a)
        if k in lut:
            return lut[k]
    for a in aliases:
        ak = _norm_key(a)
        if not ak:
            continue
        for col in cols:
            if ak in _norm_key(col):
                return col
    return None


def _canonicalize(df: pd.DataFrame, source_name: str, sheet_name: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.copy()
    d.columns = [str(c).strip() for c in d.columns]

    emp_col = _find_col_soft(d, _COL_ALIASES["emp_id"])
    name_col = _find_col_soft(d, _COL_ALIASES["name"])
    if emp_col is None and name_col is None:
        return pd.DataFrame()

    org_col = _find_col_soft(d, _COL_ALIASES["org"])
    dept_col = _find_col_soft(d, _COL_ALIASES["dept"])
    title_col = _find_col_soft(d, _COL_ALIASES["title"])
    grade_col = _find_col_soft(d, _COL_ALIASES["grade"])
    join_col = _find_col_soft(d, _COL_ALIASES["join_date"])
    exit_col = _find_col_soft(d, _COL_ALIASES["exit_date"])
    snap_col = _find_col_soft(d, _COL_ALIASES["snapshot_year"])

    def take(c: Optional[str]) -> pd.Series:
        return d[c] if c is not None else pd.Series([None] * len(d))

    out = pd.DataFrame()
    out["emp_id"] = take(emp_col).astype(str).map(lambda x: _norm(x).replace(".0", "")).replace({"": None, "nan": None, "None": None})
    out["name"] = take(name_col).astype(str).map(_norm).replace({"": None, "nan": None, "None": None})
    out["org"] = take(org_col).astype(str).map(_norm).replace({"": None, "nan": None, "None": None})
    out["dept"] = take(dept_col).astype(str).map(_norm).replace({"": None, "nan": None, "None": None})
    out["title"] = take(title_col).astype(str).map(_norm).replace({"": None, "nan": None, "None": None})
    out["grade"] = take(grade_col).astype(str).map(_norm).replace({"": None, "nan": None, "None": None})
    out["join_date"] = _coerce_datetime(take(join_col))
    out["exit_date"] = _coerce_datetime(take(exit_col))

    if snap_col is not None:
        out["snapshot_year"] = pd.to_numeric(take(snap_col), errors="coerce").astype("Int64")
    else:
        y = _extract_year_from_filename(source_name)
        out["snapshot_year"] = pd.Series([y] * len(out), dtype="Int64") if y else out["join_date"].dt.year.astype("Int64")

    out["data_source"] = source_name
    out["sheet_name"] = sheet_name
    return out.dropna(subset=["emp_id", "name"], how="all")


def _deduplicate(master: pd.DataFrame) -> pd.DataFrame:
    if master is None or master.empty:
        return master

    d = master.copy()
    d["__empkey__"] = d["emp_id"].fillna("").astype(str).str.strip()
    miss = d["__empkey__"].eq("") | d["__empkey__"].isin(["None", "nan"])
    d.loc[miss, "__empkey__"] = d.loc[miss, "name"].fillna("").astype(str).str.strip()

    filled = d[["name", "org", "dept", "title", "grade"]].notna().sum(axis=1)
    has_exit = d["exit_date"].notna().astype(int)
    jd = pd.to_datetime(d["join_date"], errors="coerce")
    ed = pd.to_datetime(d["exit_date"], errors="coerce")

    d["__score__"] = has_exit * 100 + filled
    d["__jd__"] = jd
    d["__ed__"] = ed

    d = d.sort_values(
        by=["__empkey__", "snapshot_year", "__score__", "__ed__", "__jd__"],
        ascending=[True, True, False, False, False],
        na_position="last",
    )
    d = d.drop_duplicates(subset=["__empkey__", "snapshot_year"], keep="first")
    return d.drop(columns=["__score__", "__jd__", "__ed__", "__empkey__"], errors="ignore")


def _should_skip_file(p: Path) -> bool:
    name = p.name.lower()
    return any(k.lower() in name for k in _SKIP_KEYWORDS)


def auto_ingest_engine(
    raw_dir: str | Path,
    processed_dir: str | Path,
    secrets: Optional[dict] = None,
) -> Tuple[pd.DataFrame, dict]:
    raw_dir = Path(raw_dir)
    processed_dir = Path(processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    master_path = processed_dir / "master_auto.csv"
    report_path = processed_dir / "ingest_report.json"

    raw_files = sorted([p for p in raw_dir.rglob("*") if p.is_file() and not p.name.startswith("_MOVED_")])

    read_ok = 0
    read_fail = 0
    fail_list: List[str] = []
    classified_skips = {"non_excel": [], "skipped_by_name": [], "unmatched_excel": []}
    frames: List[pd.DataFrame] = []

    total_files = len(raw_files)
    for i, p in enumerate(raw_files, 1):
        print(f"[{i}/{total_files}] {p.name}", flush=True)
        suffix = p.suffix.lower()

        if suffix not in [".xlsx", ".xlsm", ".xls"]:
            classified_skips["non_excel"].append(p.name)
            continue

        if _should_skip_file(p):
            classified_skips["skipped_by_name"].append(p.name)
            continue

        try:
            sheets = pd.read_excel(p, sheet_name=None, engine="openpyxl" if suffix != ".xls" else None)
            file_frames = []

            for sh, df in sheets.items():
                if df is None or df.empty:
                    continue
                can = _canonicalize(df, p.name, sh)
                if can is None or can.empty:
                    continue
                file_frames.append(can)

            if file_frames:
                frames.extend(file_frames)
                read_ok += 1
                print(f"  ✅ {sum(len(f) for f in file_frames)} rows", flush=True)
            else:
                classified_skips["unmatched_excel"].append(p.name)
                read_ok += 1
                print("  ⏭️ 스킵 (master 시트 없음)", flush=True)

        except Exception as e:
            read_fail += 1
            fail_list.append(f"{p.name} ({e})")
            print(f"  ❌ {e}", flush=True)

    if frames:
        master = pd.concat(frames, ignore_index=True)
    else:
        master = pd.DataFrame(columns=[
            "emp_id", "name", "org", "dept", "title", "grade",
            "join_date", "exit_date", "snapshot_year", "data_source", "sheet_name",
        ])

    for c in _DATE_COLS:
        if c in master.columns:
            master[c] = _coerce_datetime(master[c])

    master = _deduplicate(master)
    master.to_csv(master_path, index=False, encoding="utf-8-sig")

    import json
    report = {
        "raw_files": len(raw_files),
        "read_ok": read_ok,
        "read_fail": read_fail,
        "fail_list": fail_list,
        "rows": int(len(master)),
        "classified_skips": classified_skips,
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return master, report