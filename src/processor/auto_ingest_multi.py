from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.processor.ingest_manifest import IngestManifest, file_fingerprint
from src.processor.org_standardize import apply_alias_map, load_alias_map

# -------------------------
# Column aliases
# -------------------------
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


def _norm(x: object) -> str:
    if x is None:
        return ""
    s = str(x).replace("\n", " ").replace("\r", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _norm_key(x: object) -> str:
    return re.sub(r"[\s\(\)\[\]\{\}_:\-]", "", _norm(x)).lower()


def _extract_year_from_filename(name: str) -> Optional[int]:
    m = re.search(r"(19|20)\d{2}", name)
    if not m:
        return None
    y = int(m.group(0))
    return y if 1900 <= y <= 2100 else None


def _coerce_datetime(s: pd.Series) -> pd.Series:
    if s is None:
        return s
    out = pd.to_datetime(s, errors="coerce")

    # Excel serial
    sn = pd.to_numeric(s, errors="coerce")
    m = sn.between(20000, 60000)
    if m.any():
        out.loc[m] = pd.to_datetime("1899-12-30") + pd.to_timedelta(sn[m], unit="D")

    # YYYYMMDD
    ss = s.astype(str).str.strip()
    m2 = ss.str.fullmatch(r"\d{8}", na=False)
    if m2.any():
        out.loc[m2] = pd.to_datetime(ss[m2], format="%Y%m%d", errors="coerce")

    # 1970 제거
    out = out.mask(out == pd.Timestamp("1970-01-01"))
    return out


def _find_col_exact(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    cols = list(df.columns)
    lut = {_norm_key(c): c for c in cols}
    for a in aliases:
        k = _norm_key(a)
        if k in lut:
            return lut[k]
    return None


def _find_col_soft(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    """정확 매칭 → 부분 포함 매칭(예: '사원명(한글)')"""
    c = _find_col_exact(df, aliases)
    if c is not None:
        return c
    cols = list(df.columns)
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
        if c is None:
            return pd.Series([None] * len(d))
        return d[c]

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

    out = out.dropna(subset=["emp_id", "name"], how="all")
    return out


def _deduplicate(master: pd.DataFrame) -> pd.DataFrame:
    if master is None or master.empty:
        return master

    d = master.copy()
    d["__empkey__"] = d["emp_id"].fillna("").astype(str).str.strip()
    miss = d["__empkey__"].eq("") | d["__empkey__"].isin(["None", "nan"])
    d.loc[miss, "__empkey__"] = d["name"].fillna("").astype(str).str.strip()

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


# -------------------------
# Classification (plugin-ready)
# -------------------------
_AUX_KEYWORDS = {
    "org_chart": ["조직도", "orgchart"],
    "aux_pricing": ["기준단가", "단가", "제안서", "견적", "단가표"],
    "aux_recruit": ["채용", "recruit", "지원자", "면접", "skill inventory", "인벤토리"],
    "aux_skill": ["기술스택", "스킬", "skill", "경험여부", "역량", "inventory"],
    # ✅ 대량 fail 원인: 개인 인사카드/개별 문서형 엑셀
    "aux_personalcard": ["인사기록카드", "개인인력카드", "개인이력카드", "경력기술서", "경력 연차", "연차별"],
    "aux_reference_roster": ["입사자, 퇴사자 명단", "입사자 퇴사자 명단", "입사자,퇴사자명단"],
}

# ✅ “진짜 마스터 후보”라면 canonical 미매칭을 fail로 남김
_STRICT_MASTER_HINTS = ["입사", "퇴사", "입퇴사", "인원현황", "직원현황", "등급기초", "재직자", "퇴사자", "인원 현황"]
def _looks_like_master_by_content(xlsx_path: Path) -> bool:
    """
    파일명과 무관하게 '마스터로 ingest 해야 하는가'를 내용으로 판단.
    기준(하나라도 만족하면 True):
      - 사번/사원코드 계열 컬럼 존재
      - 입사일/퇴사일 계열 컬럼 존재
      - 날짜 파싱이 가능한 컬럼이 충분히 존재(간단 탐지)
    """
    try:
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    except Exception:
        return False

    # 앞쪽 1~3개 시트만 빠르게 확인(속도)
    sheet_names = xls.sheet_names[:3]
    date_alias = ["입사일", "입사일자", "퇴사일", "퇴사일자", "join", "hire", "exit", "termination"]
    id_alias = ["사번", "사원코드", "사원번호", "emp_id", "employeeid", "id"]

    def normk(s: str) -> str:
        return _norm_key(s)

    for sh in sheet_names:
        # header 0~10만 가볍게 스캔
        for hdr in range(0, 11):
            try:
                df = pd.read_excel(xlsx_path, sheet_name=sh, header=hdr, engine="openpyxl")
            except Exception:
                continue
            if df is None or df.empty:
                continue

            cols = [str(c) for c in df.columns]
            cols_n = [normk(c) for c in cols]

            # 1) 사번 계열 컬럼
            if any(any(normk(a) == cn or normk(a) in cn for a in id_alias) for cn in cols_n):
                return True

            # 2) 입/퇴사일 컬럼
            if any(any(normk(a) == cn or normk(a) in cn for a in date_alias) for cn in cols_n):
                return True

            # 3) 날짜 파싱이 가능한 컬럼이 실제로 존재(간단 테스트)
            #    - 너무 무거우면 안되니, 상위 50행만 검사
            sub = df.head(50)
            for c in sub.columns[:12]:  # 너무 많은 컬럼은 스킵(속도)
                r = _date_parse_ratio(sub[c])
                if r >= 0.3:
                    return True

    return False

def detect_file_kind(filename: str) -> str:
    fn = filename.lower()

    # 기존 키워드 분류(조직도/단가/스킬/채용/개인카드 등)는 그대로 유지
    for kind, keys in _AUX_KEYWORDS.items():
        for kw in keys:
            if kw.lower() in fn:
                return kind

    # ✅ '입사/퇴사/명단'이 들어간 파일은 "내용"으로 master vs reference 판단 (파일명만으로 결론 X)
    if ("입사" in fn) and ("퇴사" in fn) and ("명단" in fn):
        return "candidate_roster"  # 임시 타입(내용검사 후 확정)

    return "master_candidate"


def is_strict_master_candidate(filename: str) -> bool:
    n = filename.lower()
    return any(h.lower() in n for h in _STRICT_MASTER_HINTS)


def _read_excel_all_sheets(path: Path) -> List[Tuple[str, pd.DataFrame]]:
    """
    .xlsx/.xlsm만 안정 지원.
    .xls는 환경에 xlrd가 없으면 깨짐 → 본 엔진에선 legacy_xls로 분류 스킵 처리(아래에서).
    """
    out: List[Tuple[str, pd.DataFrame]] = []
    xls = pd.ExcelFile(path, engine="openpyxl")
    for sh in xls.sheet_names:
        chosen = None
        for hdr in range(0, 26):
            try:
                tmp = pd.read_excel(path, sheet_name=sh, header=hdr, engine="openpyxl").dropna(how="all")
            except Exception:
                continue
            if tmp is None or tmp.empty:
                continue
            cols = [str(c).strip() for c in tmp.columns]
            cols_norm = [_norm_key(c) for c in cols]
            unnamed = sum([(c.startswith("unnamed") or c == "") for c in cols_norm])
            if unnamed >= max(1, int(len(cols_norm) * 0.7)):
                continue
            chosen = tmp
            break
        if chosen is None or chosen.empty:
            continue
        out.append((sh, chosen))
    return out


def auto_ingest_engine(raw_dir: str | Path, processed_dir: str | Path, secrets: Optional[dict] = None) -> Tuple[pd.DataFrame, dict]:
    raw_dir = Path(raw_dir)
    processed_dir = Path(processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = processed_dir / "ingest_manifest.json"
    report_path = processed_dir / "ingest_report.json"
    master_path = processed_dir / "master_auto.csv"

    mani = IngestManifest(manifest_path).load()

    raw_files = sorted([p for p in raw_dir.rglob("*") if p.is_file()])

    # fast path
    fps = [file_fingerprint(p) for p in raw_files]
    changed = [fp for fp in fps if mani.is_new_or_changed(fp)]
    if (not changed) and master_path.exists() and report_path.exists():
        try:
            master = pd.read_csv(master_path, encoding="utf-8-sig")
            import json
            rep = json.loads(report_path.read_text(encoding="utf-8"))
            rep["fast_path"] = True
            return master, rep
        except Exception:
            pass

    read_ok = 0
    read_fail = 0
    fail_list: List[str] = []

    classified_skips: Dict[str, List[str]] = {
        "aux_pricing": [],
        "aux_recruit": [],
        "aux_skill": [],
        "aux_personalcard": [],
        "aux_reference_roster": [], 
        "org_chart": [],
        "legacy_xls": [],
        "non_excel": [],
        "unmatched_excel": [],  # ✅ v2에서 타입 추가 예정
    }

    frames: List[pd.DataFrame] = []

    alias_map = None
    try:
        alias_map = load_alias_map(processed_dir / "org_alias_map.csv")
    except Exception:
        alias_map = None

    for p in raw_files:
        suffix = p.suffix.lower()

        # --- non excel ---
        if suffix not in [".xlsx", ".xlsm", ".xls"]:
            classified_skips["non_excel"].append(p.name)
            try:
                mani.append(file_fingerprint(p))
            except Exception:
                pass
            continue
        
        kind = detect_file_kind(p.name)

        # ✅ roster 후보는 내용 검사로 확정
        if kind == "candidate_roster" and p.suffix.lower() in [".xlsx", ".xlsm"]:
            if _looks_like_master_by_content(p):
                kind = "master_candidate"          # 날짜/사번 있으면 마스터로 ingest
            else:
                kind = "aux_reference_roster"      # 없으면 검증용 참조로 분류

        # --- legacy .xls (대량 fail 원인) ---
        if suffix == ".xls":
            # 채용 인벤토리라면 aux_recruit로 이미 분류되도록(파일명 키워드 기반)
            kind = detect_file_kind(p.name)
            if kind in ["aux_recruit", "aux_skill", "aux_pricing", "aux_personalcard", "aux_reference_roster","org_chart"]:
                classified_skips[kind].append(p.name)
                read_ok += 1
            else:
                classified_skips["legacy_xls"].append(p.name)
                read_ok += 1
            try:
                mani.append(file_fingerprint(p))
            except Exception:
                pass
            continue

        # --- classify by name keywords ---
        kind = detect_file_kind(p.name)
        if kind in ["aux_pricing", "aux_recruit", "aux_skill", "aux_personalcard", "aux_reference_roster","org_chart"]:
            classified_skips[kind].append(p.name)
            read_ok += 1
            try:
                mani.append(file_fingerprint(p))
            except Exception:
                pass
            continue

        # --- master candidate ingest (.xlsx/.xlsm) ---
        try:
            sheets = _read_excel_all_sheets(p)
            if not sheets:
                raise ValueError("no readable sheets")

            got_any = False
            for sh, df in sheets:
                can = _canonicalize(df, p.name, sh)
                if can is None or can.empty:
                    continue

                can2, _miss = apply_alias_map(
                    can,
                    org_col="org",
                    dept_col="dept",
                    alias=alias_map,
                    out_org_col="std_org",
                    out_dept_col="std_dept",
                )
                if "std_org" in can2.columns:
                    can2["org"] = can2["std_org"]
                if "std_dept" in can2.columns:
                    can2["dept"] = can2["std_dept"]

                frames.append(can2)
                got_any = True

            if not got_any:
                # ✅ strict 후보면 fail로, 아니면 unmatched_excel로 분류 스킵
                if is_strict_master_candidate(p.name):
                    raise ValueError("sheets read but no canonical columns matched (emp_id/name not found)")
                classified_skips["unmatched_excel"].append(p.name)
                read_ok += 1
            else:
                read_ok += 1

        except Exception as e:
            read_fail += 1
            fail_list.append(f"{p.name} (read_fail: {e})")

        try:
            mani.append(file_fingerprint(p))
        except Exception:
            pass

    mani.save()

    if frames:
        master = pd.concat(frames, ignore_index=True)
    else:
        master = pd.DataFrame(
            columns=[
                "emp_id", "name", "org", "dept", "title", "grade",
                "join_date", "exit_date", "snapshot_year",
                "data_source", "sheet_name",
            ]
        )

    for c in _DATE_COLS:
        if c in master.columns:
            master[c] = _coerce_datetime(master[c])

    master = _deduplicate(master)
    master.to_csv(master_path, index=False, encoding="utf-8-sig")

    ingest_report = {
        "raw_files": len(raw_files),
        "read_ok": read_ok,
        "read_fail": read_fail,
        "read_fail_list": fail_list,
        "classified_skips": classified_skips,
        "skipped_files": classified_skips.get("org_chart", []),
        "master_auto_rows": int(len(master)),
        "fast_path": False,
        "manifest_path": str(manifest_path).replace("\\", "/"),
        "master_auto_path": str(master_path).replace("\\", "/"),
    }

    try:
        import json
        report_path.write_text(json.dumps(ingest_report, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

    return master, ingest_report


auto_ingest_engine = auto_ingest_engine