from __future__ import annotations

import re
import time
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.processor.ingest_manifest import IngestManifest, file_fingerprint
from src.processor.org_standardize import apply_alias_map, load_alias_map
from src.processor.schema_registry import detect_sheet_kind, _file_kind, scan_file

# -------------------------
# Column aliases
# -------------------------
_COL_ALIASES: Dict[str, List[str]] = {
    "emp_id":        ["emp_id", "사번", "사원번호", "사원 번호", "사원ID", "사원코드", "직원코드", "No", "번호"],
    "name":          ["name", "이름", "성명", "사원명", "직원명", "구성원명", "퇴사자명", "입사자명"],
    "org":           ["org", "본부", "사업본부", "조직", "소속", "부문", "센터", "실", "그룹"],
    "dept":          ["dept", "부서", "팀", "파트", "스쿼드", "unit", "소속팀", "팀명", "부서명"],
    "title":         ["title", "직무", "직책", "직무명", "직책명"],
    "grade":         ["grade", "등급", "직급", "직위", "평가등급"],
    "snapshot_year": ["snapshot_year", "년도", "연도", "기준연도", "스냅샷연도"],
    "join_date":     ["join_date", "입사일", "입사일자", "입사년월일", "입사"],
    "exit_date":     ["exit_date", "퇴사일", "퇴사일자", "퇴사년월일", "퇴사", "퇴직일", "퇴직일자"],
}
_DATE_COLS = ["join_date", "exit_date"]

# ── 민감정보 컬럼 (ingest 시 자동 드랍)
_SENSITIVE_COLS = [
    "주민등록번호", "주민번호", "rrn",
    "휴대폰", "핸드폰", "내선번호",
    "e-mail", "email", "이메일",
    "집주소", "address", "birth", "생일",
]

# ── 타임아웃 설정
_FILE_TIMEOUT_SEC  = 30   # 파일 1개 처리 제한 (초)
_TOTAL_TIMEOUT_SEC = 180  # 전체 ingest 제한 (초)


def _run_with_timeout(fn, timeout_sec: int, *args, **kwargs):
    """
    fn(*args, **kwargs)를 별도 스레드로 실행.
    timeout_sec 초 초과 시 (None, TimeoutError) 반환.
    """
    result = [None]
    error  = [None]

    def _target():
        try:
            result[0] = fn(*args, **kwargs)
        except Exception as e:
            error[0] = e

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=timeout_sec)

    if t.is_alive():
        return None, TimeoutError(f"{timeout_sec}초 초과")
    if error[0] is not None:
        return None, error[0]
    return result[0], None



# schema_registry.py 의 auto_sort 로 만들어진 폴더 구조와 1:1 대응
_FOLDER_TO_KIND: Dict[str, str] = {
    "headcount":  "master_candidate",
    "roster":     "aux_reference_roster",
    "skills":     "aux_skill",
    "recruit":    "aux_recruit",
    "evaluation": "aux_personalcard",
    "pricing":    "aux_pricing",
    "sensitive":  "sensitive_drop",
    "org_chart":  "org_chart",
    "unknown":    "unmatched_excel",
}

# ── ingest를 완전히 건너뛸 kind
_SKIP_KINDS = {
    "aux_pricing", "aux_recruit", "aux_skill", "aux_personalcard",
    "aux_reference_roster", "org_chart", "sensitive_drop", "unmatched_excel",
}


# ─────────────────────────────────────────────
# 내부 유틸
# ─────────────────────────────────────────────
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
    if s is None:
        return s
    out = pd.to_datetime(s, errors="coerce")
    sn = pd.to_numeric(s, errors="coerce")
    m = sn.between(20000, 60000)
    if m.any():
        out.loc[m] = pd.to_datetime("1899-12-30") + pd.to_timedelta(sn[m], unit="D")
    ss = s.astype(str).str.strip()
    m2 = ss.str.fullmatch(r"\d{8}", na=False)
    if m2.any():
        out.loc[m2] = pd.to_datetime(ss[m2], format="%Y%m%d", errors="coerce")
    out = out.mask(out == pd.Timestamp("1970-01-01"))
    return out


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


def _drop_sensitive_cols(df: pd.DataFrame) -> pd.DataFrame:
    """주민번호·연락처 등 민감 컬럼 자동 제거."""
    drop = []
    for c in df.columns:
        ck = _norm_key(str(c))
        for s in _SENSITIVE_COLS:
            if _norm_key(s) in ck or ck in _norm_key(s):
                drop.append(c)
                break
    return df.drop(columns=drop, errors="ignore") if drop else df


def _canonicalize(df: pd.DataFrame, source_name: str, sheet_name: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    d = _drop_sensitive_cols(df.copy())
    d.columns = [str(c).strip() for c in d.columns]

    emp_col  = _find_col_soft(d, _COL_ALIASES["emp_id"])
    name_col = _find_col_soft(d, _COL_ALIASES["name"])
    if emp_col is None and name_col is None:
        return pd.DataFrame()

    org_col   = _find_col_soft(d, _COL_ALIASES["org"])
    dept_col  = _find_col_soft(d, _COL_ALIASES["dept"])
    title_col = _find_col_soft(d, _COL_ALIASES["title"])
    grade_col = _find_col_soft(d, _COL_ALIASES["grade"])
    join_col  = _find_col_soft(d, _COL_ALIASES["join_date"])
    exit_col  = _find_col_soft(d, _COL_ALIASES["exit_date"])
    snap_col  = _find_col_soft(d, _COL_ALIASES["snapshot_year"])

    def take(c: Optional[str]) -> pd.Series:
        return d[c] if c is not None else pd.Series([None] * len(d))

    out = pd.DataFrame()
    out["emp_id"]  = take(emp_col).astype(str).map(lambda x: _norm(x).replace(".0", "")).replace({"": None, "nan": None, "None": None})
    out["name"]    = take(name_col).astype(str).map(_norm).replace({"": None, "nan": None, "None": None})
    out["org"]     = take(org_col).astype(str).map(_norm).replace({"": None, "nan": None, "None": None})
    out["dept"]    = take(dept_col).astype(str).map(_norm).replace({"": None, "nan": None, "None": None})
    out["title"]   = take(title_col).astype(str).map(_norm).replace({"": None, "nan": None, "None": None})
    out["grade"]   = take(grade_col).astype(str).map(_norm).replace({"": None, "nan": None, "None": None})
    out["join_date"] = _coerce_datetime(take(join_col))
    out["exit_date"] = _coerce_datetime(take(exit_col))

    if snap_col is not None:
        out["snapshot_year"] = pd.to_numeric(take(snap_col), errors="coerce").astype("Int64")
    else:
        y = _extract_year_from_filename(source_name)
        out["snapshot_year"] = (
            pd.Series([y] * len(out), dtype="Int64") if y
            else out["join_date"].dt.year.astype("Int64")
        )

    out["data_source"] = source_name
    out["sheet_name"]  = sheet_name
    out = out.dropna(subset=["emp_id", "name"], how="all")
    return out


def _deduplicate(master: pd.DataFrame) -> pd.DataFrame:
    if master is None or master.empty:
        return master

    d = master.copy()
    d["__empkey__"] = d["emp_id"].fillna("").astype(str).str.strip()
    miss = d["__empkey__"].eq("") | d["__empkey__"].isin(["None", "nan"])
    d.loc[miss, "__empkey__"] = d["name"].fillna("").astype(str).str.strip()

    filled   = d[["name", "org", "dept", "title", "grade"]].notna().sum(axis=1)
    has_exit = d["exit_date"].notna().astype(int)
    jd = pd.to_datetime(d["join_date"], errors="coerce")
    ed = pd.to_datetime(d["exit_date"], errors="coerce")

    d["__score__"] = has_exit * 100 + filled
    d["__jd__"]    = jd
    d["__ed__"]    = ed

    d = d.sort_values(
        by=["__empkey__", "snapshot_year", "__score__", "__ed__", "__jd__"],
        ascending=[True, True, False, False, False],
        na_position="last",
    )
    d = d.drop_duplicates(subset=["__empkey__", "snapshot_year"], keep="first")
    return d.drop(columns=["__score__", "__jd__", "__ed__", "__empkey__"], errors="ignore")


# ─────────────────────────────────────────────
# 파일 분류 (폴더 우선 → schema_registry → 파일명 키워드)
# ─────────────────────────────────────────────
def _classify_file(p: Path, raw_dir: Path) -> str:
    """
    분류 우선순위:
      1) 부모 폴더명 → _FOLDER_TO_KIND 직접 매핑  (auto_sort 결과 존중)
      2) 파일명 키워드 (파일 열지 않고 즉시 판단)
      3) schema_registry.scan_file() 컨텐츠 기반 (파일명으로 판단 불가한 경우만)
    """
    # 1) 폴더 기반
    if p.parent != raw_dir:
        folder = p.parent.name.lower()
        for fname, kind in _FOLDER_TO_KIND.items():
            if fname.lower() == folder:
                return kind

    # 2) 파일명 키워드 — 파일 열기 전에 먼저 판단 (속도 핵심)
    fn = p.name.lower()
    if any(k in fn for k in ["조직도", "orgchart"]):
        return "org_chart"
    if any(k in fn for k in ["단가", "연봉", "simu", "실적관리", "견적", "제안서"]):
        return "aux_pricing"
    if any(k in fn for k in ["채용", "recruit", "지원자", "skill inventory"]):
        return "aux_recruit"
    if any(k in fn for k in ["기술스택", "스킬", "skill", "개발스텍"]):
        return "aux_skill"
    if any(k in fn for k in ["인사기록카드", "개인이력카드", "경력기술서"]):
        return "aux_personalcard"
    if any(k in fn for k in ["연락망", "주소록"]):
        return "sensitive_drop"

    # 3) 컨텐츠 기반 — 파일명으로 판단 안 된 경우만 실제로 열어서 분석
    if p.suffix.lower() in [".xlsx", ".xlsm"]:
        try:
            results = scan_file(p)
            fk = _file_kind(results, p.name)
            _sr_map = {
                "master":           "master_candidate",
                "reference_roster": "aux_reference_roster",
                "aux_skill":        "aux_skill",
                "aux_recruit":      "aux_recruit",
                "aux_personalcard": "aux_personalcard",
                "aux_pricing":      "aux_pricing",
                "sensitive_drop":   "sensitive_drop",
                "org_chart":        "org_chart",
                "skip":             "unmatched_excel",
                "unknown":          "master_candidate",
            }
            return _sr_map.get(fk, "master_candidate")
        except Exception:
            pass

    return "master_candidate"


# ─────────────────────────────────────────────
# Excel 멀티시트 읽기 - 시트별 kind 반환
# ─────────────────────────────────────────────
def _read_excel_all_sheets(path: Path) -> List[Tuple[str, pd.DataFrame]]:
    """master 시트만 반환 (기존 호환)"""
    results = _read_excel_sheets_with_kind(path)
    return [(sh, df) for sh, df, kind in results if kind == "master"]


def _read_excel_sheets_with_kind(path: Path) -> List[Tuple[str, pd.DataFrame, str]]:
    """
    (시트명, DataFrame, kind) 리스트 반환.
    kind: master | aux_skill | aux_recruit | aux_personalcard |
          sensitive_drop | skip | unknown
    """
    out: List[Tuple[str, pd.DataFrame, str]] = []
    xls = pd.ExcelFile(path, engine="openpyxl")

    for sh in xls.sheet_names:
        chosen: Optional[pd.DataFrame] = None
        chosen_cols: List[str] = []

        for hdr in range(0, 26):
            try:
                tmp = pd.read_excel(path, sheet_name=sh, header=hdr, engine="openpyxl").dropna(how="all")
            except Exception:
                continue
            if tmp is None or tmp.empty:
                continue
            cols = [str(c).strip() for c in tmp.columns]
            cols_norm = [_norm_key(c) for c in cols]
            unnamed = sum((c.startswith("unnamed") or c == "") for c in cols_norm)
            if unnamed >= max(1, int(len(cols_norm) * 0.7)):
                continue
            chosen = tmp
            chosen_cols = cols
            break

        if chosen is None or chosen.empty:
            continue

        sheet_kind = detect_sheet_kind(sh, chosen_cols, n_rows=len(chosen))
        out.append((sh, chosen, sheet_kind))

    return out


# ─────────────────────────────────────────────
# 메인 엔진
# ─────────────────────────────────────────────
def auto_ingest_engine(
    raw_dir: str | Path,
    processed_dir: str | Path,
    secrets: Optional[dict] = None,
) -> Tuple[pd.DataFrame, dict]:

    raw_dir       = Path(raw_dir)
    processed_dir = Path(processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = processed_dir / "ingest_manifest.json"
    report_path   = processed_dir / "ingest_report.json"
    master_path   = processed_dir / "master_auto.csv"

    mani = IngestManifest(manifest_path).load()
    # 후 (_MOVED_ 파일 무시)
    raw_files = sorted([
        p for p in raw_dir.rglob("*")
        if p.is_file() and not p.name.startswith("_MOVED_")
    ])

    # ── fast path
    fps     = [file_fingerprint(p) for p in raw_files]
    changed = [fp for fp in fps if mani.is_new_or_changed(fp)]
    if (not changed) and master_path.exists() and report_path.exists():
        try:
            import json
            master = pd.read_csv(master_path, encoding="utf-8-sig")
            rep    = json.loads(report_path.read_text(encoding="utf-8"))
            rep["fast_path"] = True
            return master, rep
        except Exception:
            pass

    read_ok   = 0
    read_fail = 0
    fail_list: List[str] = []

    classified_skips: Dict[str, List[str]] = {
        "aux_pricing":          [],
        "aux_recruit":          [],
        "aux_skill":            [],
        "aux_personalcard":     [],
        "aux_reference_roster": [],
        "org_chart":            [],
        "sensitive_drop":       [],
        "legacy_xls":           [],
        "non_excel":            [],
        "unmatched_excel":      [],
    }

    frames: List[pd.DataFrame] = []
    timeout_skips: List[str] = []        # ← 타임아웃 스킵 기록
    ingest_start = time.time()           # ← 전체 시작 시간
    total_timeout_hit = False

    alias_map = None
    try:
        alias_map = load_alias_map(processed_dir / "org_alias_map.csv")
    except Exception:
        alias_map = None

    total_files = len(raw_files)
    for i, p in enumerate(raw_files, 1):
        elapsed_so_far = round(time.time() - ingest_start, 1)
        print(f"[{i}/{total_files}] {p.name}  ({elapsed_so_far}s)", flush=True)

        # ── 전체 타임아웃 체크
        if time.time() - ingest_start > _TOTAL_TIMEOUT_SEC:
            total_timeout_hit = True
            remaining = [str(q.name) for q in raw_files if q not in [Path(f.path) for f in fps]]
            timeout_skips.extend([p.name] + [q.name for q in raw_files[raw_files.index(p)+1:]])
            print(f"\n⏱️  전체 ingest {_TOTAL_TIMEOUT_SEC}초 초과 → 강제 종료. 지금까지 결과로 진행.")
            break

        suffix = p.suffix.lower()

        # ── 비엑셀
        if suffix not in [".xlsx", ".xlsm", ".xls"]:
            classified_skips["non_excel"].append(p.name)
            try:
                mani.append(file_fingerprint(p))
            except Exception:
                pass
            continue

        # ── .xls → 파일명/폴더 기반 분류만, ingest 스킵
        if suffix == ".xls":
            kind   = _classify_file(p, raw_dir)
            bucket = kind if kind in classified_skips else "legacy_xls"
            classified_skips[bucket].append(p.name)
            read_ok += 1
            try:
                mani.append(file_fingerprint(p))
            except Exception:
                pass
            continue

        # ── .xlsx/.xlsm : 분류 결정
        kind = _classify_file(p, raw_dir)

        # ── 스킵 대상
        if kind in _SKIP_KINDS:
            bucket = kind if kind in classified_skips else "unmatched_excel"
            classified_skips[bucket].append(p.name)
            read_ok += 1
            try:
                mani.append(file_fingerprint(p))
            except Exception:
                pass
            continue

        # ── master_candidate → ingest (파일당 타임아웃 적용)
        def _process_file(path):
            sheet_results = _read_excel_sheets_with_kind(path)
            if not sheet_results:
                raise ValueError("no readable sheets found")
            master_frames = []
            sheet_kinds = {}  # sh → kind
            for sh, df, kind in sheet_results:
                sheet_kinds[sh] = kind
                if kind != "master":
                    continue
                can = _canonicalize(df, path.name, sh)
                if can is None or can.empty:
                    continue
                can2, _miss = apply_alias_map(
                    can, org_col="org", dept_col="dept", alias=alias_map,
                    out_org_col="std_org", out_dept_col="std_dept",
                )
                if "std_org" in can2.columns:
                    can2["org"] = can2["std_org"]
                if "std_dept" in can2.columns:
                    can2["dept"] = can2["std_dept"]
                master_frames.append(can2)
            return master_frames, sheet_kinds

        try:
            file_frames, err = _run_with_timeout(_process_file, _FILE_TIMEOUT_SEC, p)

            if isinstance(err, TimeoutError):
                timeout_skips.append(p.name)
                print(f"  ⏱️  타임아웃 스킵 ({_FILE_TIMEOUT_SEC}초 초과): {p.name}", flush=True)
                read_fail += 1
                fail_list.append(f"{p.name} (timeout: {_FILE_TIMEOUT_SEC}s 초과)")
            elif err is not None:
                raise err
            else:
                file_frames, sheet_kinds = file_frames  # unpack tuple
                # 시트별 분류 결과를 classified_skips에 기록
                for sh, kind in (sheet_kinds or {}).items():
                    if kind == "aux_skill":
                        classified_skips["aux_skill"].append(f"{p.name}::{sh}")
                    elif kind == "aux_recruit":
                        classified_skips["aux_recruit"].append(f"{p.name}::{sh}")
                    elif kind == "aux_personalcard":
                        classified_skips["aux_personalcard"].append(f"{p.name}::{sh}")
                    elif kind == "sensitive_drop":
                        classified_skips["sensitive_drop"].append(f"{p.name}::{sh}")

                if file_frames:
                    frames.extend(file_frames)
                    read_ok += 1
                    print(f"  ✅ master {sum(len(f) for f in file_frames)}행", flush=True)
                else:
                    classified_skips["unmatched_excel"].append(p.name)
                    read_ok += 1
                    print(f"  ⏭️  스킵 (master 시트 없음)", flush=True)

        except Exception as e:
            read_fail += 1
            fail_list.append(f"{p.name} ({e})")

        try:
            mani.append(file_fingerprint(p))
        except Exception:
            pass

    mani.save()

    # ── master 합치기
    if frames:
        master = pd.concat(frames, ignore_index=True)
    else:
        master = pd.DataFrame(columns=[
            "emp_id", "name", "org", "dept", "title", "grade",
            "join_date", "exit_date", "snapshot_year",
            "data_source", "sheet_name",
        ])

    for c in _DATE_COLS:
        if c in master.columns:
            master[c] = _coerce_datetime(master[c])

    master = _deduplicate(master)
    master.to_csv(master_path, index=False, encoding="utf-8-sig")

    import json
    ingest_report = {
        "raw_files":          len(raw_files),
        "read_ok":            read_ok,
        "read_fail":          read_fail,
        "read_fail_list":     fail_list,
        "classified_skips":   classified_skips,
        "skipped_files":      classified_skips.get("org_chart", []),
        "master_auto_rows":   int(len(master)),
        "fast_path":          False,
        "manifest_path":      str(manifest_path).replace("\\", "/"),
        "master_auto_path":   str(master_path).replace("\\", "/"),
        "timeout_skips":      timeout_skips,
        "total_timeout_hit":  total_timeout_hit,
        "elapsed_sec":        round(time.time() - ingest_start, 1),
    }

    try:
        report_path.write_text(
            json.dumps(ingest_report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception:
        pass

    return master, ingest_report
