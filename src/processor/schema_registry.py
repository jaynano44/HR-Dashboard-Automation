"""
schema_registry.py
------------------
역할 1: 새 엑셀 파일이 기존 양식 레지스트리(HR_데이터_양식_레지스트리.md)와
        매칭되는지 확인하고, 신규 양식이면 alert 기록.

역할 2: --update-schema-registry 옵션으로 실행 시 새 양식을 분석해서
        레지스트리 MD를 자동 업데이트 (신규 섹션 추가).

사용법:
  # 전체 raw 폴더 스캔 (신규 양식 감지)
  python schema_registry.py --scan data/raw

  # 특정 파일 분석 후 레지스트리 업데이트
  python schema_registry.py --update-schema-registry data/raw/새파일.xlsx
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ──────────────────────────────────────────
# 레지스트리 경로 (프로젝트 루트 기준)
# ──────────────────────────────────────────
REGISTRY_MD   = Path("HR_데이터_양식_레지스트리.md")
ALERT_JSON    = Path("data/processed/bronze/unknown_schema_alert.json")


# ──────────────────────────────────────────
# 컬럼 정규화
# ──────────────────────────────────────────
def _nk(x: object) -> str:
    s = str(x).replace("\n", " ").replace("\r", " ").strip()
    return re.sub(r"[\s\(\)\[\]\{\}_:\-]", "", s).lower()


# ──────────────────────────────────────────
# 판별 규칙 (UK 컬럼 조합 기반)
# ──────────────────────────────────────────
_ID_SIGNALS    = ["사번", "사원코드", "사원번호", "emp_id", "employeeid"]
_JOIN_SIGNALS  = ["입사일", "입사일자", "joindate", "hiredate"]
_EXIT_SIGNALS  = ["퇴사일", "퇴사일자", "exitdate", "terminationdate"]
_NAME_SIGNALS  = ["이름", "성명", "사원명", "구성원명", "name"]
_SKILL_SIGNALS = ["기술스택", "개발스텍", "개발스택", "스킬", "skill", "stack", "역량"]
_PRICE_SIGNALS = ["단가", "투입공수", "매출", "연봉", "급여", "인상", "simu"]
_SENS_SIGNALS  = ["휴대폰", "핸드폰", "내선번호", "e-mail", "이메일", "email"]
_RRN_SIGNALS   = ["주민등록번호", "주민번호", "rrn"]
_RECRUIT_SIGNALS = ["채용", "지원자", "경력기술서", "skillinventory"]
_GRADE_SIGNALS = ["등급", "grade", "hr검토", "최종확정"]   # 단일 알파벳 제거 (오매칭 방지)
_ORG_SIGNALS   = ["조직도", "orgchart"]
_SKIP_SHEETS   = [
    "가이드", "guide", "설명", "기준", "작성기준",
    "실적관리가이드", "sheet2", "색인", "index",
    "아이엔소프트_직원목록", "본부별년차별인원수",
]


def _has(cols_nk: List[str], signals: List[str]) -> bool:
    return any(any(_nk(s) in c or c in _nk(s) for s in signals) for c in cols_nk)


def detect_sheet_kind(sheet_name: str, cols: List[str], n_rows: int) -> str:
    """
    시트명 + 컬럼 목록 + 행수로 시트 성격 판별.
    반환값: master | reference_roster | aux_skill | aux_recruit |
             aux_personalcard | aux_pricing | sensitive_drop |
             org_chart | skip | unknown
    """
    sn = _nk(sheet_name)
    cols_nk = [_nk(c) for c in cols]

    # ── 무조건 스킵 시트
    if any(sn == _nk(s) or _nk(s) in sn for s in _SKIP_SHEETS):
        return "skip"

    # ── 조직도
    if _has(cols_nk, _ORG_SIGNALS) or any(_nk(s) in sn for s in _ORG_SIGNALS):
        return "org_chart"

    # ── 민감정보 (연락망: 휴대폰+이메일, 사번/날짜 없는 경우만)
    has_id   = _has(cols_nk, _ID_SIGNALS)
    has_join = _has(cols_nk, _JOIN_SIGNALS)
    has_exit = _has(cols_nk, _EXIT_SIGNALS)
    has_name = _has(cols_nk, _NAME_SIGNALS)

    sens_count = sum(1 for s in _SENS_SIGNALS if any(_nk(s) in c for c in cols_nk))
    if sens_count >= 2 and not (has_id and (has_join or has_exit)):
        return "sensitive_drop"

    # ── master (최우선): 사번 + 날짜 → 주민번호 있어도 master (드랍 플래그만)
    if has_id and (has_join or has_exit):
        return "master"

    # ── master: 시트명 힌트 (재직자/퇴사자/입사자) + 날짜
    if any(k in sn for k in ["재직자", "퇴사자", "입사자", "프로그래머", "엔지니어"]):
        if has_join or has_exit:
            return "master"

    # ── reference_roster: 이름 + 날짜, 사번 없음
    if has_name and (has_join or has_exit) and not has_id:
        return "reference_roster"

    # ── reference_roster: 컬럼 자체가 "입사자"/"퇴사자" (명단 나열형)
    col_vals = [_nk(c) for c in cols]
    if any("입사자" in c for c in col_vals) or any("퇴사자" in c for c in col_vals):
        return "reference_roster"

    # ── aux_pricing (단가/투입 우선 — 등급 컬럼과 혼재 방지)
    if _has(cols_nk, _PRICE_SIGNALS):
        return "aux_pricing"

    # ── 개인 인사카드 (주민번호 — master 아닌 경우)
    if _has(cols_nk, _RRN_SIGNALS):
        return "aux_personalcard"

    # ── aux_skill
    if _has(cols_nk, _SKILL_SIGNALS):
        return "aux_skill"

    # ── aux_recruit
    if _has(cols_nk, _RECRUIT_SIGNALS):
        return "aux_recruit"

    # ── aux_personalcard: 평가 등급표
    if _has(cols_nk, _GRADE_SIGNALS) and has_name:
        return "aux_personalcard"

    # ── 판별 불가
    return "unknown"


# ──────────────────────────────────────────
# 파일 스캔
# ──────────────────────────────────────────
def scan_file(fpath: Path) -> List[Dict]:
    """
    엑셀 파일의 시트별로 kind를 판별해서 결과 리스트 반환.
    """
    results = []
    if fpath.suffix.lower() not in [".xlsx", ".xlsm"]:
        return results

    try:
        xls = pd.ExcelFile(fpath, engine="openpyxl")
    except Exception as e:
        return [{"file": fpath.name, "sheet": "N/A", "kind": "read_error", "error": str(e)}]

    for sh in xls.sheet_names:
        cols, n_rows = [], 0
        for hdr in range(8):
            try:
                df = pd.read_excel(fpath, sheet_name=sh, header=hdr, engine="openpyxl").dropna(how="all")
                if df is None or df.empty:
                    continue
                raw_cols = [str(c).strip() for c in df.columns]
                unnamed = sum(1 for c in raw_cols if c.lower().startswith("unnamed") or c in ["", "nan"])
                if unnamed < len(raw_cols) * 0.6:
                    cols = raw_cols
                    n_rows = len(df)
                    break
            except Exception:
                continue

        kind = detect_sheet_kind(sh, cols, n_rows)
        results.append({
            "file": fpath.name,
            "sheet": sh,
            "kind": kind,
            "cols_sample": cols[:10],
            "rows": n_rows,
        })

    return results


# ──────────────────────────────────────────
# 신규 양식 알림 기록
# ──────────────────────────────────────────
def record_alert(unknowns: List[Dict]) -> None:
    if not unknowns:
        return

    ALERT_JSON.parent.mkdir(parents=True, exist_ok=True)

    existing = []
    if ALERT_JSON.exists():
        try:
            existing = json.loads(ALERT_JSON.read_text(encoding="utf-8"))
        except Exception:
            existing = []

    # 중복 제거 (파일+시트 기준)
    known_keys = {(r["file"], r["sheet"]) for r in existing}
    new_alerts = []
    for u in unknowns:
        key = (u["file"], u["sheet"])
        if key not in known_keys:
            u["timestamp"] = datetime.now().isoformat(timespec="seconds")
            new_alerts.append(u)
            known_keys.add(key)

    if new_alerts:
        existing.extend(new_alerts)
        ALERT_JSON.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n⚠️  신규 양식 {len(new_alerts)}건 → {ALERT_JSON}")
        for a in new_alerts:
            print(f"   {a['file']} / {a['sheet']} | 컬럼: {a['cols_sample']}")
        print(f"\n👉 레지스트리 업데이트 명령어:")
        for a in new_alerts:
            print(f"   python schema_registry.py --update-schema-registry \"data/raw/{a['file']}\"")


# ──────────────────────────────────────────
# 레지스트리 MD 자동 업데이트
# ──────────────────────────────────────────
def update_registry(fpath: Path) -> None:
    """
    파일을 분석해서 신규 양식이면 레지스트리 MD에 섹션 추가.
    """
    print(f"\n🔍 분석 중: {fpath.name}")
    results = scan_file(fpath)

    if not results:
        print("  → 읽을 수 있는 시트 없음")
        return

    # 현재 레지스트리 로드
    registry_text = REGISTRY_MD.read_text(encoding="utf-8") if REGISTRY_MD.exists() else ""

    # 이미 등록된 파일인지 확인
    if fpath.name in registry_text:
        print(f"  → 이미 레지스트리에 등록된 파일: {fpath.name}")
        return

    # 신규 섹션 생성
    now = datetime.now().strftime("%Y-%m-%d")
    kinds = list({r["kind"] for r in results if r["kind"] not in ["skip", "read_error"]})
    all_cols = []
    for r in results:
        all_cols.extend(r["cols_sample"])
    unique_cols = list(dict.fromkeys(all_cols))[:15]

    sheet_info = "\n".join(
        f"  - [{r['sheet']}] → {r['kind']} ({r['rows']}행, 컬럼: {r['cols_sample'][:5]})"
        for r in results
    )

    # TYPE 번호 자동 부여 (현재 마지막 번호 + 1)
    existing_types = re.findall(r"### TYPE (\d+):", registry_text)
    next_type = max((int(t) for t in existing_types), default=9) + 1

    new_section = f"""

---

### TYPE {next_type}: {', '.join(kinds) if kinds else 'unknown'} (신규 감지 - 검토 필요)

#### {next_type}-1. {fpath.stem} 형
```
대표 파일: {fpath.name}
시트 구성: {[r['sheet'] for r in results]}
핵심 컬럼: {unique_cols}
UK: (검토 필요)
민감정보: (검토 필요)
시트 판별:
{sheet_info}
처리: {', '.join(kinds) if kinds else 'unknown'} — 자동 감지, 내용 검토 후 확정 필요
추가일: {now}
```
"""

    # 버전 이력 업데이트
    version_match = re.search(r"\| v(\d+)\.(\d+) \|", registry_text)
    if version_match:
        major, minor = int(version_match.group(1)), int(version_match.group(2))
        new_version = f"v{major}.{minor + 1}"
    else:
        new_version = "v1.1"

    new_row = f"| {new_version} | {now} | 신규 양식 추가: {fpath.name} |\n"

    # MD 파일에 추가
    updated = registry_text
    # 버전 이력 테이블 마지막 행 뒤에 추가
    updated = re.sub(
        r"(\| v\d+\.\d+ \| .+\|)\n$",
        r"\1\n" + new_row,
        updated,
        flags=re.MULTILINE
    )
    # 새 섹션은 버전 이력 앞에 삽입
    updated = updated.replace("---\n\n## 버전 이력", new_section + "\n---\n\n## 버전 이력")

    REGISTRY_MD.write_text(updated, encoding="utf-8")
    print(f"  ✅ 레지스트리 업데이트 완료 → {REGISTRY_MD}")
    print(f"  📋 감지된 시트 성격:")
    for r in results:
        print(f"     [{r['sheet']}] → {r['kind']}")
    print(f"\n  ⚠️  자동 추가된 섹션을 직접 확인하고 UK/민감정보 항목을 채워주세요.")


# ──────────────────────────────────────────
# kind → 폴더명 매핑
# ──────────────────────────────────────────
_KIND_TO_FOLDER: Dict[str, str] = {
    "master":            "headcount",
    "reference_roster":  "roster",
    "aux_skill":         "skills",
    "aux_recruit":       "recruit",
    "aux_personalcard":  "evaluation",
    "aux_pricing":       "pricing",
    "sensitive_drop":    "sensitive",   # 이동은 하되 ingest 안 함
    "org_chart":         "org_chart",
    "unknown":           "unknown",
}

# 파일 단위로 대표 kind 결정 (시트 중 가장 중요한 것 기준)
_KIND_PRIORITY = [
    "master", "reference_roster", "aux_skill", "aux_recruit",
    "aux_pricing", "sensitive_drop", "aux_personalcard",
    "org_chart", "unknown",
]


def _file_kind(results: List[Dict], fname: str = "") -> str:
    """시트별 kind 목록에서 파일 전체 대표 kind 결정.
    모든 시트가 skip/read_error인 경우 파일명으로 폴백."""
    kinds = [r["kind"] for r in results if r["kind"] not in ("skip", "read_error")]
    if not kinds:
        # 파일명 폴백
        fn = fname.lower()
        if any(k in fn for k in ["인원현황", "재직자", "퇴사자", "입사자", "인력등급"]):
            return "master"
        if any(k in fn for k in ["명단", "roster"]):
            return "reference_roster"
        if any(k in fn for k in ["조직도", "orgchart"]):
            return "org_chart"
        if any(k in fn for k in ["스킬", "skill", "기술스택"]):
            return "aux_skill"
        if any(k in fn for k in ["채용", "recruit"]):
            return "aux_recruit"
        if any(k in fn for k in ["단가", "연봉", "투입", "실적", "simu", "급여", "인상"]):
            return "aux_pricing"
        if any(k in fn for k in ["인사기록카드", "개인이력카드", "평가", "등급"]):
            return "aux_personalcard"
        if any(k in fn for k in ["연락망", "주소록"]):
            return "sensitive_drop"
        return "unknown"
    for k in _KIND_PRIORITY:
        if k in kinds:
            return k
    return kinds[0]


# ──────────────────────────────────────────
# 파일 자동 분류 + 폴더 이동
# ──────────────────────────────────────────
def auto_sort_files(raw_dir: Path, dry_run: bool = False) -> None:
    """
    raw_dir 직속 엑셀 파일을 판별해서 하위 폴더로 자동 이동.
    - 이미 하위 폴더에 있는 파일은 건드리지 않음
    - dry_run=True 이면 이동 없이 예정 결과만 출력
    - 이동 로그: data/processed/bronze/sort_log.json

    폴더 구조:
      raw_dir/
        headcount/   ← master (인원현황/입퇴사)
        roster/      ← reference_roster (검증용 명단)
        skills/      ← aux_skill
        recruit/     ← aux_recruit
        evaluation/  ← aux_personalcard (평가/등급)
        pricing/     ← aux_pricing (단가/연봉/프로젝트)
        sensitive/   ← sensitive_drop (연락망 등 민감정보)
        org_chart/   ← 조직도
        unknown/     ← 판별 불가
    """
    raw_dir = Path(raw_dir)

    # raw_dir 직속 파일만 대상 (.xlsx/.xlsm)
    targets = sorted([
        p for p in raw_dir.iterdir()
        if p.is_file() and p.suffix.lower() in [".xlsx", ".xlsm", ".xls"]
    ])

    if not targets:
        print("  이동할 파일 없음 (raw_dir 직속 엑셀 파일이 없습니다)")
        return

    log_path = Path("data/processed/bronze/sort_log.json")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    existing_log = []
    if log_path.exists():
        try:
            existing_log = json.loads(log_path.read_text(encoding="utf-8"))
        except Exception:
            existing_log = []

    moved_log = []
    unknowns  = []

    print(f"\n{'DRY RUN — ' if dry_run else ''}📂 자동 분류 시작: {raw_dir}\n")
    print(f"  {'파일명':<45} {'판별 결과':<20} {'이동 위치'}")
    print(f"  {'-'*85}")

    for fpath in targets:
        # .xls는 판별만 하고 이동은 동일하게 처리
        if fpath.suffix.lower() == ".xls":
            results = []   # openpyxl 미지원 → 파일명 기반 폴백
            # 파일명으로 간단 판별
            fn = fpath.name.lower()
            if any(k in fn for k in ["인원현황", "재직자", "퇴사자", "입사자", "인력등급"]):
                file_kind = "master"
            elif any(k in fn for k in ["명단", "roster"]):
                file_kind = "reference_roster"
            elif any(k in fn for k in ["조직도", "orgchart"]):
                file_kind = "org_chart"
            elif any(k in fn for k in ["스킬", "skill", "기술스택"]):
                file_kind = "aux_skill"
            elif any(k in fn for k in ["채용", "recruit"]):
                file_kind = "aux_recruit"
            elif any(k in fn for k in ["단가", "연봉", "투입", "실적", "simu", "급여", "인상"]):
                file_kind = "aux_pricing"
            elif any(k in fn for k in ["인사기록카드", "개인이력카드", "평가"]):
                file_kind = "aux_personalcard"
            else:
                file_kind = "unknown"
        else:
            results   = scan_file(fpath)
            file_kind = _file_kind(results, fpath.name)

        folder_name = _KIND_TO_FOLDER.get(file_kind, "unknown")
        dest_dir    = raw_dir / folder_name
        dest_file   = dest_dir / fpath.name

        status_icon = {
            "master":           "✅",
            "reference_roster": "📋",
            "aux_skill":        "🔧",
            "aux_recruit":      "👤",
            "aux_personalcard": "🪪",
            "aux_pricing":      "💰",
            "sensitive_drop":   "🚫",
            "org_chart":        "🏢",
            "unknown":          "❓",
        }.get(file_kind, "❓")

        print(f"  {fpath.name:<45} {status_icon} {file_kind:<18} → {folder_name}/")

        if file_kind == "unknown":
            unknowns.append({
                "file": fpath.name,
                "sheet": "N/A",
                "kind": "unknown",
                "cols_sample": [],
                "rows": 0,
            })

        if not dry_run:
            dest_dir.mkdir(parents=True, exist_ok=True)
            # 동일 파일이 이미 있으면 덮어쓰지 않고 suffix 붙임
            if dest_file.exists():
                stem   = fpath.stem
                suffix = fpath.suffix
                dest_file = dest_dir / f"{stem}_dup{suffix}"

            try:
                import shutil
                dest_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(fpath), str(dest_file))
                try:
                    fpath.unlink()
                except PermissionError:
                    # 삭제 못하면 _MOVED_ 접두사로 이름 변경 → 나중에 일괄 삭제 가능
                    try:
                        marked = fpath.parent / f"_MOVED_{fpath.name}"
                        shutil.move(str(fpath), str(marked))
                        print(f"  📦 원본 → _MOVED_{fpath.name}")
                    except Exception:
                        print(f"  ⚠️  복사됨 but 원본 이름변경 실패 (수동삭제 필요): {fpath.name}")
                moved_log.append({
                    "file":        fpath.name,
                    "kind":        file_kind,
                    "dest_folder": folder_name,
                    "dest_path":   str(dest_file).replace("\\", "/"),
                    "timestamp":   datetime.now().isoformat(timespec="seconds"),
                })
            except PermissionError:
                print(f"  ⚠️  건너뜀 (파일 사용 중): {fpath.name}")
                continue

    print(f"\n  {'='*85}")
    if dry_run:
        print(f"  [DRY RUN] 실제 이동 없음. 위 결과 확인 후 --sort 로 실행하세요.")
    else:
        print(f"  ✅ {len(moved_log)}개 파일 이동 완료 → 로그: {log_path}")
        existing_log.extend(moved_log)
        log_path.write_text(json.dumps(existing_log, ensure_ascii=False, indent=2), encoding="utf-8")

    # 신규 양식 알림
    record_alert(unknowns)


# ──────────────────────────────────────────
# 전체 raw 폴더 스캔
# ──────────────────────────────────────────
def scan_raw_dir(raw_dir: Path) -> None:
    files = sorted([p for p in raw_dir.rglob("*") if p.suffix.lower() in [".xlsx", ".xlsm"]])
    print(f"\n📂 스캔 대상: {raw_dir} ({len(files)}개 파일)\n")

    all_results = []
    unknowns = []

    for fpath in files:
        results = scan_file(fpath)
        all_results.extend(results)
        for r in results:
            if r["kind"] == "unknown":
                unknowns.append(r)
            status = {
                "master": "✅ master",
                "reference_roster": "📋 검증용",
                "aux_skill": "🔧 스킬",
                "aux_recruit": "👤 채용",
                "aux_personalcard": "🪪 개인카드",
                "aux_pricing": "💰 단가",
                "sensitive_drop": "🚫 민감정보",
                "org_chart": "🏢 조직도",
                "skip": "⏭️  스킵",
                "unknown": "❓ 신규양식",
            }.get(r["kind"], r["kind"])
            print(f"  {status:20s} | {fpath.name[:40]:40s} | [{r['sheet']}]")

    print(f"\n{'='*70}")
    print(f"총 {len(files)}개 파일, {len(all_results)}개 시트 분석 완료")
    print(f"신규 양식 감지: {len(unknowns)}건")

    record_alert(unknowns)


# ──────────────────────────────────────────
# CLI
# ──────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="HR 양식 레지스트리 관리 & 파일 자동 분류",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
사용 예시:
  # 1) raw 폴더 파일 자동 분류 (실제 이동)
  python schema_registry.py --sort data/raw

  # 2) 이동 전 미리보기 (파일 건드리지 않음)
  python schema_registry.py --sort data/raw --dry-run

  # 3) 전체 스캔만 (분류 결과 확인)
  python schema_registry.py --scan data/raw

  # 4) 단일 파일 확인
  python schema_registry.py --check data/raw/새파일.xlsx

  # 5) 신규 양식 → 레지스트리 MD 업데이트
  python schema_registry.py --update-schema-registry data/raw/새파일.xlsx
        """
    )
    parser.add_argument("--sort", metavar="RAW_DIR",
                        help="raw 폴더 직속 파일을 판별해서 하위 폴더로 자동 이동")
    parser.add_argument("--dry-run", action="store_true",
                        help="--sort 와 함께 사용: 이동 없이 예정 결과만 출력")
    parser.add_argument("--scan", metavar="RAW_DIR",
                        help="raw 폴더 전체 스캔 (신규 양식 감지, 이동 없음)")
    parser.add_argument("--update-schema-registry", metavar="FILE",
                        help="특정 파일 분석 후 레지스트리 MD 업데이트")
    parser.add_argument("--cleanup-moved", metavar="RAW_DIR",
                        help="_MOVED_ 접두사 파일 일괄 삭제")
    args = parser.parse_args()

    if args.cleanup_moved:
        raw_dir = Path(args.cleanup_moved)
        targets = list(raw_dir.glob("_MOVED_*"))
        if not targets:
            print("삭제할 _MOVED_ 파일 없음")
        else:
            for f in targets:
                try:
                    f.unlink()
                    print(f"  🗑️  삭제: {f.name}")
                except Exception as e:
                    print(f"  ⚠️  삭제 실패: {f.name} ({e})")
            print(f"\n✅ {len(targets)}개 정리 완료")

    elif args.sort:
        auto_sort_files(Path(args.sort), dry_run=args.dry_run)

    elif args.scan:
        scan_raw_dir(Path(args.scan))

    elif args.update_schema_registry:
        update_registry(Path(args.update_schema_registry))

    elif args.check:
        fpath = Path(args.check)
        results = scan_file(fpath)
        print(f"\n파일: {fpath.name}")
        for r in results:
            print(f"  [{r['sheet']:30s}] → {r['kind']:20s} | {r['cols_sample'][:8]}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
