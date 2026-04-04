from __future__ import annotations

"""
contact_org_parser.py
---------------------
연락망/조직표형 Excel을 파싱해서 조직 계층 + 인물 정보를 표준 스키마로 반환한다.

핵심 아이디어
- 병합셀 값을 먼저 복원한다.
- 빈칸을 상하/좌우 forward fill 해서 상위 조직명을 살린다.
- 헤더 후보(부서/성명/직책/내선/휴대폰/e-mail)를 탐지한다.
- 좌우 2단 배치 같은 wide sheet는 "블록"으로 분리해서 각 블록을 독립 파싱한다.
- 최종 컬럼을 org_level_1, org_level_2, org_level_3, org, dept, name, title, phone, email 로 표준화한다.

주의
- 연락망 양식은 회사마다 편차가 크므로 100% 완전 자동보다 "안전한 추출 + 검토 가능"을 목표로 한다.
- 병합셀/안내문/합계행이 많아도 최대한 조직 단서를 보존하도록 설계했다.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
from openpyxl import load_workbook


# ---------------------------------------------------------------------------
# 공통 시그널
# ---------------------------------------------------------------------------
NAME_KEYS = ["성명", "이름", "사원명", "직원명", "name"]
DEPT_KEYS = ["부서", "팀", "조직", "소속", "사업부", "사업본부", "본부"]
TITLE_KEYS = ["직책", "직급", "직위", "호칭", "title"]
PHONE_KEYS = ["휴대폰", "핸드폰", "전화", "연락처", "mobile", "phone"]
EMAIL_KEYS = ["e-mail", "email", "메일", "이메일"]
EXT_KEYS = ["내선", "내선번호", "extension"]
ORG_LIKE_KEYS = ["본부", "사업부", "사업본부", "센터", "실", "그룹", "랩", "lab", "팀", "파트"]
SKIP_TEXT = ["총원", "합계", "설명", "비고", "직원연락망", "연락망"]


@dataclass
class ParseBlockResult:
    sheet_name: str
    block_index: int
    rows: pd.DataFrame


def _clean_text(x: object) -> str:
    if x is None:
        return ""
    s = str(x).replace("\n", " ").replace("\r", " ").strip()
    return " ".join(s.split())


def _norm_key(x: object) -> str:
    return _clean_text(x).lower().replace(" ", "")


def _contains_any(text: object, keys: Iterable[str]) -> bool:
    t = _norm_key(text)
    return any(_norm_key(k) in t or t in _norm_key(k) for k in keys if _norm_key(k))


def _is_empty_val(x: object) -> bool:
    t = _clean_text(x)
    return t in {"", "nan", "none", "<na>"}


def _looks_like_person_name(x: object) -> bool:
    t = _clean_text(x)
    if not t or len(t) < 2 or len(t) > 12:
        return False
    if any(ch.isdigit() for ch in t):
        return False
    if _contains_any(t, SKIP_TEXT + DEPT_KEYS + TITLE_KEYS + PHONE_KEYS + EMAIL_KEYS + EXT_KEYS):
        return False
    return True


def _looks_like_email(x: object) -> bool:
    t = _clean_text(x)
    return "@" in t and "." in t


def _looks_like_phone(x: object) -> bool:
    t = _clean_text(x).replace("-", "").replace(" ", "")
    digits = "".join(ch for ch in t if ch.isdigit())
    return len(digits) >= 7


def _looks_like_org_name(x: object) -> bool:
    t = _clean_text(x)
    if not t or _looks_like_person_name(t):
        return False
    return _contains_any(t, ORG_LIKE_KEYS)


# ---------------------------------------------------------------------------
# Excel 읽기: 병합셀 복원 + wide sheet를 pandas로 변환
# ---------------------------------------------------------------------------
def _worksheet_to_df_merged_values(ws) -> pd.DataFrame:
    """병합 영역 값을 모두 복원한 뒤 DataFrame으로 변환."""
    data = [[cell for cell in row] for row in ws.iter_rows(values_only=True)]
    if not data:
        return pd.DataFrame()

    # 1-indexed 보정
    for merged in list(ws.merged_cells.ranges):
        min_row, min_col, max_row, max_col = merged.min_row, merged.min_col, merged.max_row, merged.max_col
        val = data[min_row - 1][min_col - 1]
        for r in range(min_row - 1, max_row):
            for c in range(min_col - 1, max_col):
                if data[r][c] is None:
                    data[r][c] = val

    # 열 길이 보정
    width = max(len(r) for r in data)
    data = [list(r) + [None] * (width - len(r)) for r in data]
    return pd.DataFrame(data)


def _split_wide_blocks(raw: pd.DataFrame, min_nonempty_cols: int = 2) -> List[pd.DataFrame]:
    """좌우 2단/3단 배치 시 빈 열을 기준으로 블록 분리."""
    if raw is None or raw.empty:
        return []

    cols_nonempty = [int(raw.iloc[:, i].notna().sum()) for i in range(raw.shape[1])]
    is_empty_col = [cnt == 0 for cnt in cols_nonempty]

    blocks: List[tuple[int, int]] = []
    start = None
    for i, empty in enumerate(is_empty_col):
        if not empty and start is None:
            start = i
        elif empty and start is not None:
            if i - start >= min_nonempty_cols:
                blocks.append((start, i))
            start = None
    if start is not None and raw.shape[1] - start >= min_nonempty_cols:
        blocks.append((start, raw.shape[1]))

    # 블록 감지 실패 시 전체 1블록
    if not blocks:
        blocks = [(0, raw.shape[1])]

    out = []
    for s, e in blocks:
        b = raw.iloc[:, s:e].copy()
        b = b.dropna(how="all").dropna(axis=1, how="all")
        if not b.empty:
            out.append(b.reset_index(drop=True))
    return out


# ---------------------------------------------------------------------------
# 헤더 탐지 및 구조 복원
# ---------------------------------------------------------------------------
def _find_header_row(block: pd.DataFrame, max_scan: int = 20) -> Optional[int]:
    scan_n = min(max_scan, len(block))
    best_idx = None
    best_score = -1
    for i in range(scan_n):
        row = block.iloc[i].tolist()
        score = 0
        if any(_contains_any(x, NAME_KEYS) for x in row):
            score += 3
        if any(_contains_any(x, DEPT_KEYS) for x in row):
            score += 2
        if any(_contains_any(x, TITLE_KEYS) for x in row):
            score += 1
        if any(_contains_any(x, PHONE_KEYS) for x in row):
            score += 1
        if any(_contains_any(x, EMAIL_KEYS) for x in row):
            score += 1
        if score > best_score:
            best_score = score
            best_idx = i
    return best_idx if best_score > 0 else None


def _make_unique_columns(row: List[object]) -> List[str]:
    cols = []
    seen = {}
    for i, v in enumerate(row):
        name = _clean_text(v) or f"col_{i}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        cols.append(name)
    return cols


def _pick_col(cols: List[str], keys: Iterable[str]) -> Optional[str]:
    for c in cols:
        if _contains_any(c, keys):
            return c
    return None


def _rowwise_org_fill(df: pd.DataFrame) -> pd.DataFrame:
    """병합셀 해제 후에도 남는 조직 단서를 좌→우, 상→하로 보강."""
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()
    d = d.ffill(axis=0)
    d = d.ffill(axis=1)
    return d


def _extract_rows_from_block(block: pd.DataFrame, sheet_name: str, block_index: int) -> ParseBlockResult:
    header_idx = _find_header_row(block)

    # 1) 정형 테이블인 경우
    if header_idx is not None:
        work = block.copy()
        work = _rowwise_org_fill(work)
        cols = _make_unique_columns(work.iloc[header_idx].tolist())
        body = work.iloc[header_idx + 1 :].copy().reset_index(drop=True)
        body.columns = cols
        body = body.dropna(how="all")

        name_col = _pick_col(cols, NAME_KEYS)
        dept_col = _pick_col(cols, DEPT_KEYS)
        title_col = _pick_col(cols, TITLE_KEYS)
        phone_col = _pick_col(cols, PHONE_KEYS)
        email_col = _pick_col(cols, EMAIL_KEYS)
        ext_col = _pick_col(cols, EXT_KEYS)

        rows = []
        for _, r in body.iterrows():
            name = _clean_text(r.get(name_col)) if name_col else ""
            dept_raw = _clean_text(r.get(dept_col)) if dept_col else ""
            title = _clean_text(r.get(title_col)) if title_col else ""
            phone = _clean_text(r.get(phone_col)) if phone_col else ""
            email = _clean_text(r.get(email_col)) if email_col else ""
            ext = _clean_text(r.get(ext_col)) if ext_col else ""

            if not _looks_like_person_name(name):
                continue

            org_levels = _split_org_path_from_text(dept_raw)
            row = {
                "org_level_1": org_levels[0],
                "org_level_2": org_levels[1],
                "org_level_3": org_levels[2],
                "org": org_levels[0] or org_levels[1] or org_levels[2],
                "dept": org_levels[2] or org_levels[1] or dept_raw,
                "name": name,
                "title": title,
                "phone": phone or ext,
                "email": email,
                "source_sheet": sheet_name,
                "source_block": block_index,
                "parse_mode": "tabular_contact",
            }
            rows.append(row)

        return ParseBlockResult(sheet_name, block_index, pd.DataFrame(rows))

    # 2) 비정형 연락망인 경우: 행 안의 조직명/이름/연락처를 휴리스틱으로 추출
    work = _rowwise_org_fill(block)
    rows = []
    current_l1 = ""
    current_l2 = ""
    current_l3 = ""

    for _, rr in work.iterrows():
        vals = [_clean_text(x) for x in rr.tolist()]
        vals = [v for v in vals if v]
        if not vals:
            continue

        # 조직 단서 수집
        org_candidates = [v for v in vals if _looks_like_org_name(v)]
        name_candidates = [v for v in vals if _looks_like_person_name(v)]
        title_candidates = [v for v in vals if _contains_any(v, TITLE_KEYS) or v.endswith("님") or v.endswith("장")]
        phone_candidates = [v for v in vals if _looks_like_phone(v)]
        email_candidates = [v for v in vals if _looks_like_email(v)]

        if org_candidates and not name_candidates:
            # 조직 헤더 행으로 판단
            current_l1, current_l2, current_l3 = _merge_org_context(current_l1, current_l2, current_l3, org_candidates)
            continue

        if not name_candidates:
            continue

        # 첫 이름 기준으로 1인 1행 추출
        name = name_candidates[0]
        title = title_candidates[0] if title_candidates else ""
        phone = phone_candidates[0] if phone_candidates else ""
        email = email_candidates[0] if email_candidates else ""

        # 행 안에 조직 단서가 있으면 current context 갱신
        if org_candidates:
            current_l1, current_l2, current_l3 = _merge_org_context(current_l1, current_l2, current_l3, org_candidates)

        rows.append(
            {
                "org_level_1": current_l1,
                "org_level_2": current_l2,
                "org_level_3": current_l3,
                "org": current_l1 or current_l2 or current_l3,
                "dept": current_l3 or current_l2,
                "name": name,
                "title": title,
                "phone": phone,
                "email": email,
                "source_sheet": sheet_name,
                "source_block": block_index,
                "parse_mode": "heuristic_contact",
            }
        )

    return ParseBlockResult(sheet_name, block_index, pd.DataFrame(rows))


def _split_org_path_from_text(text: str) -> tuple[str, str, str]:
    t = _clean_text(text)
    if not t:
        return "", "", ""
    parts = [p.strip() for p in t.replace(">", "/").replace("|", "/").split("/") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return parts[0], parts[1], parts[1]
    # 단일 값이면 성격 추정
    if any(k in t for k in ["본부", "사업부", "사업본부"]):
        return t, "", ""
    if any(k in t for k in ["그룹", "센터", "실"]):
        return "", t, ""
    return "", "", t


def _merge_org_context(cur1: str, cur2: str, cur3: str, org_candidates: List[str]) -> tuple[str, str, str]:
    l1, l2, l3 = cur1, cur2, cur3
    for org in org_candidates:
        o = _clean_text(org)
        if any(k in o for k in ["본부", "사업부", "사업본부"]):
            l1 = o
            l2 = ""
            l3 = ""
        elif any(k in o for k in ["그룹", "센터", "실"]):
            l2 = o
            l3 = ""
        elif any(k in o for k in ["팀", "파트", "lab", "랩"]):
            l3 = o
    return l1, l2, l3


# ---------------------------------------------------------------------------
# 외부 호출 함수
# ---------------------------------------------------------------------------
def parse_contact_org_xlsx(path: str | Path, sheet_names: Optional[List[str]] = None) -> pd.DataFrame:
    """연락망형 Excel을 읽어 조직 계층 + 인물 표준 DataFrame 반환."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)

    wb = load_workbook(p, data_only=True)
    targets = sheet_names or wb.sheetnames
    results = []

    for sh in targets:
        ws = wb[sh]
        raw = _worksheet_to_df_merged_values(ws)
        if raw.empty:
            continue
        blocks = _split_wide_blocks(raw)
        for bi, block in enumerate(blocks, start=1):
            parsed = _extract_rows_from_block(block, sh, bi).rows
            if parsed is not None and not parsed.empty:
                parsed["source_file"] = p.name
                results.append(parsed)

    if not results:
        return pd.DataFrame(
            columns=[
                "org_level_1", "org_level_2", "org_level_3",
                "org", "dept", "name", "title", "phone", "email",
                "source_file", "source_sheet", "source_block", "parse_mode",
            ]
        )

    out = pd.concat(results, ignore_index=True)
    out = out.drop_duplicates(subset=["org_level_1", "org_level_2", "org_level_3", "name", "title", "phone", "email"])
    return out.reset_index(drop=True)


def save_contact_org_csv(path: str | Path, out_csv: str | Path) -> pd.DataFrame:
    df = parse_contact_org_xlsx(path)
    out = Path(out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    return df


if __name__ == "__main__":
    sample = Path("data/raw/아이엔소프트_직원연락망 (23.06.01).xlsx")
    if sample.exists():
        df = parse_contact_org_xlsx(sample)
        print(df.head(20).to_string())
        print(f"rows={len(df)}")
    else:
        print("sample file not found")
