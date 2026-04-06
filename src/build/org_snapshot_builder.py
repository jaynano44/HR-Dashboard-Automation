# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
REF_DIR = Path("data/processed/reference")
REF_DIR.mkdir(parents=True, exist_ok=True)

MOVE_TO_ARCHIVE = False


def _norm(x):
    if pd.isna(x):
        return ""
    s = str(x).replace("\n", " ").replace("\r", " ").strip()
    return " ".join(s.split())


def _find_col(columns, keywords):
    for c in columns:
        txt = str(c)
        if any(k in txt for k in keywords):
            return c
    return None


def _load_with_header_guess(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, header=None)
    raw = raw.ffill(axis=0)
    raw = raw.ffill(axis=1)
    raw = raw.infer_objects(copy=False)
    pd.set_option('future.no_silent_downcasting', True)

    best_idx = 0
    best_score = -1
    for i in range(min(10, len(raw))):
        vals = raw.iloc[i].astype(str).tolist()
        score = sum(
            any(k in v for k in ["이름", "성명", "본부", "소속", "조직", "부서", "팀"])
            for v in vals
        )
        if score > best_score:
            best_score = score
            best_idx = i

    header = [str(x).strip() for x in raw.iloc[best_idx].tolist()]
    body = raw.iloc[best_idx + 1 :].copy()
    body.columns = header
    body = body.ffill(axis=0)
    return body


def build_org_snapshot():
    files = list(RAW_DIR.glob("*.xlsx"))
    org_frames = []

    print("\n===== [1] 조직 스냅샷 생성 =====")

    for f in files:
        fname = f.name

        if not any(k in fname for k in ["조직도", "연락망", "master"]):
            continue

        try:
            df = _load_with_header_guess(f)
            cols = list(df.columns)

            name_col = _find_col(cols, ["이름", "성명", "사원명", "직원명"])
            org_col = _find_col(cols, ["본부", "소속", "조직", "사업부", "센터"])
            dept_col = _find_col(cols, ["부서", "팀", "그룹", "파트"])

            if org_col is None and "master" in fname.lower():
                org_col = cols[2] if len(cols) > 2 else None
            if dept_col is None and "master" in fname.lower():
                dept_col = cols[3] if len(cols) > 3 else None

            if name_col is None or org_col is None:
                print(f"⏭ skip (name/org 없음): {fname}")
                continue

            temp = pd.DataFrame()
            temp["name"] = df[name_col].map(_norm)
            # 조직 후보 컬럼 찾기
            org_candidates = []

            for c in cols:
                txt = str(c)
                if any(k in txt for k in ["본부", "소속", "조직", "사업부", "센터", "부서", "팀", "그룹", "파트"]):
                    org_candidates.append(c)

            # 중복 제거 + 순서 유지
            seen = set()
            org_candidates = [x for x in org_candidates if not (x in seen or seen.add(x))]

            # org_level 자동 생성
            if not org_candidates:
                print(f"⚠ 조직 컬럼 없음 → fallback 적용: {fname}")

                if org_col:
                    temp["org_level_1"] = df[org_col].map(_norm)
                else:
                    temp["org_level_1"] = "UNKNOWN"

                if dept_col:
                    temp["org_level_2"] = df[dept_col].map(_norm)
                else:
                    temp["org_level_2"] = ""

            else:
                for i, col in enumerate(org_candidates):
                    temp[f"org_level_{i+1}"] = df[col].map(_norm)

            # org_path 생성 (동적)
            org_cols = [c for c in temp.columns if c.startswith("org_level_")]

            temp["org_path"] = temp[org_cols].apply(
                lambda x: " > ".join([v for v in x if v]), axis=1
            )

            # 호환용 컬럼 (안전하게)
            temp["org"] = temp[org_cols[0]] if len(org_cols) > 0 else ""
            temp["dept"] = temp[org_cols[1]] if len(org_cols) > 1 else ""

            temp = temp[~temp["name"].isin(["", "nan", "None"])]
            temp = temp[~temp["org"].isin(["", "nan", "None"])]
            temp = temp.drop_duplicates(subset=["name"], keep="first")

            if temp.empty:
                print(f"⏭ skip (empty): {fname}")
                continue

            org_frames.append(temp)
            print(f"✔ snapshot source: {fname} ({len(temp)})")

        except Exception as e:
            print(f"❌ {fname}: {e}")

    if not org_frames:
        print("❌ 조직 스냅샷 없음")
        return

    result = pd.concat(org_frames, ignore_index=True)
    result = result.drop_duplicates(subset=["name"], keep="first")
    result.to_csv(REF_DIR / "org_snapshot.csv", index=False, encoding="utf-8-sig")
    print("✅ 조직 스냅샷 생성 완료")

    return result 

    if MOVE_TO_ARCHIVE:
        archive_dir = RAW_DIR / "archive"
        archive_dir.mkdir(exist_ok=True)

        print("\n===== [2] raw → archive 이동 =====")

        for f in RAW_DIR.glob("*.xlsx"):
            try:
                target = archive_dir / f.name
                if target.exists():
                    target.unlink()
                f.rename(target)
                print(f"📦 이동: {f.name}")
            except Exception as e:
                print(f"❌ 이동 실패: {f.name} / {e}")

        print("\n===== 완료 =====")


if __name__ == "__main__":
    build_org_snapshot()
    