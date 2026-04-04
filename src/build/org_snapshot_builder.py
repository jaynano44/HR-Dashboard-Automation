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
    raw = raw.ffill(axis=0).ffill(axis=1)

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
            temp["org"] = df[org_col].map(_norm)
            temp["dept"] = df[dept_col].map(_norm) if dept_col else ""

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