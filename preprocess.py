"""
preprocess.py  — 프로젝트 루트 (main.py 옆)
============================================
Streamlit 없이 독립 실행되는 전처리 CLI.
신규/변경 파일만 증분 처리.

사용법:
  python preprocess.py               # 신규/변경 파일만 처리 (기본)
  python preprocess.py --force       # 전체 강제 재처리
  python preprocess.py --check       # 신규 파일 유무만 확인

일반 사용 흐름:
  1) python preprocess.py       ← 새 파일 추가됐을 때만 실행
  2) streamlit run main.py      ← CSV 즉시 로드, 바로 뜸
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import yaml


def _load_config(path: str = "config.yaml") -> dict:
    p = Path(path)
    if not p.exists():
        print(f"[ERROR] config not found: {p}")
        sys.exit(1)
    return yaml.safe_load(p.read_text(encoding="utf-8"))


def _cache_exists(processed_dir: Path) -> bool:
    return (processed_dir / "master_auto.csv").exists()


def _detect_new_files(raw_dir: Path, processed_dir: Path) -> list[str]:
    from src.processor.ingest_manifest import (
        file_fingerprint,
        is_new_or_changed,
        load_manifest,
    )
    manifest_path = processed_dir / "ingest_manifest.json"
    manifest = load_manifest(manifest_path)

    new_files = []
    for fpath in sorted(raw_dir.glob("*.xls*")):
        if fpath.name.startswith("~$"):
            continue
        try:
            fp = file_fingerprint(fpath)
            if is_new_or_changed(fp, manifest):
                new_files.append(fpath.name)
        except Exception:
            new_files.append(fpath.name)
    return new_files


def _save_career_rank_csv(raw_dir: Path, processed_dir: Path) -> None:
    """경력연차 엑셀 → processed/career_rank.csv 저장"""
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).parent))
        from src.processor.career_rank_loader import load_career_rank_files
        df = load_career_rank_files(raw_dir)
        if not df.empty:
            out = processed_dir / "career_rank.csv"
            df.to_csv(out, index=False, encoding="utf-8-sig")
            print(f"  📊 career_rank.csv      : {len(df)}명 저장")
        else:
            print("  ⚠️  경력연차 파일 없음 또는 파싱 실패")
    except Exception as e:
        print(f"  ⚠️  경력연차 처리 실패: {e}")


def _save_headcount_csv(config_path: str, processed_dir: Path) -> None:
    """headcount 최신파일 → processed/headcount_active.csv, headcount_exited.csv 저장"""
    import pandas as pd
    import yaml
    cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    hc_file = (cfg.get("files") or {}).get("headcount_2024_xlsx", "")
    col_map = (cfg.get("columns") or {}).get("headcount_2024", {})
    raw_dir = Path(cfg["paths"]["raw_dir"])

    # 경로 탐색
    candidates = [
        raw_dir / hc_file,
        raw_dir / "headcount" / hc_file,
    ]
    hc_dir = raw_dir / "headcount"
    if hc_dir.exists():
        # 가장 최신 파일 우선
        candidates += sorted(hc_dir.glob("*.xlsx"), reverse=True)
        candidates += sorted(hc_dir.glob("*.xls"), reverse=True)

    hc_path = next((Path(c) for c in candidates if Path(c).exists()), None)
    if not hc_path:
        print(f"  ⚠️  headcount 파일 없음 (탐색 경로: {[str(c) for c in candidates[:3]]})")
        return

    try:
        from src.processor.headcount_2024_loader import load_headcount_2024
        hc = load_headcount_2024(hc_path)
        already = set()

        def _std(df):
            if df is None or df.empty: return pd.DataFrame()
            d = df.copy()
            for std, kr in col_map.items():
                if not kr or not std or kr in already: continue
                if kr in d.columns and std not in d.columns:
                    if std in ("join_date", "exit_date"):
                        d[std] = pd.to_datetime(d[kr], errors="coerce")
                    else:
                        d[std] = d[kr].astype(str).str.strip()
                    already.add(kr)
            d = d.drop(columns=[k for k in already if k in d.columns], errors="ignore")
            if "name" in d.columns:
                d = d[d["name"].notna() & ~d["name"].isin(["nan","None",""])]
            return d.reset_index(drop=True)

        active = _std(hc.get("active"))
        exited = _std(hc.get("exited"))

        if not active.empty:
            active.to_csv(processed_dir / "headcount_active.csv", index=False, encoding="utf-8-sig")
            print(f"  👥 headcount_active.csv  : {len(active)}명 저장")
        if not exited.empty:
            exited.to_csv(processed_dir / "headcount_exited.csv", index=False, encoding="utf-8-sig")
            print(f"  🚪 headcount_exited.csv  : {len(exited)}명 저장")
        if active.empty:
            print(f"  ⚠️  headcount active 시트 인식 실패 — 시트명: {getattr(hc, 'keys', lambda: [])()}")
    except Exception as e:
        print(f"  ⚠️  headcount 저장 실패: {e}")


def run_preprocess(config_path: str = "config.yaml") -> dict:
    from src.processor.build_dataset import build_datasets

    print("=" * 60)
    print("  HR 데이터 전처리")
    print("=" * 60)

    t0 = time.time()
    result = build_datasets(config_path)
    elapsed = round(time.time() - t0, 1)

    rep = result.ingest_report or {}
    master_rows = len(result.master_auto) if result.master_auto is not None else 0

    # headcount CSV 저장
    cfg = _load_config(config_path)
    processed_dir = Path(cfg["paths"]["processed_dir"])
    raw_dir = Path(cfg["paths"]["raw_dir"])
    _save_headcount_csv(config_path, processed_dir)

    # 경력연차 파일 처리
    _save_career_rank_csv(raw_dir, processed_dir)

    print(f"\n  ✅ 완료  {elapsed}초")
    print(f"  📄 처리 파일  : {rep.get('read_ok', 0)}개  (실패 {rep.get('read_fail', 0)}개)")
    print(f"  👥 master rows: {master_rows}")

    skips = rep.get("timeout_skips", [])
    if skips:
        print(f"  ⚠️  타임아웃 스킵 {len(skips)}건:")
        for s in skips:
            print(f"      - {s}")

    print("\n  이제 앱을 실행하세요:  streamlit run main.py")
    print("=" * 60)

    return {"elapsed_sec": elapsed, "master_rows": master_rows,
            "read_ok": rep.get("read_ok", 0), "read_fail": rep.get("read_fail", 0)}


def main():
    parser = argparse.ArgumentParser(description="HR 데이터 전처리 CLI")
    parser.add_argument("--force",  action="store_true", help="전체 강제 재처리")
    parser.add_argument("--check",  action="store_true", help="신규 파일 유무만 확인")
    parser.add_argument("--config", default="config.yaml", help="config 경로")
    args = parser.parse_args()

    cfg = _load_config(args.config)
    raw_dir       = Path(cfg["paths"]["raw_dir"])
    processed_dir = Path(cfg["paths"]["processed_dir"])

    if not raw_dir.exists():
        print(f"[ERROR] raw_dir 없음: {raw_dir}")
        sys.exit(1)

    if args.check:
        new_files = _detect_new_files(raw_dir, processed_dir)
        cache_ok  = _cache_exists(processed_dir)
        print(f"캐시 CSV : {'✅ 있음' if cache_ok else '❌ 없음'}")
        if new_files:
            print(f"신규/변경 파일 {len(new_files)}개:")
            for f in new_files:
                print(f"  + {f}")
            print("\n→ 전처리 필요:  python preprocess.py")
        else:
            print("신규/변경 파일 없음. 전처리 불필요.")
        return

    if args.force:
        print("강제 재처리 모드 (--force)\n")
        run_preprocess(args.config)
        return

    # 기본: 캐시 없거나 신규 파일 있을 때만 실행
    if not _cache_exists(processed_dir):
        print("캐시 CSV 없음 → 최초 전처리 실행\n")
        run_preprocess(args.config)
        return

    new_files = _detect_new_files(raw_dir, processed_dir)
    if new_files:
        print(f"신규/변경 파일 {len(new_files)}개 감지:\n")
        for f in new_files:
            print(f"  + {f}")
        print()
        run_preprocess(args.config)
        return

    print("✅ 신규 파일 없음. 캐시 최신 상태.")
    print("   바로 실행:  streamlit run main.py")


if __name__ == "__main__":
    main()
