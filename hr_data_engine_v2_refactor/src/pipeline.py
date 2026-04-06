from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from src.ingest.ingest_v2 import read_all_records
from src.preprocess.normalizer_v2 import normalize_records
from src.preprocess.contact_org_parser import parse_contact_org_xlsx
from src.core.entity_resolver import resolve_entities
from src.build.org_snapshot_builder import build_org_snapshot
from src.build.metrics import build_metrics


def load_config(config_path: str | Path = "config.yaml") -> dict:
    p = Path(config_path)
    if not p.exists():
        return {
            "paths": {
                "raw_dir": "data/raw",
                "bronze_dir": "data/bronze",
                "silver_dir": "data/silver",
                "gold_dir": "data/gold",
            }
        }
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dirs(cfg: dict) -> dict[str, Path]:
    paths = cfg.get("paths", {})
    bronze_dir = Path(paths.get("bronze_dir", "data/bronze"))
    silver_dir = Path(paths.get("silver_dir", "data/silver"))
    gold_dir = Path(paths.get("gold_dir", "data/gold"))

    for p in [bronze_dir, silver_dir, gold_dir]:
        p.mkdir(parents=True, exist_ok=True)

    return {
        "raw_dir": Path(paths.get("raw_dir", "data/raw")),
        "bronze_dir": bronze_dir,
        "silver_dir": silver_dir,
        "gold_dir": gold_dir,
    }


def run_pipeline(config_path: str | Path = "config.yaml") -> None:
    cfg = load_config(config_path)
    paths = ensure_dirs(cfg)

    print("===== STEP1: INGEST =====")
    raw_df, manifest_df = read_all_records(paths["raw_dir"])
    manifest_path = paths["bronze_dir"] / "ingest_manifest.csv"
    manifest_df.to_csv(manifest_path, index=False, encoding="utf-8-sig")
    print(f"manifest 저장: {manifest_path}")

    if raw_df.empty:
        print("❌ 읽은 데이터가 없습니다.")
        return

    raw_path = paths["bronze_dir"] / "master_raw.csv"
    raw_df.to_csv(raw_path, index=False, encoding="utf-8-sig")
    print(f"bronze 저장: {raw_path} / rows={len(raw_df)}")

    print("===== STEP2: NORMALIZE =====")
    master_df = raw_df[raw_df["file_kind"].isin(["master", "reference_roster", "unknown"])].copy() if "file_kind" in raw_df.columns else raw_df.copy()
    norm_df = normalize_records(master_df)
    norm_path = paths["silver_dir"] / "master_clean.csv"
    norm_df.to_csv(norm_path, index=False, encoding="utf-8-sig")
    print(f"silver 저장: {norm_path} / rows={len(norm_df)}")

    review_df = norm_df[norm_df.get("needs_review", False) == True].copy() if "needs_review" in norm_df.columns else pd.DataFrame()
    if not review_df.empty:
        review_path = paths["bronze_dir"] / "review_queue.csv"
        review_df.to_csv(review_path, index=False, encoding="utf-8-sig")
        print(f"review queue 저장: {review_path} / rows={len(review_df)}")

    print("===== STEP3: CONTACT ENRICH =====")
    contact_df = _load_contact_data(paths["raw_dir"])
    if contact_df.empty:
        print("연락망 파일 없음 또는 파싱 결과 없음")
        enriched_df = norm_df.copy()
    else:
        contact_path = paths["silver_dir"] / "contact_parsed.csv"
        contact_df.to_csv(contact_path, index=False, encoding="utf-8-sig")
        print(f"contact 저장: {contact_path} / rows={len(contact_df)}")
        enriched_df = _merge_contact(norm_df, contact_df)

    enriched_path = paths["silver_dir"] / "master_enriched.csv"
    enriched_df.to_csv(enriched_path, index=False, encoding="utf-8-sig")
    print(f"enriched 저장: {enriched_path} / rows={len(enriched_df)}")

    print("===== STEP4: ENTITY RESOLUTION =====")
    result = resolve_entities(enriched_df)
    people_path = paths["silver_dir"] / "people_master.csv"
    hist_path = paths["silver_dir"] / "employment_history.csv"
    log_path = paths["silver_dir"] / "match_log.csv"
    result.employee_master.to_csv(people_path, index=False, encoding="utf-8-sig")
    result.employment_history.to_csv(hist_path, index=False, encoding="utf-8-sig")
    result.match_log.to_csv(log_path, index=False, encoding="utf-8-sig")
    print(f"people 저장: {people_path} / rows={len(result.employee_master)}")
    print(f"history 저장: {hist_path} / rows={len(result.employment_history)}")

    print("===== STEP5: ORG SNAPSHOT =====")
    snapshot_df = build_org_snapshot(
        history_df=result.employment_history,
        people_df=result.employee_master,
        as_of_date=pd.Timestamp.today().normalize(),
    )
    snapshot_path = paths["gold_dir"] / "org_snapshot.csv"
    snapshot_df.to_csv(snapshot_path, index=False, encoding="utf-8-sig")
    print(f"snapshot 저장: {snapshot_path} / rows={len(snapshot_df)}")

    print("===== STEP6: METRICS =====")
    metrics = build_metrics(history_df=result.employment_history, snapshot_df=snapshot_df)
    metrics.yearly.to_csv(paths["gold_dir"] / "metrics_yearly.csv", index=False, encoding="utf-8-sig")
    metrics.org_level.to_csv(paths["gold_dir"] / "metrics_org_level.csv", index=False, encoding="utf-8-sig")
    metrics.headcount.to_csv(paths["gold_dir"] / "metrics_headcount.csv", index=False, encoding="utf-8-sig")
    print(f"metrics 저장 완료: {paths['gold_dir']}")
    print(f"사람 수: {result.employee_master['emp_uid'].nunique() if not result.employee_master.empty else 0}")


def _load_contact_data(raw_dir: Path) -> pd.DataFrame:
    candidates = sorted([p for p in raw_dir.rglob("*") if p.is_file() and "연락망" in p.name and p.suffix.lower() in {".xlsx", ".xlsm"}])
    frames: list[pd.DataFrame] = []
    for path in candidates:
        try:
            parsed = parse_contact_org_xlsx(path)
            if parsed is not None and not parsed.empty:
                frames.append(parsed)
        except Exception as e:
            print(f"⚠ contact parse 실패: {path.name} / {e}")
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _merge_contact(master_df: pd.DataFrame, contact_df: pd.DataFrame) -> pd.DataFrame:
    right_cols = [c for c in ["name", "org", "dept", "phone", "email"] if c in contact_df.columns]
    if "name" not in right_cols:
        return master_df.copy()

    merged = master_df.merge(
        contact_df[right_cols].drop_duplicates(subset=["name"], keep="first"),
        on="name",
        how="left",
        suffixes=("", "_contact"),
    )

    for base_col in ["org", "dept", "phone", "email"]:
        contact_col = f"{base_col}_contact"
        if base_col in merged.columns and contact_col in merged.columns:
            merged[base_col] = merged[contact_col].combine_first(merged[base_col])

    return merged


if __name__ == "__main__":
    run_pipeline()
