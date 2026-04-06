import yaml
import pandas as pd

from src.ingest.ingest_v2 import read_all_excels

# 🔥 여기만 핵심 수정 (파일명 맞춰서)
from src.preprocess.normalizer import normalize_records

from src.preprocess.contact_org_parser import parse_contact_org_xlsx


def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_pipeline():
    cfg = load_config()

    # ============================================================
    # STEP1: INGEST
    # ============================================================
    print("===== STEP1: INGEST =====")

    df_all = read_all_excels(cfg["paths"]["raw_dir"])

    if df_all is None or df_all.empty:
        print("❌ 데이터 없음")
        return

    print(f"총 행 수: {len(df_all)}")

    df_all.to_csv("data/bronze/master_raw.csv", index=False, encoding="utf-8-sig")
    print("✅ 저장 완료 → data/bronze/master_raw.csv")

    # ============================================================
    # STEP1-1: MASTER 필터
    # ============================================================
    df_master = df_all[
        df_all["source_file"].str.contains("master", case=False, na=False)
    ].copy()

    if df_master.empty:
        print("⚠️ master 데이터 없음 → 전체 데이터로 진행")
        df_master = df_all.copy()

    # ============================================================
    # STEP2: NORMALIZE (🔥 기준 변경 완료)
    # ============================================================
    print("===== STEP2: NORMALIZE =====")

    norm = normalize_records(df_master)

    if norm is None or norm.empty:
        print("❌ normalize 결과 없음")
        return

    norm.to_csv("data/silver/master_clean.csv", index=False, encoding="utf-8-sig")

    print(f"정제 데이터 행 수: {len(norm)}")

    # ============================================================
    # STEP2-2: CONTACT PARSE (그대로 유지)
    # ============================================================
    print("===== STEP2-2: CONTACT PARSE =====")

    try:
        contact_df = parse_contact_org_xlsx(
            "data/raw/아이엔소프트_직원연락망 (23.12.01).xlsx"
        )
    except Exception as e:
        print(f"⚠️ 연락망 파싱 실패: {e}")
        contact_df = pd.DataFrame()

    if contact_df is not None and not contact_df.empty:
        contact_df.to_csv(
            "data/silver/contact_parsed.csv",
            index=False,
            encoding="utf-8-sig"
        )
        print(f"연락망 데이터 수: {len(contact_df)}")
    else:
        print("⚠️ 연락망 데이터 없음")

    # ============================================================
    # STEP3: MERGE MASTER + CONTACT
    # ============================================================
    print("===== STEP3: MERGE MASTER + CONTACT =====")

    if contact_df is not None and not contact_df.empty:
        merged = norm.merge(
            contact_df[["name", "org", "dept", "phone", "email"]],
            on="name",
            how="left",
            suffixes=("", "_contact"),
        )

        merged["org"] = merged["org_contact"].combine_first(merged["org"])
        merged["dept"] = merged["dept_contact"].combine_first(merged["dept"])
    else:
        merged = norm.copy()

    merged.to_csv(
        "data/silver/master_enriched.csv",
        index=False,
        encoding="utf-8-sig"
    )

    print(f"보강 데이터 행 수: {len(merged)}")

    # ============================================================
    # STEP4: ENTITY RESOLUTION
    # ============================================================
    print("===== STEP4: ENTITY RESOLUTION =====")

    from src.core.entity_resolver import resolve_entities

    result = resolve_entities(merged)

    entity_df = result.employee_master

    if entity_df is None:
        raise ValueError("resolve_entities 결과 구조 확인 필요")

    entity_df.to_csv(
        "data/silver/people_master.csv",
        index=False,
        encoding="utf-8-sig"
    )

    result.employment_history.to_csv(
        "data/silver/employment_history.csv",
        index=False,
        encoding="utf-8-sig",
    )

    print(f"사람 수: {entity_df['emp_uid'].nunique()}")

    # ============================================================
    # STEP5: ORG SNAPSHOT (지금은 유지)
    # ============================================================
    print("===== STEP5: ORG SNAPSHOT =====")

    try:
        from src.build.org_snapshot_builder import build_org_snapshot

        snapshot_df = build_org_snapshot()

        if snapshot_df is not None and not snapshot_df.empty:
            snapshot_df.to_csv(
                "data/gold/org_snapshot.csv",
                index=False,
                encoding="utf-8-sig"
            )
            print(f"스냅샷 수: {len(snapshot_df)}")
        else:
            print("⚠️ snapshot 없음")

    except Exception as e:
        print(f"⚠️ snapshot 생성 실패: {e}")

    # ============================================================
    # STEP6: METRICS
    # ============================================================
    print("===== STEP6: METRICS =====")

    try:
        from src.build.metrics import build_metrics

        metrics_df = build_metrics(snapshot_df)

        if metrics_df is not None and not metrics_df.empty:
            metrics_df.to_csv(
                "data/gold/metrics.csv",
                index=False,
                encoding="utf-8-sig"
            )
            print(f"metrics 수: {len(metrics_df)}")
        else:
            print("⚠️ metrics 없음")

    except Exception as e:
        print(f"⚠️ metrics 생성 실패: {e}")


if __name__ == "__main__":
    run_pipeline()