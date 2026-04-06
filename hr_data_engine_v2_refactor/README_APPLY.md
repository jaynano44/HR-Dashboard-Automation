# 적용 가이드

## 교체 대상 파일
아래 파일을 기존 프로젝트에 덮어쓰면 된다.

- `src/pipeline.py`
- `src/ingest/ingest_v2.py`
- `src/preprocess/normalizer_v2.py`
- `src/core/entity_resolver.py`
- `src/build/org_snapshot_builder.py`
- `src/build/metrics.py`

## 유지 파일
아래 파일은 그대로 유지한다.

- `src/preprocess/contact_org_parser.py`
- `src/app/app.py`
- `schema_registry.py`

## 주의
- 기존 `normalize_v2.py`는 남겨도 되지만, 파이프라인에서는 새 `normalizer_v2.py`를 사용한다.
- `org_snapshot_builder.py`는 raw 파일 스캔 방식이 아니라 `employment_history` 기반으로 바뀌었다.
- `metrics.py` 출력 형식이 바뀌므로 앱에서 필요한 컬럼만 확인 후 연결하면 된다.

## 실행
```bash
python src/pipeline.py
```
