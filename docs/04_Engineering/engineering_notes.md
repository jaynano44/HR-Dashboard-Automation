# Engineering Notes
> 개발 중 발생한 기술 결정, 트러블슈팅, 구현 주의사항 기록  
> 설계서(01_Architecture)는 "무엇을 왜" / 이 문서는 "어떻게 + 삽질 기록"

---

## 환경 설정

```
Python : 3.11+
가상환경: .venv (python -m venv .venv)
실행위치: D:\HR\HR-Dashboard-Automation_MVP\
streamlit 실행: streamlit run main.py
전처리 실행: python preprocess.py --force
```

**필수 패키지 주의사항**
- `openpyxl` — xlsx 읽기. 암호화된 파일은 못 읽음 → `msoffcrypto` 필요
- `xlrd` — xls(구형) 읽기 전용. xlsx에 쓰면 에러
- `pandas` — `read_excel` 시 engine 명시 필수 (`engine="openpyxl"`)

---

## 폴더/경로 규칙

```
실행 위치 기준 상대경로 사용
data/raw/          ← 원본 엑셀 (절대 수정 금지)
data/raw/headcount/ ← 인원현황 파일 전용 서브폴더
data/processed/    ← preprocess.py 산출물
outputs/           ← 모델, 리포트
src/processor/     ← 전처리 모듈
src/dashboard/     ← app.py
```

**경로 탐색 순서 (headcount)**
1. `data/raw/{파일명}` 직접
2. `data/raw/headcount/{파일명}`
3. `data/raw/headcount/` 폴더 내 최신 xlsx

---

## 데이터 파이프라인 핵심 결정사항

### 총재직인원 = headcount_active.csv 기준 (고정값)
- `master_auto.csv`는 연도별 스냅샷 합본 → 중복 발생 → 인원 계산 불가
- `preprocess.py --force` 실행 시 `headcount_active.csv` 자동 생성
- KPI 카드의 "기준일 재직인원"은 기간 필터 무관하게 고정 표시

### master_auto.csv 용도
- 연도별/월별 입퇴사 **추세** 계산용
- 재직인원 카운트에는 사용하지 않음

### snapshot_year
- 각 파일이 "어느 연도 기준" 데이터인지 추정한 값
- 조직현황 탭 fallback 시 최신 snapshot_year 1개만 사용

---

## 알려진 버그 및 해결 이력

### [해결] dept 컬럼이 datetime으로 파싱되는 문제
- **원인**: config.yaml에서 `dept: 팀`, `team: 팀` 둘 다 같은 한글값
  → rename_map `{팀: dept, 팀: team}` 충돌 → 마지막 값만 남음
  → join_date rename 타이밍과 겹쳐 dept가 datetime으로 변환됨
- **해결**: `_standardize_hc()`에서 날짜 컬럼 먼저 별도 처리,
  이미 매핑된 한글 컬럼 재사용 방지 (`already_mapped` set)

### [해결] 총재직인원 0명
- **원인**: headcount 파일이 `data/raw/headcount/` 서브폴더에 있었으나
  config.yaml 경로는 `data/raw/`만 지정
- **해결**: `preprocess.py`에 서브폴더 탐색 추가 + `headcount_active.csv` 사전 저장

### [해결] 조직현황 부서 Bar 차트 `00:00:00` 표시
- **원인**: master_auto.csv fallback 시 전체 연도 스냅샷 사용
  → 폐쇄된 부서까지 포함
- **해결**: 최신 snapshot_year 단일 연도 + 퇴사자 제거 + emp_id 중복 제거

### [미해결] 2026년 입사 0명
- **증상**: KPI 카드 "2026년 입사" 항목이 0명
- **원인 추정**: yearly.csv가 2024년까지만 집계됨 (데이터 없음)
- **확인 방법**: `data/processed/turnover_yearly.csv` 열어서 연도 범위 확인

---

## preprocess.py 실행 시 출력 의미

```
처리파일 305 / 실패 0     ← raw 폴더 전체 xlsx/xls 파일 수
master rows: 1698         ← 중복 포함 전체 스냅샷 행 수 (인원 수 아님)
headcount_active.csv: 126명  ← 실제 현재 재직자 수 (이게 맞는 숫자)
headcount_exited.csv: 359명  ← 퇴사자 누적
career_rank.csv: 161명    ← 경력연차 파일 파싱 결과
```

---

## 모듈별 역할 요약

| 파일 | 역할 | 주의 |
|---|---|---|
| `auto_ingest_multi.py` | raw 파일 스캔 + fingerprint | 암호 파일은 timeout 스킵 |
| `build_dataset.py` | gold CSV 생성 | master 중복 있음, 추세용으로만 |
| `headcount_2024_loader.py` | headcount xlsx 시트 분류 | 시트명 키워드 의존 |
| `career_rank_loader.py` | 경력연차 파일 파싱 | 주민번호 자동 드랍 |
| `org_standardize.py` | 조직명 정규화 | org_alias_map.csv 수동 관리 |
| `schema_registry.py` | 파일 양식 분류 규칙 | 신규 양식 → unknown 기록 |
| `metrics.py` | 전체 KPI 계산 | |
| `metrics_dept.py` | 부서별 KPI | |
| `entity_resolver.py` | (예정 v1.3) 직원 중복 매핑 | |

---

## 다음 개발 시 주의할 것

1. **app.py `@st.cache_data`** — 데이터 변경 후 반드시 "새로고침" 버튼 클릭
2. **Plotly dual axis** — `make_subplots(specs=[[{"secondary_y": True}]])` 사용
   `update_layout`에 `yaxis`/`yaxis2` 동시 지정 시 충돌 주의
3. **민감정보** — 주민번호, 핸드폰, 이메일 컬럼은 ingest 시 자동 드랍
   `SENSITIVE_COLS` 패턴은 `schema_registry.py`에서 관리
4. **새 파일 추가** → `python preprocess.py` (자동 감지)
   강제 재처리 → `python preprocess.py --force`

---

## v1.3 이후 개발 예정 (여기 추가할 것)

- [ ] `universal_schema_mapper.py` — 모르는 양식 자동 인식
- [ ] `entity_resolver.py` — 이름 기반 fuzzy 매칭
- [ ] `correction_logger.py` — 보정 이력 추적
- [ ] multi-profile config (`configs/` 폴더)

---

*최종 업데이트: 2026-03-10*
