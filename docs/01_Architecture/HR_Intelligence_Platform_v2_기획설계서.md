# HR Intelligence Platform v2
## 범용 데이터 엔진 설계서

> **버전**: v2.0-draft  
> **기준**: v1.1-stable 현재 구현 → v2 범용화 + AI 확장  
> **목표**: 어떤 조직의 HR 파일이 들어와도 자동 처리되고, ML/LLM 챗봇까지 연결되는 엔터프라이즈급 HR 데이터 플랫폼  
> **Git 전략**: v1.2 → v1.3 → v2.0 → v2.1 마일스톤별 배포

---

## 0. 왜 v2인가 — 현재 한계와 목표

### v1.1의 실제 한계 (현재 코드 분석 기반)

| 한계 | 현재 코드 | 영향 |
|---|---|---|
| 특정 파일 종속 | `headcount_2024_loader.py` | 다른 조직 파일 → 즉시 실패 |
| 수동 조직 매핑 | `org_alias_map.csv` 직접 편집 | 조직 개편마다 수작업 |
| 단순 concat | `build_dataset.py` 스냅샷 합산 | 중복 인원, 이력 없음 |
| Excel만 처리 | `auto_ingest_multi.py` xls/xlsx only | PDF·이력서·이미지 불가 |
| 한국어 컬럼만 | `_COL_ALIASES` 한국어 위주 | 영문·다국어 HR 파일 불가 |
| 신규 양식 → 중단 | `unknown` 분류 후 alert만 | 자동 해결 안 됨 |

### v2 목표 — 3가지 범용성

```
1. 파일 범용성   : Excel / PDF / DOCX / JPG / CSV — 어떤 포맷이든 수용
2. 양식 범용성   : 모르는 컬럼 구조 → 규칙→ML→LLM으로 자동 해석
3. 조직 범용성   : config만 바꾸면 다른 회사 HR 데이터도 즉시 처리
```

---

## 1. 데이터 레이어 재설계 — 4계층 아키텍처

### 현재 (v1.1) vs 목표 (v2)

```
현재:  raw Excel → master_auto.csv → gold CSV
문제:  Bronze/Silver 경계 없음, 원본 변형, 이력 없음

목표 (v2):
  ┌─────────────────────────────────────────────────────┐
  │  BRONZE  원본 불변 보존 (절대 수정 금지)              │
  │  data/bronze/{source_type}/{YYYYMMDD}_{filename}     │
  ├─────────────────────────────────────────────────────┤
  │  SILVER  표준화·정제·통합 (1인 1행)                  │
  │  data/silver/employee_master.parquet                 │
  │  data/silver/employment_history.parquet              │
  │  data/silver/org_history.parquet                     │
  ├─────────────────────────────────────────────────────┤
  │  GOLD    분석용 집계 (KPI 시계열)                    │
  │  data/gold/turnover_yearly.csv                       │
  │  data/gold/monthly.csv                               │
  │  data/gold/org_health.csv                            │
  │  data/gold/attrition_features.csv                    │
  ├─────────────────────────────────────────────────────┤
  │  PLATINUM  AI 추론용 (임베딩·벡터·모델 입력)         │
  │  data/platinum/embeddings/                           │
  │  data/platinum/llm_context/                          │
  └─────────────────────────────────────────────────────┘
```

### Bronze Layer 규칙 (절대 불변)

```python
# Bronze 저장 원칙
# 1. 원본 파일을 날짜+해시로 복사 저장 (덮어쓰기 금지)
# 2. 어떤 처리도 하지 않음 (읽기 전용)
# 3. 파일 타입별 서브폴더 분류

data/bronze/
├── excel/          ← xlsx, xls
├── pdf/            ← PDF 이력서, 문서
├── image/          ← JPG, PNG 사원증, 스캔본
├── docx/           ← Word 문서
└── unknown/        ← 분류 불가 파일
```

### Silver Layer 핵심 — SCD Type 2

```sql
-- 과거 조직 구조 복원을 위한 시계열 모델
-- "스냅샷 딜레마" 해결의 핵심

employee_master:
  emp_uid        -- 시스템 내부 UUID (불변)
  emp_id         -- 원본 사번 (없으면 null)
  name           -- 이름
  valid_from     -- 이 레코드가 유효한 시작일
  valid_to       -- 이 레코드가 유효한 종료일 (현재면 9999-12-31)
  is_current     -- 현재 유효 레코드 여부
  org            -- 해당 기간의 소속 본부
  dept           -- 해당 기간의 팀
  grade          -- 직급
  source_file    -- 어느 파일에서 왔는가 (Traceability)
  ingest_at      -- 언제 처리됐는가
```

---

## 2. 파일 처리 파이프라인 — 범용 포맷 대응

### 2-1. 지원 포맷별 처리 전략

```
포맷          현재    v2 처리 방식
─────────────────────────────────────────────────────────
.xlsx/.xls    ✅      기존 유지 + 시트별 자동 분류 강화
.csv          ⚠️     인코딩 자동 감지 (UTF-8/EUC-KR/CP949)
.pdf          ❌      pdfplumber → 표 추출 → 컬럼 분류
.docx         ❌      python-docx → 표/본문 분리 → 구조화
.jpg/.png     ❌      EasyOCR or Claude Vision → 텍스트 추출
.hwp          ❌      hwpx 변환 후 처리 (한국 특화)
```

### 2-2. 범용 파일 처리기 구조

```python
# src/loaders/universal_loader.py

class UniversalLoader:
    """어떤 포맷이든 → 표준 DataFrame으로 변환"""

    LOADERS = {
        ".xlsx": ExcelLoader,
        ".xls":  ExcelLoader,
        ".csv":  CSVLoader,
        ".pdf":  PDFLoader,       # pdfplumber + 표 추출
        ".docx": DocxLoader,      # python-docx
        ".jpg":  VisionLoader,    # OCR / Claude Vision
        ".png":  VisionLoader,
        ".hwp":  HWPLoader,       # hwpx 변환
    }

    def load(self, path: Path) -> list[RawSheet]:
        """
        Returns: RawSheet 리스트
        RawSheet = {name, df, source_file, page_or_sheet, confidence}
        """
        ext = path.suffix.lower()
        loader = self.LOADERS.get(ext, UnknownLoader)
        return loader(path).extract()
```

### 2-3. Excel 시트 처리 강화 (현재 가장 중요)

```python
# 현재 문제: 시트 판별이 키워드 매칭에만 의존
# v2: 헤더 위치 자동 탐지 + 병합셀 해제 + 멀티헤더 처리

class ExcelSheetParser:
    def parse(self, ws) -> pd.DataFrame:
        # 1. 병합셀 해제 (상위 값 채우기)
        self._unmerge_cells(ws)
        # 2. 헤더 행 자동 탐지 (데이터 밀도 기반)
        hdr_row = self._detect_header_row(ws)
        # 3. 멀티헤더 → 단일 컬럼명으로 평탄화
        df = self._flatten_multiheader(ws, hdr_row)
        # 4. 완전 빈 행/열 제거
        df = df.dropna(how="all").dropna(axis=1, how="all")
        return df
```

---

## 3. 범용 스키마 매핑 — 규칙 → ML → LLM 3단계

### 3-1. 전체 흐름

```
알 수 없는 파일/시트 도착
         │
         ▼
┌─────────────────────────────┐
│  STAGE 1: 규칙 기반         │  신뢰도 ≥ 0.85 → 자동 처리
│  schema_registry 매칭       │
│  _COL_ALIASES 확장 매칭     │  신뢰도 0.5~0.85 → Stage 2
└─────────────────────────────┘
         │ 낮음
         ▼
┌─────────────────────────────┐
│  STAGE 2: ML 분류기         │  신뢰도 ≥ 0.7 → 자동 처리
│  컬럼명 TF-IDF 임베딩       │
│  학습된 분류 모델           │  신뢰도 < 0.7 → Stage 3
└─────────────────────────────┘
         │ 낮음
         ▼
┌─────────────────────────────┐
│  STAGE 3: LLM 판단          │  결과 → 자동 처리 + 학습 저장
│  Claude API (컬럼+샘플)     │
│  "이 컬럼은 emp_id입니까?"  │  실패 → Human Review Queue
└─────────────────────────────┘
         │ 실패
         ▼
┌─────────────────────────────┐
│  Human Review Queue         │  대시보드 알림 → 사람이 확인
│  Validation UI              │  확인 결과 → ML 학습 데이터로
└─────────────────────────────┘
```

### 3-2. Stage 2 — ML 컬럼 분류기

```python
# src/processor/ml_column_classifier.py
# 필요 라이브러리: scikit-learn (TF-IDF + LogisticRegression)
# 학습 데이터: schema_registry에 등록된 컬럼-표준명 쌍

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression

class MLColumnClassifier:
    """
    입력: 컬럼명 문자열 (한국어/영어 혼합)
    출력: 표준 필드명 + 신뢰도
    예: "사원코드" → ("emp_id", 0.95)
        "입사년월일" → ("join_date", 0.88)
        "col_41" → ("unknown", 0.12) → LLM으로 넘김
    """
    STANDARD_FIELDS = [
        "emp_id", "name", "org", "dept", "grade",
        "join_date", "exit_date", "title", "snapshot_year",
        "career_months", "education", "certification",
        "sensitive_drop", "skip"
    ]
```

### 3-3. Stage 3 — LLM 컬럼 분류 프롬프트

```python
# src/processor/llm_schema_mapper.py

SYSTEM_PROMPT = """
당신은 HR 데이터 전문가입니다.
엑셀 파일의 컬럼명과 샘플 데이터를 보고,
각 컬럼이 표준 HR 스키마의 어느 필드에 해당하는지 판단합니다.

표준 필드 목록:
- emp_id: 직원 식별자 (사번, 사원코드, 직원ID 등)
- name: 이름 (성명, 사원명 등)
- org: 상위 조직 (본부, 사업부, 부문 등)
- dept: 하위 조직 (팀, 파트, 그룹 등)
- join_date: 입사일
- exit_date: 퇴사일
- grade: 직급/등급
- sensitive_drop: 민감정보 (주민번호, 휴대폰, 이메일)
- skip: 불필요 컬럼

JSON으로만 응답하세요:
{"매핑": [{"원본컬럼": "...", "표준필드": "...", "신뢰도": 0.0~1.0}]}
"""

def map_columns_with_llm(columns: list, samples: list[dict]) -> dict:
    """Claude API 호출 → 컬럼 매핑 결과 반환"""
```

### 3-4. 매핑 학습 저장 (선순환)

```
LLM 매핑 결과 or 사람 확인 결과
         │
         ▼
data/processed/schema_learnings.json 누적 저장
         │
         ▼
ML 분류기 재학습 (주 1회 or 100건 누적 시)
         │
         ▼
Stage 1 규칙 자동 업데이트 제안
```

---

## 4. Entity Resolution — 중복 제거 및 직원 통합

### 4-1. 현재 문제

```
현재: 이름으로만 매칭 → 동명이인 충돌, 오탈자 무시
예:
  파일A: "김철수" (입사 2020-03)
  파일B: "김 철수" (입사 2020.03)
  파일C: "金哲洙" or "KIM CHEOL SU"
  → 현재: 3명으로 처리됨 (실제: 1명)
```

### 4-2. 해결 — 다단계 매칭

```python
# src/processor/entity_resolver.py

class EntityResolver:
    def resolve(self, records: list[Record]) -> list[Entity]:

        # STEP 1: emp_id 직접 매칭 (신뢰도 1.0)
        matched_by_id = self._match_by_emp_id(records)

        # STEP 2: (이름 + 입사일) fuzzy 매칭 (신뢰도 0.85+)
        # - 이름: 편집거리 ≤ 1 OR 자모 분해 후 유사도
        # - 입사일: ±30일 이내 동일 인물로 추정
        matched_by_name_date = self._fuzzy_match(unmatched)

        # STEP 3: LLM 판단 (신뢰도 < 0.7인 경우)
        # - "이 두 레코드가 같은 사람입니까?" + 컨텍스트
        llm_resolved = self._llm_resolve(ambiguous)

        # STEP 4: 모든 매칭 결과 → employment_history로
        return self._build_history(all_matched)
```

### 4-3. 보정 로그 (Audit Trail)

```csv
# data/processed/correction_log.csv
timestamp, file_source, original_value, corrected_value, field, method, confidence, reviewer
2026-03-10, 파일A.xlsx, "김 철수", "김철수", name, fuzzy_match, 0.91, auto
2026-03-10, 파일B.xlsx, "2020.03", "2020-03-01", join_date, date_parse, 0.99, auto
2026-03-10, 파일C.xlsx, "col_41", "emp_id", column_map, llm, 0.87, auto
```

---

## 5. 다중 포맷 처리 — PDF / DOCX / 이미지

### 5-1. PDF 처리 (이력서, 보고서)

```python
# src/loaders/pdf_loader.py
import pdfplumber

class PDFLoader:
    def extract(self, path: Path) -> list[RawSheet]:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                # 1. 표(Table) 우선 추출
                tables = page.extract_tables()
                if tables:
                    yield RawSheet(df=pd.DataFrame(tables[0][1:], columns=tables[0][0]))

                # 2. 표 없으면 텍스트 → LLM 구조화
                else:
                    text = page.extract_text()
                    yield self._llm_structurize(text)

    def _llm_structurize(self, text: str) -> RawSheet:
        """LLM에 텍스트 전달 → {이름, 소속, 입사일, ...} JSON 반환"""
```

### 5-2. 이미지 처리 (스캔 문서, 사원증)

```python
# src/loaders/vision_loader.py

class VisionLoader:
    """
    옵션 A (로컬): EasyOCR → 텍스트 → 규칙 파싱
    옵션 B (API): Claude Vision → 직접 구조화 JSON
    """
    def extract(self, path: Path) -> list[RawSheet]:
        if self.use_vision_api:
            return self._claude_vision(path)  # 정확도 높음
        else:
            return self._easyocr(path)        # 오프라인 가능
```

### 5-3. 처리 우선순위 결정

```
파일 도착
   │
   ├── 표 구조 있음? → 직접 파싱 (빠름, 무료)
   │
   ├── 텍스트 추출 가능? → 규칙 파싱 → ML 분류
   │
   └── 이미지/스캔? → OCR → LLM 구조화 (느림, 비용)

비용 절약 원칙: 규칙으로 해결 가능하면 LLM 호출 안 함
```

---

## 6. 조직 정규화 자동화

### 6-1. 현재 문제

```
org_alias_map.csv 수동 편집
→ 새 조직명 생기면 매번 추가해야 함
→ 다른 회사 데이터엔 완전히 새로 작성
```

### 6-2. 자동 정규화 엔진

```python
# src/processor/org_normalizer.py

class OrgNormalizer:
    def normalize(self, raw_org: str) -> NormalizedOrg:

        # 1. 정확 매칭 (alias_map)
        if raw_org in self.alias_map:
            return self.alias_map[raw_org]

        # 2. 편집거리 기반 유사 매칭
        best, score = self._levenshtein_match(raw_org)
        if score >= 0.85:
            self._learn(raw_org, best)  # 학습 저장
            return best

        # 3. LLM 판단
        # "이 조직명들이 같은 조직입니까?: ['서비스사업본부', '서비스 사업 본부']"
        result = self._llm_judge(raw_org, candidates)
        if result.confidence >= 0.8:
            self._learn(raw_org, result.canonical)
            return result.canonical

        # 4. 신규 조직으로 등록
        return self._register_new_org(raw_org)
```

### 6-3. 조직 이력 관리 (시계열)

```csv
# data/silver/org_history.csv
org_code, org_name, parent_code, valid_from, valid_to, is_active, alias_list
ORG_001, 서비스사업본부, ROOT, 2020-01-01, 9999-12-31, true, "서비스본부|서비스사업부"
ORG_002, 클라우드사업본부, ROOT, 2021-06-01, 2023-12-31, false, "클라우드본부"
ORG_003, AI사업본부, ROOT, 2024-01-01, 9999-12-31, true, ""
```

---

## 7. 범용 Config 설계 — 회사별 프로파일

### 7-1. 디렉토리 구조

```
configs/
├── default.yaml        ← 공통 기본값
├── iensoft.yaml        ← 현재 회사 (기존 config.yaml 이전)
├── company_b.yaml      ← 다른 회사 적용 예시
└── schema/
    ├── columns_kr.yaml ← 한국어 컬럼 매핑
    ├── columns_en.yaml ← 영어 컬럼 매핑
    └── columns_jp.yaml ← 일본어 (확장 예시)
```

### 7-2. default.yaml

```yaml
# 공통 설정 (회사별 override 가능)
pipeline:
  bronze_immutable: true        # Bronze 불변 원칙
  auto_llm_threshold: 0.5       # 이 이하 신뢰도면 LLM 호출
  human_review_threshold: 0.3   # 이 이하면 사람 검토 큐
  sensitive_auto_drop: true     # 민감정보 자동 드랍

layers:
  bronze: data/bronze
  silver: data/silver
  gold:   data/gold
  platinum: data/platinum

llm:
  provider: anthropic           # anthropic | openai | local
  model: claude-sonnet-4-6
  max_tokens: 1000
  use_for: [column_map, entity_resolve, org_normalize, pdf_structure]

org:
  hierarchy: [division, hq, team]   # 조직 계층 정의
  alias_learning: true              # 자동 alias 학습

sensitive_cols:                     # 민감 컬럼 패턴 (정규식)
  - "주민.*번호"
  - "rrn"
  - "휴대.*폰|핸드폰"
  - "e.?mail|이메일"
  - "집.*주소|address"
```

### 7-3. iensoft.yaml (현재 회사용)

```yaml
extends: default.yaml           # default 상속

company:
  name: 아이엔소프트
  org_lang: ko
  emp_id_field: "사번"          # 이 회사의 사번 컬럼명

paths:
  raw: data/raw
  headcount_subdir: headcount   # data/raw/headcount/

files:
  headcount_xlsx: "2024년 인원현황-20241121-*.xlsx"  # glob 패턴

columns:                        # 이 회사 특화 컬럼 매핑 추가
  extra_aliases:
    emp_id: ["사번", "직원번호"]
    org:    ["소속", "사업본부"]
```

---

## 8. LLM 챗봇 연동 — Platinum Layer 활용

### 8-1. RAG 기반 HR 챗봇 구조

```
사용자 질문: "우리 팀 백엔드 개발자 평균 근속연수는?"
         │
         ▼
┌─────────────────────────────┐
│  의도 분류                   │
│  조회/예측/시뮬레이션/보고   │
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  컨텍스트 수집 (RAG)         │
│  Gold CSV에서 관련 데이터    │
│  → DataFrame 요약 → 텍스트  │
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  Claude API 호출             │
│  system: HR 분석가 역할      │
│  user: 질문 + 데이터 컨텍스트│
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  답변 + 근거 차트 반환       │
│  Streamlit 챗 인터페이스     │
└─────────────────────────────┘
```

### 8-2. Platinum Layer — LLM용 컨텍스트 사전 생성

```
# 매일 새벽 자동 생성 (preprocess --platinum)
data/platinum/
├── llm_context/
│   ├── org_summary.txt        ← "현재 조직 현황 요약"
│   ├── risk_summary.txt       ← "현재 리스크 요약"
│   └── monthly_brief.txt      ← "이번 달 입퇴사 동향"
├── embeddings/
│   └── employee_vectors.npy   ← 직원 정보 임베딩 (스킬 매칭용)
└── qa_context/
    └── recent_anomalies.json  ← 이상 탐지 결과
```

---

## 9. 모듈 구조 — v2 전체

```
src/
├── loaders/                    ← 포맷별 원본 파일 읽기
│   ├── universal_loader.py     ← 포맷 자동 감지 + 라우팅
│   ├── excel_loader.py         ← xlsx/xls (시트 파싱 강화)
│   ├── csv_loader.py           ← 인코딩 자동 감지
│   ├── pdf_loader.py           ← pdfplumber + 표 추출
│   ├── docx_loader.py          ← python-docx
│   └── vision_loader.py        ← EasyOCR / Claude Vision
│
├── processor/
│   ├── universal_schema_mapper.py  ← 규칙→ML→LLM 3단계
│   ├── ml_column_classifier.py     ← TF-IDF 기반 ML 분류기
│   ├── llm_schema_mapper.py        ← Claude API 컬럼 분류
│   ├── entity_resolver.py          ← 직원 중복 통합
│   ├── org_normalizer.py           ← 조직명 자동 정규화
│   ├── scd_builder.py              ← SCD Type2 이력 구성
│   ├── build_dataset.py            ← Gold KPI 집계 (유지)
│   ├── correction_logger.py        ← 보정 이력 기록
│   └── schema_registry.py          ← 기존 유지 + 확장
│
├── ai/
│   ├── attrition_model.py          ← 퇴사 예측 (기존 강화)
│   ├── org_health.py               ← 조직 건강도 스코어
│   ├── hr_chatbot.py               ← RAG 챗봇
│   └── recommend_engine.py         ← 인력 추천 (v2.1)
│
├── dashboard/
│   └── app.py                      ← Streamlit (탭 확장)
│
└── governance/
    ├── audit_log.py                ← 전체 처리 감사 로그
    ├── validation_gate.py          ← Silver 커밋 전 검증
    └── human_review_queue.py       ← 사람 검토 대기열 UI
```

---

## 10. 구현 우선순위 — 단계별 Git 태그

### v1.2 (현재 진행 중, 2주)
```
목표: 현재 대시보드 완성도 높이기
- [ ] KPI 카드 정확도 (2026 입사 0명 수정)
- [ ] 리스크 탭 — 부서별 퇴사율 히트맵
- [ ] 스킬 현황 탭 — career_rank.csv 활용
- [ ] README + 스크린샷 업데이트
git tag: v1.2.0-stable
```

### v1.3 (3주, 범용화 기반)
```
목표: 다른 조직 데이터도 처리 가능한 기반 구축
- [ ] configs/ 폴더 + default.yaml + iensoft.yaml 분리
- [ ] universal_schema_mapper.py (Stage 1+2, LLM 없이)
- [ ] entity_resolver.py (fuzzy 매칭)
- [ ] correction_logger.py
- [ ] Bronze layer 폴더 구조 전환
git tag: v1.3.0-universal-base
```

### v2.0 (6주, AI 본격 도입)
```
목표: 모르는 양식 자동 처리 + LLM 연동
- [ ] universal_schema_mapper.py Stage 3 (LLM)
- [ ] pdf_loader.py + vision_loader.py
- [ ] SCD Type2 silver layer
- [ ] org_normalizer.py (LLM 연동)
- [ ] hr_chatbot.py (기본 RAG)
- [ ] 대시보드 챗봇 탭
git tag: v2.0.0-ai-engine
```

### v2.1 (4주, 추천 + 배포)
```
목표: 다른 조직 배포 가능한 완성형
- [ ] recommend_engine.py
- [ ] Human Review Queue UI
- [ ] multi-profile 배포 가이드
- [ ] Docker 컨테이너화
- [ ] GitHub Actions CI/CD
git tag: v2.1.0-platform
```

---

## 11. 배포 전략 — 다른 조직에 퍼뜨리기

### 11-1. 3가지 배포 모드

```
모드 A: 로컬 설치
  git clone + pip install + config.yaml 수정
  → HR 담당자가 직접 PC에서 실행
  → 민감 데이터 외부 미전송

모드 B: Docker
  docker run -v /data:/app/data hr-intelligence
  → IT 팀이 내부 서버에 배포
  → 환경 설정 최소화

모드 C: SaaS (장기)
  회사별 테넌트 분리
  → 클라우드 배포
```

### 11-2. 신규 조직 온보딩 절차

```
1. git clone 또는 pip install hr-intelligence
2. python setup_wizard.py
   → 회사명, 조직 계층, 사번 컬럼명 입력
   → configs/{company}.yaml 자동 생성
3. data/raw/ 에 HR 파일 복사
4. python preprocess.py --profile {company}
   → 자동 컬럼 매핑 (모르는 양식 → LLM 판단)
5. streamlit run main.py --profile {company}
```

---

## 12. 문서 체계 최종 정리

```
docs/
├── 01_Architecture/
│   ├── HR_Intelligence_Platform_v2_기획설계서.md   ← 이 문서 (최상위)
│   └── HR_시스템_설계서_GAP분석_및_업그레이드_로드맵.md
│
├── 02_Data/
│   ├── HR_데이터_양식_레지스트리.md    ← 유지 (schema_registry 기준)
│   └── HR_KPI_Calculation_Logic_Document.md  ← 내용 채워야 함
│
├── 03_AI/
│   └── (v2.0 구현 후 채울 것)
│
├── 04_Engineering/
│   └── engineering_notes.md           ← 개발 트러블슈팅 기록
│
└── 05_archive/
    ├── HR_AI_시스템_전체_아키텍처_취업용.docx
    ├── HR_AI_분석_시스템_설계_퇴사예측_조직분석.docx
    ├── 2026_지능형_HR_자동화_시스템_에이전틱_UX.docx
    └── HR_AI_시스템용_DB_설계.docx
```

---

*이 설계서는 v2 개발 기준 문서입니다. 각 모듈 구현 시 해당 섹션을 실제 코드 기준으로 업데이트하세요.*  
*최종 업데이트: 2026-03-10 | 다음 리뷰: v1.3 완성 시*
