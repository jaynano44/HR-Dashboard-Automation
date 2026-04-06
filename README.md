# HR Data Engine v2

범용 HR 데이터 전처리 + 분석 엔진

## 구조

- raw → bronze → silver → gold

## 실행

```bash
python main.py

핵심 기능
다양한 Excel 자동 ingest
조직/사람 데이터 표준화
동일인 식별
재직 세션 생성
연도별 snapshot

---

# 📐 3. 설계 문서 (설계.md) ⭐ 중요

👉 이건 네 포트폴리오 핵심이다  
👉 내가 바로 써준다

---

## 📄 docs/architecture_v2.md

```markdown
# HR Data Engine v2 설계

## 1. 목적

다양한 형식의 HR 데이터를 통합하여
표준화된 인력/조직 DB를 구축하고
분석 및 예측에 활용한다.

---

## 2. 핵심 문제

- 파일마다 컬럼 다름
- 조직명 불일치
- 사람 중복
- 날짜 오류
- 조직도/연락망 따로 존재

---

## 3. 해결 전략

---

# 📐 3. 설계 문서 (설계.md) ⭐ 중요

👉 이건 네 포트폴리오 핵심이다  
👉 내가 바로 써준다

---

## 📄 docs/architecture_v2.md

```markdown
# HR Data Engine v2 설계

## 1. 목적

다양한 형식의 HR 데이터를 통합하여
표준화된 인력/조직 DB를 구축하고
분석 및 예측에 활용한다.

---

## 2. 핵심 문제

- 파일마다 컬럼 다름
- 조직명 불일치
- 사람 중복
- 날짜 오류
- 조직도/연락망 따로 존재

---

## 3. 해결 전략

### 3.1 Raw Layer
- 원본 그대로 저장

### 3.2 Bronze Layer
- 파일 → sheet → block 단위 분해

### 3.3 Silver Layer (핵심)
- employee_master
- employment_session
- org_unit
- org_membership_history

### 3.4 Gold Layer
- KPI
- 조직별 인원
- 입퇴사율

---

## 4. 데이터 흐름
raw
→ ingest
→ preprocess
→ entity resolve
→ session build
→ snapshot
→ KPI


---

## 5. 주요 모듈

### ingest
- auto_ingest_multi
- schema_registry

### preprocess
- normalizer
- org_standardize
- contact_org_parser

### core
- entity_resolver
- employment_session_builder

### build
- org_snapshot_builder
- metrics

---

## 6. 확장 계획

- Attrition 예측
- 조직 시뮬레이션
- 스킬 기반 인력 추천
- 프로젝트 인력 배치

---

## 7. 핵심 설계 원칙

1. raw 불변
2. silver = 기준 데이터
3. app은 조회만 수행
4. QA 필수
5. 구조 우선, 기능은 나중

