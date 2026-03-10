<div align="center">

![header](https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=0,2,5,10,30&height=220&section=header&text=HR%20Intelligence%20Platform&fontSize=52&fontAlignY=38&desc=범용%20HR%20데이터%20엔진%20%2B%20AI%20의사결정%20플랫폼&descAlignY=58&descAlign=50)

[![Status](https://img.shields.io/badge/Status-v1.2--dev-orange?style=for-the-badge)]()
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)]()
[![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)]()
[![License](https://img.shields.io/badge/License-Private-red?style=for-the-badge)]()

</div>

---

## 🎯 한 줄 요약

> **어떤 양식의 HR 파일이든 자동 처리 → KPI 분석 → 채용 매칭 → 퇴사 위험 탐지 → AI 의사결정까지 연결되는 범용 HR 데이터 엔진**

HR 담당자가 며칠씩 하던 서류검토·인원 집계를 수십 분으로 단축합니다.

---

## 📌 Quick Navigation

[![시스템 개요](https://img.shields.io/badge/🏗️_시스템_개요-1F3864?style=for-the-badge)](#️-시스템-개요)&nbsp;
[![데이터 아키텍처](https://img.shields.io/badge/🗄️_데이터_아키텍처-1F3864?style=for-the-badge)](#️-4계층-데이터-아키텍처)&nbsp;
[![모듈 구성](https://img.shields.io/badge/📦_모듈_구성-1F3864?style=for-the-badge)](#-모듈-구성-module-15)&nbsp;
[![현재 기능](https://img.shields.io/badge/✅_현재_기능-1a6b3c?style=for-the-badge)](#-현재-구현-기능-v12-dev)&nbsp;
[![로드맵](https://img.shields.io/badge/🗺️_로드맵-1F3864?style=for-the-badge)](#️-로드맵)&nbsp;
[![문서](https://img.shields.io/badge/📄_기획_문서-555555?style=for-the-badge)](#-기획-문서)&nbsp;
[![시작하기](https://img.shields.io/badge/🚀_시작하기-1F3864?style=for-the-badge)](#-시작하기)

---

## 🏗️ 시스템 개요

```
Raw HR Excel / PDF 이력서 / 이미지 증빙서류
  (어떤 양식, 어떤 포맷이든)
        ↓
┌────────────────────────────────────┐
│  Auto Ingest Engine                │  시트 단위 자동 분류
│  · 시트 단위 자동 분류             │  fingerprint 증분 처리
│  · fingerprint 기반 증분 처리      │  파일/전체 타임아웃
│  · 민감정보 자동 드랍              │
└────────────────────────────────────┘
        ↓
┌────────────────────────────────────┐
│  Universal Schema Mapper           │  규칙(≥0.85) → ML(≥0.70)
│  Stage 1: 규칙 기반               │  → LLM(≥0.60) → Human Queue
│  Stage 2: ML 분류기 (TF-IDF)      │
│  Stage 3: LLM (Claude API)        │
└────────────────────────────────────┘
        ↓
┌────────────────────────────────────┐
│  Entity Resolver                   │  fuzzy 매칭 + LLM
│  · 직원 중복 통합 (사번 없어도)    │  → emp_uid 불변 보장
│  · 조직명 자동 정규화             │
└────────────────────────────────────┘
        ↓
┌────────────────────────────────────────────────┐
│  BRONZE → SILVER → GOLD → PLATINUM            │
│  원본보존  정제·SCD2  KPI집계  AI추론컨텍스트  │
└────────────────────────────────────────────────┘
        ↓
┌────────────────────────────────────────────────────────────────┐
│  Streamlit Dashboard                                           │
│  인력현황 │ 조직현황 │ 리스크 │ 채용매칭 │ 인력소요 │ HR챗봇  │
└────────────────────────────────────────────────────────────────┘
```

---

## 🗄️ 4계층 데이터 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│  🟤 BRONZE  —  원본 불변 보존 (절대 수정 금지)              │
│  data/bronze/{excel|pdf|image|docx}/{YYYYMMDD}_{file}       │
├─────────────────────────────────────────────────────────────┤
│  ⚪ SILVER  —  표준화 · 정제 · 통합 (1인 1행, SCD Type2)   │
│  data/silver/employee_master.parquet                        │
│  data/silver/employment_history.parquet                     │
│  data/silver/skill_inventory.parquet                        │
│  data/silver/project_history.parquet        …외 다수        │
├─────────────────────────────────────────────────────────────┤
│  🟡 GOLD  —  분석용 집계 (KPI + ML 피처)                   │
│  data/gold/turnover_yearly.csv                              │
│  data/gold/monthly_trend.csv                                │
│  data/gold/dept_headcount.csv                               │
│  data/gold/attrition_features.csv           …외 다수        │
├─────────────────────────────────────────────────────────────┤
│  💎 PLATINUM  —  AI 추론 · LLM 컨텍스트                    │
│  data/platinum/llm_context/org_summary.txt                  │
│  data/platinum/embeddings/employee_vectors.npy              │
│  data/platinum/qa_context/recent_anomalies.json             │
└─────────────────────────────────────────────────────────────┘
```

### SCD Type2 — 과거 조직 구조 완벽 복원

```sql
-- "2023년 상반기 디자인팀 인원은?" 같은 질문에 정확히 답할 수 있습니다
employee_master:
  emp_uid     -- 시스템 내부 UUID (절대 불변)
  valid_from  -- 이 레코드 유효 시작일
  valid_to    -- 유효 종료일 (현재 재직 = 9999-12-31)
  is_current  -- 현재 유효 레코드 여부
  org / dept / grade  -- 해당 기간의 소속 정보
```

---

## 📦 모듈 구성 (Module 1~5)

| 모듈 | 기능 요약 | 핵심 가치 | 상태 |
|---|---|---|---|
| **M1. 인력 현황** | 총인원·입퇴사·부서별·월별 추세·조기퇴사 | 지금 인력 상태를 정확히 파악 | ✅ v1.1 완료 |
| **M2. 인력 소요** | 프로젝트별 필요 인력·스킬 갭·가용 인원 분석 | 프로젝트 전 인력 계획 자동화 | 🔨 v2.0 |
| **M3. 조달 추천** | 내부차출 vs 정규직 vs 프리랜서 비용/속도 비교 | 최적 조달 방법 자동 비교 | 🔨 v2.0 |
| **M4. 채용 매칭** | 이력서 파싱 → 스킬 추출 → 적합도 점수 → 면접 체크리스트 | 서류검토 시간 90% 단축 | 🔨 v2.1 |
| **M5. 성과·보상** | 평가이력·Compa-ratio·승진 패턴·후보 추천 | 보상 경쟁력·진급 근거 확보 | 🔨 v2.2 |

### 채용 매칭 (M4) — 핵심 병목 해결

```
부서 요청서 입력 (직무 · 스킬 · 경력 · 고용형태)
      ↓
① 내부 인력 자동 검색  →  스킬 매칭 + 가용성 확인
② 내부 불가  →  이력서 파일 업로드 (PDF / DOCX / 이미지)
③ LLM 이력서 자동 파싱  →  스킬 · 경력 구조화
④ 요청 조건 vs 지원자  →  매칭 점수 0~100 산출
⑤ 갭 항목 기반  →  "면접에서 이것만 확인하세요" 체크리스트 자동 생성

수일 걸리던 서류검토  →  수십 분으로 단축
```

---

## ✅ 현재 구현 기능 (v1.2-dev)

- ✅ **시트 단위 자동 분류** — master / aux_skill / reference_roster 혼재 처리
- ✅ **파일/전체 타임아웃** — threading 기반, hang 완전 방지
- ✅ **fingerprint 증분 처리** — 변경된 파일만 재처리
- ✅ **민감정보 자동 드랍** — 주민번호·이메일·핸드폰 ingest 시 제거
- ✅ **raw 서브폴더 구조** — headcount / roster / skills / pricing / org_chart
- ✅ **Bronze / Silver / Gold 4계층** 데이터 아키텍처 설계 완료
- ✅ **QA 검증** — reference roster 대비 누락 탐지
- ✅ **KPI 대시보드** — 총인원·입퇴사·부서별·월별 추세·조기퇴사

<details>
<summary>📋 기술 스택 전체 보기</summary>

<table>
  <tr>
    <td width="160px" align="center"><b>구분</b></td>
    <td><b>기술</b></td>
  </tr>
  <tr>
    <td align="center"><b>Language</b></td>
    <td>
      <img src="https://img.shields.io/badge/Python_3.11-3776AB?style=for-the-badge&logo=python&logoColor=white">
    </td>
  </tr>
  <tr>
    <td align="center"><b>Data Processing</b></td>
    <td>
      <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white">
      <img src="https://img.shields.io/badge/NumPy-013243?style=for-the-badge&logo=numpy&logoColor=white">
      <img src="https://img.shields.io/badge/OpenPyXL-217346?style=for-the-badge&logo=microsoft-excel&logoColor=white">
      <img src="https://img.shields.io/badge/pdfplumber-FF0000?style=for-the-badge">
      <img src="https://img.shields.io/badge/python--docx-2B579A?style=for-the-badge">
      <img src="https://img.shields.io/badge/EasyOCR-4285F4?style=for-the-badge">
    </td>
  </tr>
  <tr>
    <td align="center"><b>AI & LLM</b></td>
    <td>
      <img src="https://img.shields.io/badge/Claude_API-CC785C?style=for-the-badge">
      <img src="https://img.shields.io/badge/RAG-00ADD8?style=for-the-badge">
      <img src="https://img.shields.io/badge/LightGBM-02569B?style=for-the-badge">
      <img src="https://img.shields.io/badge/XGBoost-EB5424?style=for-the-badge">
      <img src="https://img.shields.io/badge/scikit--learn-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white">
    </td>
  </tr>
  <tr>
    <td align="center"><b>ML / NLP</b></td>
    <td>
      <img src="https://img.shields.io/badge/TF--IDF-FF6F00?style=for-the-badge">
      <img src="https://img.shields.io/badge/Fuzzy_Matching-6B46C1?style=for-the-badge">
      <img src="https://img.shields.io/badge/SHAP-FF4B4B?style=for-the-badge">
      <img src="https://img.shields.io/badge/Embedding-00ADD8?style=for-the-badge">
    </td>
  </tr>
  <tr>
    <td align="center"><b>Dashboard</b></td>
    <td>
      <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white">
      <img src="https://img.shields.io/badge/Plotly-3F4F75?style=for-the-badge&logo=plotly&logoColor=white">
    </td>
  </tr>
  <tr>
    <td align="center"><b>Config & Infra</b></td>
    <td>
      <img src="https://img.shields.io/badge/PyYAML-CC0000?style=for-the-badge">
      <img src="https://img.shields.io/badge/SQLite-003B57?style=for-the-badge&logo=sqlite&logoColor=white">
      <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white">
      <img src="https://img.shields.io/badge/PyInstaller-1F3864?style=for-the-badge">
    </td>
  </tr>
  <tr>
    <td align="center"><b>Version Control</b></td>
    <td>
      <img src="https://img.shields.io/badge/git-F05032?style=for-the-badge&logo=git&logoColor=white">
      <img src="https://img.shields.io/badge/github-181717?style=for-the-badge&logo=github&logoColor=white">
    </td>
  </tr>
</table>

</details>

---

## 🗺️ 로드맵

| 버전 | 목표 | 핵심 구현 | 배포 | 기간 |
|---|---|---|---|---|
| **v1.2-stable** | 대시보드 완성 | KPI 정확도 · Attrition Risk · 리스크 탭 · 스킬 탭 | 개발 중 | 2주 |
| **v1.3** | 범용화 기반 | schema mapper · entity resolver · exe 패키징 | **exe 배포** | 3주 |
| **v2.0** | AI 인력 소요 | M2·M3 인력소요+조달 추천 · SCD Type2 · LLM 연동 | 실무 검증 | 6주 |
| **v2.1** | AI 채용 매칭 | M4 이력서 파싱 · 매칭 엔진 · HR 챗봇 (RAG) | Docker | 4주 |
| **v2.2** | 성과·보상 | M5 평가이력 · Compa-ratio · 승진 추천 | — | 4주 |
| **v3.0** | SaaS 전환 | 멀티 테넌트 · 구독 모델 · 클라우드 | SaaS | 장기 |

### 배포 전략

```
Phase 1 (v1.3~v2.0)  →  exe 로컬 배포
  HR_Intelligence.exe — 더블클릭 실행, 민감 데이터 외부 전송 없음

Phase 2 (v2.1)        →  Docker → 사내 서버
  IT팀 설치, 전사 접근, 업데이트 용이

Phase 3 (v3.0)        →  SaaS (클라우드)
  회사별 테넌트 분리, 구독 모델
```

---

## 📄 기획 문서

<div align="center">

| 문서 | 내용 | 링크 |
|---|---|---|
| 📐 완전 기획설계서 v2 | Module 1~5 전체 · 통합 데이터 모델 · 로드맵 | [docs/01_Architecture/](./docs/01_Architecture/HR_Intelligence_Platform_v2_기획설계서.md) |
| 🗃️ 데이터 양식 레지스트리 | 파일 양식 분류 기준 · schema 유형 9가지 | [docs/02_Data/HR_데이터_양식_레지스트리.md](./docs/02_Data/HR_데이터_양식_레지스트리.md) |
| 📊 KPI 계산 로직 | 이직률 · 조기퇴사 · 조직건강도 계산 로직 | [docs/02_Data/HR_KPI_Calculation_Logic.md](./docs/02_Data/HR_KPI_Calculation_Logic_Document.md) |
| 🔧 엔지니어링 노트 | 트러블슈팅 · 기술 결정 · 버그 이력 | [docs/04_Engineering/engineering_notes.md](./docs/04_Engineering/engineering_notes.md) |

</div>

---

## 🚀 시작하기

### 설치

```bash
git clone https://github.com/jaynano44/HR-Dashboard-Automation.git
cd HR-Dashboard-Automation

python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Mac/Linux

pip install -r requirements.txt
```

### 실행

```bash
# 1. HR 파일을 data/raw/ 하위 폴더에 복사
#    data/raw/headcount/   ← 인원현황 xlsx
#    data/raw/roster/      ← 직원 명단

# 2. 전처리 실행
python preprocess.py          # 변경분만 처리
python preprocess.py --force  # 전체 강제 재처리

# 3. 대시보드 실행
streamlit run main.py
```

### preprocess.py 출력 의미

```
처리파일 305 / 실패 0         ← raw 폴더 전체 파일 수
master rows: 1698             ← 전체 스냅샷 행 수 (인원 수 아님)
headcount_active.csv: 126명   ← 현재 재직자 수  ← 이게 정확한 수
headcount_exited.csv: 359명   ← 퇴사자 누적
career_rank.csv: 161명        ← 경력연차 파싱 결과
```

### 폴더 구조

```
HR-Dashboard-Automation/
├── main.py                  ← Streamlit 앱 진입점
├── preprocess.py            ← 전처리 실행 (분리된 단계)
├── config.yaml              ← 설정
├── configs/                 ← 회사별 프로파일 (v1.3~)
│   ├── default.yaml
│   └── iensoft.yaml
├── data/
│   ├── raw/                 ← 원본 HR 파일 (절대 수정 금지)
│   │   ├── headcount/
│   │   ├── roster/
│   │   └── skills/
│   ├── processed/           ← preprocess.py 산출물 CSV
│   └── bronze/              ← 불변 원본 보존 (v1.3~)
├── src/
│   ├── dashboard/app.py     ← Streamlit 대시보드
│   └── processor/           ← 전처리 모듈들
├── outputs/                 ← 모델, 리포트
├── docs/                    ← 기획 문서
└── requirements.txt
```

---

## ⚠️ 주의사항

- `data/raw/` 원본 파일 **절대 수정 금지** — 항상 Bronze 원본 보존
- 전처리 후 반드시 Streamlit **새로고침** 버튼 클릭 (`@st.cache_data` 갱신)
- 민감정보(주민번호·이메일·핸드폰)는 ingest 시 **자동 드랍** — 별도 조치 불필요
- 암호화된 xlsx → `msoffcrypto`로 해제 후 처리

---

<div align="center">

![footer](https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=0,2,5,10,30&height=100&section=footer)

</div>
