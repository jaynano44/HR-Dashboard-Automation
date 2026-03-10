# HR Intelligence Platform

> 파편화된 HR Excel 데이터를 자동 수집·표준화하고,  
> KPI 분석 · 퇴사 위험 탐지 · 인력 추천까지 연결되는 **범용 HR 데이터 엔진**

---

## 버전 현황

| 태그 | 설명 | 상태 |
|---|---|---|
| `v1_baseline` | HR Dashboard MVP 초기 프로토타입 | 완료 |
| `v1.1-stable` | 데이터 파이프라인 안정화 + QA 구조 | 완료 |
| `v1.2-dev` | 시트 단위 분류 + 타임아웃 + docs 구조화 | **진행 중** |
| `v1.3` | 범용 schema mapper + entity resolution | 예정 |
| `v2.0` | ML 퇴사 예측 + HR 챗봇 | 예정 |

---

## 시스템 구조

```
Raw HR Excel (다양한 양식)
        ↓
Auto Ingest Engine       ← 시트 단위 자동 분류 + fingerprint
        ↓
Schema Registry          ← master / aux_skill / reference_roster 등 분류
        ↓
Build Dataset            ← 표준 스키마로 정규화 + 중복 제거
        ↓
Bronze / Silver / Gold   ← 원본 보존 → 정제 → KPI 집계
        ↓
Streamlit Dashboard      ← 인력 현황 / 월별 추세 / 조기퇴사 / QA
```

---

## 주요 기능

### v1.2-dev 현재
- **시트 단위 자동 분류** — 한 파일 안에 `재직자(master)` + `프로그래머(aux_skill)` 혼재해도 자동 분리
- **파일/전체 타임아웃** — 파일당 30초, 전체 180초 제한으로 hang 방지
- **증분 처리** — fingerprint 기반으로 변경된 파일만 재처리
- **민감정보 자동 드랍** — 주민번호, 핸드폰, 이메일 ingest 시 자동 제거
- **다양한 날짜 포맷 대응** — Excel serial / YYYYMMDD / 한국식 날짜 자동 변환
- **QA 검증** — reference roster 대비 누락/불일치 인원 자동 탐지

### 대시보드 탭 구성
| 탭 | 내용 |
|---|---|
| 요약 | 총인원 KPI 카드 + master 전체 현황 |
| 총인력변동(연도) | 연도별 입/퇴사/순증감 |
| 월별 추세 | 월별 입/퇴사 라인차트 |
| 부서(팀) 지표 | 팀별 인력 변화 |
| 30일 이내 퇴사 | 조기퇴사자 목록 |
| QA | 데이터 정합성 검증 결과 |
| Addons | 스킬 인벤토리 / 채용 데이터 |

---

## 실행 방법

### 1. 환경 설정

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2. 데이터 전처리

```bash
python preprocess.py          # 신규/변경 파일만 (기본)
python preprocess.py --force  # 전체 강제 재처리
python preprocess.py --check  # 신규 파일 유무만 확인
```

### 3. 대시보드 실행

```bash
streamlit run main.py
```

---

## 폴더 구조

```
HR-Dashboard-Automation_MVP/
├── src/
│   ├── processor/
│   │   ├── auto_ingest_multi.py    ← 핵심 ingest 엔진 (시트 단위 분류)
│   │   ├── schema_registry.py      ← 파일/시트 양식 분류 규칙
│   │   ├── build_dataset.py        ← Gold CSV 생성
│   │   ├── preprocess.py           ← CLI 전처리 진입점
│   │   ├── career_rank_loader.py   ← 경력연차 파싱
│   │   ├── headcount_2024_loader.py
│   │   ├── org_standardize.py
│   │   └── metrics.py / metrics_dept.py
│   └── dashboard/
│       └── app.py                  ← Streamlit 대시보드
├── data/
│   ├── raw/                        ← 원본 Excel (수정 금지)
│   │   ├── headcount/
│   │   ├── roster/
│   │   ├── skills/
│   │   └── ...
│   └── processed/                  ← 전처리 산출물
│       ├── bronze/
│       ├── silver/
│       ├── gold/
│       ├── qa/
│       └── addons/
├── docs/
│   ├── 01_Architecture/            ← 플랫폼 설계서
│   ├── 02_Data/                    ← 데이터 모델 / KPI 로직
│   ├── 04_Engineering/             ← 기술 결정 / 트러블슈팅
│   └── 05_archive/                 ← 구버전 문서 보관
├── config.yaml                     ← 경로 / 컬럼 설정
├── preprocess.py                   ← 전처리 CLI
└── main.py                         ← 대시보드 진입점
```

---

## 캐시 초기화 (전체 재처리 필요 시)

```powershell
Remove-Item "data\processed\ingest_manifest.json" -ErrorAction SilentlyContinue
Remove-Item "data\processed\ingest_report.json" -ErrorAction SilentlyContinue
Remove-Item "data\processed\master_auto.csv" -ErrorAction SilentlyContinue
```

이후 `python preprocess.py --force` 실행

---

## 로드맵

```
v1.2  KPI 정확도 + Attrition Risk 안정화 + 로컬 exe 배포
v1.3  범용 schema mapper + entity resolver + multi-profile config
v2.0  ML 퇴사 예측 + 조직 건강도 + HR 챗봇 (Claude API RAG)
v2.1  인력 추천 엔진 + 채용 매칭
v3.0  성과/보상/진급 + 시뮬레이터
```

---

## 기술 스택

`Python 3.11` `Pandas` `Streamlit` `OpenPyXL` `Plotly` `PyYAML`

---

*현재 버전: v1.2-dev | 다음 목표: v1.2-stable*
