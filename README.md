# HR Dashboard Automation

Excel 기반 HR 데이터를 자동 정규화하고 KPI 데이터셋을 생성하는 **HR
데이터 파이프라인 + 분석 대시보드 프로젝트**입니다.

이 프로젝트는 다양한 HR Excel 데이터를 자동 수집·정규화하고, 분석용 HR
KPI 데이터셋을 생성한 뒤 Streamlit 기반 대시보드로 시각화합니다.

------------------------------------------------------------------------

# 프로젝트 목적

많은 조직에서 HR 데이터는 다양한 Excel 파일로 분산 관리됩니다.

대표적인 문제

-   여러 부서에서 서로 다른 HR Excel 양식 사용
-   입사 / 퇴사 기록이 여러 파일에 분산
-   조직명 / 부서명 표기 방식 불일치
-   분석용 데이터셋을 만들기 위해 반복적인 수작업 정리 필요

이 프로젝트는 이러한 문제를 해결하기 위해

**HR Excel 데이터를 자동 수집 → 표준화 → 분석 데이터셋 생성 → 대시보드
시각화**

까지 이어지는 **HR 데이터 파이프라인 시스템**을 구축하는 것을 목표로
합니다.

------------------------------------------------------------------------

# 시스템 아키텍처

Raw HR Excel Files\
↓\
Auto Ingest Engine\
↓\
Canonical HR Dataset (master_auto)\
↓\
Dataset Builder\
↓\
HR KPI Dataset\
↓\
Streamlit Dashboard

------------------------------------------------------------------------

# 주요 기능

## 1. 자동 Ingest

raw 폴더의 여러 Excel 파일을 자동 스캔합니다.

지원 기능

-   여러 시트를 포함한 Excel 파일 처리
-   다양한 HR 양식에서 공통 컬럼 추출
-   표준 HR 스키마로 정규화

표준 HR 컬럼 예시

-   emp_id
-   name
-   org
-   dept
-   join_date
-   exit_date

정규화된 데이터는

    master_auto.csv

로 생성됩니다.

------------------------------------------------------------------------

## 2. 중복 제거

여러 HR 파일에서 동일 인력이 등장할 수 있기 때문에 다음 규칙으로 중복을
제거합니다.

1.  사번(emp_id) 우선
2.  사번이 없는 경우 이름 기반 보조 매칭
3.  퇴사 정보 / 최신 스냅샷 우선 반영

이를 통해 **HR Master Dataset**을 자동 생성합니다.

------------------------------------------------------------------------

## 3. KPI Dataset Builder

master dataset을 기반으로 분석용 KPI 데이터셋을 생성합니다.

생성 데이터셋

-   연도별 총인력 변동
-   월별 입사 / 퇴사 추세
-   부서(팀)별 인력 변화
-   30일 이내 퇴사 분석
-   연간 퇴사율

이 데이터셋은

    data/processed/gold/

레이어에 저장됩니다.

------------------------------------------------------------------------

## 4. QA / 데이터 검증

reference roster 기반 데이터 검증을 수행합니다.

예시

-   reference roster에만 존재하는 인원
-   master dataset에만 존재하는 인원

검증 결과는

    data/processed/qa/

폴더에 저장됩니다.

------------------------------------------------------------------------

## 5. Addons 확장 데이터

HR master 외에도 다음 데이터를 별도로 관리합니다.

-   Skill Inventory
-   Recruit Inventory
-   Reference Roster

이 데이터는

    data/processed/addons/

레이어로 분리하여 관리합니다.

이 구조를 통해 향후 HR 분석 기능 확장을 쉽게 할 수 있도록 설계했습니다.

------------------------------------------------------------------------

# 폴더 구조

    src/
      processor/
      dashboard/

    data/
      raw/
      processed/
        bronze/
        silver/
        gold/
        qa/
        addons/

------------------------------------------------------------------------

# 실행 방법

## 1. 패키지 설치

pip install -r requirements.txt

## 2. 실행

streamlit run main.py

------------------------------------------------------------------------

# 캐시 / 재처리 초기화

전체 재처리를 수행하려면 다음 파일을 삭제합니다.

-   ingest_manifest.json
-   ingest_report.json
-   master_auto.csv

그리고 Streamlit에서 **Clear cache**를 수행합니다.

------------------------------------------------------------------------

# 현재 버전

## v1.1-stable

MVP 이후 **데이터 파이프라인 안정화 버전**

포함 기능

-   raw 자동 ingest 구조
-   multi-sheet Excel 처리
-   canonical HR 컬럼 통합
-   KPI 데이터셋 생성
-   QA 검증 구조
-   addons 확장 데이터 구조
-   Streamlit UI 안정화

------------------------------------------------------------------------

# 이전 버전

## MVP

초기 HR 자동화 대시보드 프로토타입.

### 포함 데이터 (예시)

-   2021\~2024년 입사/퇴사 명단
-   인력 등급 데이터
-   조직도 (참고용)

### 실행

pip install -r requirements.txt

streamlit run main.py

### 산출물

-   data/processed/grade_clean.csv
-   data/processed/turnover_clean.csv
-   data/processed/turnover_yearly.csv
-   data/processed/grade_fixed.csv

------------------------------------------------------------------------

# 다음 버전 계획

v2에서 다음 기능을 확장할 예정입니다.

-   intake validation 구조
-   신규 HR Excel 양식 자동 판별 보조
-   skill / recruit 데이터 정규화
-   HR AI Agent
-   데이터 파이프라인 모니터링
