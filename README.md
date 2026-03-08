# HR 자동화 대시보드 (MVP)

## 포함 데이터(예시)
- 2021~2024년 입사/퇴사 명단 (비밀번호: 1111)
- 인력등급
- 조직도 (MVP에서는 참고용: 추후 정교 매핑 확장)

## 설치
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
