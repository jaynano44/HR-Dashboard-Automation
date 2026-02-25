# HR 자동화 대시보드 (MVP)

## 포함 데이터(예시)
- 2021~2024년 입사/퇴사 명단 
- 인력등급
- 조직도 (MVP에서는 참고용: 추후 정교 매핑 확장)

## 설치
pip install -r requirements.txt

## 실행
streamlit run main.py

## 산출물
- data/processed/grade_clean.csv
- data/processed/turnover_clean.csv
- data/processed/turnover_yearly.csv
- (선택) data/processed/grade_fixed.csv  # 대화형 보정 결과
