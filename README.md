# Minei Park — Operations & Data Portfolio

공유 모빌리티 운영기획 실무에서 설계·구현한 시스템들을 정리한 포트폴리오입니다.
수요 예측, 실시간 모니터링, 자동화 워크플로우 등 데이터 기반 운영 최적화 프로젝트를 포함합니다.

---

## 프로젝트 목록

### 1. [demand_forecast_portfolio/](./demand_forecast_portfolio/)
**공유 모빌리티 수요 예측 시스템**

구역(District) 단위 일별 라이딩 수요를 예측하는 2단계 앙상블 모델 시스템.
앱 오픈 수 예측과 전환율(RPO) 예측을 결합하여 최종 라이딩 수를 산출합니다.

- **핵심 구조**: `예측 라이딩 = 예측 앱오픈 × 예측 RPO`
- **피처 엔지니어링**: 7단계 (롤링 → 공간 → 권역 → 시차 → 캘린더 → 날씨 → 인터랙션)
- **후처리**: RPO 축소, 소규모 구역 클리핑, 구역/요일 보정, 공휴일 감쇠
- **Skills**: `Python` `LightGBM` `Pandas` `SciPy` `H3 Spatial Indexing` `Feature Engineering` `Time Series`

---

### 2. [demand_forecast/](./demand_forecast/)
**수요 예측 운영 파이프라인**

실제 운영 환경에서 매일 자동 실행되는 수요 예측 파이프라인.
날씨 데이터 수집, 모델 예측, 스프레드시트 동기화, 수급 갭 분석까지 엔드투엔드 자동화.

- **일일 자동화**: 날씨 수집 → 예측 → 실적 비교 → 리포트
- **수급 갭 분석**: 구역×시간대 수요 분해 → 재배치 태스크 자동 생성
- **CI/CD**: GitHub Actions 기반 일별/월별 스케줄링
- **Skills**: `Python` `BigQuery` `Google Sheets API` `GitHub Actions` `Weather API` `CI/CD`

---

### 3. [ops_dashboard/](./ops_dashboard/)
**운영 모니터링 대시보드**

센터별 실시간 운영 현황을 모니터링하는 멀티페이지 대시보드.
역할 기반 접근 제어(RBAC)로 센터 직원, 외주 파트너, 관리자별 뷰를 분리.

- **7개 페이지**: 전체 현황, 센터별, 작업자 동선, 월간 성과, 정비 KPI, 관리자, 경쟁사 비교
- **실시간 추적**: 현장 작업자 위치 및 이동 경로 시각화
- **생산성 분석**: 작업자별 처리량, 교대 분석, 센터 간 효율 비교
- **Skills**: `Streamlit` `BigQuery` `Folium` `Plotly` `RBAC` `Geospatial`

---

### 4. [reallocation/](./reallocation/)
**기기 재배치 최적화 알고리즘**

H3 헥사곤 기반 수급 분석으로 과잉 공급 지역을 식별하고 최적 수거 대상을 선정하는 알고리즘.

- **수급 비율 분석**: 헥사곤 단위 수요/공급 비율 산출
- **수거 대상 선정**: 잉여 지역에서 저수요 기기 자동 선별
- **특수 규칙**: 피크 시간대, 특정 구역별 예외 처리
- **Skills**: `Python` `H3 Indexing` `Pandas` `Spatial Analysis` `Optimization`

---

### 5. [return_zone_approval/](./return_zone_approval/)
**반납존 승인 자동화 워크플로우**

Slack/이메일로 들어오는 반납존 변경 요청을 AI가 파싱하고, 승인 후 관리자 웹에서 자동 처리하는 시스템.

- **멀티채널 모니터링**: Slack 실시간 + Gmail 이메일 동시 감시
- **AI 파싱**: Claude API로 요청 의도 구조화 (구역, 작업 유형, 사유)
- **브라우저 자동화**: 승인 시 Playwright로 관리자 웹 자동 입력
- **이력 관리**: TinyDB 기반 요청 감사 추적
- **Skills**: `Slack Bot SDK` `Gmail API` `Claude API` `Playwright` `TinyDB` `Event-Driven`

---

### 6. [service_flow_visualizer/](./service_flow_visualizer/)
**서비스 흐름 실시간 시각화**

라이딩, 앱 오픈, 정비 작업 이벤트를 지도 위에 애니메이션/스냅샷으로 시각화하는 대시보드.

- **듀얼 뷰**: 타임라인 애니메이션 (재생 컨트롤) + 시점 스냅샷
- **이벤트 필터링**: 유형별, 센터별, 권역별, 시간대별 필터
- **실시간 통계**: 전환율, 수급 갭, 배터리 교체, 재배치 현황
- **Skills**: `Streamlit` `Folium` `BigQuery` `Real-time Visualization` `Event Processing`

---

### 7. [bike_stats_report.py](./bike_stats_report.py)
**월간 자동 리포트**

매월 1일 BigQuery에서 플릿 통계를 집계하여 Slack으로 자동 발송하는 리포트 스크립트.

- **자동 실행**: GitHub Actions 월간 스케줄 (매월 1일 09:00 KST)
- **집계 항목**: 가용률, 기기 타입 변경, 프랜차이즈 현황
- **센터별 분석**: 10개 센터 × 기기 유형별 통계
- **Skills**: `BigQuery` `Slack API` `GitHub Actions` `Pandas`

---

### 8. [seed_data.py](./seed_data.py)
**합성 데이터 생성기**

운영 데이터 스키마를 그대로 반영한 합성 데이터를 생성하여 로컬 개발·테스트 환경을 지원.

- **9개 센터**, 55개 구역, 2,000+ 기기 생성
- **시계열 스냅샷**: 날짜×시간대별 기기 상태 분포
- **라이딩 세션**: 출발/도착, 거리, 요금, 소요 시간
- **Skills**: `Python` `Pandas` `NumPy` `Data Modeling`

---

## 기술 스택 요약

| 분류 | 기술 |
|------|------|
| **언어** | Python |
| **ML/통계** | LightGBM, scikit-learn, SciPy, NumPy |
| **데이터** | BigQuery, Pandas, Google Sheets |
| **대시보드** | Streamlit, Folium, Plotly |
| **자동화** | GitHub Actions, Slack Bot, Playwright |
| **AI** | Claude API (자연어 파싱) |
| **공간 분석** | H3 Hexagonal Indexing, Geospatial |
| **인프라** | Google Cloud Platform, CI/CD |
