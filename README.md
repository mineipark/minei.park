# 공유 모빌리티 운영 플랫폼

**수요 예측**, **수급 갭 분석**, **경로 최적화**, **실시간 운영 대시보드**, **워크플로우 자동화**를 아우르는 공유 모빌리티 운영 인텔리전스 플랫폼입니다.

PMO(Project Management Office) 팀원으로 근무하며, 차량 가동률 최적화, 운영 비용 절감, 데이터 기반 의사결정을 통한 매출 극대화를 위해 구축했습니다.

> **참고:** 포트폴리오용으로 정제된 버전입니다. 회사 고유 데이터, 인증 정보, 식별 가능한 정보는 모두 제거 또는 일반화되었습니다.

---

## 아키텍처

```
┌──────────────────────────────────────────────────────────────┐
│                      데이터 소스                               │
│  BigQuery  ·  기상청 API  ·  Google Sheets  ·  앱 이벤트      │
└──────────┬───────────────────────────────────┬───────────────┘
           │                                   │
           ▼                                   ▼
┌─────────────────────┐         ┌──────────────────────────────┐
│    수요 예측 모델     │         │       운영 대시보드            │
│  ─────────────────  │         │  ──────────────────────────  │
│  · ML 모델 (V7/V8)  │         │  · Streamlit 멀티페이지 앱    │
│  · 전환율 모델       │         │  · 센터별 KPI                │
│  · 구역×시간대 모델  │         │  · 작업자 동선 추적           │
│  · 날씨 보정         │         │  · 유지보수 성과 분석         │
└────────┬────────────┘         └──────────────────────────────┘
         │
         ▼
┌─────────────────────┐         ┌──────────────────────────────┐
│   수급 갭 분석       │         │      워크플로우 자동화         │
│  ─────────────────  │         │  ──────────────────────────  │
│  · 갭 분석           │         │  · 이메일 → AI 파서           │
│  · 작업 지시서 생성  │         │  · Slack 봇 승인 처리         │
│  · 우선순위 스코어링 │         │  · 관제웹 자동화              │
│  · Folium 지도       │         │  · 반납구역 처리              │
└────────┬────────────┘         └──────────────────────────────┘
         │
         ▼
┌─────────────────────┐         ┌──────────────────────────────┐
│    경로 최적화       │         │       자동 리포트              │
│  ─────────────────  │         │  ──────────────────────────  │
│  · TSP 솔버          │         │  · 월간 기기 현황 리포트      │
│  · 클러스터 기반     │         │  · Slack 연동                 │
│  · 시간대 분리       │         │  · GitHub Actions CI/CD      │
│  · AntPath 시각화    │         │  · Google Sheets 동기화       │
└─────────────────────┘         └──────────────────────────────┘
```

---

## 주요 모듈

### 1. 수요 예측 (`demand_forecast/`)

ML 기반 구역×시간대 수요 예측 시스템

- **V7 모델** (`demand_model_v7.py`): GradientBoosting + 권역별 날씨/요일 보정
- **V8 앱오픈 모델** (`app_open_model.py`): 공급 제약 데이터의 순환 의존성을 해결하기 위해 앱 오픈을 먼저 예측한 뒤 전환율을 적용
- **전환율 모델** (`conversion_model.py`): 지수 포화 곡선으로 바이크 수 → 전환율 학습: `CVR = base + gain * (1 - e^(-decay * bikes))`
- **구역-시간 모델** (`district_hour_model.py`): 권역 → 구역 → 시간대 계층적 분해
- **자동 튜너** (`district_hour_tuner.py`, `auto_improve.py`): 백테스팅 기반 파라미터 자동 최적화

### 2. 수급 갭 분석 (`demand_forecast/supply_demand_gap.py`)

예측 수요와 현재 공급을 비교하여 실행 가능한 작업 지시서 생성

- 시간대별 분석 (야간 준비 / 오전 / 오후 / 저녁)
- 비제약 수요 가중치 기반 우선순위 스코어링
- 센터별 Excel 작업 지시서 출력
- Folium 기반 수급 갭 시각화 지도

### 3. 경로 최적화 (`demand_forecast/route_optimization_v5.py`)

리밸런싱 및 배터리 교체 작업자의 이동 경로 최적화

- 시간대 분리 (오후 수요 대응 vs. 저녁 사전 배치)
- 수요 클러스터 기반 TSP 라우팅
- 작업 유형 묶음 처리 (리밸런싱, 배터리, 수리)
- Folium AntPath 애니메이션 경로 시각화

### 4. 운영 대시보드 (`ops_dashboard/`)

멀티페이지 Streamlit 실시간 운영 모니터링 대시보드

- **전센터 대시보드**: 차량 가동률, 수리율, 현장조치율 등 KPI
- **센터별 대시보드**: 서비스센터별 상세 지표
- **직원별 동선**: GPS 기반 작업자 이동 경로 시각화
- **인원별 월간**: 작업자별 월간 실적 통계
- **유지보수 성과**: 수리 효율, 비용 분석

### 5. 반납구역 승인 자동화 (`return_zone_approval/`)

반납구역 요청 처리 End-to-End 자동화

- Gmail 수신 모니터링
- AI 기반 문서 파싱 (Claude API)
- Slack 봇 인터랙티브 승인 워크플로우
- Playwright 기반 관제웹 자동화

### 6. 자동 리포트 (`bike_stats_report.py`)

월간 기기 현황 리포트 자동 생성 및 Slack 전송

- BigQuery 데이터 집계
- Slack Block Kit 메시지 포매팅
- GitHub Actions 스케줄 실행

---

## 기술 스택

| 분류 | 기술 |
|------|------|
| **언어** | Python 3.11 |
| **데이터 웨어하우스** | Google BigQuery |
| **ML/통계** | LightGBM, scikit-learn, SciPy (curve fitting) |
| **시각화** | Streamlit, Folium, Plotly |
| **공간 분석** | H3 헥사곤 인덱싱, GeoJSON |
| **자동화** | Playwright, Gmail API, Slack Bolt |
| **AI** | Anthropic Claude API (문서 파싱) |
| **인프라** | GitHub Actions, Firebase |
| **연동** | Google Sheets API, Slack API, 기상청 API |

---

## 설치 가이드

### 사전 요구사항

- Python 3.11+
- Google Cloud 서비스 계정 (BigQuery 접근 권한) 또는 샘플 데이터 사용

### 설치

```bash
# 레포지토리 클론
git clone https://github.com/mineipark/minei.park.git
cd minei.park

# 가상환경 생성
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# 의존성 설치 (모듈별 선택)
pip install pandas numpy scikit-learn scipy google-cloud-bigquery folium
# 대시보드용:
pip install -r ops_dashboard/requirements.txt
# 반납구역 승인용:
pip install -r return_zone_approval/requirements.txt

# 환경 변수 설정
cp .env.example .env
# .env 파일에 실제 값 입력
```

### 샘플 데이터 생성

BigQuery 접근이 없는 경우 합성 데이터 생성:

```bash
python seed_data.py --days 90 --bikes 500
```

### 대시보드 실행

```bash
cd ops_dashboard
streamlit run ops_worker_dashboard.py
```

### 수요 예측 실행

```bash
# 단일 날짜 예측
python demand_forecast/app_open_model.py --date 2026-02-25

# 수급 갭 분석
python demand_forecast/supply_demand_gap.py --date 2026-02-25

# 경로 최적화
python demand_forecast/route_optimization_v5.py
```

---

## 프로젝트 구조

```
.
├── demand_forecast/           # ML 수요 예측 및 최적화
│   ├── app_open_model.py      # V8: 앱오픈 기반 예측
│   ├── demand_model_v7.py     # V7: 권역 보정 모델
│   ├── conversion_model.py    # 바이크 수 → 전환율
│   ├── district_hour_model.py # 구역×시간대 분해
│   ├── supply_demand_gap.py   # 수급 갭 분석 + 작업 지시서
│   ├── route_optimization_v5.py # 작업자 경로 최적화
│   ├── relocation_task_system.py # 저녁 리밸런싱 지원
│   ├── daily_pipeline.py      # 일일 자동 예측 파이프라인
│   └── ...
├── ops_dashboard/             # Streamlit 운영 대시보드
│   ├── ops_worker_dashboard.py # 메인 앱 진입점
│   ├── pages/                 # 멀티페이지 대시보드 뷰
│   └── utils/                 # BigQuery, Sheets, 계산 헬퍼
├── return_zone_approval/      # 반납구역 승인 자동화
│   ├── main.py                # 오케스트레이터
│   ├── email_monitor/         # Gmail 연동
│   ├── parser/                # AI 문서 파서
│   ├── slack_bot/             # Slack 인터랙티브 봇
│   ├── automation/            # 관제웹 자동화
│   └── workflow/              # 승인 상태 머신
├── service_flow_visualizer/   # 서비스 플로우 지도 시각화
├── reallocation/              # 바이크 재배치 알고리즘
├── bike_stats_report.py       # 월간 기기 현황 → Slack
├── seed_data.py               # 샘플 데이터 생성기
├── .env.example               # 환경 변수 템플릿
└── .github/workflows/         # CI/CD 자동화
```

---

## 핵심 지표

| 지표 | 산식 | 설명 |
|------|------|------|
| **접근성률** | `접근 가능 앱오픈 / 전체 앱오픈` | 100m 이내 바이크가 있는 앱오픈 비율 |
| **전환율** | `라이딩 수 / 접근 가능 앱오픈` | 접근 가능한 사용자 중 실제 라이딩 비율 |
| **가동률** | `사용 가능 바이크 / 전체 바이크` | 라이딩 가능한 차량 비율 |
| **현장조치율** | `사용 가능 바이크 / 현장 바이크` | 현장 배치 차량 중 라이딩 가능 비율 |
| **수급 갭** | `예측 수요 - 가용 바이크` | 구역별 필요 바이크 수 |

### 분석 퍼널

```
앱 오픈 → 접근 가능 (100m 이내 바이크) → 전환 (실제 라이딩)
  │              │                              │
  └─ Stage 1     └─ Stage 2                     └─ 매출
     이탈:          이탈:
     공급 부족       품질/UX 이슈
```

---

## 라이선스

본 프로젝트는 포트폴리오 및 학습 목적으로 공유됩니다. 코드 아키텍처와 알고리즘은 본인의 작업물이며, 모든 사업 관련 데이터는 익명화되었습니다.
