# 수요 예측 시스템 (Demand Forecast)

BikeShare 전기자전거 **District-level 수요 예측** 시스템 (Production v2)

## 핵심 아키텍처

```
predicted_rides = predicted_app_opens × predicted_rpo
```

- **Opens 모델** (LightGBM): "내일 이 district에서 앱을 여는 사람이 몇 명일까?"
- **RPO 모델** (LightGBM): "앱을 연 사람 중 몇 %가 실제 라이딩할까?"
- 두 예측을 곱해 최종 라이딩 건수를 산출

## 시스템 구조

```
┌───────────────────────────────────────────────────────────────┐
│                Production v2 파이프라인                         │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  BigQuery (앱오픈/라이딩/공휴일)                                │
│       │                                                       │
│       ▼                                                       │
│  district_production_pipeline.py                              │
│  (학습: 피처 생성 → Opens/RPO 모델 → Calibration → 저장)       │
│       │                                                       │
│       ▼                                                       │
│  models/production_v2.pkl                                     │
│  (Opens 모델 + RPO 모델 + Calibration 번들)                   │
│       │                                                       │
│       ▼                                                       │
│  production_v2_predictor.py  ←──  weather CSV + 예보 API      │
│  (예측 모듈: 단일 날짜/기간 평가)                               │
│       │                                                       │
│  ┌────┼────────────────┐                                      │
│  ▼    ▼                ▼                                      │
│  supply_demand_gap   route_optimization_v5                    │
│  (갭 분석+작업지시서)  (동선 최적화)                             │
│       │                                                       │
│       ▼                                                       │
│  relocation_task_system                                       │
│  (재배치 시스템)                                               │
│                                                               │
├───────────────────────────────────────────────────────────────┤
│                    날씨 데이터 파이프라인                        │
│                                                               │
│  auto_update_weather.py  ──→  weather_2025_202601.csv         │
│  (기상청 ASOS API, 매일 자동)   (관측 데이터)                    │
│                                                               │
│  fetch_weather_forecast.py ──→ 단기예보 API (최대 3일)         │
│  (미래 날짜 예보 + ASOS 보완)                                  │
│                                                               │
├───────────────────────────────────────────────────────────────┤
│                    레거시 자동 학습 루프                         │
│                                                               │
│  district_hour_tuner ──→ district_hour_params.json            │
│  auto_improve ──→ region_params.json                          │
└───────────────────────────────────────────────────────────────┘
```

## 핵심 파일

### 예측 엔진 (Production v2)

| 파일 | 역할 |
|------|------|
| `district_production_pipeline.py` | 학습 파이프라인 (데이터→피처→모델학습→평가→저장) |
| `production_v2_predictor.py` | **예측 모듈** (대시보드/CLI 통합용) |
| `models/production_v2.pkl` | 학습된 모델 번들 (Opens+RPO+Calibration) |

### 피처 구조 (7단계)

| 단계 | 카테고리 | 주요 피처 | 대상 모델 |
|------|----------|----------|----------|
| 1 | Self Rolling | app_opens_rolling, rides_per_open_rolling, opens_cv 등 | Opens + RPO |
| 2 | Neighbor + Hub | neighbor_avg_rpo, hub_prev_rpo, neighbor_weighted_rpo 등 | RPO |
| 3 | Area | area_total_opens_prev, district_area_share 등 | Opens + RPO |
| 4 | Lag | opens_lag1/7, opens_same_dow_avg, 변화율 피처 등 | Opens + RPO |
| 5 | Calendar | dow, is_off, is_holiday, is_major_holiday 등 | Opens + RPO |
| 6 | Weather | temp, rain, windspeed, humidity, snow_depth 등 | Opens + RPO |
| 7 | Interaction | rain_off, cold_off, month, major_holiday_adj 등 | Opens + RPO |

### 후처리 (Post-processing)

| 처리 | 설명 |
|------|------|
| RPO Shrinkage | 모델 예측 → rolling 평균 방향 블렌딩 (alpha=0.6) |
| 소형 RPO 클리핑 | opens < 15인 district의 RPO를 area 중앙값 × 1.15로 상한 제한 |
| District Calibration | 학습 데이터의 district별 bias 보정 (0.5~1.5 범위) |
| 요일 Calibration | 요일별 체계적 bias 보정 (0.7~1.3 범위) |
| 명절 감쇄 | 설/추석 등 대형연휴 추가 감쇄 factor 적용 |

### 운영 시스템

| 파일 | 역할 |
|------|------|
| `supply_demand_gap.py` | 수요-공급 갭 분석 → 센터별 시간대별 작업지시서 (Excel/Map) |
| `route_optimization_v5.py` | 현장 작업 동선 최적화 |
| `relocation_task_system.py` | 재배치 시스템 (district_hour_model 연동) |
| `district_hour_model.py` | district × hour 수요 배분 |

### 레거시 (V7 기반)

| 파일 | 역할 |
|------|------|
| `demand_model_v7.py` | V7 기본 예측 모델 (region 일별) |
| `visualize_prediction_map.py` | quick_predict() 경량 예측 + 지도 시각화 |
| `district_hour_tuner.py` | 3단계 자동 학습 (매일 Level1, 주1회 Level2, ML Level3) |
| `auto_improve.py` | region_params.json 자동 보정 |

### 설정 / 데이터

| 파일 | 역할 |
|------|------|
| `models/production_v2.pkl` | Production v2 모델 번들 |
| `models/opens_feature_importance.csv` | Opens 모델 피처 중요도 |
| `models/rpo_feature_importance.csv` | RPO 모델 피처 중요도 |
| `region_params.json` | 101개 region 파라미터 (센터 매핑 포함) |
| `district_hour_params.json` | 보정된 district 비율 + hour 비율 |
| `weather_2025_202601.csv` | 날씨 관측 데이터 (기상청 ASOS) |
| `korean_holidays.py` | 한국 공휴일 목록 |

### 유틸리티

| 파일 | 역할 |
|------|------|
| `auto_update_weather.py` | 기상청 ASOS API 날씨 자동 수집 (GitHub Actions) |
| `fetch_weather_forecast.py` | 단기예보 API (미래 3일) + ASOS 보완 |
| `fetch_weather.py` | 날씨 데이터 수동 수집 |
| `sheets_sync.py` | Google Sheets 예측 결과 동기화 |
| `export_rolling_excel.py` | 롤링 예측 엑셀 출력 |

## 사용법

### 모델 학습

```bash
# 전체 파이프라인 실행 (데이터 추출 → 피처 → 학습 → 평가 → 저장)
python3 district_production_pipeline.py
```

학습 설정:
- 데이터 기간: 2025-08-01 ~ 2026-02-26 (추석+설 포함)
- Train: ~2026-02-15, Test: 2026-02-16~
- A그룹 기준: 일평균 앱오픈 > 8
- B2B 제외: 현대미포조선, 삼성디지털시티

### 수요 예측

```python
from production_v2_predictor import predict_district_rides, evaluate_period

# 단일 날짜 예측 (미래 또는 과거)
district_df, region_df = predict_district_rides('2026-02-27')

# 기간별 배치 평가 (주간 성과용)
eval_df = evaluate_period('2026-02-20', '2026-02-26')
```

```bash
# CLI 테스트
python3 production_v2_predictor.py 2026-02-27
```

### 갭 분석 + 작업지시서

```bash
python3 supply_demand_gap.py --date 2026-02-27
python3 supply_demand_gap.py --date 2026-02-27 --center Center_North
```

### 동선 최적화

```bash
python3 route_optimization_v5.py
```

## 모델 상세

### Opens 모델

- 알고리즘: LightGBM (GBDT, regression)
- 피처 수: 37개
- Top 5 피처: opens_ma7, opens_same_dow_avg, opens_lag1, app_opens_rolling, is_off
- 하이퍼파라미터: num_leaves=31, lr=0.03, feature_fraction=0.7

### RPO 모델

- 알고리즘: LightGBM (GBDT, **huber** loss — 이상치 robust)
- 피처 수: 40개 (Neighbor/Hub 피처 포함)
- Top 5 피처: rides_per_open_rolling, area_avg_rpo_prev, neighbor_weighted_rpo, rpo_x_opens, avg_bikes_400m_rolling
- 하이퍼파라미터: num_leaves=15, min_child_samples=50
- 전처리: RPO=0 제외, RPO > 3.5 클리핑

### 학습 데이터 흐름

```
BigQuery
├── bike_accessibility_raw → district×date 앱오픈/접근성 집계
├── tf_riding → district×date 라이딩 건수
├── korean_holiday → 공휴일 목록
└── weather CSV → 기온/강수/풍속/습도/적설

→ 7단계 피처 엔지니어링
→ Train/Test split (시간 기반)
→ Opens/RPO 분리 학습
→ Calibration Factor 산출
→ production_v2.pkl 저장
```

## 디렉토리 구조

```
demand_forecast/
├── 예측 엔진 (Production v2)
│   ├── district_production_pipeline.py   # 학습 파이프라인
│   └── production_v2_predictor.py        # 예측 모듈
├── 모델/데이터
│   ├── models/
│   │   ├── production_v2.pkl             # 모델 번들
│   │   ├── opens_feature_importance.csv
│   │   └── rpo_feature_importance.csv
│   ├── region_params.json
│   ├── district_hour_params.json
│   └── weather_2025_202601.csv
├── 운영 시스템
│   ├── supply_demand_gap.py
│   ├── route_optimization_v5.py
│   ├── relocation_task_system.py
│   └── district_hour_model.py
├── 날씨 파이프라인
│   ├── auto_update_weather.py            # ASOS 자동 수집
│   ├── fetch_weather_forecast.py         # 단기예보 + ASOS 보완
│   └── fetch_weather.py                  # 수동 수집
├── 레거시
│   ├── demand_model_v7.py
│   ├── visualize_prediction_map.py
│   ├── district_hour_tuner.py
│   └── auto_improve.py
├── 유틸리티
│   ├── sheets_sync.py
│   ├── export_rolling_excel.py
│   └── korean_holidays.py
├── visualizations/                       # 출력물 (지도 HTML, Excel)
└── archive/                              # 구버전 파일 보관
    ├── models_old/                       # v1~v6, v8, location 모델
    ├── route_old/                        # 동선 v1~v4
    ├── analysis/                         # 1회성 분석 스크립트
    ├── visualize_old/
    └── misc/
```

## 대상 센터

내부 (5): Center_North, Center_West, Center_South, Center_Central, Center_East
CJ 외주 (4): Partner_Seoul, Partner_Daejeon, Partner_Gwacheon, Partner_Ansan

## 환경 설정

```bash
# BigQuery 인증
GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json

# 의존성
pip install google-cloud-bigquery pandas numpy lightgbm scipy folium openpyxl
```

## CI/CD

- `weather-update.yml`: 매일 오전 9시 (KST) 기상청 ASOS 날씨 자동 수집
- `forecast-sync.yml`: 예측 결과 동기화
