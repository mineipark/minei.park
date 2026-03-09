"""
수요 예측 시스템 설정 상수

모든 하이퍼파라미터, 피처 목록, 모델 설정을
이 파일에 집중시켜 재현성과 튜닝 편의성을 확보합니다.
"""

# ═══════════════════════════════════════════════════════════════
# 데이터 설정
# ═══════════════════════════════════════════════════════════════

DATE_START = '2025-01-01'       # 롤링 윈도우 워밍업 포함 시작일
DATE_END = '2026-02-26'         # 학습 데이터 종료일

MODEL_START = '2025-02-15'      # 사용 가능 데이터 시작 (45일 워밍업 이후)
TRAIN_END = '2026-02-15'        # 학습/테스트 분할 시점
TEST_START = '2026-02-16'       # 테스트 기간 시작

# ═══════════════════════════════════════════════════════════════
# 피처 엔지니어링 파라미터
# ═══════════════════════════════════════════════════════════════

ROLLING_WINDOW = 14             # 롤링 통계 윈도우 (일)
NEIGHBOR_RADIUS_KM = 2.0        # 공간 이웃 탐색 반경 (km)
SUBWAY_RADIUS_M = 500           # POI: 지하철역 인접 반경 (m)

# ═══════════════════════════════════════════════════════════════
# 모델 학습 파라미터
# ═══════════════════════════════════════════════════════════════

A_GROUP_THRESHOLD = 8           # A그룹 기준: 일평균 앱 오픈 최소값
WEIGHT_HALF_LIFE_DAYS = 90      # 지수 감쇠 반감기: 90일 전 → 가중치 0.5
CALIBRATION_WINDOW = 7          # 캘리브레이션 팩터 산출 윈도우 (최근 N일)

B2B_EXCLUDE = [                 # 산업단지 등 일반 수요와 다른 구역 (익명화)
    'industrial_zone_A',
    'industrial_zone_B',
]

# ═══════════════════════════════════════════════════════════════
# LightGBM 하이퍼파라미터
# ═══════════════════════════════════════════════════════════════

OPENS_LGB_PARAMS = {
    'objective': 'regression',
    'metric': ['mae', 'mape'],
    'boosting_type': 'gbdt',
    'num_leaves': 31,
    'learning_rate': 0.03,
    'feature_fraction': 0.7,
    'bagging_fraction': 0.7,
    'bagging_freq': 5,
    'min_child_samples': 30,
    'reg_alpha': 0.1,
    'reg_lambda': 1.0,
    'verbose': -1,
    'seed': 42,
}

RPO_LGB_PARAMS = {
    'objective': 'huber',       # 이상치 전환율에 강건
    'metric': ['mae', 'mape'],
    'boosting_type': 'gbdt',
    'num_leaves': 15,           # 보수적: 적은 리프 수
    'learning_rate': 0.03,
    'feature_fraction': 0.7,
    'bagging_fraction': 0.7,
    'bagging_freq': 5,
    'min_child_samples': 50,    # 보수적: 큰 리프 크기
    'reg_alpha': 0.1,
    'reg_lambda': 1.0,
    'verbose': -1,
    'seed': 42,
}

# ═══════════════════════════════════════════════════════════════
# 피처 목록
# ═══════════════════════════════════════════════════════════════

OPENS_FEATURES = [
    # Stage 1: 셀프 롤링
    'app_opens_rolling', 'avg_bikes_400m_rolling', 'accessibility_rate_rolling',
    'hour_std_rolling', 'opens_cv_rolling',
    # Stage 3: 권역
    'area_district_count', 'area_total_opens_prev', 'area_opens_lag7',
    'area_opens_ma7', 'district_area_share',
    # Stage 4: 시차
    'opens_lag1', 'opens_lag7', 'opens_ma7', 'opens_same_dow_avg',
    'opens_ratio_to_avg', 'opens_wow_change',
    'opens_trend_7d', 'opens_momentum',
    # Stage 5: 캘린더
    'dow', 'is_weekend', 'is_holiday', 'is_off',
    'is_holiday_eve', 'near_holiday', 'days_to_holiday', 'is_consecutive_off',
    # Stage 6: 날씨
    'temp_low', 'temp_high', 'temp_avg', 'rain_sum',
    'is_cold', 'is_rain',
    'windspeed_avg', 'snow_depth', 'is_snow',
    'humidity_avg', 'temp_range',
    'is_heavy_rain', 'is_severe_weather',
    # Stage 7: 인터랙션
    'month', 'rain_off', 'cold_off', 'rain_weekend',
    'is_major_holiday', 'major_holiday_adj',
    'severe_weather_off', 'heavy_rain_off',
    'days_since_major_holiday', 'is_recovery_phase',
    # POI
    'has_subway', 'is_commercial', 'subway_x_off', 'commercial_x_off',
    'commute_ratio', 'evening_ratio', 'subway_x_commute', 'commercial_x_evening',
]

RPO_FEATURES = [
    # Stage 1: 셀프 롤링
    'app_opens_rolling', 'avg_bikes_400m_rolling', 'accessibility_rate_rolling',
    'rides_per_open_rolling', 'hour_std_rolling', 'opens_cv_rolling',
    # Stage 2: 공간 (이웃 + 허브)
    'neighbor_avg_rpo', 'neighbor_avg_bikes_400m', 'neighbor_count',
    'neighbor_weighted_rpo', 'hub_prev_rpo', 'neighbor_max_rpo',
    'hub_distance', 'hub_prev_opens',
    # Stage 3: 권역
    'area_district_count', 'area_avg_rpo_prev', 'area_avg_access_prev',
    # Stage 4: 시차
    'opens_lag1', 'opens_lag7', 'opens_ma7', 'opens_same_dow_avg',
    'opens_ratio_to_avg', 'opens_wow_change', 'rpo_ratio_to_avg',
    'log_opens_rolling', 'rpo_x_opens',
    'opens_trend_7d', 'opens_momentum', 'rpo_momentum',
    # Stage 5: 캘린더
    'dow', 'is_weekend', 'is_holiday', 'is_off',
    'is_holiday_eve', 'near_holiday', 'days_to_holiday', 'is_consecutive_off',
    # Stage 6: 날씨
    'temp_low', 'temp_high', 'temp_avg', 'rain_sum',
    'is_rain',
    'windspeed_avg', 'snow_depth', 'is_snow',
    'humidity_avg', 'temp_range',
    'is_heavy_rain', 'is_severe_weather',
    # Stage 7: 인터랙션
    'month', 'rain_off', 'cold_off', 'rain_weekend',
    'is_major_holiday', 'major_holiday_adj',
    'severe_weather_off', 'heavy_rain_off',
    'days_since_major_holiday', 'is_recovery_phase',
    # POI
    'has_subway', 'is_commercial', 'subway_x_off', 'commercial_x_off',
    'commute_ratio', 'evening_ratio', 'subway_x_commute', 'commercial_x_evening',
]

# ═══════════════════════════════════════════════════════════════
# 후처리 파라미터
# ═══════════════════════════════════════════════════════════════

RPO_SHRINKAGE_ALPHA = 0.6       # 블렌딩: 60% 모델, 40% 롤링 평균
SMALL_DISTRICT_THRESHOLD = 15   # 이 이하 opens → RPO 클리핑 적용
SMALL_DISTRICT_RPO_MULT = 1.15  # RPO 상한 = 권역 중위수 × 배수
CALIBRATION_RANGE = (0.5, 1.5)  # 구역 캘리브레이션 팩터 범위
DOW_CALIBRATION_RANGE = (0.7, 1.3)  # 요일 캘리브레이션 범위
RPO_CLIP_MAX = 3.5              # RPO 최대 허용값
