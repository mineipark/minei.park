"""
District-level 수요 예측 모델 v1
공간 피처 + 시계열 피처 기반 LightGBM

Pipeline:
  Step 1: BigQuery → Daily Aggregation (district×date)
  Step 2: Level 피처 (self rolling, neighbor, area)
  Step 3: Variation 피처 (lag, calendar, weather)
  Step 4: Train/Test split
  Step 5: LightGBM 학습 + 평가

사용법:
    python district_prediction_model.py
"""

import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# ─── 설정 ────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BQ_CREDENTIALS = os.path.expanduser('~/Downloads/service-account.json')

# 분석 기간
DATE_START = '2025-12-01'  # 롤링 윈도우 워밍업 포함
DATE_END = '2026-02-25'

# 모델 기간 (워밍업 제외)
MODEL_START = '2026-01-01'
TRAIN_END = '2026-02-15'   # 학습 종료
TEST_START = '2026-02-16'  # 테스트 시작

# A그룹 기준 (일평균 앱오픈 > 5)
A_GROUP_THRESHOLD = 5

# Level 피처 롤링 윈도우
ROLLING_WINDOW = 14  # 14일 이동평균

# 이웃 반경 (km)
NEIGHBOR_RADIUS_KM = 2.0


# ─── Step 1: Raw → Daily Aggregation ─────────────────────────────

def fetch_daily_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """BigQuery에서 district×date 일별 집계 데이터 추출"""
    from google.cloud import bigquery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_CREDENTIALS
    client = bigquery.Client()

    print("[Step 1] BigQuery에서 일별 데이터 추출 중...")

    # 1-1. 앱오픈 일별 집계
    q_opens = f'''
    SELECT
        date,
        h3_area_name,
        h3_district_name,
        COUNT(*) as app_opens,
        AVG(bike_count_100) as avg_bikes_100m,
        AVG(bike_count_400) as avg_bikes_400m,
        AVG(distance) as avg_distance,
        AVG(IF(is_accessible, 1, 0)) as accessibility_rate,
        AVG(IF(is_converted, 1, 0)) as conversion_rate,
        SAFE_DIVIDE(
            SUM(IF(is_converted, 1, 0)),
            SUM(IF(is_accessible, 1, 0))
        ) as cond_conversion_rate,
        STDDEV(EXTRACT(HOUR FROM event_time)) as hour_std,
        AVG(ST_Y(location)) as center_lat,
        AVG(ST_X(location)) as center_lng
    FROM service.bike_accessibility_raw
    WHERE date BETWEEN "{DATE_START}" AND "{DATE_END}"
        AND near_geoblock = True
    GROUP BY date, h3_area_name, h3_district_name
    '''

    # 1-2. 라이딩 일별 집계
    q_rides = f'''
    SELECT
        start_date as date,
        h3_start_district_name as h3_district_name,
        COUNT(*) as rides,
        AVG(fee) as avg_fee,
        AVG(distance) as avg_ride_distance
    FROM service.tf_riding
    WHERE start_date BETWEEN "{DATE_START}" AND "{DATE_END}"
        AND bike_type = 1
    GROUP BY start_date, h3_start_district_name
    '''

    df_opens = client.query(q_opens).to_dataframe()
    df_rides = client.query(q_rides).to_dataframe()

    print(f"  앱오픈: {len(df_opens):,}행, 라이딩: {len(df_rides):,}행")

    # 날짜 타입 통일
    df_opens['date'] = pd.to_datetime(df_opens['date'])
    df_rides['date'] = pd.to_datetime(df_rides['date'])

    return df_opens, df_rides


def merge_daily_data(df_opens: pd.DataFrame, df_rides: pd.DataFrame) -> pd.DataFrame:
    """앱오픈 + 라이딩 merge → district×date 데이터"""
    df = df_opens.merge(
        df_rides[['date', 'h3_district_name', 'rides']],
        on=['date', 'h3_district_name'],
        how='left'
    )
    df['rides'] = df['rides'].fillna(0).astype(int)
    df['rides_per_open'] = df['rides'] / df['app_opens']

    print(f"  Merged: {len(df):,}행, {df.h3_district_name.nunique()} districts")
    return df


# ─── Step 2: Level 피처 생성 ─────────────────────────────────────

def create_self_features(df: pd.DataFrame) -> pd.DataFrame:
    """Self Features: 롤링 통계 기반 district 자체 특성"""
    print("[Step 2-1] Self 피처 생성 (롤링 윈도우)...")

    df = df.sort_values(['h3_district_name', 'date']).copy()

    rolling_cols = ['app_opens', 'avg_bikes_400m', 'accessibility_rate',
                    'rides', 'rides_per_open', 'hour_std']

    for col in rolling_cols:
        # 자기 자신 날짜 제외 (shift 1) 후 14일 이동평균
        df[f'{col}_rolling'] = (
            df.groupby('h3_district_name')[col]
            .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=3).mean())
        )

    # 앱오픈 변동성 (CV)
    df['opens_cv_rolling'] = (
        df.groupby('h3_district_name')['app_opens']
        .transform(lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=3).std()
                   / x.shift(1).rolling(ROLLING_WINDOW, min_periods=3).mean())
    )

    print(f"  Self 피처 {len(rolling_cols) + 1}개 생성 완료")
    return df


def create_neighbor_features(df: pd.DataFrame) -> pd.DataFrame:
    """Neighbor Features: 좌표 기반 인접 district 특성"""
    print("[Step 2-2] Neighbor 피처 생성...")

    from scipy.spatial.distance import cdist

    # district별 평균 좌표 계산
    district_coords = (
        df.groupby('h3_district_name')
        .agg(lat=('center_lat', 'mean'), lng=('center_lng', 'mean'))
        .reset_index()
    )

    # 좌표 → km 변환 (서울 위도 기준)
    lat_km, lng_km = 111.0, 111.0 * np.cos(np.radians(37.0))
    coords_km = np.column_stack([
        district_coords['lat'].values * lat_km,
        district_coords['lng'].values * lng_km
    ])

    dist_matrix = cdist(coords_km, coords_km, 'euclidean')
    neighbor_mask = (dist_matrix > 0) & (dist_matrix <= NEIGHBOR_RADIUS_KM)

    # 이웃 매핑 딕셔너리
    districts = district_coords['h3_district_name'].values
    neighbor_map = {}
    for i, dist in enumerate(districts):
        nbr_idx = np.where(neighbor_mask[i])[0]
        neighbor_map[dist] = districts[nbr_idx].tolist()

    # 날짜별 이웃 평균 계산 (전일 기준)
    # 먼저 전일 district별 지표 준비
    df = df.sort_values(['h3_district_name', 'date']).copy()
    df['prev_rpo'] = df.groupby('h3_district_name')['rides_per_open'].shift(1)
    df['prev_bikes_400m'] = df.groupby('h3_district_name')['avg_bikes_400m'].shift(1)
    df['prev_access'] = df.groupby('h3_district_name')['accessibility_rate'].shift(1)

    # date별 lookup 테이블
    date_lookup = df.set_index(['date', 'h3_district_name'])[['prev_rpo', 'prev_bikes_400m', 'prev_access']]

    nbr_rpo = []
    nbr_bikes = []
    nbr_count_list = []

    for _, row in df.iterrows():
        nbrs = neighbor_map.get(row['h3_district_name'], [])
        rpos, bikes = [], []
        for nbr in nbrs:
            key = (row['date'], nbr)
            if key in date_lookup.index:
                nbr_row = date_lookup.loc[key]
                if not np.isnan(nbr_row['prev_rpo']):
                    rpos.append(nbr_row['prev_rpo'])
                if not np.isnan(nbr_row['prev_bikes_400m']):
                    bikes.append(nbr_row['prev_bikes_400m'])

        nbr_rpo.append(np.mean(rpos) if rpos else np.nan)
        nbr_bikes.append(np.mean(bikes) if bikes else np.nan)
        nbr_count_list.append(len(nbrs))

    df['neighbor_avg_rpo'] = nbr_rpo
    df['neighbor_avg_bikes_400m'] = nbr_bikes
    df['neighbor_count'] = nbr_count_list

    print(f"  이웃 {NEIGHBOR_RADIUS_KM}km: 평균 {np.mean(nbr_count_list):.1f}개, "
          f"이웃있는 비율 {(np.array(nbr_count_list) > 0).mean():.1%}")
    return df


def create_neighbor_features_fast(df: pd.DataFrame) -> pd.DataFrame:
    """Neighbor Features: 벡터화된 빠른 버전"""
    print("[Step 2-2] Neighbor 피처 생성 (벡터화)...")

    from scipy.spatial.distance import cdist

    # district별 평균 좌표
    district_coords = (
        df.groupby('h3_district_name')
        .agg(lat=('center_lat', 'mean'), lng=('center_lng', 'mean'))
        .reset_index()
    )
    districts = district_coords['h3_district_name'].values
    dist_to_idx = {d: i for i, d in enumerate(districts)}

    # 거리 행렬 → 이웃 마스크
    lat_km, lng_km = 111.0, 111.0 * np.cos(np.radians(37.0))
    coords_km = np.column_stack([
        district_coords['lat'].values * lat_km,
        district_coords['lng'].values * lng_km
    ])
    dist_matrix = cdist(coords_km, coords_km, 'euclidean')
    neighbor_mask = (dist_matrix > 0) & (dist_matrix <= NEIGHBOR_RADIUS_KM)

    # 이웃 수 (정적)
    neighbor_counts = {d: int(neighbor_mask[i].sum()) for i, d in enumerate(districts)}
    df['neighbor_count'] = df['h3_district_name'].map(neighbor_counts)

    # 날짜별 pivot → 행렬 연산으로 이웃 평균 계산
    df = df.sort_values(['h3_district_name', 'date']).copy()

    for col, new_col in [('rides_per_open', 'neighbor_avg_rpo'),
                         ('avg_bikes_400m', 'neighbor_avg_bikes_400m')]:
        # 전일값으로 shift
        df[f'_prev_{col}'] = df.groupby('h3_district_name')[col].shift(1)

        # pivot: date × district
        pivot = df.pivot_table(index='date', columns='h3_district_name',
                               values=f'_prev_{col}', aggfunc='first')

        # 이웃 마스크를 pivot 컬럼 순서에 맞추기
        col_order = pivot.columns.tolist()
        idx_map = [dist_to_idx[d] for d in col_order if d in dist_to_idx]
        mask_reordered = neighbor_mask[np.ix_(idx_map, idx_map)]

        # 이웃 평균 계산 (행렬 연산)
        vals = pd.DataFrame(pivot).astype(float).values  # (n_dates, n_districts)
        nan_mask = np.isnan(vals)
        vals_filled = np.where(nan_mask, 0, vals)
        count_valid = (~nan_mask).astype(float)

        nbr_sum = vals_filled @ mask_reordered.T.astype(float)
        nbr_cnt = count_valid @ mask_reordered.T.astype(float)

        nbr_avg = np.where(nbr_cnt > 0, nbr_sum / nbr_cnt, np.nan)
        nbr_pivot = pd.DataFrame(nbr_avg, index=pivot.index, columns=pivot.columns)

        # unpivot → merge back
        nbr_melted = nbr_pivot.reset_index().melt(
            id_vars='date', var_name='h3_district_name', value_name=new_col
        )
        df = df.drop(columns=[new_col], errors='ignore')
        df = df.merge(nbr_melted, on=['date', 'h3_district_name'], how='left')
        df = df.drop(columns=[f'_prev_{col}'], errors='ignore')

    nc = df['neighbor_count']
    print(f"  이웃 {NEIGHBOR_RADIUS_KM}km: 평균 {nc.mean():.1f}개, "
          f"이웃있는 비율 {(nc > 0).mean():.1%}")
    return df


def create_area_features(df: pd.DataFrame) -> pd.DataFrame:
    """Area Features: 상위 지역(h3_area_name) 수준 집계"""
    print("[Step 2-3] Area 피처 생성...")

    df = df.sort_values(['date', 'h3_area_name', 'h3_district_name']).copy()

    # 날짜별 area 집계 (전일 기준)
    area_daily = (
        df.groupby(['date', 'h3_area_name'])
        .agg(
            area_total_opens=('app_opens', 'sum'),
            area_total_rides=('rides', 'sum'),
            area_avg_rpo=('rides_per_open', 'mean'),
            area_avg_access=('accessibility_rate', 'mean'),
            area_district_count=('h3_district_name', 'nunique'),
        )
        .reset_index()
    )

    # 전일 shift
    area_daily = area_daily.sort_values(['h3_area_name', 'date'])
    for col in ['area_total_opens', 'area_total_rides', 'area_avg_rpo', 'area_avg_access']:
        area_daily[f'{col}_prev'] = (
            area_daily.groupby('h3_area_name')[col].shift(1)
        )

    # merge
    merge_cols = ['date', 'h3_area_name', 'area_district_count',
                  'area_total_opens_prev', 'area_total_rides_prev',
                  'area_avg_rpo_prev', 'area_avg_access_prev']
    df = df.merge(area_daily[merge_cols], on=['date', 'h3_area_name'], how='left')

    print(f"  Area 피처 4개 생성 완료 ({df.h3_area_name.nunique()} areas)")
    return df


# ─── Step 3: Variation 피처 생성 ─────────────────────────────────

def create_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Lag Features: 앱오픈/라이딩 시차 변수"""
    print("[Step 3-1] Lag 피처 생성...")

    df = df.sort_values(['h3_district_name', 'date']).copy()
    g = df.groupby('h3_district_name')

    # 앱오픈 lag
    df['opens_lag1'] = g['app_opens'].shift(1)
    df['opens_lag7'] = g['app_opens'].shift(7)
    df['opens_ma7'] = g['app_opens'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=3).mean()
    )

    # 같은 요일 4주 평균
    df['_dow'] = df['date'].dt.dayofweek
    df['opens_same_dow_avg'] = g['app_opens'].transform(
        lambda x: x.shift(7).rolling(4, min_periods=1).mean()
    )
    # 더 정확한 same_dow: 7일 간격으로만 평균
    # (위 rolling(4)는 연속 4일 평균이므로, 아래에서 보정)
    for lag in [14, 21, 28]:
        col = f'_opens_lag{lag}'
        df[col] = g['app_opens'].shift(lag)

    df['opens_same_dow_avg'] = df[['opens_lag7', '_opens_lag14', '_opens_lag21', '_opens_lag28']].mean(axis=1)
    df = df.drop(columns=['_opens_lag14', '_opens_lag21', '_opens_lag28'])

    # 라이딩 lag
    df['rides_lag1'] = g['rides'].shift(1)
    df['rides_lag7'] = g['rides'].shift(7)
    df['rides_ma7'] = g['rides'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=3).mean()
    )

    print(f"  Lag 피처 8개 생성 완료")
    return df


def fetch_holidays() -> set:
    """BigQuery service.korean_holiday 테이블에서 공휴일 목록 조회"""
    from google.cloud import bigquery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_CREDENTIALS
    client = bigquery.Client()

    query = f'''
    SELECT date, name
    FROM sources.korean_holiday
    WHERE date BETWEEN "{DATE_START}" AND "{DATE_END}"
    ORDER BY date
    '''
    df_hol = client.query(query).to_dataframe()
    df_hol['date'] = pd.to_datetime(df_hol['date'])

    holidays_dict = {}  # date → name
    for _, row in df_hol.iterrows():
        holidays_dict[row['date']] = row['name']

    print(f"  BigQuery 공휴일 {len(holidays_dict)}일 로드: "
          + ", ".join(f"{d.strftime('%m/%d')}({n})" for d, n in sorted(holidays_dict.items())))
    return holidays_dict


def create_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calendar Features: 요일, 주말, 공휴일 (BigQuery korean_holiday 기반)"""
    print("[Step 3-2] Calendar 피처 생성...")

    df['dow'] = df['date'].dt.dayofweek        # 0=월 ~ 6=일
    df['is_weekend'] = (df['dow'] >= 5).astype(int)
    df['day_of_month'] = df['date'].dt.day
    df['week_of_year'] = df['date'].dt.isocalendar().week.astype(int)

    # BigQuery에서 공휴일 조회
    holidays_dict = fetch_holidays()
    holidays_set = set(holidays_dict.keys())

    df['is_holiday'] = df['date'].isin(holidays_set).astype(int)

    # 설날/추석 등 연휴 파생 피처
    # is_holiday_eve: 공휴일 전날 (연휴 전 이동 수요 감소 캡처)
    holiday_eves = set()
    for h in holidays_set:
        eve = h - pd.Timedelta(days=1)
        if eve not in holidays_set:
            holiday_eves.add(eve)
    df['is_holiday_eve'] = df['date'].isin(holiday_eves).astype(int)

    # days_to_nearest_holiday: 가장 가까운 공휴일까지 일수 (±)
    if holidays_set:
        holidays_sorted = sorted(holidays_set)
        def days_to_holiday(dt):
            diffs = [(h - dt).days for h in holidays_sorted]
            abs_diffs = [abs(d) for d in diffs]
            min_idx = abs_diffs.index(min(abs_diffs))
            return diffs[min_idx]  # 양수=미래 공휴일, 음수=지난 공휴일
        df['days_to_holiday'] = df['date'].apply(days_to_holiday)
        df['near_holiday'] = (df['days_to_holiday'].abs() <= 2).astype(int)
    else:
        df['days_to_holiday'] = 99
        df['near_holiday'] = 0

    # 공휴일 or 주말
    df['is_off'] = ((df['is_weekend'] == 1) | (df['is_holiday'] == 1)).astype(int)

    # 연속 휴일 여부 (전일도 off였는지)
    df = df.sort_values(['h3_district_name', 'date'])
    df['prev_is_off'] = df.groupby('h3_district_name')['is_off'].shift(1)
    df['is_consecutive_off'] = ((df['is_off'] == 1) & (df['prev_is_off'] == 1)).astype(int)
    df = df.drop(columns=['prev_is_off'])

    holiday_count = df['is_holiday'].sum()
    off_count = df['is_off'].sum()
    print(f"  Calendar 피처 생성 완료 (공휴일 {holiday_count}건, 휴일전날 {df['is_holiday_eve'].sum()}건)")
    return df


def create_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    """Weather Features: 날씨 변수 merge"""
    print("[Step 3-3] Weather 피처 생성...")

    weather_csv = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')

    if os.path.exists(weather_csv):
        wdf = pd.read_csv(weather_csv)
        wdf['date'] = pd.to_datetime(wdf['date'])

        weather = wdf[['date', 'temp_low', 'temp_high', 'rain_sum']].copy()
        weather['rain_sum'] = weather['rain_sum'].fillna(0)

        # 파생 피처
        weather['temp_avg'] = (weather['temp_low'] + weather['temp_high']) / 2
        weather['is_cold'] = (weather['temp_low'] <= -8).astype(int)
        weather['is_freeze'] = (weather['temp_high'] <= 0).astype(int)
        weather['is_rain'] = (weather['rain_sum'] > 0).astype(int)

        df = df.merge(weather, on='date', how='left')

        # 결측 보간 (기간 끝에 날씨 없을 수 있음)
        for col in ['temp_low', 'temp_high', 'temp_avg', 'rain_sum']:
            df[col] = df[col].fillna(method='ffill')
        for col in ['is_cold', 'is_freeze', 'is_rain']:
            df[col] = df[col].fillna(0).astype(int)

        print(f"  날씨 피처 merge 완료 (날씨 데이터 {len(weather)}일)")
    else:
        print(f"  ⚠ 날씨 CSV 없음 → 날씨 피처 생략")
        for col in ['temp_low', 'temp_high', 'temp_avg', 'rain_sum',
                    'is_cold', 'is_freeze', 'is_rain']:
            df[col] = 0

    return df


# ─── Step 4~5: 모델 학습 ────────────────────────────────────────

def prepare_model_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """A그룹 필터링 + Train/Test split"""
    print("\n[Step 4] 학습 데이터 준비...")

    # 모델 기간만
    df_model = df[df['date'] >= MODEL_START].copy()

    # A그룹 필터: 전체 기간 일평균 앱오픈 > 5
    district_avg = df_model.groupby('h3_district_name')['app_opens'].mean()
    a_group = district_avg[district_avg > A_GROUP_THRESHOLD].index
    df_model = df_model[df_model['h3_district_name'].isin(a_group)]

    print(f"  A그룹 district: {len(a_group)}개")
    print(f"  모델 데이터: {len(df_model):,}행")

    # 피처 컬럼 정의
    feature_cols = [
        # Level - Self (rolling)
        'app_opens_rolling', 'avg_bikes_400m_rolling', 'accessibility_rate_rolling',
        'rides_rolling', 'rides_per_open_rolling', 'hour_std_rolling', 'opens_cv_rolling',
        # Level - Neighbor
        'neighbor_avg_rpo', 'neighbor_avg_bikes_400m', 'neighbor_count',
        # Level - Area
        'area_district_count', 'area_avg_rpo_prev', 'area_avg_access_prev',
        # Variation - Lag
        'opens_lag1', 'opens_lag7', 'opens_ma7', 'opens_same_dow_avg',
        'rides_lag1', 'rides_lag7', 'rides_ma7',
        # Variation - Calendar
        'dow', 'is_weekend', 'is_holiday', 'is_off',
        'is_holiday_eve', 'near_holiday', 'is_consecutive_off',
        # Variation - Weather
        'temp_low', 'temp_high', 'temp_avg', 'rain_sum',
        'is_cold', 'is_freeze', 'is_rain',
    ]

    target_col = 'rides'

    # 사용 가능한 피처만
    available_features = [c for c in feature_cols if c in df_model.columns]
    missing = [c for c in feature_cols if c not in df_model.columns]
    if missing:
        print(f"  ⚠ 누락 피처: {missing}")

    # NaN 행 제거 (lag 워밍업으로 인한 초기 결측)
    df_clean = df_model.dropna(subset=['opens_lag7', 'rides_lag7', 'app_opens_rolling'])
    print(f"  결측 제거 후: {len(df_clean):,}행 ({len(df_model) - len(df_clean)}행 제거)")

    # Train / Test split
    train = df_clean[df_clean['date'] <= TRAIN_END].copy()
    test = df_clean[df_clean['date'] > TRAIN_END].copy()

    print(f"  Train: {len(train):,}행 (~{TRAIN_END})")
    print(f"  Test:  {len(test):,}행 ({TEST_START}~)")

    return train, test, available_features


def train_lightgbm(train: pd.DataFrame, test: pd.DataFrame,
                   feature_cols: List[str],
                   target_mode: str = 'rides') -> dict:
    """
    LightGBM 모델 학습 + 평가

    target_mode:
        'rides': rides 직접 예측
        'rpo':   rides_per_open 예측 → app_opens × rpo로 rides 환산
    """
    import lightgbm as lgb

    print(f"\n[Step 5] LightGBM 학습 (target: {target_mode})...")

    if target_mode == 'rpo':
        target = 'rides_per_open'
        # rpo 예측용 피처에서 rides lag 제거 (순환 방지), app_opens lag 강화
        rpo_exclude = ['rides_lag1', 'rides_lag7', 'rides_ma7', 'rides_rolling']
        feature_cols_used = [c for c in feature_cols if c not in rpo_exclude]
    else:
        target = 'rides'
        feature_cols_used = feature_cols

    X_train = train[feature_cols_used].astype(float).values
    y_train = train[target].astype(float).values
    X_test = test[feature_cols_used].astype(float).values
    y_test_target = test[target].astype(float).values

    # 데이터셋
    dtrain = lgb.Dataset(X_train, label=y_train, feature_name=feature_cols_used)
    dvalid = lgb.Dataset(X_test, label=y_test_target, feature_name=feature_cols_used, reference=dtrain)

    # 하이퍼파라미터
    params = {
        'objective': 'regression',
        'metric': ['mae', 'mape'],
        'boosting_type': 'gbdt',
        'num_leaves': 63,
        'learning_rate': 0.05,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'min_child_samples': 10,
        'verbose': -1,
        'seed': 42,
    }

    # 학습
    callbacks = [
        lgb.log_evaluation(period=100),
        lgb.early_stopping(stopping_rounds=50),
    ]

    model = lgb.train(
        params,
        dtrain,
        num_boost_round=1000,
        valid_sets=[dtrain, dvalid],
        valid_names=['train', 'valid'],
        callbacks=callbacks,
    )

    # 예측
    y_pred_raw = model.predict(X_test)
    y_pred_raw = np.maximum(y_pred_raw, 0)

    # rides 환산
    if target_mode == 'rpo':
        # predicted_rides = app_opens × predicted_rpo
        y_pred_rides = test['app_opens'].values * y_pred_raw
        y_true_rides = test['rides'].values
        print(f"  RPO 예측 → rides 환산 (app_opens × predicted_rpo)")
    else:
        y_pred_rides = y_pred_raw
        y_true_rides = test['rides'].values

    # 평가 (rides 기준)
    results = evaluate_model(y_true_rides, y_pred_rides, test)

    # Feature Importance
    importance = pd.DataFrame({
        'feature': feature_cols_used,
        'importance': model.feature_importance(importance_type='gain'),
    }).sort_values('importance', ascending=False)

    print("\n--- Feature Importance (Top 15) ---")
    for _, row in importance.head(15).iterrows():
        bar = '█' * int(row['importance'] / importance['importance'].max() * 30)
        print(f"  {row['feature']:30s} {row['importance']:10.0f} {bar}")

    results['model'] = model
    results['importance'] = importance
    results['predictions'] = pd.DataFrame({
        'date': test['date'].values,
        'h3_district_name': test['h3_district_name'].values,
        'actual': y_true_rides,
        'predicted': y_pred_rides,
    })

    return results


def evaluate_model(y_true: np.ndarray, y_pred: np.ndarray,
                   test_df: pd.DataFrame) -> dict:
    """모델 평가 (다양한 관점)"""
    print("\n" + "="*60)
    print("=== 모델 평가 결과 ===")
    print("="*60)

    # 전체 지표
    mae = np.mean(np.abs(y_true - y_pred))
    mape = np.mean(np.abs(y_true - y_pred) / np.maximum(y_true, 1)) * 100
    bias = np.mean(y_pred - y_true) / np.mean(y_true) * 100
    rmse = np.sqrt(np.mean((y_true - y_pred) ** 2))
    r2 = 1 - np.sum((y_true - y_pred)**2) / np.sum((y_true - np.mean(y_true))**2)

    print(f"\n전체 성과:")
    print(f"  MAE:  {mae:.2f}")
    print(f"  MAPE: {mape:.1f}%")
    print(f"  RMSE: {rmse:.2f}")
    print(f"  Bias: {bias:+.1f}%")
    print(f"  R²:   {r2:.4f}")

    # 기존 모델 비교 (단순 과거 비율 = rides_lag7)
    baseline_pred = test_df['rides_lag7'].fillna(test_df['rides_ma7']).values
    baseline_pred = np.nan_to_num(baseline_pred, nan=np.nanmean(y_true))
    baseline_mape = np.mean(np.abs(y_true - baseline_pred) / np.maximum(y_true, 1)) * 100
    baseline_mae = np.mean(np.abs(y_true - baseline_pred))

    print(f"\n기준선 비교 (lag7 단순 예측):")
    print(f"  Baseline MAE:  {baseline_mae:.2f} → Model MAE:  {mae:.2f} ({(mae/baseline_mae-1)*100:+.1f}%)")
    print(f"  Baseline MAPE: {baseline_mape:.1f}% → Model MAPE: {mape:.1f}% ({mape - baseline_mape:+.1f}%p)")

    # District 규모별 성과
    pred_df = pd.DataFrame({
        'actual': y_true, 'predicted': y_pred,
        'district': test_df['h3_district_name'].values,
        'daily_avg': test_df['app_opens_rolling'].values,
    })

    print(f"\n규모별 성과:")
    for label, lo, hi in [('소형(5-15)', 5, 15), ('중형(15-40)', 15, 40), ('대형(40+)', 40, 999)]:
        mask = (pred_df['daily_avg'] >= lo) & (pred_df['daily_avg'] < hi)
        if mask.sum() > 0:
            sub = pred_df[mask]
            sub_mape = np.mean(np.abs(sub['actual'] - sub['predicted']) / np.maximum(sub['actual'], 1)) * 100
            sub_mae = np.mean(np.abs(sub['actual'] - sub['predicted']))
            print(f"  {label:15s}: MAPE {sub_mape:5.1f}%, MAE {sub_mae:5.1f} ({mask.sum()}건)")

    # 일별 집계 성과 (district 합산)
    daily = pred_df.groupby(test_df['date'].values).agg(
        actual_sum=('actual', 'sum'),
        predicted_sum=('predicted', 'sum'),
    )
    daily_mape = np.mean(np.abs(daily['actual_sum'] - daily['predicted_sum'])
                         / daily['actual_sum']) * 100
    print(f"\n일별 합산 성과: MAPE {daily_mape:.1f}%")

    return {
        'mae': mae, 'mape': mape, 'rmse': rmse, 'bias': bias, 'r2': r2,
        'baseline_mape': baseline_mape,
    }


# ─── 메인 파이프라인 ─────────────────────────────────────────────

def run_pipeline():
    """전체 파이프라인 실행"""
    print("="*60)
    print("District 수요 예측 모델 v1 — 파이프라인 시작")
    print("="*60)
    print()

    # Step 1: 데이터 추출
    cache_path = os.path.join(SCRIPT_DIR, '_cache_daily_district.pkl')
    if os.path.exists(cache_path):
        print("[Step 1] 캐시된 데이터 로드...")
        df = pd.read_pickle(cache_path)
        print(f"  {len(df):,}행, {df.h3_district_name.nunique()} districts")
    else:
        df_opens, df_rides = fetch_daily_data()
        df = merge_daily_data(df_opens, df_rides)
        df.to_pickle(cache_path)
        print(f"  캐시 저장: {cache_path}")

    # Step 2: Level 피처
    df = create_self_features(df)
    df = create_neighbor_features_fast(df)
    df = create_area_features(df)

    # Step 3: Variation 피처
    df = create_lag_features(df)
    df = create_calendar_features(df)
    df = create_weather_features(df)

    # Step 4-5: 학습
    train, test, feature_cols = prepare_model_data(df)

    # Model A: rides 직접 예측
    print("\n" + "="*60)
    print("===== Model A: rides 직접 예측 =====")
    results_rides = train_lightgbm(train, test, feature_cols, target_mode='rides')

    # Model B: rides_per_open 예측 → app_opens × rpo
    print("\n" + "="*60)
    print("===== Model B: rpo 예측 → rides 환산 =====")
    results_rpo = train_lightgbm(train, test, feature_cols, target_mode='rpo')

    # 비교 요약
    print("\n" + "="*60)
    print("===== Model A vs B 비교 =====")
    print("="*60)
    for metric in ['mae', 'mape', 'rmse', 'r2', 'bias']:
        a = results_rides.get(metric, 0)
        b = results_rpo.get(metric, 0)
        if metric == 'mape':
            print(f"  {metric:8s}: A={a:.1f}%  B={b:.1f}%")
        elif metric in ['bias']:
            print(f"  {metric:8s}: A={a:+.1f}%  B={b:+.1f}%")
        else:
            print(f"  {metric:8s}: A={a:.2f}  B={b:.2f}")

    # 더 나은 모델 저장
    best = results_rpo if results_rpo['mape'] < results_rides['mape'] else results_rides
    best_name = 'B(rpo)' if results_rpo['mape'] < results_rides['mape'] else 'A(rides)'
    print(f"\n  → 최종 선택: Model {best_name}")

    best['importance'].to_csv(
        os.path.join(SCRIPT_DIR, 'district_model_importance.csv'), index=False
    )
    best['predictions'].to_csv(
        os.path.join(SCRIPT_DIR, 'district_model_predictions.csv'), index=False
    )
    print(f"\n결과 저장 완료")

    return {'rides': results_rides, 'rpo': results_rpo, 'best': best_name}


if __name__ == '__main__':
    results = run_pipeline()
