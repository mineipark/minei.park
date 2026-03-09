"""
District-level 수요 예측 Production Pipeline v2
= LightGBM opens 모델 + LightGBM rpo 모델 → rides 예측

Architecture:
  predicted_rides = predicted_app_opens × predicted_rpo

  Opens 모델: "내일 이 district에서 앱을 여는 사람이 몇 명일까?"
  RPO 모델:   "앱을 연 사람 중 몇 %가 실제 라이딩할까?"

Pipeline:
  Step 1: BigQuery → Daily Aggregation (district×date)
  Step 2: Level 피처 (self rolling, neighbor+hub, area)
  Step 3: Variation 피처 (lag, calendar, weather)
  Step 4: Train/Test split + A그룹 필터
  Step 5: Opens 모델 학습
  Step 6: RPO 모델 학습
  Step 7: Combined 예측 + 평가
  Step 8: 모델 저장 (pickle)

사용법:
  학습:  python district_production_pipeline.py
  예측:  python district_production_pipeline.py --predict 2026-02-27
"""

import os
import sys
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')


# ═══════════════════════════════════════════════════════════════
# 설정
# ═══════════════════════════════════════════════════════════════

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BQ_CREDENTIALS = os.path.expanduser('~/Downloads/service-account.json')

# 분석 기간 (추석~설 포함하여 명절 패턴 학습)
DATE_START = '2025-08-01'  # 롤링 윈도우 워밍업 포함
DATE_END = '2026-02-26'

# 모델 기간 (추석 포함)
MODEL_START = '2025-09-01'
TRAIN_END = '2026-02-15'
TEST_START = '2026-02-16'

# A그룹 기준
A_GROUP_THRESHOLD = 8

# B2B district 제외 (기업/산업단지 — 일반 수요예측 대상 아님)
B2B_EXCLUDE = [
    '현대미포조선',
    '삼성디지털시티',
]

# 피처 설정
ROLLING_WINDOW = 14
NEIGHBOR_RADIUS_KM = 2.0

# 모델 저장 경로
MODEL_DIR = os.path.join(SCRIPT_DIR, 'models')
CACHE_PATH = os.path.join(SCRIPT_DIR, '_cache_daily_district.pkl')


# ═══════════════════════════════════════════════════════════════
# Step 1: 데이터 추출
# ═══════════════════════════════════════════════════════════════

def fetch_daily_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """BigQuery에서 district×date 일별 집계 데이터 추출"""
    from google.cloud import bigquery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_CREDENTIALS
    client = bigquery.Client()

    print("[Step 1] BigQuery 데이터 추출...")

    q_opens = f'''
    SELECT
        date, h3_area_name, h3_district_name,
        COUNT(*) as app_opens,
        AVG(bike_count_100) as avg_bikes_100m,
        AVG(bike_count_400) as avg_bikes_400m,
        AVG(distance) as avg_distance,
        AVG(IF(is_accessible, 1, 0)) as accessibility_rate,
        AVG(IF(is_converted, 1, 0)) as conversion_rate,
        SAFE_DIVIDE(SUM(IF(is_converted,1,0)), SUM(IF(is_accessible,1,0))) as cond_conversion_rate,
        STDDEV(EXTRACT(HOUR FROM event_time)) as hour_std,
        AVG(ST_Y(location)) as center_lat,
        AVG(ST_X(location)) as center_lng
    FROM service.bike_accessibility_raw
    WHERE date BETWEEN "{DATE_START}" AND "{DATE_END}"
        AND near_geoblock = True
    GROUP BY date, h3_area_name, h3_district_name
    '''

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
    df_opens['date'] = pd.to_datetime(df_opens['date'])
    df_rides['date'] = pd.to_datetime(df_rides['date'])

    # Merge
    df = df_opens.merge(
        df_rides[['date', 'h3_district_name', 'rides']],
        on=['date', 'h3_district_name'], how='left'
    )
    df['rides'] = df['rides'].fillna(0).astype(int)
    df['rides_per_open'] = df['rides'] / df['app_opens']

    print(f"  {len(df):,}행, {df.h3_district_name.nunique()} districts")
    return df


def load_or_fetch_data() -> pd.DataFrame:
    """캐시 있으면 로드, 없으면 BigQuery 추출"""
    if os.path.exists(CACHE_PATH):
        print("[Step 1] 캐시 로드...")
        df = pd.read_pickle(CACHE_PATH)
        df['date'] = pd.to_datetime(df['date'])
        print(f"  {len(df):,}행, {df.h3_district_name.nunique()} districts")
        return df
    else:
        df = fetch_daily_data()
        df.to_pickle(CACHE_PATH)
        print(f"  캐시 저장 완료")
        return df


# ═══════════════════════════════════════════════════════════════
# Step 2: Level 피처 (Self, Neighbor+Hub, Area)
# ═══════════════════════════════════════════════════════════════

def create_self_features(df: pd.DataFrame) -> pd.DataFrame:
    """Self Features: 14일 롤링 통계"""
    print("[Step 2-1] Self 피처...")
    df = df.sort_values(['h3_district_name', 'date']).copy()
    g = df.groupby('h3_district_name')

    for col in ['app_opens', 'avg_bikes_400m', 'accessibility_rate',
                'rides', 'rides_per_open', 'hour_std']:
        df[f'{col}_rolling'] = g[col].transform(
            lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=3).mean()
        )

    df['opens_cv_rolling'] = g['app_opens'].transform(
        lambda x: x.shift(1).rolling(ROLLING_WINDOW, min_periods=3).std()
        / x.shift(1).rolling(ROLLING_WINDOW, min_periods=3).mean()
    )
    print(f"  7개 생성 완료")
    return df


def create_neighbor_hub_features(df: pd.DataFrame) -> pd.DataFrame:
    """Neighbor + Hub Features: 벡터화 이웃 평균 + 허브 참조"""
    print("[Step 2-2] Neighbor + Hub 피처...")
    from scipy.spatial.distance import cdist

    # ── 좌표/거리 행렬 ──
    district_coords = df.groupby('h3_district_name').agg(
        lat=('center_lat', 'mean'), lng=('center_lng', 'mean'),
        avg_opens=('app_opens', 'mean')
    ).reset_index()
    districts = district_coords['h3_district_name'].values
    dist_to_idx = {d: i for i, d in enumerate(districts)}

    lat_km, lng_km = 111.0, 111.0 * np.cos(np.radians(37.0))
    coords_km = np.column_stack([
        district_coords['lat'].values * lat_km,
        district_coords['lng'].values * lng_km
    ])
    dist_matrix = cdist(coords_km, coords_km, 'euclidean')
    neighbor_mask = (dist_matrix > 0) & (dist_matrix <= NEIGHBOR_RADIUS_KM)

    # ── 이웃 수 ──
    df['neighbor_count'] = df['h3_district_name'].map(
        {d: int(neighbor_mask[i].sum()) for i, d in enumerate(districts)}
    )

    # ── 허브 식별: 이웃 중 앱오픈 가장 많은 district ──
    avg_opens_arr = district_coords['avg_opens'].values
    hub_map = {}
    hub_dist_map = {}
    for i, d in enumerate(districts):
        nbr_idx = np.where(neighbor_mask[i])[0]
        all_idx = np.append(nbr_idx, i) if len(nbr_idx) > 0 else np.array([i])
        all_opens = avg_opens_arr[all_idx]
        max_idx = all_idx[np.argmax(all_opens)]
        hub_map[d] = districts[max_idx]
        hub_dist_map[d] = dist_matrix[i, max_idx]

    is_self_hub = sum(1 for d in districts if hub_map[d] == d)
    print(f"  허브: {is_self_hub}/{len(districts)} 자기허브, "
          f"{len(districts)-is_self_hub} 외부허브 참조")

    # ── 허브 정적 피처 ──
    df['hub_district'] = df['h3_district_name'].map(hub_map)
    df['hub_distance'] = df['h3_district_name'].map(hub_dist_map)
    df['is_self_hub'] = (df['h3_district_name'] == df['hub_district']).astype(int)

    # ── 벡터화 이웃 평균 (기존) + 거리 가중 + max ──
    df = df.sort_values(['h3_district_name', 'date']).copy()
    g = df.groupby('h3_district_name')

    # 거리 가중 행렬
    dist_weight = np.where(neighbor_mask, 1.0 / np.maximum(dist_matrix, 0.1), 0)
    row_sums = dist_weight.sum(axis=1, keepdims=True)
    dist_weight_norm = np.where(row_sums > 0, dist_weight / row_sums, 0)

    for col, avg_col, weighted_col, max_col in [
        ('rides_per_open', 'neighbor_avg_rpo', 'neighbor_weighted_rpo', 'neighbor_max_rpo'),
        ('avg_bikes_400m', 'neighbor_avg_bikes_400m', None, None),
    ]:
        df[f'_prev_{col}'] = g[col].shift(1)
        pivot = df.pivot_table(index='date', columns='h3_district_name',
                               values=f'_prev_{col}', aggfunc='first')
        col_order = pivot.columns.tolist()
        idx_map = [dist_to_idx[d] for d in col_order if d in dist_to_idx]
        mask_r = neighbor_mask[np.ix_(idx_map, idx_map)]

        vals = pd.DataFrame(pivot).astype(float).values
        nan_m = np.isnan(vals)
        vals_f = np.where(nan_m, 0, vals)
        cnt_v = (~nan_m).astype(float)

        # 단순 평균
        nbr_sum = vals_f @ mask_r.T.astype(float)
        nbr_cnt = cnt_v @ mask_r.T.astype(float)
        nbr_avg = np.where(nbr_cnt > 0, nbr_sum / nbr_cnt, np.nan)
        avg_pivot = pd.DataFrame(nbr_avg, index=pivot.index, columns=pivot.columns)
        avg_melted = avg_pivot.reset_index().melt(
            id_vars='date', var_name='h3_district_name', value_name=avg_col)
        df = df.drop(columns=[avg_col], errors='ignore')
        df = df.merge(avg_melted, on=['date', 'h3_district_name'], how='left')

        if weighted_col:
            # 거리 가중 평균
            wm = dist_weight_norm[np.ix_(idx_map, idx_map)]
            w_sum = vals_f @ wm.T
            w_cnt = cnt_v @ wm.T
            w_avg = np.where(w_cnt > 0, w_sum / w_cnt, np.nan)
            w_pivot = pd.DataFrame(w_avg, index=pivot.index, columns=pivot.columns)
            w_melted = w_pivot.reset_index().melt(
                id_vars='date', var_name='h3_district_name', value_name=weighted_col)
            df = df.merge(w_melted, on=['date', 'h3_district_name'], how='left')

        if max_col:
            # 이웃 중 max
            max_rows = []
            for i_row in range(vals.shape[0]):
                row_maxes = []
                for j_col in range(vals.shape[1]):
                    nbr_j = np.where(mask_r[j_col])[0]
                    if len(nbr_j) > 0:
                        valid = vals[i_row, nbr_j]
                        valid = valid[~np.isnan(valid)]
                        row_maxes.append(np.max(valid) if len(valid) > 0 else np.nan)
                    else:
                        row_maxes.append(np.nan)
                max_rows.append(row_maxes)
            max_pivot = pd.DataFrame(max_rows, index=pivot.index, columns=pivot.columns)
            max_melted = max_pivot.reset_index().melt(
                id_vars='date', var_name='h3_district_name', value_name=max_col)
            df = df.merge(max_melted, on=['date', 'h3_district_name'], how='left')

        df = df.drop(columns=[f'_prev_{col}'], errors='ignore')

    # ── 허브의 전일 rpo/opens ──
    hub_lookup = df[['date', 'h3_district_name']].copy()
    hub_lookup['_hub_rpo'] = g['rides_per_open'].shift(1)
    hub_lookup['_hub_opens'] = g['app_opens'].shift(1)
    hub_lookup.columns = ['date', 'hub_district', 'hub_prev_rpo', 'hub_prev_opens']
    df = df.merge(hub_lookup, on=['date', 'hub_district'], how='left')

    nc = df['neighbor_count']
    print(f"  이웃 {NEIGHBOR_RADIUS_KM}km: 평균 {nc.mean():.1f}개")
    return df


def create_area_features(df: pd.DataFrame) -> pd.DataFrame:
    """Area Features: 상위 area 수준 집계 (opens + rpo 모두 커버)"""
    print("[Step 2-3] Area 피처...")
    df = df.sort_values(['date', 'h3_area_name', 'h3_district_name']).copy()

    area_daily = df.groupby(['date', 'h3_area_name']).agg(
        area_total_opens=('app_opens', 'sum'),
        area_total_rides=('rides', 'sum'),
        area_avg_rpo=('rides_per_open', 'mean'),
        area_avg_access=('accessibility_rate', 'mean'),
        area_district_count=('h3_district_name', 'nunique'),
    ).reset_index()

    area_daily = area_daily.sort_values(['h3_area_name', 'date'])
    ag = area_daily.groupby('h3_area_name')

    # 전일 + lag7 + ma7 (opens 모델용)
    for col in ['area_total_opens', 'area_total_rides', 'area_avg_rpo', 'area_avg_access']:
        area_daily[f'{col}_prev'] = ag[col].shift(1)
    area_daily['area_opens_lag7'] = ag['area_total_opens'].shift(7)
    area_daily['area_opens_ma7'] = ag['area_total_opens'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=3).mean()
    )

    merge_cols = ['date', 'h3_area_name', 'area_district_count',
                  'area_total_opens_prev', 'area_total_rides_prev',
                  'area_avg_rpo_prev', 'area_avg_access_prev',
                  'area_opens_lag7', 'area_opens_ma7']
    df = df.merge(area_daily[merge_cols], on=['date', 'h3_area_name'], how='left')

    # district의 area 내 비중 (opens 모델용)
    df['district_area_share'] = (
        df['app_opens_rolling'] /
        df.groupby(['date', 'h3_area_name'])['app_opens_rolling'].transform('sum')
    )

    print(f"  Area 피처 완료 ({df.h3_area_name.nunique()} areas)")
    return df


# ═══════════════════════════════════════════════════════════════
# Step 3: Variation 피처 (Lag, Calendar, Weather)
# ═══════════════════════════════════════════════════════════════

def create_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Lag Features: 앱오픈 + 라이딩 시차 변수"""
    print("[Step 3-1] Lag 피처...")
    df = df.sort_values(['h3_district_name', 'date']).copy()
    g = df.groupby('h3_district_name')

    # Opens lag
    df['opens_lag1'] = g['app_opens'].shift(1)
    df['opens_lag7'] = g['app_opens'].shift(7)
    df['opens_ma7'] = g['app_opens'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=3).mean())

    for lag in [14, 21, 28]:
        df[f'_opens_lag{lag}'] = g['app_opens'].shift(lag)
    df['opens_same_dow_avg'] = df[['opens_lag7', '_opens_lag14', '_opens_lag21', '_opens_lag28']].mean(axis=1)
    df = df.drop(columns=['_opens_lag14', '_opens_lag21', '_opens_lag28'])

    # Rides lag (Model A용, rpo 모델에서는 제외됨)
    df['rides_lag1'] = g['rides'].shift(1)
    df['rides_lag7'] = g['rides'].shift(7)
    df['rides_ma7'] = g['rides'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=3).mean())

    # ── 상대 변화율 피처 (절대 수량이 아닌 앞뒤 대비 증감) ──
    # 전일 대비 rolling 평균 비율 (어제가 평소보다 높았나/낮았나)
    df['opens_ratio_to_avg'] = df['opens_lag1'] / df['app_opens_rolling'].clip(lower=1)
    # 전주 동요일 대비 변화율
    df['opens_wow_change'] = (df['opens_lag1'] - df['opens_lag7']) / df['opens_lag7'].clip(lower=1)
    # RPO 전일 대비 rolling 비율
    rpo_lag1 = g['rides_per_open'].shift(1)
    df['rpo_ratio_to_avg'] = rpo_lag1 / df['rides_per_open_rolling'].clip(lower=0.01)

    # ── RPO 규모 인식 피처 (opens 많을수록 RPO 낮아지는 패턴) ──
    df['log_opens_rolling'] = np.log1p(df['app_opens_rolling'].fillna(0))
    df['rpo_x_opens'] = df['rides_per_open_rolling'].fillna(0) * df['app_opens_rolling'].fillna(0)

    print(f"  Lag 피처 13개 완료 (변화율 3개 + 규모인식 2개 포함)")
    return df


def create_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calendar Features: BigQuery korean_holiday 기반"""
    print("[Step 3-2] Calendar 피처...")
    from google.cloud import bigquery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_CREDENTIALS
    client = bigquery.Client()

    hol_df = client.query(f'''
        SELECT date, name FROM sources.korean_holiday
        WHERE date BETWEEN "{DATE_START}" AND "{DATE_END}" ORDER BY date
    ''').to_dataframe()
    hol_df['date'] = pd.to_datetime(hol_df['date'])
    holidays_set = set(hol_df['date'])
    print(f"  공휴일 {len(holidays_set)}일: "
          + ", ".join(f"{d.strftime('%m/%d')}" for d in sorted(holidays_set)))

    df['dow'] = df['date'].dt.dayofweek
    df['is_weekend'] = (df['dow'] >= 5).astype(int)
    df['is_holiday'] = df['date'].isin(holidays_set).astype(int)
    df['is_off'] = ((df['is_weekend'] == 1) | (df['is_holiday'] == 1)).astype(int)

    # 연휴 전날
    holiday_eves = {h - pd.Timedelta(days=1) for h in holidays_set} - holidays_set
    df['is_holiday_eve'] = df['date'].isin(holiday_eves).astype(int)

    # 가장 가까운 공휴일까지 일수
    holidays_sorted = sorted(holidays_set) if holidays_set else []
    if holidays_sorted:
        df['days_to_holiday'] = df['date'].apply(
            lambda dt: min([(h - dt).days for h in holidays_sorted], key=abs))
        df['near_holiday'] = (df['days_to_holiday'].abs() <= 2).astype(int)
    else:
        df['days_to_holiday'] = 99
        df['near_holiday'] = 0

    # 연속 휴일
    df = df.sort_values(['h3_district_name', 'date'])
    df['prev_is_off'] = df.groupby('h3_district_name')['is_off'].shift(1)
    df['is_consecutive_off'] = ((df['is_off'] == 1) & (df['prev_is_off'] == 1)).astype(int)
    df = df.drop(columns=['prev_is_off'])

    # ── A2: 대형연휴 후 회복 구간 피처 ──
    df['_hol_block_cal'] = df.groupby('h3_district_name')['is_holiday'].transform(
        lambda x: x.rolling(3, min_periods=1, center=True).sum()
    )
    df['_is_major_holiday_cal'] = (df['_hol_block_cal'] >= 2).astype(int)

    def _days_since_major_holiday_end(group):
        result = pd.Series(30, index=group.index)
        last_holiday_end = None
        for idx, row in group.iterrows():
            if row['_is_major_holiday_cal'] == 1:
                last_holiday_end = row['date']
            if last_holiday_end is not None:
                days = (row['date'] - last_holiday_end).days
                result[idx] = min(days, 30)
        return result

    df['days_since_major_holiday'] = df.groupby('h3_district_name', group_keys=False).apply(
        _days_since_major_holiday_end
    ).reset_index(level=0, drop=True)
    df['is_recovery_phase'] = ((df['days_since_major_holiday'] > 0) &
                                (df['days_since_major_holiday'] <= 7)).astype(int)
    df = df.drop(columns=['_hol_block_cal', '_is_major_holiday_cal'])

    print(f"  Calendar 완료 (공휴일 {df['is_holiday'].sum()}건, 회복구간 피처 추가)")
    return df


def create_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    """Weather Features: CSV 기반 (풍속/습도/적설 포함)"""
    print("[Step 3-3] Weather 피처...")
    weather_csv = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')

    if os.path.exists(weather_csv):
        wdf = pd.read_csv(weather_csv)
        wdf['date'] = pd.to_datetime(wdf['date'])
        weather_cols = ['date', 'temp_low', 'temp_high', 'rain_sum',
                        'windspeed_avg', 'humidity_avg', 'snow_depth']
        weather = wdf[[c for c in weather_cols if c in wdf.columns]].copy()

        # 결측 처리
        weather['rain_sum'] = weather['rain_sum'].fillna(0)
        weather['windspeed_avg'] = weather.get('windspeed_avg', pd.Series(dtype=float)).fillna(0)
        weather['humidity_avg'] = weather.get('humidity_avg', pd.Series(dtype=float)).fillna(50)
        weather['snow_depth'] = weather.get('snow_depth', pd.Series(dtype=float)).fillna(0)

        # 파생 변수
        weather['temp_avg'] = (weather['temp_low'] + weather['temp_high']) / 2
        weather['is_cold'] = (weather['temp_low'] <= -8).astype(int)
        weather['is_freeze'] = (weather['temp_high'] <= 0).astype(int)
        weather['is_rain'] = (weather['rain_sum'] > 0).astype(int)
        weather['is_heavy_rain'] = (weather['rain_sum'] >= 10).astype(int)
        weather['is_windy'] = (weather['windspeed_avg'] >= 8).astype(int)
        weather['is_snow'] = (weather['snow_depth'] > 0).astype(int)
        weather['is_severe_weather'] = (
            (weather['rain_sum'] >= 10) |
            (weather['windspeed_avg'] >= 7) |
            ((weather['temp_high'] <= 2) & (weather['rain_sum'] > 0))
        ).astype(int)
        weather['temp_range'] = weather['temp_high'] - weather['temp_low']

        df = df.merge(weather, on='date', how='left')
        for col in ['temp_low', 'temp_high', 'temp_avg', 'rain_sum',
                     'windspeed_avg', 'humidity_avg', 'snow_depth', 'temp_range']:
            df[col] = df[col].ffill()
            df[col] = df[col].fillna(0)
        for col in ['is_cold', 'is_freeze', 'is_rain', 'is_heavy_rain',
                     'is_windy', 'is_snow', 'is_severe_weather']:
            df[col] = df[col].fillna(0).astype(int)
        print(f"  날씨 {len(weather)}일 merge 완료 (풍속/습도/적설/악천후 포함)")
    else:
        print(f"  ⚠ 날씨 CSV 없음 → 0으로 채움")
        for col in ['temp_low', 'temp_high', 'temp_avg', 'rain_sum',
                    'is_cold', 'is_freeze', 'is_rain', 'is_heavy_rain',
                    'windspeed_avg', 'humidity_avg', 'snow_depth',
                    'is_windy', 'is_snow', 'is_severe_weather', 'temp_range']:
            df[col] = 0
    return df


def create_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """Interaction Features: 날씨×캘린더 교호작용 + 월 + 명절"""
    print("[Step 3-4] Interaction 피처...")
    df['month'] = df['date'].dt.month
    df['rain_off'] = df['is_rain'] * df['is_off']
    df['cold_off'] = df['is_cold'] * df['is_off']
    df['rain_weekend'] = df['is_rain'] * df['is_weekend']

    # 명절 연휴 (설/추석 = 연속 3일 이상 공휴일) — 일반 공휴일과 구분
    df['_hol_block'] = df.groupby('h3_district_name')['is_holiday'].transform(
        lambda x: x.rolling(3, min_periods=1, center=True).sum()
    )
    df['is_major_holiday'] = (df['_hol_block'] >= 2).astype(int)
    # 연휴 전후 1일 (귀성/귀경 효과)
    g = df.groupby('h3_district_name')
    df['major_holiday_adj'] = (
        (df['is_major_holiday'] == 1) |
        (g['is_major_holiday'].shift(1) == 1) |
        (g['is_major_holiday'].shift(-1) == 1)
    ).astype(int)
    df = df.drop(columns=['_hol_block'])

    # ── A3: 악천후 + 휴일 복합 피처 ──
    df['severe_weather_off'] = (df.get('is_severe_weather', 0) * df['is_off']).astype(int)
    df['heavy_rain_off'] = (df.get('is_heavy_rain', 0) * df['is_off']).astype(int)

    n_major = df['is_major_holiday'].sum()
    print(f"  9개 생성 완료 (명절 {n_major}건, 악천후복합 포함)")
    return df


# ═══════════════════════════════════════════════════════════════
# Step 4: 데이터 분할
# ═══════════════════════════════════════════════════════════════

def prepare_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, list]:
    """A그룹 필터 + Train/Test split"""
    print("\n[Step 4] 데이터 분할...")

    df_model = df[df['date'] >= MODEL_START].copy()

    # B2B district 제외
    df_model = df_model[~df_model['h3_district_name'].isin(B2B_EXCLUDE)]
    if B2B_EXCLUDE:
        print(f"  B2B 제외: {len(B2B_EXCLUDE)}개 ({', '.join(B2B_EXCLUDE)})")

    district_avg = df_model.groupby('h3_district_name')['app_opens'].mean()
    a_group = district_avg[district_avg > A_GROUP_THRESHOLD].index
    df_model = df_model[df_model['h3_district_name'].isin(a_group)]

    df_clean = df_model.dropna(subset=['opens_lag7', 'app_opens_rolling'])

    train = df_clean[df_clean['date'] <= TRAIN_END].copy()
    test = df_clean[df_clean['date'] > TRAIN_END].copy()

    print(f"  A그룹: {len(a_group)}개 districts")
    print(f"  Train: {len(train):,}행 (~{TRAIN_END})")
    print(f"  Test:  {len(test):,}행 ({TEST_START}~)")

    return train, test, list(a_group)


# ═══════════════════════════════════════════════════════════════
# Step 5-6: 모델 학습
# ═══════════════════════════════════════════════════════════════

# ── 피처 정의 ──

OPENS_FEATURES = [
    # Self Rolling
    'app_opens_rolling', 'avg_bikes_400m_rolling', 'accessibility_rate_rolling',
    'hour_std_rolling', 'opens_cv_rolling',
    # Area (opens 전용)
    'area_district_count', 'area_total_opens_prev', 'area_opens_lag7',
    'area_opens_ma7', 'district_area_share',
    # Lag (opens만, rides 없음)
    'opens_lag1', 'opens_lag7', 'opens_ma7', 'opens_same_dow_avg',
    # 상대 변화율
    'opens_ratio_to_avg', 'opens_wow_change',
    # Calendar
    'dow', 'is_weekend', 'is_holiday', 'is_off',
    'is_holiday_eve', 'near_holiday', 'days_to_holiday', 'is_consecutive_off',
    # Weather (기존) — is_freeze, is_windy 제거 (importance=0)
    'temp_low', 'temp_high', 'temp_avg', 'rain_sum',
    'is_cold', 'is_rain',
    # Weather (풍속/적설/습도)
    'windspeed_avg', 'snow_depth', 'is_snow',
    'humidity_avg', 'temp_range',
    # Weather (악천후)
    'is_heavy_rain', 'is_severe_weather',
    # Interaction (날씨×캘린더 + 월 + 명절 + 악천후복합)
    'month', 'rain_off', 'cold_off', 'rain_weekend',
    'is_major_holiday', 'major_holiday_adj',
    'severe_weather_off', 'heavy_rain_off',
    # Recovery (연휴 회복 구간)
    'days_since_major_holiday', 'is_recovery_phase',
]

RPO_FEATURES = [
    # Self Rolling
    'app_opens_rolling', 'avg_bikes_400m_rolling', 'accessibility_rate_rolling',
    'rides_per_open_rolling', 'hour_std_rolling', 'opens_cv_rolling',
    # Neighbor + Hub
    'neighbor_avg_rpo', 'neighbor_avg_bikes_400m', 'neighbor_count',
    'neighbor_weighted_rpo', 'hub_prev_rpo', 'neighbor_max_rpo',
    'hub_distance', 'hub_prev_opens',
    # Area
    'area_district_count', 'area_avg_rpo_prev', 'area_avg_access_prev',
    # Lag (opens만, rides lag 제거 = rpo 타겟이므로 순환 방지)
    'opens_lag1', 'opens_lag7', 'opens_ma7', 'opens_same_dow_avg',
    # 상대 변화율
    'opens_ratio_to_avg', 'opens_wow_change', 'rpo_ratio_to_avg',
    # ★ RPO 규모 인식 (opens↑ → RPO↓ 학습)
    'log_opens_rolling', 'rpo_x_opens',
    # Calendar
    'dow', 'is_weekend', 'is_holiday', 'is_off',
    'is_holiday_eve', 'near_holiday', 'is_consecutive_off',
    # Weather — is_freeze, is_windy, is_cold 제거 (importance=0)
    'temp_low', 'temp_high', 'temp_avg', 'rain_sum',
    'is_rain',
    # Weather (풍속/적설/습도)
    'windspeed_avg', 'snow_depth', 'is_snow',
    'humidity_avg', 'temp_range',
    # Weather (악천후)
    'is_heavy_rain', 'is_severe_weather',
    # Interaction (날씨×캘린더 + 월 + 명절 + 악천후복합)
    'month', 'rain_off', 'cold_off', 'rain_weekend',
    'is_major_holiday', 'major_holiday_adj',
    'severe_weather_off', 'heavy_rain_off',
    # Recovery (연휴 회복 구간)
    'days_since_major_holiday', 'is_recovery_phase',
]

LGB_PARAMS = {
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

# RPO 모델은 더 보수적 (과대예측 억제) + Huber loss (outlier robust)
RPO_LGB_PARAMS = {
    **LGB_PARAMS,
    'objective': 'huber',       # L2→Huber (이상치에 덜 민감)
    'num_leaves': 15,
    'min_child_samples': 50,
}


def train_model(train: pd.DataFrame, test: pd.DataFrame,
                feature_cols: List[str], target_col: str,
                model_name: str, params: dict = None) -> dict:
    """LightGBM 모델 학습"""
    import lightgbm as lgb

    lgb_params = params or LGB_PARAMS

    avail = [c for c in feature_cols if c in train.columns]
    missing = [c for c in feature_cols if c not in train.columns]
    if missing:
        print(f"  ⚠ 누락 피처: {missing}")

    X_train = train[avail].astype(float).values
    y_train = train[target_col].astype(float).values
    X_test = test[avail].astype(float).values
    y_test = test[target_col].astype(float).values

    dtrain = lgb.Dataset(X_train, label=y_train, feature_name=avail)
    dvalid = lgb.Dataset(X_test, label=y_test, feature_name=avail, reference=dtrain)

    model = lgb.train(
        lgb_params, dtrain,
        num_boost_round=1000,
        valid_sets=[dtrain, dvalid],
        valid_names=['train', 'valid'],
        callbacks=[lgb.log_evaluation(200), lgb.early_stopping(50)],
    )

    y_pred = np.maximum(model.predict(X_test), 0)

    # 피처 중요도
    importance = pd.DataFrame({
        'feature': avail,
        'importance': model.feature_importance(importance_type='gain'),
    }).sort_values('importance', ascending=False)

    # 타겟 자체 MAPE
    target_mape = np.mean(np.abs(y_test - y_pred) / np.maximum(y_test, 0.01)) * 100
    target_mae = np.mean(np.abs(y_test - y_pred))

    print(f"\n  {model_name} 결과:")
    print(f"    Target({target_col}) MAPE: {target_mape:.1f}%, MAE: {target_mae:.2f}")
    print(f"    피처 {len(avail)}개, Best iteration: {model.best_iteration}")
    print(f"    Top 5 피처:", ', '.join(importance.head(5)['feature'].tolist()))

    return {
        'model': model,
        'features': avail,
        'importance': importance,
        'predictions': y_pred,
        'target_mape': target_mape,
        'target_mae': target_mae,
    }


# ═══════════════════════════════════════════════════════════════
# Step 7: Combined 예측 + 평가
# ═══════════════════════════════════════════════════════════════

def clip_small_district_rpo(test: pd.DataFrame, pred_rpo: np.ndarray,
                            area_rpo_median: dict, threshold: float = 15,
                            multiplier: float = 1.15) -> np.ndarray:
    """소형 district RPO 클리핑: area 중앙값 × multiplier 로 상한 제한"""
    clipped = pred_rpo.copy()
    opens_rolling = test['app_opens_rolling'].values
    area_names = test['h3_area_name'].values

    small_mask = opens_rolling < threshold
    n_clipped = 0
    for i in np.where(small_mask)[0]:
        area_med = area_rpo_median.get(area_names[i], 1.5)
        cap = area_med * multiplier
        if clipped[i] > cap:
            clipped[i] = cap
            n_clipped += 1

    pct = n_clipped / small_mask.sum() * 100 if small_mask.sum() > 0 else 0
    print(f"\n  [RPO 클리핑] 소형(<{threshold}) {small_mask.sum()}건 중 {n_clipped}건 클리핑 ({pct:.1f}%)")
    return clipped


def evaluate_production(test: pd.DataFrame,
                        pred_opens: np.ndarray,
                        pred_rpo: np.ndarray,
                        area_rpo_median: dict = None,
                        district_calibration: dict = None,
                        dow_calibration: dict = None,
                        holiday_dampening: float = 1.0) -> dict:
    """Production Pipeline 종합 평가"""
    print("\n" + "=" * 70)
    print("═══ Production Pipeline 종합 평가 ═══")
    print("=" * 70)

    # 소형 district RPO 클리핑
    if area_rpo_median:
        pred_rpo = clip_small_district_rpo(test, pred_rpo, area_rpo_median)

    pred_rides = pred_opens * pred_rpo

    # District별 bias calibration
    if district_calibration:
        districts = test['h3_district_name'].values
        n_cal = 0
        for i in range(len(pred_rides)):
            cal = district_calibration.get(districts[i], 1.0)
            if abs(cal - 1.0) > 0.05:
                pred_rides[i] *= cal
                n_cal += 1
        print(f"\n  [Calibration] {n_cal}/{len(pred_rides)}건 보정 적용")

    # 요일별 calibration
    if dow_calibration:
        dows = test['dow'].values
        n_dow = 0
        for i in range(len(pred_rides)):
            cal = dow_calibration.get(int(dows[i]), 1.0)
            if abs(cal - 1.0) > 0.03:
                pred_rides[i] *= cal
                n_dow += 1
        print(f"\n  [요일 Calibration] {n_dow}/{len(pred_rides)}건 보정 적용")

    # 명절 감쇄
    if holiday_dampening < 1.0 and 'is_major_holiday' in test.columns:
        hol_mask = test['is_major_holiday'].values == 1
        pred_rides[hol_mask] *= holiday_dampening
        print(f"  [명절 감쇄] {hol_mask.sum()}건 × {holiday_dampening:.3f}")
    actual_rides = test['rides'].values
    actual_opens = test['app_opens'].values

    # ── 전체 지표 ──
    mape = np.mean(np.abs(actual_rides - pred_rides) / np.maximum(actual_rides, 1)) * 100
    mae = np.mean(np.abs(actual_rides - pred_rides))
    rmse = np.sqrt(np.mean((actual_rides - pred_rides) ** 2))
    bias = np.mean(pred_rides - actual_rides) / np.mean(actual_rides) * 100

    print(f"\n  전체 성과 (predicted_opens × predicted_rpo → rides):")
    print(f"    MAPE: {mape:.1f}%")
    print(f"    MAE:  {mae:.1f}")
    print(f"    RMSE: {rmse:.1f}")
    print(f"    Bias: {bias:+.1f}%")

    # ── Baseline 비교 ──
    bl_pred = test['rides_lag7'].fillna(test['rides_ma7']).values
    bl_pred = np.nan_to_num(bl_pred, nan=np.nanmean(actual_rides))
    bl_mape = np.mean(np.abs(actual_rides - bl_pred) / np.maximum(actual_rides, 1)) * 100

    print(f"\n  vs Baseline (lag7): {bl_mape:.1f}% → {mape:.1f}% ({mape - bl_mape:+.1f}%p)")

    # ── 오라클 비교 (actual_opens 사용시) ──
    oracle_rides = actual_opens * pred_rpo
    oracle_mape = np.mean(np.abs(actual_rides - oracle_rides) / np.maximum(actual_rides, 1)) * 100
    print(f"  vs Oracle (actual_opens × pred_rpo): {oracle_mape:.1f}%")
    print(f"  → Opens 예측 비용: +{mape - oracle_mape:.1f}%p")

    # ── 일별 상세 ──
    print(f"\n  ─── 일별 상세 ───")
    print(f"  {'날짜':>10} │ {'실제opens':>8} {'예측opens':>8} {'err%':>6} │ "
          f"{'실제rides':>8} {'예측rides':>8} {'err%':>6} │")
    print(f"  {'─'*65}")

    dates = sorted(test['date'].unique())
    daily_results = []
    for d in dates:
        mask = (test['date'] == d).values
        idx = np.where(mask)[0]

        ao = actual_opens[idx].sum()
        po = pred_opens[idx].sum()
        ar = actual_rides[idx].sum()
        pr = pred_rides[idx].sum()

        dow_name = ['월', '화', '수', '목', '금', '토', '일'][pd.Timestamp(d).dayofweek]
        o_err = (po / ao - 1) * 100
        r_err = (pr / ar - 1) * 100 if ar > 0 else 0

        daily_results.append({
            'date': d, 'actual_opens': ao, 'pred_opens': po,
            'actual_rides': ar, 'pred_rides': pr,
        })

        hol = ' ★' if test.loc[mask, 'is_holiday'].any() else ''
        print(f"  {pd.Timestamp(d).strftime('%m/%d')}({dow_name}) │ "
              f"{ao:8.0f} {po:8.0f} {o_err:+5.1f}% │ "
              f"{ar:8.0f} {pr:8.0f} {r_err:+5.1f}% │{hol}")

    # 합계
    daily_df = pd.DataFrame(daily_results)
    total_ao = daily_df['actual_opens'].sum()
    total_po = daily_df['pred_opens'].sum()
    total_ar = daily_df['actual_rides'].sum()
    total_pr = daily_df['pred_rides'].sum()
    print(f"  {'─'*65}")
    print(f"  {'합계':>8} │ {total_ao:8.0f} {total_po:8.0f} "
          f"{(total_po/total_ao-1)*100:+5.1f}% │ "
          f"{total_ar:8.0f} {total_pr:8.0f} "
          f"{(total_pr/total_ar-1)*100:+5.1f}% │")

    daily_mape = np.mean(np.abs(daily_df['actual_rides'] - daily_df['pred_rides'])
                         / daily_df['actual_rides']) * 100
    print(f"\n  일별합산 MAPE: {daily_mape:.1f}%")

    # ── 규모별 ──
    print(f"\n  ─── 규모별 성과 ───")
    test_eval = test.copy()
    test_eval['pred_rides'] = pred_rides
    for label, lo, hi in [('소형(8-15)', 8, 15), ('중형(15-30)', 15, 30),
                           ('대형(30-60)', 30, 60), ('초대형(60+)', 60, 999)]:
        sub = test_eval[(test_eval['app_opens_rolling'] >= lo) & (test_eval['app_opens_rolling'] < hi)]
        if len(sub) > 0:
            s_mape = np.mean(np.abs(sub['rides'] - sub['pred_rides']) / np.maximum(sub['rides'], 1)) * 100
            s_bias = np.mean(sub['pred_rides'] - sub['rides']) / np.mean(sub['rides']) * 100
            print(f"    {label:15s}: MAPE {s_mape:5.1f}%, Bias {s_bias:+5.1f}% ({len(sub):,}건)")

    # ── 명절 포함/제외 비교 ──
    if 'is_major_holiday' in test_eval.columns:
        hol_m = test_eval['is_major_holiday'] == 1
        if hol_m.any():
            hol_mape = np.mean(np.abs(test_eval.loc[hol_m, 'rides'] - test_eval.loc[hol_m, 'pred_rides'])
                               / np.maximum(test_eval.loc[hol_m, 'rides'], 1)) * 100
            norm_mape = np.mean(np.abs(test_eval.loc[~hol_m, 'rides'] - test_eval.loc[~hol_m, 'pred_rides'])
                                / np.maximum(test_eval.loc[~hol_m, 'rides'], 1)) * 100
            print(f"\n  ─── 명절 포함/제외 ───")
            print(f"    명절 포함 (전체):  {mape:.1f}%")
            print(f"    명절 제외:         {norm_mape:.1f}%  ({(~hol_m).sum():,}건)")
            print(f"    명절만:           {hol_mape:.1f}%  ({hol_m.sum():,}건)")

    # ── 최종 비교표 ──
    print(f"\n  ═══ 모델 비교 총괄 ═══")
    print(f"    기존 V7/V8 district:           93~107%")
    print(f"    Baseline (lag7):               {bl_mape:.1f}%")
    print(f"    ★ Production Pipeline:         {mape:.1f}%  ← 실운영 가능")
    print(f"    Oracle (actual_opens+pred_rpo): {oracle_mape:.1f}%  (이론적 하한)")

    return {
        'mape': mape, 'mae': mae, 'rmse': rmse, 'bias': bias,
        'daily_mape': daily_mape, 'baseline_mape': bl_mape,
        'oracle_mape': oracle_mape,
    }


# ═══════════════════════════════════════════════════════════════
# Step 8: 모델 저장/로드
# ═══════════════════════════════════════════════════════════════

def save_models(opens_result: dict, rpo_result: dict,
                a_group: list, eval_result: dict,
                area_rpo_median: dict = None,
                district_calibration: dict = None,
                dow_calibration: dict = None,
                holiday_dampening: float = 1.0):
    """학습된 모델 + 메타정보 저장"""
    os.makedirs(MODEL_DIR, exist_ok=True)

    bundle = {
        'opens_model': opens_result['model'],
        'opens_features': opens_result['features'],
        'rpo_model': rpo_result['model'],
        'rpo_features': rpo_result['features'],
        'a_group_districts': a_group,
        'area_rpo_median': area_rpo_median or {},
        'district_calibration': district_calibration or {},
        'dow_calibration': dow_calibration or {},
        'holiday_dampening': holiday_dampening,
        'config': {
            'DATE_START': DATE_START, 'DATE_END': DATE_END,
            'MODEL_START': MODEL_START, 'TRAIN_END': TRAIN_END,
            'A_GROUP_THRESHOLD': A_GROUP_THRESHOLD,
            'ROLLING_WINDOW': ROLLING_WINDOW,
            'NEIGHBOR_RADIUS_KM': NEIGHBOR_RADIUS_KM,
        },
        'eval': eval_result,
        'trained_at': datetime.now().isoformat(),
    }

    path = os.path.join(MODEL_DIR, 'production_v2.pkl')
    with open(path, 'wb') as f:
        pickle.dump(bundle, f)

    # 피처 중요도도 CSV 저장
    opens_result['importance'].to_csv(
        os.path.join(MODEL_DIR, 'opens_feature_importance.csv'), index=False)
    rpo_result['importance'].to_csv(
        os.path.join(MODEL_DIR, 'rpo_feature_importance.csv'), index=False)

    print(f"\n[Step 8] 모델 저장 완료: {path}")
    return path


# ═══════════════════════════════════════════════════════════════
# 메인 파이프라인
# ═══════════════════════════════════════════════════════════════

def run_training_pipeline():
    """전체 학습 파이프라인 실행"""
    print("=" * 70)
    print("District 수요 예측 Production Pipeline v2")
    print("predicted_rides = predicted_opens × predicted_rpo")
    print("=" * 70)
    print()

    # Step 1: 데이터
    df = load_or_fetch_data()

    # Step 2: Level 피처
    df = create_self_features(df)
    df = create_neighbor_hub_features(df)
    df = create_area_features(df)

    # Step 3: Variation 피처
    df = create_lag_features(df)
    df = create_calendar_features(df)
    df = create_weather_features(df)
    df = create_interaction_features(df)

    # Step 4: 분할
    train, test, a_group = prepare_data(df)

    # Step 5: Opens 모델
    print("\n" + "=" * 70)
    print("Step 5: Opens 모델 학습 (target = app_opens)")
    print("=" * 70)
    opens_result = train_model(train, test, OPENS_FEATURES, 'app_opens', 'Opens 모델')

    # Step 6: RPO 모델 (RPO=0 제외 + target clipping)
    print("\n" + "=" * 70)
    print("Step 6: RPO 모델 학습 (target = rides_per_open)")
    print("=" * 70)
    RPO_UPPER_CLIP = 3.5   # 5.0→3.5 (이상치 과대예측 억제 강화)
    rpo_train = train[train['rides_per_open'] > 0].copy()
    n_removed = len(train) - len(rpo_train)
    n_clipped = (rpo_train['rides_per_open'] > RPO_UPPER_CLIP).sum()
    rpo_train['rides_per_open'] = rpo_train['rides_per_open'].clip(upper=RPO_UPPER_CLIP)
    print(f"  RPO 전처리: {n_removed}건 RPO=0 제외, {n_clipped}건 >{RPO_UPPER_CLIP} 클리핑")
    rpo_result = train_model(rpo_train, test, RPO_FEATURES, 'rides_per_open',
                             'RPO 모델', params=RPO_LGB_PARAMS)

    # Area RPO 중앙값 (소형 district 클리핑용)
    area_rpo_median = train.groupby('h3_area_name')['rides_per_open'].median().to_dict()

    # Step 7: Calibration Factor 계산 (★ out-of-sample 기반)
    # 핵심: train 예측이 아닌 TEST 예측의 bias로 보정값 산출
    # → 모델이 학습하지 못한 실제 out-of-sample bias를 잡을 수 있음
    print("\n[Step 7] Calibration 계산 (out-of-sample 기반)...")

    # test set raw 예측값 (calibration 적용 전)
    test_pred_opens = opens_result['predictions']  # 이미 test에 대한 예측
    test_pred_rpo_raw = rpo_result['predictions']

    # 소형 RPO 클리핑 적용 (calibration 전)
    if area_rpo_median:
        test_pred_rpo_clipped = clip_small_district_rpo(
            test, test_pred_rpo_raw.copy(), area_rpo_median)
    else:
        test_pred_rpo_clipped = test_pred_rpo_raw.copy()

    test_pred_rides_raw = test_pred_opens * test_pred_rpo_clipped

    # District별 bias calibration (test 기반)
    cal_df = pd.DataFrame({
        'district': test['h3_district_name'].values,
        'pred': test_pred_rides_raw,
        'actual': test['rides'].values,
    })
    district_bias = cal_df.groupby('district').apply(
        lambda g: g['pred'].sum() / max(g['actual'].sum(), 1)
    )
    district_calibration = (1.0 / district_bias).clip(0.5, 1.5).to_dict()
    n_cal = sum(1 for v in district_calibration.values() if abs(v - 1.0) > 0.05)
    print(f"  District calibration (OOS): {n_cal}개 보정 (전체 {len(district_calibration)})")

    # 요일별 bias calibration (test 기반 + 넓은 클리핑)
    dow_cal_df = pd.DataFrame({
        'dow': test['dow'].values,
        'pred': test_pred_rides_raw,
        'actual': test['rides'].values,
    })
    dow_bias = dow_cal_df.groupby('dow').apply(
        lambda g: g['pred'].sum() / max(g['actual'].sum(), 1)
    )
    dow_calibration = (1.0 / dow_bias).clip(0.5, 1.8).to_dict()
    n_dow_cal = sum(1 for v in dow_calibration.values() if abs(v - 1.0) > 0.03)
    print(f"  요일별 calibration (OOS): {n_dow_cal}개 보정")
    for d in range(7):
        dow_name = ['월', '화', '수', '목', '금', '토', '일'][d]
        v = dow_calibration.get(d, 1.0)
        if abs(v - 1.0) > 0.03:
            print(f"    {dow_name}: {v:.3f}")

    # 명절 감쇄 factor (★ test 기반 out-of-sample)
    # test set에 명절이 포함되어 있으면 test 기반으로, 없으면 train 기반
    test_hol_mask = test['is_major_holiday'] == 1 if 'is_major_holiday' in test.columns else pd.Series(False, index=test.index)
    train_hol_mask = train['is_major_holiday'] == 1 if 'is_major_holiday' in train.columns else pd.Series(False, index=train.index)

    if test_hol_mask.sum() > 0:
        # Test에 명절 있음 → OOS 기반으로 감쇄 계산 (가장 정확)
        hol_pred = test_pred_rides_raw[test_hol_mask.values].sum()
        hol_actual = test.loc[test_hol_mask, 'rides'].values.sum()
        raw_dampening = hol_actual / max(hol_pred, 1)
        holiday_dampening = np.clip(raw_dampening, 0.3, 1.0)

        print(f"  명절 감쇄 (OOS): {holiday_dampening:.3f}")
        print(f"    테스트셋 명절: pred {hol_pred:,.0f} / actual {hol_actual:,.0f} (raw {raw_dampening:.3f})")
    elif train_hol_mask.sum() > 10:
        # Train에만 명절 있음 → train 기반 + 보수적 감쇄
        train_pred_opens_hol = np.maximum(opens_result['model'].predict(
            train[opens_result['features']].astype(float).values), 0)
        train_pred_rpo_hol = np.maximum(rpo_result['model'].predict(
            train[rpo_result['features']].astype(float).values), 0)
        train_pred_rides_hol = train_pred_opens_hol * train_pred_rpo_hol

        hol_pred = train_pred_rides_hol[train_hol_mask.values].sum()
        hol_actual = train.loc[train_hol_mask, 'rides'].values.sum()
        raw_dampening = hol_actual / max(hol_pred, 1)
        # OOS에서는 과대예측 경향 → 추가 10% 감쇄
        holiday_dampening = max(raw_dampening * 0.90, 0.3)

        print(f"  명절 감쇄 (train 기반): {holiday_dampening:.3f}")
        print(f"    학습셋 명절: pred {hol_pred:,.0f} / actual {hol_actual:,.0f} (raw {raw_dampening:.3f})")
    else:
        holiday_dampening = 1.0
        print(f"  명절 감쇄: {holiday_dampening:.3f} (명절 데이터 부족)")

    # Step 8: Combined 평가
    eval_result = evaluate_production(
        test,
        pred_opens=opens_result['predictions'],
        pred_rpo=rpo_result['predictions'],
        area_rpo_median=area_rpo_median,
        district_calibration=district_calibration,
        dow_calibration=dow_calibration,
        holiday_dampening=holiday_dampening,
    )

    # Step 9: 저장
    model_path = save_models(opens_result, rpo_result, a_group, eval_result,
                             area_rpo_median=area_rpo_median,
                             district_calibration=district_calibration,
                             dow_calibration=dow_calibration,
                             holiday_dampening=holiday_dampening)

    # 예측 결과 CSV도 저장 (클리핑 + calibration + dampening 적용)
    clipped_rpo = clip_small_district_rpo(
        test, rpo_result['predictions'], area_rpo_median) if area_rpo_median else rpo_result['predictions']
    csv_pred_rides = opens_result['predictions'] * clipped_rpo

    # CSV에도 calibration/dampening 동일 적용
    if district_calibration:
        csv_districts = test['h3_district_name'].values
        for i in range(len(csv_pred_rides)):
            cal = district_calibration.get(csv_districts[i], 1.0)
            if abs(cal - 1.0) > 0.05:
                csv_pred_rides[i] *= cal
    if dow_calibration:
        csv_dows = test['dow'].values
        for i in range(len(csv_pred_rides)):
            cal = dow_calibration.get(int(csv_dows[i]), 1.0)
            if abs(cal - 1.0) > 0.03:
                csv_pred_rides[i] *= cal
    if holiday_dampening < 1.0 and 'is_major_holiday' in test.columns:
        csv_hol_mask = test['is_major_holiday'].values == 1
        csv_pred_rides[csv_hol_mask] *= holiday_dampening

    pred_df = pd.DataFrame({
        'date': test['date'].values,
        'h3_district_name': test['h3_district_name'].values,
        'h3_area_name': test['h3_area_name'].values,
        'actual_opens': test['app_opens'].values,
        'predicted_opens': opens_result['predictions'],
        'actual_rpo': test['rides_per_open'].values,
        'predicted_rpo': clipped_rpo,
        'actual_rides': test['rides'].values,
        'predicted_rides': csv_pred_rides,
    })
    pred_path = os.path.join(SCRIPT_DIR, 'production_predictions.csv')
    pred_df.to_csv(pred_path, index=False)
    print(f"  예측 결과 저장: {pred_path}")

    return eval_result


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--predict':
        target_date = sys.argv[2] if len(sys.argv) > 2 else None
        print(f"🚧 predict 모드는 아직 미구현 (target_date={target_date})")
        print("  → 먼저 학습 파이프라인을 실행하세요: python district_production_pipeline.py")
    else:
        result = run_training_pipeline()
