"""
Production v2 District-level Predictor Module
대시보드/CLI 통합용 예측 모듈

Architecture:
    predicted_rides = predicted_app_opens × predicted_rpo

사용법:
    from production_v2_predictor import predict_district_rides, evaluate_period

    # 단일 날짜 예측 (미래 또는 과거)
    district_df, region_df = predict_district_rides('2026-02-27')

    # 기간 평가 (배치 - 주간 성과용)
    eval_df = evaluate_period('2026-02-20', '2026-02-26')
"""

import os
import json
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(SCRIPT_DIR, 'models', 'production_v2.pkl')
WEATHER_CSV = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')

# Authentication: set GOOGLE_APPLICATION_CREDENTIALS env var
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    _default_cred = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'credentials', 'service-account.json')
    if os.path.exists(_default_cred):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _default_cred

ROLLING_WINDOW = 14
NEIGHBOR_RADIUS_KM = 2.0
LOOKBACK_DAYS = 45  # rolling(14) + lags(28) + buffer


# ============================================================
# Model Loading (cached)
# ============================================================

_model_cache = {}


def load_model_bundle() -> dict:
    """모델 번들 로드 (메모리 캐싱)

    NOTE: pickle.load()는 신뢰할 수 있는 로컬 모델 파일에만 사용합니다.
    외부 출처의 pickle 파일은 임의 코드 실행 위험이 있으므로 사용하지 마세요.
    """
    if 'bundle' not in _model_cache:
        if not os.path.exists(MODEL_PATH):
            raise FileNotFoundError(f"모델 파일 없음: {MODEL_PATH}")
        with open(MODEL_PATH, 'rb') as f:
            # SECURITY: 로컬에서 직접 학습한 모델만 로드 (외부 파일 사용 금지)
            _model_cache['bundle'] = pickle.load(f)
    return _model_cache['bundle']


def get_center_map() -> Dict[str, str]:
    """region → center 매핑 (region_params.json 기반)"""
    params_path = os.path.join(SCRIPT_DIR, 'region_params.json')
    if os.path.exists(params_path):
        with open(params_path) as f:
            params = json.load(f)
        return {r: v.get('center', '') for r, v in params.items()}
    return {}


# ============================================================
# Data Fetching
# ============================================================

def _get_bq_client():
    """BigQuery 클라이언트 생성"""
    from google.cloud import bigquery
    return bigquery.Client()


def fetch_data_range(
    start_date: str,
    end_date: str,
    verbose: bool = True
) -> pd.DataFrame:
    """BigQuery에서 district×date 데이터 추출

    Args:
        start_date: 시작일 (YYYY-MM-DD)
        end_date: 종료일 (YYYY-MM-DD)
        verbose: 진행 상황 출력
    """
    client = _get_bq_client()

    if verbose:
        print(f"  BQ 데이터 추출: {start_date} ~ {end_date}")

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
    WHERE date BETWEEN "{start_date}" AND "{end_date}"
        AND near_geoblock = True
    GROUP BY date, h3_area_name, h3_district_name
    '''

    q_rides = f'''
    SELECT
        start_date as date,
        h3_start_district_name as h3_district_name,
        COUNT(*) as rides
    FROM service.tf_riding
    WHERE start_date BETWEEN "{start_date}" AND "{end_date}"
        AND bike_type = 1
    GROUP BY start_date, h3_start_district_name
    '''

    df_opens = client.query(q_opens).to_dataframe()
    df_rides = client.query(q_rides).to_dataframe()
    df_opens['date'] = pd.to_datetime(df_opens['date'])
    df_rides['date'] = pd.to_datetime(df_rides['date'])

    df = df_opens.merge(
        df_rides[['date', 'h3_district_name', 'rides']],
        on=['date', 'h3_district_name'], how='left'
    )
    df['rides'] = df['rides'].fillna(0).astype(int)
    df['rides_per_open'] = df['rides'] / df['app_opens']

    if verbose:
        print(f"  {len(df):,}행, {df.h3_district_name.nunique()} districts, "
              f"{df.date.nunique()}일")

    return df


def fetch_holidays(start_date: str, end_date: str) -> set:
    """BigQuery에서 공휴일 조회"""
    client = _get_bq_client()
    hol_df = client.query(f'''
        SELECT date FROM sources.korean_holiday
        WHERE date BETWEEN "{start_date}" AND "{end_date}"
    ''').to_dataframe()
    hol_df['date'] = pd.to_datetime(hol_df['date'])
    return set(hol_df['date'])


# ============================================================
# Feature Engineering
# ============================================================

def _add_target_date_rows(
    df: pd.DataFrame,
    target_date: str,
    a_group_districts: list
) -> pd.DataFrame:
    """미래 예측용: target_date에 dummy 행 추가"""
    target = pd.Timestamp(target_date)

    # 이미 target_date 데이터가 있으면 스킵
    if target in df['date'].values:
        return df

    # 각 A그룹 district에 대해 좌표/area 정보를 최근 데이터에서 가져옴
    district_info = df.groupby('h3_district_name').agg(
        h3_area_name=('h3_area_name', 'first'),
        center_lat=('center_lat', 'mean'),
        center_lng=('center_lng', 'mean'),
    ).reset_index()

    target_districts = district_info[
        district_info['h3_district_name'].isin(a_group_districts)
    ].copy()
    target_districts['date'] = target

    # 미지값 = NaN
    for col in ['app_opens', 'rides', 'rides_per_open', 'avg_bikes_100m',
                'avg_bikes_400m', 'avg_distance', 'accessibility_rate',
                'conversion_rate', 'cond_conversion_rate', 'hour_std']:
        target_districts[col] = np.nan

    df = pd.concat([df, target_districts], ignore_index=True)
    df = df.sort_values(['h3_district_name', 'date']).reset_index(drop=True)
    return df


def build_self_features(df: pd.DataFrame) -> pd.DataFrame:
    """Self Features: 14일 롤링 통계 (shift(1) → 데이터 누출 없음)"""
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
    return df


def build_neighbor_hub_features(df: pd.DataFrame) -> pd.DataFrame:
    """Neighbor + Hub Features: 벡터화 이웃 평균 + 허브 참조"""
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

    # ── 허브 식별 ──
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

    df['hub_district'] = df['h3_district_name'].map(hub_map)
    df['hub_distance'] = df['h3_district_name'].map(hub_dist_map)
    df['is_self_hub'] = (df['h3_district_name'] == df['hub_district']).astype(int)

    # ── 거리 가중 행렬 ──
    dist_weight = np.where(neighbor_mask, 1.0 / np.maximum(dist_matrix, 0.1), 0)
    row_sums = dist_weight.sum(axis=1, keepdims=True)
    dist_weight_norm = np.where(row_sums > 0, dist_weight / row_sums, 0)

    df = df.sort_values(['h3_district_name', 'date']).copy()
    g = df.groupby('h3_district_name')

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
            wm = dist_weight_norm[np.ix_(idx_map, idx_map)]
            w_sum = vals_f @ wm.T
            w_cnt = cnt_v @ wm.T
            w_avg = np.where(w_cnt > 0, w_sum / w_cnt, np.nan)
            w_pivot = pd.DataFrame(w_avg, index=pivot.index, columns=pivot.columns)
            w_melted = w_pivot.reset_index().melt(
                id_vars='date', var_name='h3_district_name', value_name=weighted_col)
            df = df.merge(w_melted, on=['date', 'h3_district_name'], how='left')

        if max_col:
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

    # ── 허브 전일 rpo/opens ──
    hub_lookup = df[['date', 'h3_district_name']].copy()
    hub_lookup['_hub_rpo'] = g['rides_per_open'].shift(1)
    hub_lookup['_hub_opens'] = g['app_opens'].shift(1)
    hub_lookup.columns = ['date', 'hub_district', 'hub_prev_rpo', 'hub_prev_opens']
    df = df.merge(hub_lookup, on=['date', 'hub_district'], how='left')

    return df


def build_area_features(df: pd.DataFrame) -> pd.DataFrame:
    """Area Features: 상위 area 수준 집계"""
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

    df['district_area_share'] = (
        df['app_opens_rolling'] /
        df.groupby(['date', 'h3_area_name'])['app_opens_rolling'].transform('sum')
    )
    return df


def build_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Lag Features: 시차 변수"""
    df = df.sort_values(['h3_district_name', 'date']).copy()
    g = df.groupby('h3_district_name')

    df['opens_lag1'] = g['app_opens'].shift(1)
    df['opens_lag7'] = g['app_opens'].shift(7)
    df['opens_ma7'] = g['app_opens'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=3).mean())

    for lag in [14, 21, 28]:
        df[f'_opens_lag{lag}'] = g['app_opens'].shift(lag)
    df['opens_same_dow_avg'] = df[['opens_lag7', '_opens_lag14', '_opens_lag21', '_opens_lag28']].mean(axis=1)
    df = df.drop(columns=['_opens_lag14', '_opens_lag21', '_opens_lag28'])

    df['rides_lag1'] = g['rides'].shift(1)
    df['rides_lag7'] = g['rides'].shift(7)
    df['rides_ma7'] = g['rides'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=3).mean())

    # ── 상대 변화율 피처 (절대 수량이 아닌 앞뒤 대비 증감) ──
    df['opens_ratio_to_avg'] = df['opens_lag1'] / df['app_opens_rolling'].clip(lower=1)
    df['opens_wow_change'] = (df['opens_lag1'] - df['opens_lag7']) / df['opens_lag7'].clip(lower=1)

    rpo_lag1 = g['rides_per_open'].shift(1)
    df['rpo_ratio_to_avg'] = rpo_lag1 / df['rides_per_open_rolling'].clip(lower=0.01)

    # ── RPO 규모 인식 피처 (opens 많을수록 RPO 낮아지는 패턴) ──
    df['log_opens_rolling'] = np.log1p(df['app_opens_rolling'].fillna(0))
    df['rpo_x_opens'] = df['rides_per_open_rolling'].fillna(0) * df['app_opens_rolling'].fillna(0)

    return df


def build_calendar_features(df: pd.DataFrame, holidays_set: set) -> pd.DataFrame:
    """Calendar Features: 공휴일 기반"""
    df['dow'] = df['date'].dt.dayofweek
    df['is_weekend'] = (df['dow'] >= 5).astype(int)
    df['is_holiday'] = df['date'].isin(holidays_set).astype(int)
    df['is_off'] = ((df['is_weekend'] == 1) | (df['is_holiday'] == 1)).astype(int)

    holiday_eves = {h - pd.Timedelta(days=1) for h in holidays_set} - holidays_set
    df['is_holiday_eve'] = df['date'].isin(holiday_eves).astype(int)

    holidays_sorted = sorted(holidays_set) if holidays_set else []
    if holidays_sorted:
        df['days_to_holiday'] = df['date'].apply(
            lambda dt: min([(h - dt).days for h in holidays_sorted], key=abs))
        df['near_holiday'] = (df['days_to_holiday'].abs() <= 2).astype(int)
    else:
        df['days_to_holiday'] = 99
        df['near_holiday'] = 0

    df = df.sort_values(['h3_district_name', 'date'])
    df['prev_is_off'] = df.groupby('h3_district_name')['is_off'].shift(1)
    df['is_consecutive_off'] = ((df['is_off'] == 1) & (df['prev_is_off'] == 1)).astype(int)
    df = df.drop(columns=['prev_is_off'])

    # ── 대형연휴 후 회복 구간 피처 (A2) ──
    # 대형연휴 블록을 먼저 계산 (build_interaction_features 전에 필요)
    df['_hol_block_cal'] = df.groupby('h3_district_name')['is_holiday'].transform(
        lambda x: x.rolling(3, min_periods=1, center=True).sum()
    )
    df['_is_major_holiday_cal'] = (df['_hol_block_cal'] >= 2).astype(int)

    def _days_since_major_holiday_end(group):
        """대형연휴 마지막 날 이후 경과 일수 계산"""
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

    return df


def build_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    """Weather Features: 관측(CSV) + 예보(API) 통합, 풍속/습도/적설 포함

    개선(v2.1): ASOS 백필 우선 → 예보 API → 7일 평균 fallback (기존 ffill 대신)
    """
    # 0. ASOS 백필 시도 (CSV 갭 해소)
    try:
        from fetch_weather_forecast import backfill_asos_weather
        backfill_asos_weather(WEATHER_CSV)
    except Exception:
        pass

    # 1. CSV 관측 데이터 로드
    if os.path.exists(WEATHER_CSV):
        wdf = pd.read_csv(WEATHER_CSV)
        wdf['date'] = pd.to_datetime(wdf['date'])
        weather_cols = ['date', 'temp_low', 'temp_high', 'rain_sum',
                        'windspeed_avg', 'humidity_avg', 'snow_depth']
        weather = wdf[[c for c in weather_cols if c in wdf.columns]].copy()
        weather['rain_sum'] = weather['rain_sum'].fillna(0)
        if 'windspeed_avg' not in weather.columns:
            weather['windspeed_avg'] = 0.0
        if 'humidity_avg' not in weather.columns:
            weather['humidity_avg'] = 50.0
        if 'snow_depth' not in weather.columns:
            weather['snow_depth'] = 0.0
    else:
        weather = pd.DataFrame(columns=['date', 'temp_low', 'temp_high', 'rain_sum',
                                         'windspeed_avg', 'humidity_avg', 'snow_depth'])

    # 2. 예보 데이터로 미래 날짜 보완
    all_dates = df['date'].unique()
    max_observed = weather['date'].max() if len(weather) > 0 else pd.Timestamp('2000-01-01')
    future_dates = [d for d in all_dates if d > max_observed]

    forecast_rows = []
    if future_dates:
        try:
            from fetch_weather_forecast import get_forecast_weather
            for d in sorted(future_dates):
                fw = get_forecast_weather(pd.Timestamp(d).strftime('%Y-%m-%d'), verbose=False)
                if fw:
                    forecast_rows.append({
                        'date': pd.Timestamp(fw['date']),
                        'temp_low': fw['temp_low'],
                        'temp_high': fw['temp_high'],
                        'rain_sum': fw.get('rain_sum', 0),
                        'windspeed_avg': fw.get('windspeed_avg', 0),
                        'humidity_avg': fw.get('humidity_avg', 50),
                        'snow_depth': fw.get('snow_sum', 0),
                    })
        except ImportError:
            pass

        if forecast_rows:
            forecast_df = pd.DataFrame(forecast_rows)
            weather = pd.concat([weather, forecast_df], ignore_index=True)
            weather = weather.drop_duplicates(subset=['date'], keep='last')

    # 2.5. 예보 실패한 미래 날짜 → 최근 7일 평균 fallback (ffill 대신)
    covered_dates = set(weather['date'].values) if len(weather) > 0 else set()
    uncovered = [d for d in future_dates if d not in covered_dates]
    if uncovered and len(weather) >= 7:
        recent_avg = weather.sort_values('date').tail(7).mean(numeric_only=True)
        for d in uncovered:
            fallback_row = {'date': pd.Timestamp(d)}
            for col in ['temp_low', 'temp_high', 'rain_sum', 'windspeed_avg',
                         'humidity_avg', 'snow_depth']:
                fallback_row[col] = recent_avg.get(col, 0)
            weather = pd.concat([weather, pd.DataFrame([fallback_row])], ignore_index=True)
        staleness_days = (pd.Timestamp(max(uncovered)) - max_observed).days
        if staleness_days > 2:
            print(f"  [날씨 경고] {len(uncovered)}일치 예보 실패 → 7일 평균 fallback "
                  f"(CSV 마지막: {max_observed.strftime('%Y-%m-%d')}, 갭: {staleness_days}일)")

    # 3. 파생 변수
    weather['windspeed_avg'] = weather['windspeed_avg'].fillna(0)
    weather['humidity_avg'] = weather['humidity_avg'].fillna(50)
    weather['snow_depth'] = weather['snow_depth'].fillna(0)
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

    # 4. 결측 처리: ffill → 기본값
    for col in ['temp_low', 'temp_high', 'temp_avg', 'rain_sum',
                 'windspeed_avg', 'humidity_avg', 'snow_depth', 'temp_range']:
        df[col] = df[col].ffill()
        df[col] = df[col].fillna(0)
    for col in ['is_cold', 'is_freeze', 'is_rain', 'is_heavy_rain',
                 'is_windy', 'is_snow', 'is_severe_weather']:
        df[col] = df[col].fillna(0).astype(int)

    return df


def build_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """Interaction Features: 날씨×캘린더 교호작용 + 월 + 대형연휴 + 악천후복합(A3)"""
    df['month'] = df['date'].dt.month
    df['rain_off'] = df.get('is_rain', 0) * df.get('is_off', 0)
    df['cold_off'] = df.get('is_cold', 0) * df.get('is_off', 0)
    df['rain_weekend'] = df.get('is_rain', 0) * df.get('is_weekend', 0)

    # ── 대형연휴 (설/추석 등 3일 이상 연속 휴일) ──
    df['_hol_block'] = df.groupby('h3_district_name')['is_holiday'].transform(
        lambda x: x.rolling(3, min_periods=1, center=True).sum()
    )
    df['is_major_holiday'] = (df['_hol_block'] >= 2).astype(int)

    g = df.groupby('h3_district_name')
    df['major_holiday_adj'] = (
        (df['is_major_holiday'] == 1) |
        (g['is_major_holiday'].shift(1) == 1) |
        (g['is_major_holiday'].shift(-1) == 1)
    ).astype(int)
    df = df.drop(columns=['_hol_block'])

    # ── A3: 악천후 + 휴일 복합 피처 ──
    is_heavy_rain = df.get('is_heavy_rain', pd.Series(0, index=df.index))
    is_severe = df.get('is_severe_weather', pd.Series(0, index=df.index))
    is_off = df.get('is_off', pd.Series(0, index=df.index))
    df['severe_weather_off'] = (is_severe * is_off).astype(int)
    df['heavy_rain_off'] = (is_heavy_rain * is_off).astype(int)

    return df


def build_all_features(
    df: pd.DataFrame,
    holidays_set: set,
    verbose: bool = False
) -> pd.DataFrame:
    """전체 피처 파이프라인 (self → neighbor → area → lag → calendar → weather → interaction)"""
    if verbose:
        print("  피처 생성: self...", end='', flush=True)
    df = build_self_features(df)

    if verbose:
        print(" neighbor...", end='', flush=True)
    df = build_neighbor_hub_features(df)

    if verbose:
        print(" area...", end='', flush=True)
    df = build_area_features(df)

    if verbose:
        print(" lag...", end='', flush=True)
    df = build_lag_features(df)

    if verbose:
        print(" calendar...", end='', flush=True)
    df = build_calendar_features(df, holidays_set)

    if verbose:
        print(" weather...", end='', flush=True)
    df = build_weather_features(df)

    if verbose:
        print(" interaction...", end='', flush=True)
    df = build_interaction_features(df)

    if verbose:
        print(" 완료!")

    return df


# ============================================================
# 단일 날짜 예측
# ============================================================

def predict_district_rides(
    target_date: str,
    cache_data: Optional[pd.DataFrame] = None,
    verbose: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    District-level 수요 예측 (단일 날짜)

    Args:
        target_date: 예측 대상 날짜 (YYYY-MM-DD)
        cache_data: 캐시된 BQ 데이터 (None이면 BQ에서 추출)
        verbose: 진행 상황 출력

    Returns:
        (district_df, region_df)
        - district_df: columns [region, district, adj_pred, pred_opens, pred_rpo,
                                lat, lng, center, desc, ratio]
        - region_df:   columns [region, adj_pred, center, n_districts, desc]
    """
    bundle = load_model_bundle()
    opens_model = bundle['opens_model']
    rpo_model = bundle['rpo_model']
    opens_features = bundle['opens_features']
    rpo_features = bundle['rpo_features']
    a_group = bundle['a_group_districts']
    area_rpo_median = bundle.get('area_rpo_median', {})
    district_calibration = bundle.get('district_calibration', {})
    dow_calibration = bundle.get('dow_calibration', {})
    holiday_dampening = bundle.get('holiday_dampening', 1.0)

    if verbose:
        print(f"\n[Production v2] {target_date} 예측")

    # 1. 데이터
    if cache_data is not None:
        df = cache_data.copy()
        if verbose:
            print(f"  캐시 사용: {len(df):,}행")
    else:
        target = pd.Timestamp(target_date)
        fetch_start = (target - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
        # end_date: target_date 포함 (과거) 또는 target_date-1 (미래)
        fetch_end = target_date
        df = fetch_data_range(fetch_start, fetch_end, verbose=verbose)

    # 2. 미래 예측인 경우 dummy row 추가
    df = _add_target_date_rows(df, target_date, a_group)

    # 3. 공휴일 조회
    min_d = df['date'].min().strftime('%Y-%m-%d')
    max_d = (pd.Timestamp(target_date) + timedelta(days=30)).strftime('%Y-%m-%d')
    holidays_set = fetch_holidays(min_d, max_d)

    # 4. Feature Engineering
    df = build_all_features(df, holidays_set, verbose=verbose)

    # 5. target_date + A그룹 추출
    target = pd.Timestamp(target_date)
    tdf = df[
        (df['date'] == target) &
        (df['h3_district_name'].isin(a_group))
    ].copy()

    # 피처 결측 행 제거
    required_feats = ['opens_lag7', 'app_opens_rolling']
    available_required = [f for f in required_feats if f in tdf.columns]
    if available_required:
        tdf = tdf.dropna(subset=available_required)

    if verbose:
        print(f"  예측 대상: {len(tdf)} districts")

    if len(tdf) == 0:
        return pd.DataFrame(), pd.DataFrame()

    # 6. 모델 예측
    # 결측 피처 0 채움 (모델 입력)
    for feat in opens_features + rpo_features:
        if feat in tdf.columns:
            tdf[feat] = tdf[feat].fillna(0)

    X_opens = tdf[opens_features].astype(float).values
    pred_opens = np.maximum(opens_model.predict(X_opens), 0)

    X_rpo = tdf[rpo_features].astype(float).values
    pred_rpo = np.maximum(rpo_model.predict(X_rpo), 0)

    # 소형 district RPO 클리핑 (area 중앙값 × 1.15)
    if area_rpo_median:
        opens_rolling = tdf['app_opens_rolling'].values
        area_names = tdf['h3_area_name'].values
        for i in range(len(pred_rpo)):
            if opens_rolling[i] < 15:
                area_med = area_rpo_median.get(area_names[i], 1.5)
                pred_rpo[i] = min(pred_rpo[i], area_med * 1.15)

    pred_rides = pred_opens * pred_rpo

    # District별 bias calibration
    if district_calibration:
        districts = tdf['h3_district_name'].values
        for i in range(len(pred_rides)):
            cal = district_calibration.get(districts[i], 1.0)
            if abs(cal - 1.0) > 0.05:
                pred_rides[i] *= cal

    # 요일별 calibration
    if dow_calibration:
        dows = tdf['dow'].values if 'dow' in tdf.columns else tdf['date'].dt.dayofweek.values
        for i in range(len(pred_rides)):
            cal = dow_calibration.get(int(dows[i]), 1.0)
            if abs(cal - 1.0) > 0.03:
                pred_rides[i] *= cal

    # 명절 감쇄
    if holiday_dampening < 1.0 and 'is_major_holiday' in tdf.columns:
        hol_mask = tdf['is_major_holiday'].values == 1
        pred_rides[hol_mask] *= holiday_dampening

    # A3: 악천후 + 공휴일/주말 복합 감쇄
    if 'is_severe_weather' in tdf.columns and 'is_off' in tdf.columns:
        rain_vals = tdf['rain_sum'].values if 'rain_sum' in tdf.columns else np.zeros(len(tdf))
        wind_vals = tdf['windspeed_avg'].values if 'windspeed_avg' in tdf.columns else np.zeros(len(tdf))
        major_hol = tdf['is_major_holiday'].values if 'is_major_holiday' in tdf.columns else np.zeros(len(tdf))
        is_off_vals = tdf['is_off'].values

        for i in range(len(pred_rides)):
            if major_hol[i] == 1 and (rain_vals[i] >= 10 or (wind_vals[i] >= 7 and rain_vals[i] > 0)):
                pred_rides[i] *= 0.6  # 대형연휴 + 폭우/강풍우 → 추가 40% 감쇄
            elif is_off_vals[i] == 1 and rain_vals[i] >= 10:
                pred_rides[i] *= 0.75  # 주말/공휴일 + 폭우 → 추가 25% 감쇄

    # A2: 연휴 회복 구간 상승 억제
    if 'is_recovery_phase' in tdf.columns and 'opens_wow_change' in tdf.columns:
        recovery = tdf['is_recovery_phase'].values
        wow = tdf['opens_wow_change'].values
        for i in range(len(pred_rides)):
            if recovery[i] == 1 and wow[i] > 0.3:
                # 회복 구간에서 WoW 변화율이 30% 이상이면 예측을 10% 하향
                pred_rides[i] *= 0.90

    if verbose:
        print(f"  합계: {pred_rides.sum():,.0f}건 "
              f"(opens {pred_opens.sum():,.0f}, rpo avg {pred_rpo.mean():.3f})")

    # 7. 출력 포맷팅
    center_map = get_center_map()

    district_df = pd.DataFrame({
        'region': tdf['h3_area_name'].values,
        'district': tdf['h3_district_name'].values,
        'adj_pred': np.round(pred_rides).astype(int),
        'pred_opens': np.round(pred_opens).astype(int),
        'pred_rpo': np.round(pred_rpo, 4),
        'lat': tdf['center_lat'].values,
        'lng': tdf['center_lng'].values,
    })
    district_df['center'] = district_df['region'].map(center_map).fillna('')
    district_df['desc'] = 'Production v2'

    # region 내 비율
    region_totals = district_df.groupby('region')['adj_pred'].sum().rename('_rtotal')
    district_df = district_df.merge(region_totals, on='region', how='left')
    district_df['ratio'] = np.where(
        district_df['_rtotal'] > 0,
        district_df['adj_pred'] / district_df['_rtotal'],
        0
    )
    district_df = district_df.drop(columns=['_rtotal'])

    # region 레벨 집계
    region_df = district_df.groupby('region').agg(
        adj_pred=('adj_pred', 'sum'),
        center=('center', 'first'),
        n_districts=('district', 'count'),
    ).reset_index()
    region_df['desc'] = 'Production v2'

    if verbose:
        print(f"  완료: {len(district_df)} districts, {len(region_df)} regions\n")

    return district_df, region_df


# ============================================================
# 기간별 배치 평가 (주간 성과 탭용)
# ============================================================

def evaluate_period(
    start_date: str,
    end_date: str,
    verbose: bool = True
) -> pd.DataFrame:
    """
    기간별 예측 성과 배치 평가

    실제 데이터가 있는 기간에 대해 한 번에 피처를 만들고 예측.
    BQ 1회 + 피처 1회 → 전체 기간 예측 (효율적)

    Args:
        start_date: 평가 시작일 (YYYY-MM-DD)
        end_date: 평가 종료일 (YYYY-MM-DD)

    Returns:
        DataFrame: columns [date, h3_area_name, h3_district_name,
                   app_opens, rides, pred_opens, pred_rpo, pred_rides, ...]
    """
    bundle = load_model_bundle()
    opens_model = bundle['opens_model']
    rpo_model = bundle['rpo_model']
    opens_features = bundle['opens_features']
    rpo_features = bundle['rpo_features']
    a_group = bundle['a_group_districts']
    area_rpo_median = bundle.get('area_rpo_median', {})
    district_calibration = bundle.get('district_calibration', {})
    dow_calibration = bundle.get('dow_calibration', {})
    holiday_dampening = bundle.get('holiday_dampening', 1.0)

    if verbose:
        print(f"\n[Production v2] 기간 평가: {start_date} ~ {end_date}")

    # 1. 데이터 (lookback 포함)
    fetch_start = (pd.Timestamp(start_date) - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    df = fetch_data_range(fetch_start, end_date, verbose=verbose)

    # 2. 공휴일
    max_hol = (pd.Timestamp(end_date) + timedelta(days=30)).strftime('%Y-%m-%d')
    holidays_set = fetch_holidays(fetch_start, max_hol)

    # 3. Feature Engineering (전체 기간 한 번에)
    if verbose:
        print("  피처 생성 중...")
    df = build_all_features(df, holidays_set, verbose=verbose)

    # 4. 대상 기간 + A그룹 필터
    target_mask = (
        (df['date'] >= pd.Timestamp(start_date)) &
        (df['date'] <= pd.Timestamp(end_date)) &
        (df['h3_district_name'].isin(a_group))
    )
    tdf = df[target_mask].copy()

    # 피처 결측 행 제거
    tdf = tdf.dropna(subset=['opens_lag7', 'app_opens_rolling'])

    if verbose:
        print(f"  평가 대상: {len(tdf):,}행, {tdf.date.nunique()}일, "
              f"{tdf.h3_district_name.nunique()} districts")

    if len(tdf) == 0:
        return pd.DataFrame()

    # 5. 모델 예측
    for feat in opens_features + rpo_features:
        if feat in tdf.columns:
            tdf[feat] = tdf[feat].fillna(0)

    X_opens = tdf[opens_features].astype(float).values
    pred_opens = np.maximum(opens_model.predict(X_opens), 0)

    X_rpo = tdf[rpo_features].astype(float).values
    pred_rpo = np.maximum(rpo_model.predict(X_rpo), 0)

    # 소형 district RPO 클리핑
    if area_rpo_median:
        opens_rolling = tdf['app_opens_rolling'].values
        area_names = tdf['h3_area_name'].values
        for i in range(len(pred_rpo)):
            if opens_rolling[i] < 15:
                area_med = area_rpo_median.get(area_names[i], 1.5)
                pred_rpo[i] = min(pred_rpo[i], area_med * 1.15)

    pred_rides = pred_opens * pred_rpo

    # District별 bias calibration
    if district_calibration:
        districts = tdf['h3_district_name'].values
        for i in range(len(pred_rides)):
            cal = district_calibration.get(districts[i], 1.0)
            if abs(cal - 1.0) > 0.05:
                pred_rides[i] *= cal

    # 요일별 calibration
    if dow_calibration:
        dows = tdf['dow'].values if 'dow' in tdf.columns else tdf['date'].dt.dayofweek.values
        for i in range(len(pred_rides)):
            cal = dow_calibration.get(int(dows[i]), 1.0)
            if abs(cal - 1.0) > 0.03:
                pred_rides[i] *= cal

    # 명절 감쇄
    if holiday_dampening < 1.0 and 'is_major_holiday' in tdf.columns:
        hol_mask = tdf['is_major_holiday'].values == 1
        pred_rides[hol_mask] *= holiday_dampening

    # A3: 악천후 + 공휴일/주말 복합 감쇄
    if 'is_severe_weather' in tdf.columns and 'is_off' in tdf.columns:
        rain_vals = tdf['rain_sum'].values if 'rain_sum' in tdf.columns else np.zeros(len(tdf))
        wind_vals = tdf['windspeed_avg'].values if 'windspeed_avg' in tdf.columns else np.zeros(len(tdf))
        major_hol = tdf['is_major_holiday'].values if 'is_major_holiday' in tdf.columns else np.zeros(len(tdf))
        is_off_vals = tdf['is_off'].values

        for i in range(len(pred_rides)):
            if major_hol[i] == 1 and (rain_vals[i] >= 10 or (wind_vals[i] >= 7 and rain_vals[i] > 0)):
                pred_rides[i] *= 0.6
            elif is_off_vals[i] == 1 and rain_vals[i] >= 10:
                pred_rides[i] *= 0.75

    # A2: 연휴 회복 구간 상승 억제
    if 'is_recovery_phase' in tdf.columns and 'opens_wow_change' in tdf.columns:
        recovery = tdf['is_recovery_phase'].values
        wow = tdf['opens_wow_change'].values
        for i in range(len(pred_rides)):
            if recovery[i] == 1 and wow[i] > 0.3:
                pred_rides[i] *= 0.90

    tdf['pred_opens'] = pred_opens
    tdf['pred_rpo'] = pred_rpo
    tdf['pred_rides'] = pred_rides

    if verbose:
        # 일별 요약
        daily = tdf.groupby('date').agg(
            actual=('rides', 'sum'),
            predicted=('pred_rides', 'sum')
        )
        daily['err'] = (daily['predicted'] / daily['actual'] - 1) * 100
        mape = np.mean(np.abs(daily['err']))
        total_a = daily['actual'].sum()
        total_p = daily['predicted'].sum()
        print(f"  합계: 실제 {total_a:,.0f} / 예측 {total_p:,.0f} "
              f"({(total_p/total_a-1)*100:+.1f}%)")
        print(f"  일별 MAPE: {mape:.1f}%")

    return tdf


# ============================================================
# 대시보드용 헬퍼 함수들
# ============================================================

def get_model_info() -> Dict:
    """모델 메타정보 반환 (사이드바 표시용)"""
    try:
        bundle = load_model_bundle()
        return {
            'trained_at': bundle.get('trained_at', 'unknown'),
            'n_districts': len(bundle.get('a_group_districts', [])),
            'eval': bundle.get('eval', {}),
            'config': bundle.get('config', {}),
        }
    except FileNotFoundError:
        return None


def format_for_weekly_performance(
    eval_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """evaluate_period() 결과를 주간 성과 탭 형식으로 변환

    Returns:
        (actual_df, pred_df) - 기존 대시보드 형식과 호환
        - actual_df: [date, region, actual_rides]
        - pred_df:   [date, region, predicted_rides, center]
    """
    if eval_df is None or len(eval_df) == 0:
        return pd.DataFrame(), pd.DataFrame()

    center_map = get_center_map()

    # Region 레벨 집계
    region_daily = eval_df.groupby(['date', 'h3_area_name']).agg(
        actual_rides=('rides', 'sum'),
        predicted_rides=('pred_rides', 'sum'),
    ).reset_index()
    region_daily = region_daily.rename(columns={'h3_area_name': 'region'})
    region_daily['center'] = region_daily['region'].map(center_map).fillna('')

    actual_df = region_daily[['date', 'region', 'actual_rides']].copy()
    pred_df = region_daily[['date', 'region', 'predicted_rides', 'center']].copy()
    actual_df['date'] = actual_df['date'].dt.date
    pred_df['date'] = pred_df['date'].dt.date

    return actual_df, pred_df


# ============================================================
# CLI 테스트
# ============================================================

if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        target = sys.argv[1]
    else:
        target = '2026-02-26'

    print(f"{'='*60}")
    print(f"Production v2 Predictor 테스트: {target}")
    print(f"{'='*60}")

    district_df, region_df = predict_district_rides(target)

    if len(district_df) > 0:
        print(f"\n── District Top 10 (예측 건수 순) ──")
        top10 = district_df.nlargest(10, 'adj_pred')
        for _, r in top10.iterrows():
            print(f"  {r['region']:20s} │ {r['district']:20s} │ "
                  f"pred {r['adj_pred']:5d} (opens {r['pred_opens']:5d} × rpo {r['pred_rpo']:.3f})")

        print(f"\n── Region Top 10 ──")
        top_r = region_df.nlargest(10, 'adj_pred')
        for _, r in top_r.iterrows():
            print(f"  {r['region']:20s} │ pred {r['adj_pred']:6d} │ "
                  f"{r.get('n_districts', 0)} districts │ {r.get('center', '')}")
