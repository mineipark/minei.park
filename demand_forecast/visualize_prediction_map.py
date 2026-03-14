"""
수요 예측 대시보드 (Streamlit) — Production v2

예측 모델: Production v2 (district별 직접 예측, opens × rpo)
  - V7 코드는 보관 상태 (demand_model_v7.py, CLI --model v7 으로 접근 가능)

탭 구성:
  1. 🗺️ 예측 지도: 일별 예측/실제 라이딩 위치 비교
  2. 📊 주간 성과: 일별 추이, 권역별 히트맵
  3. 📋 District 상세: district×날짜 매트릭스 (예측/실제/오차율)

사용법:
    streamlit run visualize_prediction_map.py

    CLI 모드:
    python visualize_prediction_map.py --date 2026-02-20
"""
import os
import sys
import json
import argparse
import pandas as pd
import numpy as np
import folium
from folium import FeatureGroup, LayerControl
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Authentication: set GOOGLE_APPLICATION_CREDENTIALS env var
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    _default_cred = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'credentials', 'service-account.json')
    if os.path.exists(_default_cred):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _default_cred

OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'visualizations')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 색상 정의
COLOR_ACTUAL = '#1976D2'       # 실제 - 파랑
COLOR_ACTUAL_FILL = '#42A5F5'
COLOR_PRED = '#E65100'         # 예측 - 주황
COLOR_PRED_FILL = '#FF9800'
COLOR_UNDER = '#D32F2F'        # 과소예측 - 빨강
COLOR_OVER = '#388E3C'         # 과대예측 - 초록
COLOR_MATCH = '#757575'        # 일치 - 회색

# H3 육각형 색상 팔레트 (노랑 → 주황 → 빨강, 7단계)
_HEX_PALETTE = ['#FFFFB2', '#FED976', '#FEB24C', '#FD8D3C',
                '#FC4E2A', '#E31A1C', '#B10026']


# ============================================================
# H3 육각형 렌더링 헬퍼
# ============================================================

def _add_h3_hexagons(fmap, hex_df, value_col='predicted_rides',
                     max_value=None, opacity=0.6,
                     popup_fn=None, tooltip_fn=None,
                     layer_group=None):
    """H3 hex 인덱스를 육각형 폴리곤으로 folium 지도에 렌더링

    Args:
        fmap: folium.Map 객체
        hex_df: DataFrame (h3_index, value_col 필수)
        value_col: 색상 강도 기준 컬럼명
        max_value: 스케일 최대값 (None → 95%ile 자동)
        opacity: fill_opacity (0~1)
        popup_fn: row → popup HTML str (None이면 생략)
        tooltip_fn: row → tooltip str (None이면 "{val}건")
        layer_group: folium.FeatureGroup에 추가 (None이면 fmap 직접)
    """
    import h3 as _h3

    if hex_df is None or len(hex_df) == 0:
        return

    valid = hex_df[
        hex_df['h3_index'].notna() &
        ~hex_df['h3_index'].isin(['unknown', 'other', ''])
    ].copy()

    if len(valid) == 0:
        return

    values = valid[value_col].fillna(0)
    if max_value is None:
        max_value = max(values.quantile(0.95), 0.1)

    target = layer_group if layer_group is not None else fmap

    def _color(val):
        r = min(val / max(max_value, 0.01), 1.0)
        return _HEX_PALETTE[min(int(r * (len(_HEX_PALETTE) - 1)),
                                len(_HEX_PALETTE) - 1)]

    for _, row in valid.iterrows():
        val = row[value_col]
        if val <= 0:
            continue

        try:
            boundary = _h3.cell_to_boundary(row['h3_index'])
            coords = [[lat, lng] for lat, lng in boundary]
        except Exception:
            continue

        color = _color(val)
        tt = tooltip_fn(row) if tooltip_fn else f"{val:,.1f}건"

        popup_obj = None
        if popup_fn:
            popup_obj = folium.Popup(popup_fn(row), max_width=220)

        folium.Polygon(
            locations=coords,
            color='#666666',
            weight=0.5,
            fill=True,
            fill_color=color,
            fill_opacity=opacity,
            tooltip=tt,
            popup=popup_obj,
        ).add_to(target)


# ============================================================
# 데이터 로딩 함수들
# ============================================================

def get_bigquery_client():
    """BigQuery 클라이언트 생성"""
    from google.cloud import bigquery
    return bigquery.Client()


def get_actual_rides_district(client, target_date: str) -> pd.DataFrame:
    """실제 라이딩 데이터 (h3_district 레벨 - 세밀)"""
    query = f"""
    SELECT
        h3_start_area_name as region,
        h3_start_district_name as district,
        COUNT(*) as ride_count,
        AVG(ST_Y(start_location)) as lat,
        AVG(ST_X(start_location)) as lng
    FROM `service.rides`
    WHERE DATE(start_time) = '{target_date}'
        AND h3_start_area_name IS NOT NULL
        AND h3_start_district_name IS NOT NULL
        AND start_location IS NOT NULL
    GROUP BY 1, 2
    """
    return client.query(query).to_dataframe()


def get_actual_rides_region(client, target_date: str) -> pd.DataFrame:
    """실제 라이딩 데이터 (region 레벨 - 예측 비교용)"""
    query = f"""
    SELECT
        h3_start_area_name as region,
        COUNT(*) as ride_count,
        AVG(ST_Y(start_location)) as lat,
        AVG(ST_X(start_location)) as lng
    FROM `service.rides`
    WHERE DATE(start_time) = '{target_date}'
        AND h3_start_area_name IS NOT NULL
        AND start_location IS NOT NULL
    GROUP BY 1
    """
    return client.query(query).to_dataframe()


def get_region_centers(client) -> Dict[str, Dict]:
    """권역별 중심 좌표 (최근 데이터 기반)"""
    query = """
    SELECT
        h3_area_name as region,
        AVG(ST_Y(location)) as lat,
        AVG(ST_X(location)) as lng
    FROM `service.app_accessibility`
    WHERE DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND h3_area_name IS NOT NULL
        AND location IS NOT NULL
    GROUP BY 1
    """
    df = client.query(query).to_dataframe()
    return {row['region']: {'lat': row['lat'], 'lng': row['lng']}
            for _, row in df.iterrows()}


def get_district_ride_ratios(client) -> pd.DataFrame:
    """최근 30일 기준 region 내 district별 라이딩 비율 (예측 배분용)

    각 region 안에서 district가 차지하는 비율을 계산.
    이 비율로 region 레벨 예측을 district로 쪼갬.
    """
    query = """
    SELECT
        h3_start_area_name as region,
        h3_start_district_name as district,
        COUNT(*) as ride_count,
        AVG(ST_Y(start_location)) as lat,
        AVG(ST_X(start_location)) as lng
    FROM `service.rides`
    WHERE DATE(start_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND h3_start_area_name IS NOT NULL
        AND h3_start_district_name IS NOT NULL
        AND start_location IS NOT NULL
    GROUP BY 1, 2
    """
    df = client.query(query).to_dataframe()

    # region 내 비율 계산
    region_totals = df.groupby('region')['ride_count'].sum().reset_index()
    region_totals.columns = ['region', 'region_total']
    df = df.merge(region_totals, on='region')
    df['ratio'] = df['ride_count'] / df['region_total']

    return df


def distribute_to_districts(
    pred_df: pd.DataFrame,
    district_ratios: pd.DataFrame
) -> pd.DataFrame:
    """region 레벨 예측을 district 레벨로 비례 배분

    각 region의 예측값을 해당 region 내 district의
    과거 30일 라이딩 비율로 배분.
    district 비율 데이터가 없는 region은 건너뜀.

    잠재수요 정보가 있으면 함께 배분한다.
    """
    # 전환율 모델 로드 (있으면)
    conv_model = None
    avg_bikes = None
    try:
        from conversion_model import ConversionModel
        conv_model = ConversionModel(verbose=False)
        if conv_model.fitted:
            avg_bikes = conv_model.get_avg_bike_counts(lookback_days=90)
    except Exception:
        pass

    results = []

    for _, pred_row in pred_df.iterrows():
        region = pred_row['region']
        region_pred = pred_row['adj_pred']

        # 해당 region의 district 비율
        dists = district_ratios[district_ratios['region'] == region]

        if len(dists) == 0:
            continue  # district 데이터 없으면 스킵

        for _, d in dists.iterrows():
            district_pred = round(region_pred * d['ratio'])
            row = {
                'region': region,
                'district': d['district'],
                'adj_pred': district_pred,
                'ratio': round(d['ratio'], 4),
                'lat': d['lat'],
                'lng': d['lng'],
                'center': pred_row.get('center', ''),
                'desc': pred_row.get('desc', ''),
            }

            # 잠재수요 역산 (전환율 모델 있을 때)
            if conv_model and conv_model.fitted and avg_bikes is not None:
                district_bikes = avg_bikes[
                    (avg_bikes['region'] == region) &
                    (avg_bikes['district'] == d['district'])
                ]
                if len(district_bikes) > 0:
                    avg_bc = district_bikes['avg_bike_count_100'].mean()
                else:
                    avg_bc = avg_bikes['avg_bike_count_100'].mean()

                unc = conv_model.estimate_unconstrained(district_pred, avg_bc)
                row['unconstrained_demand'] = round(unc)
                row['suppressed_demand'] = round(max(unc - district_pred, 0))

            results.append(row)

    return pd.DataFrame(results) if results else pd.DataFrame()


# ============================================================
# 날씨 데이터 fallback (CSV → 단기예보 API → 기본값)
# ============================================================

def _fetch_short_term_weather(target_date: str) -> Optional[Dict]:
    """
    기상청 단기예보 API로 날씨 가져오기 (D-1, D-0 fallback용)

    CSV에 실측 데이터가 없는 최근 날짜에 대해
    단기예보(VilageFcst)에서 최저/최고기온, 적설량을 조회.
    """
    import urllib.request
    from urllib.parse import urlencode

    API_KEY = os.getenv("WEATHER_API_KEY", "")
    SHORT_TERM_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"

    target = pd.Timestamp(target_date)
    # 발표 기준일: 전날 또는 당일 (05시 발표 기준)
    now = datetime.now()
    if now.hour >= 5:
        base_date = now.strftime("%Y%m%d")
    else:
        base_date = (now - timedelta(days=1)).strftime("%Y%m%d")

    target_key = target.strftime("%Y%m%d")

    params = f"?serviceKey={API_KEY}&" + urlencode({
        "pageNo": "1",
        "numOfRows": "1000",
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": "0500",
        "nx": "60",   # 서울
        "ny": "127"
    })

    try:
        req = urllib.request.Request(SHORT_TERM_URL + params)
        req.add_header('User-Agent', 'Mozilla/5.0')
        response = urllib.request.urlopen(req, timeout=10)
        data = json.loads(response.read())

        if data['response']['header']['resultCode'] != '00':
            return None

        items = data['response']['body']['items']['item']

        temps = []
        snow_val = 0
        for item in items:
            if item['fcstDate'] != target_key:
                continue
            if item['category'] == 'TMP':
                temps.append(float(item['fcstValue']))
            elif item['category'] == 'SNO' and item['fcstValue'] != '적설없음':
                try:
                    snow_val = float(item['fcstValue'].replace('cm', ''))
                except (ValueError, AttributeError):
                    pass

        if temps:
            return {
                'temp_low': min(temps),
                'temp_high': max(temps),
                'snow_depth': snow_val,
                'source': '단기예보'
            }
        return None

    except Exception:
        return None


def _get_weather_for_date(target_date: str, weather_data: Dict) -> Tuple[Dict, str]:
    """
    날씨 데이터 조회 (CSV → 단기예보 API → 기본값 순서로 fallback)

    Returns:
        (weather_dict, source_label)
    """
    # 1) CSV 실측 데이터
    if target_date in weather_data:
        return weather_data[target_date], '실측'

    # 2) 단기예보 API (최근 3일 이내만 시도)
    target = pd.Timestamp(target_date)
    days_diff = (datetime.now() - target.to_pydatetime()).days
    if -3 <= days_diff <= 2:
        forecast = _fetch_short_term_weather(target_date)
        if forecast:
            return forecast, '단기예보'

    # 3) 기본값
    return {'temp_low': 0, 'temp_high': 5, 'snow_depth': 0}, '기본값'


# ============================================================
# Lag 피처: 최근 실제 라이딩 데이터 기반 예측 보강
# ============================================================

def _fetch_lag_features(target_date: str) -> Dict[str, Dict]:
    """
    최근 28일 실제 라이딩 기반 lag 피처 조회 (~5초)

    - 공휴일 제외: rolling/same_dow 평균에서 공휴일 왜곡 방지
    - lag1d/lag7d: 공휴일이어도 그대로 (특정일 스냅샷)
    - growth: 최근 7일 vs 이전 21일 (공휴일 제외 평균)

    Returns:
        {region: {lag1d, lag7d, rolling_7d, rolling_28d, same_dow_avg, growth}}
    """
    try:
        client = get_bigquery_client()
    except Exception:
        return {}

    target = pd.Timestamp(target_date)
    bq_dow = (target.dayofweek + 2) % 7 or 7  # BigQuery DOW: 1=일..7=토

    # 공휴일 목록 (28일 범위 내)
    try:
        from korean_holidays import ADDITIONAL_HOLIDAYS
        holidays = ADDITIONAL_HOLIDAYS
    except ImportError:
        holidays = set()

    from datetime import timedelta
    start_28d = (target - timedelta(days=28)).date()
    end_1d = (target - timedelta(days=1)).date()
    holidays_in_range = [
        h for h in holidays
        if start_28d <= h <= end_1d
    ]

    # 공휴일 제외 조건 (rolling/same_dow용)
    if holidays_in_range:
        holiday_str = ','.join([f"'{h}'" for h in holidays_in_range])
        holiday_filter = f"AND ride_date NOT IN ({holiday_str})"
    else:
        holiday_filter = ""

    query = f"""
    WITH daily AS (
        SELECT
            h3_start_area_name as region,
            DATE(start_time) as ride_date,
            EXTRACT(DAYOFWEEK FROM start_time) as dow,
            COUNT(*) as rides
        FROM `service.rides`
        WHERE DATE(start_time) BETWEEN DATE_SUB('{target_date}', INTERVAL 28 DAY)
            AND DATE_SUB('{target_date}', INTERVAL 1 DAY)
            AND h3_start_area_name IS NOT NULL
            AND bike_type = 1
        GROUP BY 1, 2, 3
    )
    SELECT
        region,
        -- lag1d/lag7d: 특정일 스냅샷 (공휴일이어도 그대로)
        MAX(CASE WHEN ride_date = DATE_SUB('{target_date}', INTERVAL 1 DAY)
            THEN rides END) as lag1d,
        MAX(CASE WHEN ride_date = DATE_SUB('{target_date}', INTERVAL 7 DAY)
            THEN rides END) as lag7d,
        -- rolling/same_dow: 공휴일 제외 평균
        AVG(CASE WHEN ride_date >= DATE_SUB('{target_date}', INTERVAL 7 DAY)
            {holiday_filter} THEN rides END) as rolling_7d,
        AVG(CASE WHEN ride_date < DATE_SUB('{target_date}', INTERVAL 7 DAY)
            {holiday_filter} THEN rides END) as rolling_prior_21d,
        AVG(CASE WHEN 1=1 {holiday_filter} THEN rides END) as rolling_28d,
        AVG(CASE WHEN dow = {bq_dow} {holiday_filter} THEN rides END) as same_dow_avg
    FROM daily
    GROUP BY 1
    """
    try:
        df = client.query(query).to_dataframe()
        result = {}
        for _, row in df.iterrows():
            rolling_7d = float(row['rolling_7d']) if pd.notna(row['rolling_7d']) else None
            rolling_prior = float(row['rolling_prior_21d']) if pd.notna(row['rolling_prior_21d']) else None

            # 성장 팩터: 최근 7일 평균 / 이전 21일 평균 (week-over-prior)
            growth = None
            if rolling_7d and rolling_prior and rolling_prior > 0:
                growth = rolling_7d / rolling_prior

            result[row['region']] = {
                'lag1d': float(row['lag1d']) if pd.notna(row['lag1d']) else None,
                'lag7d': float(row['lag7d']) if pd.notna(row['lag7d']) else None,
                'rolling_7d': rolling_7d,
                'rolling_28d': float(row['rolling_28d']) if pd.notna(row['rolling_28d']) else None,
                'same_dow_avg': float(row['same_dow_avg']) if pd.notna(row['same_dow_avg']) else None,
                'growth': growth,
            }
        return result
    except Exception as e:
        print(f"  ⚠️ Lag 피처 조회 실패: {e}")
        return {}


def _blend_base_rides(avg_rides: float, lag: Optional[Dict]) -> Tuple[float, str]:
    """
    정적 avg_rides와 lag 피처를 블렌딩 + 성장 트렌드 반영

    1단계: 가중 블렌딩
    - same_dow_avg (같은 요일 4주 평균): 요일 패턴 → 0.35
    - rolling_7d  (최근 7일 평균): 최근 트렌드 → 0.30
    - lag7d       (지난주 같은 요일): 직접 비교 → 0.20
    - lag1d       (어제): 초단기 시그널 → 0.10
    - avg_rides   (장기 평균): 안정 앵커 → 0.05 (lag 데이터 있으면 비중 축소)

    2단계: 성장 트렌드 반영
    - growth = rolling_7d / rolling_prior_21d
    - growth > 1이면 상승 모멘텀 → 블렌딩 결과에 부스트
    - 범위: 0.85 ~ 1.25 (급변 방지)
    """
    if not lag:
        return avg_rides, "static"

    components = []
    weights = []

    same_dow = lag.get('same_dow_avg')
    rolling_7d = lag.get('rolling_7d')
    lag7d = lag.get('lag7d')
    lag1d = lag.get('lag1d')
    growth = lag.get('growth')

    # 1단계: 가중 블렌딩 (실시간 데이터 비중 높임)
    if same_dow and same_dow > 0:
        components.append(same_dow)
        weights.append(0.35)

    if rolling_7d and rolling_7d > 0:
        components.append(rolling_7d)
        weights.append(0.30)

    if lag7d and lag7d > 0:
        components.append(lag7d)
        weights.append(0.20)

    if lag1d and lag1d > 0:
        components.append(lag1d)
        weights.append(0.10)

    if avg_rides > 0:
        components.append(avg_rides)
        weights.append(0.05)

    if not components:
        return avg_rides, "static"

    total_weight = sum(weights)
    blended = sum(c * w for c, w in zip(components, weights)) / total_weight

    # 2단계: 성장 트렌드 부스트
    if growth and growth > 0:
        # 성장률 범위 제한: 0.85 ~ 1.25
        growth_factor = max(0.85, min(1.25, growth))
        blended *= growth_factor

    return blended, "lag_blended"


# ============================================================
# 경량 예측 (region_params 기반, ML 없이 즉시)
# ============================================================

def quick_predict(target_date: str, use_lag: bool = True) -> pd.DataFrame:
    """
    region_params.json 기반 경량 예측
    base_rides × 요일보정 × 날씨보정 × bias

    use_lag=True (기본):
      - BigQuery에서 최근 28일 실제 라이딩 lag 피처 조회 (~5초)
      - avg_rides + lag1d/lag7d/rolling_7d/same_dow_avg 가중 블렌딩
      - 최근 트렌드 반영으로 정확도 개선

    use_lag=False:
      - region_params.json의 정적 avg_rides만 사용 (<1초)

    날씨: CSV 실측 → 단기예보 API → 기본값 순서로 fallback
    """
    from demand_model_v7 import (
        RegionWeatherCorrection, load_weather_data,
        DAY_SCALE, WEATHER_SCALE
    )

    # 파라미터 로드
    params_path = os.path.join(SCRIPT_DIR, 'region_params.json')
    with open(params_path, 'r') as f:
        region_params = json.load(f)

    # 날씨 데이터 (CSV → 단기예보 → 기본값)
    weather_csv = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')
    weather_data = load_weather_data(weather_csv) if os.path.exists(weather_csv) else {}
    weather, weather_source = _get_weather_for_date(target_date, weather_data)

    # 공휴일 데이터
    try:
        from korean_holidays import ADDITIONAL_HOLIDAYS
        holidays = ADDITIONAL_HOLIDAYS
    except ImportError:
        holidays = set()

    target = pd.Timestamp(target_date)
    dow = target.dayofweek  # 0=월 ~ 6=일
    is_saturday = (dow == 5)
    is_sunday = (dow == 6)
    is_monday = (dow == 0)
    is_holiday = target.date() in holidays

    # Lag 피처 조회 (use_lag=True일 때)
    lag_features = _fetch_lag_features(target_date) if use_lag else {}

    # 보정 계산기
    correction = RegionWeatherCorrection()

    predictions = []
    lag_used_count = 0
    for region, params in region_params.items():
        avg_rides = params.get('avg_rides', 0)

        # base_rides: lag 블렌딩 or 정적 avg_rides
        lag = lag_features.get(region)
        base_rides, blend_method = _blend_base_rides(avg_rides, lag)
        if blend_method == "lag_blended":
            lag_used_count += 1

        # 요일+날씨 보정
        factor, desc = correction.calculate(
            region,
            temp_low=weather.get('temp_low', 0),
            temp_high=weather.get('temp_high', 5),
            is_saturday=is_saturday,
            is_sunday=is_sunday,
            is_monday=is_monday,
            snow_depth=weather.get('snow_depth', 0),
            is_holiday=is_holiday
        )

        # bias 보정
        bias = params.get('bias', 0.05)
        pred = base_rides * factor * (1 + bias)

        # desc에 날씨 소스 정보 추가
        if weather_source != '실측':
            source_label = f"[{weather_source}] "
        else:
            source_label = ""
        full_desc = f"{source_label}{desc}" if desc != "보정 없음" else (
            f"{source_label}보정 없음" if source_label else "보정 없음"
        )

        predictions.append({
            'region': region,
            'center': params.get('center', ''),
            'adj_pred': round(pred),
            'avg_rides': round(avg_rides),
            'base_rides': round(base_rides, 1),
            'blend': blend_method,
            'factor': round(factor, 3),
            'bias': bias,
            'desc': full_desc
        })

    return pd.DataFrame(predictions)


def full_model_predict(client, target_date: str) -> pd.DataFrame:
    """
    전체 ML 모델 예측 (정확하지만 30~60초 소요)
    """
    from demand_model_v7 import DemandForecastModelV7, load_weather_data

    model = DemandForecastModelV7()

    # 날씨 (CSV → 단기예보 → 기본값)
    weather_csv = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')
    weather_data = load_weather_data(weather_csv) if os.path.exists(weather_csv) else {}
    weather, _ = _get_weather_for_date(target_date, weather_data)

    # 과거 데이터
    target = pd.Timestamp(target_date)
    start = (target - timedelta(days=400)).strftime('%Y-%m-%d')
    end = target.strftime('%Y-%m-%d')
    historical = model.fetch_data(start, end)

    # 예측
    result = model.predict(target_date, weather, historical)

    if 'region_details' in result:
        df = pd.DataFrame(result['region_details'])
        return df
    return pd.DataFrame()


# ============================================================
# 지도 생성
# ============================================================

def create_prediction_map(
    actual_district_df: pd.DataFrame,
    actual_region_df: pd.DataFrame,
    pred_df: pd.DataFrame,
    region_centers: Dict[str, Dict],
    target_date: str,
    pred_district_df: Optional[pd.DataFrame] = None
) -> folium.Map:
    """
    예측 vs 실제 라이딩 Folium 지도 생성

    Args:
        actual_district_df: 실제 라이딩 (district 레벨)
        actual_region_df: 실제 라이딩 (region 레벨, 비교용)
        pred_df: 예측 결과 (region 레벨)
        region_centers: 권역별 중심좌표
        target_date: 대상 날짜
        pred_district_df: 예측 결과 (district 레벨, 비례배분). None이면 region 레벨 표시
    """
    # 지도 중심점
    if len(actual_district_df) > 0:
        center_lat = actual_district_df['lat'].mean()
        center_lng = actual_district_df['lng'].mean()
    else:
        center_lat, center_lng = 37.45, 126.95

    m = folium.Map(
        location=[center_lat, center_lng],
        zoom_start=10,
        tiles='cartodbpositron'
    )

    # --- 레이어 그룹 ---
    actual_group = FeatureGroup(name='🔵 실제 라이딩 (district)', show=True)
    pred_label = 'district' if (pred_district_df is not None and len(pred_district_df) > 0) else 'region'
    pred_group = FeatureGroup(name=f'🟠 예측 라이딩 ({pred_label})', show=True)
    error_group = FeatureGroup(name='🔴 오차 표시 (region)', show=True)

    # --- 실제 라이딩 (파란 점) - district 레벨 ---
    if len(actual_district_df) > 0:
        max_actual = actual_district_df['ride_count'].max()
        for _, row in actual_district_df.iterrows():
            if pd.isna(row['lat']) or pd.isna(row['lng']):
                continue

            radius = max(3, min(np.sqrt(row['ride_count'] / max(max_actual, 1)) * 15, 18))

            popup_html = f"""
            <div style="font-family:Arial;font-size:12px;min-width:150px;">
                <b style="color:{COLOR_ACTUAL};">{row.get('district', '')}</b><br>
                <span style="color:#666;">{row.get('region', '')}</span><br>
                <hr style="margin:4px 0;">
                라이딩: <b>{row['ride_count']:,}건</b>
            </div>
            """

            folium.CircleMarker(
                location=[row['lat'], row['lng']],
                radius=radius,
                color=COLOR_ACTUAL,
                weight=1,
                fill=True,
                fill_color=COLOR_ACTUAL_FILL,
                fill_opacity=0.55,
                popup=folium.Popup(popup_html, max_width=200),
                tooltip=f"실제 {row['ride_count']:,}건 | {row.get('district', '')}"
            ).add_to(actual_group)

    # --- 예측/오차 데이터 결합 ---
    # actual_region과 pred를 region으로 join
    if len(pred_df) > 0 and len(actual_region_df) > 0:
        if 'actual' in pred_df.columns:
            # 풀 모델: 이미 actual 컬럼 포함 → 그대로 사용
            merged = pred_df.copy()
        else:
            # 경량 모델: actual_region에서 merge
            merged = pred_df.merge(
                actual_region_df[['region', 'ride_count']].rename(columns={'ride_count': 'actual'}),
                on='region', how='left'
            )
        merged['actual'] = merged['actual'].fillna(0)
    elif len(pred_df) > 0:
        merged = pred_df.copy()
        if 'actual' not in merged.columns:
            merged['actual'] = 0
    else:
        merged = pd.DataFrame()

    # --- 예측 라이딩 (주황 점) ---
    if pred_district_df is not None and len(pred_district_df) > 0:
        # District 레벨 예측 점 (세밀한 점)
        max_pred_d = pred_district_df['adj_pred'].max()
        for _, row in pred_district_df.iterrows():
            if pd.isna(row.get('lat')) or pd.isna(row.get('lng')):
                continue
            pred_val = row['adj_pred']
            if pred_val <= 0:
                continue

            radius = max(3, min(np.sqrt(pred_val / max(max_pred_d, 1)) * 15, 18))

            # 잠재수요 정보 (있을 때만)
            unc_val = row.get('unconstrained_demand', 0)
            supp_val = row.get('suppressed_demand', 0)
            unc_html = ''
            if supp_val > 0:
                unc_html = (f"<br><span style='color:#FF6F00;font-size:11px;'>"
                           f"잠재 {unc_val:,.0f}건 (억제 +{supp_val:,.0f})</span>")

            popup_html = f"""
            <div style="font-family:Arial;font-size:12px;min-width:180px;">
                <b style="color:{COLOR_PRED};">{row.get('district', '')}</b><br>
                <span style="color:#999;font-size:11px;">{row.get('region', '')} ({row.get('center', '')})</span>
                <hr style="margin:4px 0;">
                예측: <b style="color:{COLOR_PRED};">{pred_val:,.0f}건</b>
                <span style="font-size:11px;color:#999;"> (비율 {row.get('ratio', 0):.1%})</span>
                {unc_html}
            </div>
            """

            # 잠재수요가 크면 테두리 두껍게
            weight = 2.5 if supp_val > pred_val * 0.1 else 1

            folium.CircleMarker(
                location=[row['lat'], row['lng']],
                radius=radius,
                color='#FF6F00' if supp_val > pred_val * 0.1 else COLOR_PRED,
                weight=weight,
                fill=True,
                fill_color=COLOR_PRED_FILL,
                fill_opacity=0.45,
                popup=folium.Popup(popup_html, max_width=220),
                tooltip=f"예측 {pred_val:,.0f}건 | {row.get('district', '')}"
            ).add_to(pred_group)

    elif len(merged) > 0:
        # Fallback: Region 레벨 예측 점
        max_pred = merged['adj_pred'].max()
        for _, row in merged.iterrows():
            region = row['region']
            coords = region_centers.get(region)
            if not coords:
                continue
            pred_val = row['adj_pred']
            radius = max(5, min(np.sqrt(pred_val / max(max_pred, 1)) * 20, 25))

            popup_html = f"""
            <div style="font-family:Arial;font-size:12px;min-width:200px;">
                <b style="color:{COLOR_PRED};">{region}</b>
                <span style="color:#999;font-size:11px;"> ({row.get('center', '')})</span>
                <hr style="margin:4px 0;">
                예측: <b style="color:{COLOR_PRED};">{pred_val:,.0f}건</b>
            </div>
            """

            folium.CircleMarker(
                location=[coords['lat'], coords['lng']],
                radius=radius,
                color=COLOR_PRED,
                weight=1.5,
                fill=True,
                fill_color=COLOR_PRED_FILL,
                fill_opacity=0.45,
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=f"예측 {pred_val:,.0f}건 | {region}"
            ).add_to(pred_group)

    # --- 오차 표시 (항상 region 레벨) ---
    if len(merged) > 0:
        for _, row in merged.iterrows():
            region = row['region']
            coords = region_centers.get(region)
            if not coords:
                continue

            pred_val = row['adj_pred']
            actual_val = row.get('actual', 0)

            if actual_val > 0:
                error_pct = (pred_val - actual_val) / actual_val * 100
            else:
                error_pct = 0

            if actual_val == 0:
                border_color = COLOR_MATCH
            elif error_pct < -15:
                border_color = COLOR_UNDER
            elif error_pct > 15:
                border_color = COLOR_OVER
            else:
                border_color = COLOR_MATCH

            radius = max(5, min(np.sqrt(pred_val / max(merged['adj_pred'].max(), 1)) * 20, 25))

            if abs(error_pct) > 15 and actual_val > 0:
                error_label = f"{'과소' if error_pct < 0 else '과대'} {abs(error_pct):.1f}%"
                folium.CircleMarker(
                    location=[coords['lat'], coords['lng']],
                    radius=radius + 5,
                    color=border_color,
                    weight=3,
                    fill=False,
                    dash_array='5,5',
                    tooltip=f"⚠️ {error_label} | {region}: 예측 {pred_val:,.0f} / 실제 {actual_val:,.0f}"
                ).add_to(error_group)

    # 레이어 추가
    actual_group.add_to(m)
    pred_group.add_to(m)
    error_group.add_to(m)
    LayerControl(collapsed=False).add_to(m)

    # --- 요약 통계 ---
    total_actual = actual_region_df['ride_count'].sum() if len(actual_region_df) > 0 else 0
    total_pred = merged['adj_pred'].sum() if len(merged) > 0 else 0
    overall_error = ((total_pred - total_actual) / total_actual * 100) if total_actual > 0 else 0

    target_dt = pd.Timestamp(target_date)
    dow_names = ['월', '화', '수', '목', '금', '토', '일']
    dow_str = dow_names[target_dt.dayofweek]

    # --- 범례 + 통계 ---
    stats_html = f"""
    <div style="position:fixed; bottom:30px; left:30px; z-index:1000;
                background:white; padding:15px 18px; border-radius:10px;
                box-shadow:0 2px 8px rgba(0,0,0,0.25); font-family:Arial; font-size:13px;
                max-width:260px;">
        <div style="font-size:15px;font-weight:bold;margin-bottom:8px;">
            📅 {target_date} ({dow_str})
        </div>
        <table style="width:100%;font-size:13px;border-collapse:collapse;">
            <tr>
                <td style="padding:3px 0;">
                    <span style="display:inline-block;width:10px;height:10px;background:{COLOR_ACTUAL_FILL};border-radius:50%;margin-right:6px;"></span>
                    실제 라이딩
                </td>
                <td style="text-align:right;font-weight:bold;">{total_actual:,.0f}건</td>
            </tr>
            <tr>
                <td style="padding:3px 0;">
                    <span style="display:inline-block;width:10px;height:10px;background:{COLOR_PRED_FILL};border-radius:50%;margin-right:6px;"></span>
                    예측 라이딩
                </td>
                <td style="text-align:right;font-weight:bold;">{total_pred:,.0f}건</td>
            </tr>
            <tr style="border-top:1px solid #eee;">
                <td style="padding:4px 0;">오차율</td>
                <td style="text-align:right;font-weight:bold;color:{'#D32F2F' if abs(overall_error) > 15 else '#388E3C'};">
                    {abs(overall_error):.1f}% {'과소' if overall_error < 0 else '과대'}
                </td>
            </tr>
        </table>
        <hr style="margin:8px 0;">
        <div style="font-size:11px;color:#999;">
            <div style="margin:2px 0;">
                <span style="display:inline-block;width:10px;height:10px;border:2px dashed {COLOR_UNDER};border-radius:50%;margin-right:4px;"></span>
                과소예측 (&gt;15%)
            </div>
            <div style="margin:2px 0;">
                <span style="display:inline-block;width:10px;height:10px;border:2px dashed {COLOR_OVER};border-radius:50%;margin-right:4px;"></span>
                과대예측 (&gt;15%)
            </div>
            <div style="margin-top:4px;">원 크기 = 라이딩/예측 건수</div>
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(stats_html))

    # 타이틀
    title_html = f"""
    <div style="position:fixed; top:10px; left:50%; transform:translateX(-50%); z-index:1000;
                background:white; padding:8px 20px; border-radius:8px;
                box-shadow:0 2px 6px rgba(0,0,0,0.2); font-family:Arial;">
        <span style="font-size:16px;font-weight:bold;color:#1976D2;">
            🗺️ 예측 vs 실제 라이딩 비교
        </span>
        <span style="font-size:13px;color:#666;margin-left:10px;">
            {target_date} ({dow_str})
        </span>
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    return m


# ============================================================
# Streamlit 앱
# ============================================================

def run_streamlit():
    """Streamlit 앱 실행"""
    import streamlit as st

    st.set_page_config(
        page_title="수요 예측 대시보드",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("🗺️ 수요 예측 대시보드")

    # ── Production v2 모델 로드 ──
    from production_v2_predictor import (
        predict_district_rides as v2_predict,
        evaluate_period as v2_evaluate,
        format_for_weekly_performance as v2_format_weekly,
        get_model_info as v2_get_info,
    )
    v2_info = v2_get_info()
    use_v2 = True  # Production v2 고정

    # 사이드바 공통 설정
    with st.sidebar:
        st.header("⚙️ 설정")

        target_date = st.date_input(
            "📅 날짜 선택 (오늘 = 실시간 추적)",
            value=datetime.now(),
            min_value=datetime(2025, 10, 1),
            max_value=datetime.now() + timedelta(days=1)
        )
        target_date_str = target_date.strftime('%Y-%m-%d')
        is_future = target_date > datetime.now().date() if hasattr(target_date, 'year') else False

        # 모델 정보 표시
        st.divider()
        st.markdown("🤖 **Production v2** (district 직접)")
        if v2_info:
            trained = v2_info.get('trained_at', '')[:10]
            n_dist = v2_info.get('n_districts', 0)
            eval_info = v2_info.get('eval', {})
            daily_mape = eval_info.get('daily_mape', 0)
            st.caption(f"{n_dist} districts, 학습 {trained}, "
                       f"테스트 MAPE {daily_mape:.1f}%")

    # 탭 구성 (3탭: 예측 검증 / 주간 성과 / D+1 시간대 예측)
    tab_map, tab_perf, tab_hourly = st.tabs([
        "🗺️ 예측 검증", "📊 주간 성과", "⏰ D+1 시간대 예측"])

    # ===========================================================
    # TAB 1: 예측 지도 (기존)
    # ===========================================================
    with tab_map:
        st.caption("예측 라이딩 위치(🟠)와 실제 라이딩 위치(🔵)를 비교합니다")

        with st.sidebar:
            st.divider()
            st.markdown("**범례**")
            st.markdown("🔵 **실제 라이딩** (district 레벨)")
            st.markdown("🟠 **예측 라이딩** (district 레벨)")
            st.markdown("🔴 **과소예측** (>15% 미달)")
            st.markdown("🟢 **과대예측** (>15% 초과)")

        # 데이터 로딩
        with st.spinner('📊 데이터 로딩 중...'):
            client = get_bigquery_client()

            actual_district = get_actual_rides_district(client, target_date_str)
            actual_region = get_actual_rides_region(client, target_date_str)
            region_centers = get_region_centers(client)

            # ── Production v2: district 직접 예측 ──
            pred_district, pred_df = v2_predict(target_date_str, verbose=False)

        # 요약 메트릭
        total_actual = actual_region['ride_count'].sum() if len(actual_region) > 0 else 0
        total_pred = pred_df['adj_pred'].sum() if len(pred_df) > 0 else 0
        overall_error = ((total_pred - total_actual) / total_actual * 100) if total_actual > 0 else 0
        n_districts = len(actual_district)
        n_pred_districts = len(pred_district) if len(pred_district) > 0 else 0

        if len(actual_region) == 0 and len(pred_district) == 0:
            st.warning(f"⚠️ {target_date_str}에 데이터가 없습니다.")
            return

        if len(actual_region) == 0:
            # D+1 미래 날짜: 예측만 표시
            st.info(f"📅 {target_date_str}은 미래 날짜입니다. 예측 데이터만 표시합니다.")
            st.markdown(
                f"""<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:8px;">
                <div style="background:#FFF3E0;border-radius:6px;padding:6px 14px;font-size:13px;">
                    <span style="color:#888;">예측 합계</span> <b>{total_pred:,.0f}</b>건</div>
                <div style="background:#FFF3E0;border-radius:6px;padding:6px 14px;font-size:13px;">
                    <span style="color:#888;">포인트</span> <b>{n_pred_districts}</b> districts</div>
                </div>""",
                unsafe_allow_html=True
            )
        else:
            # 과거 날짜: 예측 vs 실제 비교
            bias_label = f"{'과대' if overall_error > 0 else '과소'}예측 {abs(overall_error):.1f}%"
            st.markdown(
                f"""<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:8px;">
                <div style="background:#f8f9fa;border-radius:6px;padding:6px 14px;font-size:13px;">
                    <span style="color:#888;">실제</span> <b>{total_actual:,.0f}</b>건</div>
                <div style="background:#f8f9fa;border-radius:6px;padding:6px 14px;font-size:13px;">
                    <span style="color:#888;">예측</span> <b>{total_pred:,.0f}</b>건</div>
                <div style="background:#f8f9fa;border-radius:6px;padding:6px 14px;font-size:13px;">
                    <span style="color:#888;">MAPE</span> <b>{abs(overall_error):.1f}%</b></div>
                <div style="background:#f8f9fa;border-radius:6px;padding:6px 14px;font-size:13px;">
                    <span style="color:#888;">편향</span> <b>{bias_label}</b></div>
                <div style="background:#f8f9fa;border-radius:6px;padding:6px 14px;font-size:13px;">
                    <span style="color:#888;">포인트</span> <b>{n_districts}/{n_pred_districts}</b> districts</div>
                </div>""",
                unsafe_allow_html=True
            )

        # 지도
        m = create_prediction_map(
            actual_district, actual_region, pred_df,
            region_centers, target_date_str,
            pred_district_df=pred_district
        )

        try:
            from streamlit_folium import st_folium
            st_folium(m, width=None, height=650, returned_objects=[])
        except ImportError:
            import streamlit.components.v1 as components
            map_html = m._repr_html_()
            components.html(map_html, height=650)

        # 권역별 상세 테이블
        st.subheader("📊 권역별 예측 vs 실제 비교")

        if len(pred_df) > 0 and len(actual_region) > 0:
            if 'actual' in pred_df.columns:
                display = pred_df.copy()
            else:
                display = pred_df.merge(
                    actual_region[['region', 'ride_count']].rename(columns={'ride_count': 'actual'}),
                    on='region', how='outer'
                )
            display['actual'] = display['actual'].fillna(0)
            display['adj_pred'] = display['adj_pred'].fillna(0)
            display['error_pct'] = np.where(
                display['actual'] > 0,
                ((display['adj_pred'] - display['actual']) / display['actual'] * 100).round(1),
                0
            )

            display_cols = {
                'region': '권역',
                'center': '센터',
                'actual': '실제',
                'adj_pred': '예측',
                'error_pct': '오차(%)',
                'desc': '보정내용'
            }
            cols_to_show = [c for c in display_cols.keys() if c in display.columns]
            show_df = display[cols_to_show].rename(columns=display_cols)
            show_df = show_df.sort_values('실제', ascending=False)

            st.dataframe(
                show_df.style.applymap(
                    lambda v: 'color: #D32F2F' if isinstance(v, (int, float)) and v < -15
                    else ('color: #388E3C' if isinstance(v, (int, float)) and v > 15 else ''),
                    subset=['오차(%)'] if '오차(%)' in show_df.columns else []
                ),
                width='stretch',
                height=400
            )

        # ── 실시간 시간대 정합성 (오늘 날짜일 때만) ──
        today_str = datetime.now().strftime('%Y-%m-%d')
        if target_date_str == today_str:
            st.markdown("---")
            st.subheader("⏱️ 실시간 시간대별 정합성")
            st.caption("오늘 예측 vs 현재까지 실제 라이딩 — 시간별 자동 업데이트")
            _render_live_hourly_tracking(st, today_str)

    # ===========================================================
    # TAB 2: 주간 성과
    # ===========================================================
    with tab_perf:
        _render_weekly_performance(st, target_date_str, use_v2=use_v2)

    # ===========================================================
    # TAB 3: D+1 시간대 예측 (시간별 지도 + Hex 일합산 통합)
    # ===========================================================
    with tab_hourly:
        # D+1 탭은 항상 내일 날짜 사용 (사이드바 날짜 무시)
        tomorrow_str = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        _render_hourly_prediction(st, tomorrow_str)


def _render_hex_map(st, target_date_str: str):
    """Hex 수요 지도 탭 렌더링"""
    import folium
    from folium.plugins import MarkerCluster
    from streamlit_folium import st_folium

    st.subheader("📍 Hex 수요 지도")
    st.caption("District 예측을 H3 hex (~174m) 단위로 공간 배분한 결과입니다")

    # ── 설정 ──
    col_top, col_center, col_min = st.columns(3)
    with col_top:
        top_n = st.selectbox("District당 표시 hex", [5, 10, 15, 20], index=2)
    with col_center:
        center_filter = st.selectbox("센터 필터", ["전체"] + [
            "Center_North", "Center_West", "Center_South", "Center_Gimpo", "Center_East", "Center_Central",
            "Partner_Gwacheon", "Partner_Seoul", "Partner_Ansan", "Partner_Daejeon"])
    with col_min:
        min_rides = st.selectbox("최소 예측 건수", [0, 1, 2, 3, 5], index=1)

    # ── 데이터 로드 ──
    with st.spinner("📍 Hex 예측 계산 중..."):
        try:
            from district_v2_hex import DistrictV2Hex
            predictor = DistrictV2Hex(verbose=False, top_n=top_n)
            pred_df = predictor.predict(target_date_str, top_n=top_n)
        except Exception as e:
            st.error(f"Hex 예측 실패: {e}")
            return

    if len(pred_df) == 0:
        st.warning("예측 데이터가 없습니다")
        return

    # 필터 적용
    df = pred_df[~pred_df['h3_index'].isin(['unknown', 'other'])].copy()
    if center_filter != "전체":
        df = df[df['center'] == center_filter]
    if min_rides > 0:
        df = df[df['predicted_rides'] >= min_rides]

    if len(df) == 0:
        st.warning("조건에 맞는 데이터가 없습니다")
        return

    # ── KPI ──
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("총 예측 건수", f"{df['predicted_rides'].sum():,.0f}건")
    with k2:
        st.metric("표시 hex 수", f"{len(df):,}개")
    with k3:
        n_districts = df[['region', 'district']].drop_duplicates().shape[0]
        st.metric("District", f"{n_districts}개")
    with k4:
        # Top 5 hex 비중
        top5 = df[df['hex_rank'] <= 5]['predicted_rides'].sum()
        total = df['predicted_rides'].sum()
        st.metric("Top5 hex 비중", f"{top5/total*100:.1f}%" if total > 0 else "-")

    # ── 실제 데이터 (비교용) ──
    actual_df = None
    try:
        client = get_bigquery_client()
        actual_query = f"""
        SELECT
            h3_start_area_name as region,
            h3_start_district_name as district,
            udf.geo_to_h3(ST_Y(start_location), ST_X(start_location), 9) as h3_index,
            AVG(ST_Y(start_location)) as hex_lat,
            AVG(ST_X(start_location)) as hex_lng,
            COUNT(*) as actual_rides
        FROM `service.rides`
        WHERE DATE(start_time) = '{target_date_str}'
            AND h3_start_area_name IS NOT NULL
            AND h3_start_district_name IS NOT NULL
            AND start_location IS NOT NULL
            AND bike_type = 1
        GROUP BY 1, 2, 3
        """
        actual_df = client.query(actual_query).to_dataframe()
    except Exception:
        pass

    # ── 지도 생성 ──
    # "전체" → 수도권(서울/경기) 중심으로, 센터 필터 시 해당 지역 fit
    if center_filter == "전체":
        # 수도권 중심점 (서울시청 근처)
        center_lat, center_lng = 37.52, 126.98
        m = folium.Map(location=[center_lat, center_lng], zoom_start=11,
                       tiles='CartoDB positron')
    else:
        center_lat = df['hex_lat'].mean()
        center_lng = df['hex_lng'].mean()
        m = folium.Map(location=[center_lat, center_lng], zoom_start=13,
                       tiles='CartoDB positron')
        # auto-fit to data bounds
        sw = [df['hex_lat'].min() - 0.003, df['hex_lng'].min() - 0.003]
        ne = [df['hex_lat'].max() + 0.003, df['hex_lng'].max() + 0.003]
        m.fit_bounds([sw, ne])

    # 색상 스케일 최대값 (95%ile)
    max_rides = max(df['predicted_rides'].quantile(0.95), 1)

    # 예측 hex 육각형 폴리곤 표시
    df_sorted = df.sort_values('predicted_rides', ascending=True)

    def _hex_daily_tt(row):
        return (f"{row['district']} #{int(row['hex_rank'])} | "
                f"{row['predicted_rides']:.1f}건")

    def _hex_daily_popup(row):
        return (
            f"<div style='font-family:Arial;font-size:12px;min-width:180px;'>"
            f"<b>{row['district']}</b>"
            f"<span style='color:#999;font-size:11px;'> "
            f"{row.get('region', '')}</span>"
            f"<hr style='margin:4px 0;'>"
            f"예측: <b>{row['predicted_rides']:.1f}건</b><br>"
            f"비율: {row['hex_ratio']*100:.1f}%<br>"
            f"순위: {int(row['hex_rank'])}위<br>"
            f"<span style='font-size:10px;color:#aaa;'>"
            f"H3: {row['h3_index'][:12]}...</span>"
            f"</div>"
        )

    _add_h3_hexagons(
        m, df_sorted,
        value_col='predicted_rides',
        max_value=max_rides,
        opacity=0.75,
        tooltip_fn=_hex_daily_tt,
        popup_fn=_hex_daily_popup,
    )

    st.caption("💡 줌인하면 개별 H3 육각형(~174m)을 자세히 볼 수 있습니다. "
               "Scroll right for other regions.")
    st_folium(m, width=None, height=550, returned_objects=[])

    # ── 실제 vs 예측 비교 (병합) ──
    if actual_df is not None and len(actual_df) > 0:
        st.subheader("📊 Hex 예측 vs 실제 비교")

        merged = df.merge(
            actual_df[['region', 'district', 'h3_index', 'actual_rides']],
            on=['region', 'district', 'h3_index'],
            how='left'
        )
        merged['actual_rides'] = merged['actual_rides'].fillna(0)
        has_actual = merged[merged['actual_rides'] > 0].copy()

        if len(has_actual) > 0:
            has_actual['error_pct'] = (
                (has_actual['predicted_rides'] - has_actual['actual_rides'])
                / has_actual['actual_rides'] * 100
            ).round(1)

            mc1, mc2, mc3 = st.columns(3)
            with mc1:
                ape = ((has_actual['predicted_rides'] - has_actual['actual_rides']).abs()
                       / has_actual['actual_rides'] * 100)
                st.metric("Hex MAPE", f"{ape.mean():.1f}%")
            with mc2:
                # 가중 MAPE = SUM(|pred-actual|) / SUM(actual) * 100
                wmape = ((has_actual['predicted_rides'] - has_actual['actual_rides']).abs().sum()
                         / has_actual['actual_rides'].sum() * 100)
                st.metric("가중 MAPE", f"{wmape:.1f}%")
            with mc3:
                covered = has_actual['actual_rides'].sum()
                total_actual = actual_df['actual_rides'].sum()
                st.metric("커버리지", f"{covered/total_actual*100:.1f}%")

            # 오차 상위/하위 hex
            st.markdown("**🔴 과소예측 Top 10**")
            worst = has_actual.nsmallest(10, 'error_pct')[
                ['district', 'h3_index', 'predicted_rides', 'actual_rides', 'error_pct']
            ].copy()
            worst.columns = ['District', 'H3 Index', '예측', '실제', '오차(%)']
            worst['H3 Index'] = worst['H3 Index'].str[:15] + '...'
            st.dataframe(worst, use_container_width=True, hide_index=True)

            st.markdown("**🟢 과대예측 Top 10**")
            best = has_actual.nlargest(10, 'error_pct')[
                ['district', 'h3_index', 'predicted_rides', 'actual_rides', 'error_pct']
            ].copy()
            best.columns = ['District', 'H3 Index', '예측', '실제', '오차(%)']
            best['H3 Index'] = best['H3 Index'].str[:15] + '...'
            st.dataframe(best, use_container_width=True, hide_index=True)

    # ── District별 hex 분포 상세 ──
    with st.expander("📋 District별 hex 분포 상세"):
        dist_summary = df.groupby(['center', 'district']).agg(
            hex_count=('h3_index', 'nunique'),
            total_pred=('predicted_rides', 'sum'),
            top1_ratio=('hex_ratio', 'max'),
        ).reset_index().sort_values('total_pred', ascending=False)

        dist_summary.columns = ['센터', 'District', 'hex수', '총예측', 'Top1비율']
        dist_summary['총예측'] = dist_summary['총예측'].round(0).astype(int)
        dist_summary['Top1비율'] = (dist_summary['Top1비율'] * 100).round(1)

        st.dataframe(dist_summary.head(30), use_container_width=True, hide_index=True)

    # ── CSV 다운로드 ──
    with st.expander("💾 Hex 예측 데이터 다운로드"):
        csv_data = df[['date', 'region', 'district', 'h3_index', 'center',
                        'predicted_rides', 'hex_ratio', 'hex_rank',
                        'hex_lat', 'hex_lng']].copy()
        csv_data = csv_data.sort_values(['center', 'district', 'hex_rank'])

        csv_bytes = csv_data.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label=f"📥 Hex 예측 CSV 다운로드 ({len(csv_data):,}행)",
            data=csv_bytes,
            file_name=f"hex_prediction_{target_date_str}.csv",
            mime="text/csv",
        )


def _render_weekly_performance(st, base_date_str: str, use_v2: bool = True):
    """주간 예측 성과 탭 렌더링 (Production v2)"""
    import altair as alt

    with st.sidebar:
        st.divider()
        period = st.selectbox("📅 조회 기간", [7, 14, 30], index=0,
                              format_func=lambda x: f"최근 {x}일")

    base_date = pd.Timestamp(base_date_str)
    start_date = (base_date - timedelta(days=period - 1)).strftime('%Y-%m-%d')
    end_date = base_date_str

    # --- 데이터 로딩 ---
    with st.spinner('📊 주간 성과 데이터 로딩 중 (Production v2)...'):
        from production_v2_predictor import (
            evaluate_period as v2_evaluate,
            format_for_weekly_performance as v2_format_weekly,
        )
        eval_df = v2_evaluate(start_date, end_date, verbose=False)
        if eval_df is None or len(eval_df) == 0:
            st.error("평가 데이터를 생성할 수 없습니다.")
            return
        actual_df, pred_df = v2_format_weekly(eval_df)

    actual_df['date'] = pd.to_datetime(actual_df['date']).dt.date
    pred_df['date'] = pd.to_datetime(pred_df['date']).dt.date

    # --- 일별 합계 ---
    daily_actual = actual_df.groupby('date')['actual_rides'].sum().reset_index()
    daily_pred = pred_df.groupby('date')['predicted_rides'].sum().reset_index()
    daily = daily_actual.merge(daily_pred, on='date', how='inner')

    if daily.empty:
        st.warning("매칭되는 데이터가 없습니다.")
        return

    daily['error_pct'] = (daily['predicted_rides'] - daily['actual_rides']) / daily['actual_rides'] * 100
    daily['abs_error_pct'] = daily['error_pct'].abs()

    avg_mape = daily['abs_error_pct'].mean()
    avg_bias = daily['error_pct'].mean()
    latest = daily.sort_values('date').iloc[-1]

    # --- KPI ---
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("평균 오차율 (MAPE)", f"{avg_mape:.1f}%")
    k2.metric("평균 Bias", f"{avg_bias:+.1f}%",
              delta="과소예측" if avg_bias < 0 else "과대예측",
              delta_color="inverse" if avg_bias < 0 else "normal")
    k3.metric(f"{latest['date']} 실제", f"{latest['actual_rides']:,.0f}건")
    k4.metric(f"{latest['date']} 예측", f"{latest['predicted_rides']:,.0f}건",
              delta=f"{latest['error_pct']:+.1f}%",
              delta_color="off" if abs(latest['error_pct']) < 10 else "inverse")

    st.markdown("---")

    # --- 일별 추이 차트 ---
    st.subheader("📈 일별 예측 vs 실제 추이")

    chart_data = daily.melt(id_vars=['date'], value_vars=['actual_rides', 'predicted_rides'],
                            var_name='type', value_name='rides')
    chart_data['type'] = chart_data['type'].map({'actual_rides': '실제', 'predicted_rides': '예측'})
    chart_data['date'] = pd.to_datetime(chart_data['date'])

    line = alt.Chart(chart_data).mark_line(point=True, strokeWidth=2).encode(
        x=alt.X('date:T', title='날짜', axis=alt.Axis(format='%m/%d')),
        y=alt.Y('rides:Q', title='라이딩 건수'),
        color=alt.Color('type:N', scale=alt.Scale(domain=['실제', '예측'], range=['#1f77b4', '#ff7f0e'])),
        tooltip=[alt.Tooltip('date:T', format='%Y-%m-%d'), 'type:N', alt.Tooltip('rides:Q', format=',.0f')]
    ).properties(height=320)

    err_data = daily.copy()
    err_data['date'] = pd.to_datetime(err_data['date'])
    err_bar = alt.Chart(err_data).mark_bar(opacity=0.6).encode(
        x=alt.X('date:T', title='날짜', axis=alt.Axis(format='%m/%d')),
        y=alt.Y('error_pct:Q', title='오차율 (%)'),
        color=alt.condition(alt.datum.error_pct > 0, alt.value('#e74c3c'), alt.value('#3498db')),
        tooltip=[alt.Tooltip('date:T', format='%Y-%m-%d'), alt.Tooltip('error_pct:Q', format='+.1f')]
    ).properties(height=200)
    zero = alt.Chart(pd.DataFrame({'y': [0]})).mark_rule(color='gray', strokeDash=[4, 4]).encode(y='y:Q')

    c1, c2 = st.columns([3, 2])
    with c1:
        st.altair_chart(line, use_container_width=True)
    with c2:
        st.altair_chart(err_bar + zero, use_container_width=True)

    st.markdown("---")

    # --- Top 5 / Worst 5 ---
    st.subheader("🏆 권역별 예측 성과")

    rgn_actual = actual_df.groupby('region')['actual_rides'].sum().reset_index()
    rgn_pred = pred_df.groupby('region')['predicted_rides'].sum().reset_index()
    rgn = rgn_actual.merge(rgn_pred, on='region', how='inner')
    rgn = rgn[rgn['actual_rides'] >= 10].copy()
    rgn['error_pct'] = (rgn['predicted_rides'] - rgn['actual_rides']) / rgn['actual_rides'] * 100
    rgn['abs_error'] = rgn['error_pct'].abs()
    rgn = rgn.sort_values('abs_error')

    top5 = rgn.head(5)
    worst5 = rgn.tail(5).sort_values('abs_error', ascending=False)

    ct, cw = st.columns(2)
    with ct:
        st.markdown("#### ✅ 예측 정확도 Top 5")
        for _, r in top5.iterrows():
            e = r['error_pct']
            icon = "🟢" if abs(e) < 10 else "🟡"
            st.markdown(f"{icon} **{r['region']}** — 오차 `{e:+.1f}%` "
                        f"(예측 {r['predicted_rides']:,.0f} / 실제 {r['actual_rides']:,.0f})")

    with cw:
        st.markdown("#### ❌ 예측 오차 Worst 5")
        for _, r in worst5.iterrows():
            e = r['error_pct']
            st.markdown(f"🔴 **{r['region']}** — 오차 `{e:+.1f}%` "
                        f"({'과소' if e < 0 else '과대'}) "
                        f"(예측 {r['predicted_rides']:,.0f} / 실제 {r['actual_rides']:,.0f})")

    st.markdown("---")

    # --- 권역별 오차 분포 ---
    st.subheader("📊 권역별 오차 분포 (상위 30개 권역)")

    top30 = rgn.nlargest(30, 'actual_rides').sort_values('error_pct')
    bar = alt.Chart(top30).mark_bar().encode(
        x=alt.X('error_pct:Q', title='오차율 (%)', scale=alt.Scale(domain=[-80, 80])),
        y=alt.Y('region:N', title='', sort=alt.EncodingSortField('error_pct', order='ascending')),
        color=alt.condition(alt.datum.error_pct > 0, alt.value('#e74c3c'), alt.value('#3498db')),
        tooltip=[alt.Tooltip('region:N', title='권역'),
                 alt.Tooltip('error_pct:Q', title='오차율', format='+.1f'),
                 alt.Tooltip('actual_rides:Q', title='실제', format=',.0f'),
                 alt.Tooltip('predicted_rides:Q', title='예측', format=',.0f')]
    ).properties(height=max(400, len(top30) * 18))
    vline = alt.Chart(pd.DataFrame({'x': [0]})).mark_rule(color='black', strokeWidth=1).encode(x='x:Q')

    st.altair_chart(bar + vline, use_container_width=True)

    # --- 권역별 일별 상세 비교 ---
    st.markdown("---")
    st.subheader("📋 권역별 일별 예측 vs 실제 비교")
    st.caption(f"기간: {start_date} ~ {end_date} ({period}일)")

    # 권역 × 날짜 병합
    rgn_daily = actual_df.merge(pred_df[['date', 'region', 'predicted_rides']], on=['date', 'region'], how='inner')
    rgn_daily['error_pct'] = np.where(
        rgn_daily['actual_rides'] > 0,
        ((rgn_daily['predicted_rides'] - rgn_daily['actual_rides']) / rgn_daily['actual_rides'] * 100).round(1),
        np.nan
    )
    rgn_daily['date'] = pd.to_datetime(rgn_daily['date'])

    # 날짜 레이블
    dow_kr = ['월', '화', '수', '목', '금', '토', '일']
    date_order = sorted(rgn_daily['date'].unique())
    date_labels = {d: f"{pd.Timestamp(d).strftime('%m/%d')}({dow_kr[pd.Timestamp(d).dayofweek]})" for d in date_order}
    rgn_daily['date_label'] = rgn_daily['date'].map(date_labels)
    ordered_labels = [date_labels[d] for d in date_order]

    # 권역별 합계 (정렬 기준)
    rgn_totals = rgn_daily.groupby('region').agg(
        total_actual=('actual_rides', 'sum'),
        total_pred=('predicted_rides', 'sum'),
    ).reset_index()
    rgn_totals['total_error'] = ((rgn_totals['total_pred'] - rgn_totals['total_actual']) / rgn_totals['total_actual'] * 100).round(1)
    rgn_totals['abs_error'] = rgn_totals['total_error'].abs()

    # 센터 매핑
    if 'center' in pred_df.columns:
        rc_map_rgn = pred_df[['region', 'center']].drop_duplicates()
        rgn_totals = rgn_totals.merge(rc_map_rgn, on='region', how='left')
    else:
        rgn_totals['center'] = ''

    # --- 필터 ---
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        sort_opt = st.selectbox("정렬 기준", ["실제 라이딩 많은 순", "오차율 큰 순", "오차율 작은 순", "센터별"], key="rgn_sort")
    with fc2:
        center_choices = ["전체"] + sorted(rgn_totals['center'].dropna().unique().tolist())
        center_sel = st.selectbox("센터 필터", center_choices, key="rgn_center")
    with fc3:
        min_actual = st.selectbox("최소 실제 건수", [0, 10, 30, 50, 100], index=2, key="rgn_min")

    # 필터 적용
    view = rgn_totals.copy()
    if center_sel != "전체":
        view = view[view['center'] == center_sel]
    view = view[view['total_actual'] >= min_actual]

    # 정렬
    if sort_opt == "실제 라이딩 많은 순":
        view = view.sort_values('total_actual', ascending=False)
    elif sort_opt == "오차율 큰 순":
        view = view.sort_values('abs_error', ascending=False)
    elif sort_opt == "오차율 작은 순":
        view = view.sort_values('abs_error', ascending=True)
    elif sort_opt == "센터별":
        view = view.sort_values(['center', 'total_actual'], ascending=[True, False])

    # --- 히트맵 (권역 × 날짜 오차율) ---
    top_regions = view.head(40)['region'].tolist()
    heatmap_data = rgn_daily[rgn_daily['region'].isin(top_regions)].copy()

    if len(heatmap_data) > 0:
        st.markdown("**오차율 히트맵** (빨강=과대예측, 파랑=과소예측)")

        heatmap = alt.Chart(heatmap_data).mark_rect(stroke='white', strokeWidth=0.5).encode(
            x=alt.X('date_label:N', title='', sort=ordered_labels,
                     axis=alt.Axis(labelAngle=0, labelFontSize=11)),
            y=alt.Y('region:N', title='',
                     sort=alt.EncodingSortField(field='region', order='ascending'),
                     axis=alt.Axis(labelFontSize=10)),
            color=alt.Color('error_pct:Q', title='오차율(%)',
                            scale=alt.Scale(scheme='redblue', domain=[-50, 50], domainMid=0,
                                            reverse=True)),
            tooltip=[
                alt.Tooltip('region:N', title='권역'),
                alt.Tooltip('date_label:N', title='날짜'),
                alt.Tooltip('actual_rides:Q', title='실제', format=',.0f'),
                alt.Tooltip('predicted_rides:Q', title='예측', format=',.0f'),
                alt.Tooltip('error_pct:Q', title='오차율(%)', format='+.1f'),
            ]
        ).properties(
            height=max(300, len(top_regions) * 18),
            width='container'
        )

        # 정렬 반영: view의 region 순서를 히트맵에 적용
        sort_order = view[view['region'].isin(top_regions)]['region'].tolist()
        heatmap = heatmap.encode(
            y=alt.Y('region:N', title='', sort=sort_order, axis=alt.Axis(labelFontSize=10))
        )

        st.altair_chart(heatmap, use_container_width=True)

    # --- 요약 테이블 ---
    st.markdown("**권역별 요약 테이블**")

    # 날짜별 오차율 피벗
    pivot_err = rgn_daily[rgn_daily['region'].isin(view['region'])].pivot_table(
        index='region', columns='date_label', values='error_pct', aggfunc='first'
    )
    pivot_err = pivot_err.reindex(columns=ordered_labels)

    # 합계 정보 합치기
    summary = view[['region', 'center', 'total_actual', 'total_pred', 'total_error']].copy()
    summary = summary.set_index('region')
    summary = summary.join(pivot_err)
    summary = summary.reset_index()

    # 컬럼명 정리
    summary = summary.rename(columns={
        'region': '권역', 'center': '센터',
        'total_actual': '실제(합)', 'total_pred': '예측(합)', 'total_error': '오차(%)'
    })
    summary['실제(합)'] = summary['실제(합)'].apply(lambda x: f"{x:,.0f}")
    summary['예측(합)'] = summary['예측(합)'].apply(lambda x: f"{x:,.0f}")
    summary['오차(%)'] = summary['오차(%)'].apply(lambda x: f"{x:+.1f}%")

    # 일별 오차율 포맷
    for lbl in ordered_labels:
        if lbl in summary.columns:
            summary[lbl] = summary[lbl].apply(
                lambda x: f"{x:+.0f}%" if pd.notna(x) else "-"
            )

    st.dataframe(
        summary.head(40),
        use_container_width=True,
        hide_index=True,
        height=min(600, 35 + len(summary.head(40)) * 35)
    )

    # --- 다운로드 ---
    with st.expander("📥 데이터 다운로드"):
        dl_data = rgn_daily[rgn_daily['region'].isin(view['region'])].copy()
        dl_data['date'] = dl_data['date'].dt.strftime('%Y-%m-%d')
        dl_cols = ['date', 'region', 'actual_rides', 'predicted_rides', 'error_pct']
        csv_dl = dl_data[dl_cols].sort_values(['region', 'date']).to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            f"📥 권역별 일별 비교 CSV ({len(dl_data):,}행)",
            data=csv_dl,
            file_name=f"region_daily_comparison_{start_date}_{end_date}.csv",
            mime="text/csv",
        )

    # --- 센터별 집계 ---
    st.markdown("---")
    st.subheader("🏢 센터별 예측 성과")

    if 'center' in pred_df.columns:
        rc_map = pred_df[['region', 'center']].drop_duplicates()
        actual_c = actual_df.merge(rc_map, on='region', how='left')
        c_actual = actual_c.groupby('center')['actual_rides'].sum().reset_index()
        c_pred = pred_df.groupby('center')['predicted_rides'].sum().reset_index()
        c_merged = c_actual.merge(c_pred, on='center', how='inner')
        c_merged['error_pct'] = (c_merged['predicted_rides'] - c_merged['actual_rides']) / c_merged['actual_rides'] * 100
        c_merged = c_merged.sort_values('error_pct', key=abs)

        disp = c_merged.copy()
        disp.columns = ['센터', '실제 라이딩', '예측 라이딩', '오차율(%)']
        disp['실제 라이딩'] = disp['실제 라이딩'].apply(lambda x: f"{x:,.0f}")
        disp['예측 라이딩'] = disp['예측 라이딩'].apply(lambda x: f"{x:,.0f}")
        disp['오차율(%)'] = disp['오차율(%)'].apply(lambda x: f"{x:+.1f}%")
        st.dataframe(disp, use_container_width=True, hide_index=True)

    # --- 시스템 상태 ---
    st.markdown("---")
    with st.expander("⚙️ 예측 시스템 상태", expanded=False):
        import json
        s1, s2 = st.columns(2)

        params_path = os.path.join(SCRIPT_DIR, 'district_hour_params.json')
        with s1:
            if os.path.exists(params_path):
                with open(params_path) as f:
                    params = json.load(f)
                n_d = sum(len(v) for v in params.get('district_ratios', {}).values())
                n_h = len(params.get('hourly_profiles', {}).get('global_weekday', {}))
                st.markdown(f"""
                **보정 파라미터** (`district_hour_params.json`)
                - 마지막 업데이트: `{params.get('last_updated', '-')}`
                - 보정된 district: **{n_d}개**
                - 보정된 시간대: **{n_h}개**
                """)
            else:
                st.info("보정 파라미터 없음 (tuner 미실행)")

        perf_path = os.path.join(SCRIPT_DIR, 'district_hour_performance_log.json')
        with s2:
            if os.path.exists(perf_path):
                with open(perf_path) as f:
                    perf = json.load(f)
                dl = perf.get('daily', [])
                if dl:
                    last = dl[-1]
                    st.markdown(f"""
                    **자동 학습 로그** (`district_hour_tuner`)
                    - 평가 데이터: **{len(dl)}일** 축적
                    - 마지막: `{last.get('date', '-')}` (D-MAPE {last.get('district_mape', '-')}%)
                    - 보정 횟수: **{len(perf.get('corrections', []))}회**
                    """)
            else:
                st.info("성능 로그 없음 (tuner 미실행)")



# ============================================================
# TAB 3: D+1 시간대 예측 렌더링
# ============================================================

def _render_live_hourly_tracking(st, target_date_str: str):
    """⏱️ 실시간 시간대별 예측 정합성 추적

    오늘 날짜일 때 Tab 1 하단에 표시.
    현재 시각까지의 실제 데이터와 예측을 시간별로 비교.

    구성:
        1. 누적 KPI (완료시간/24, 누적 예측 vs 실제, 누적 오차)
        2. 24시간 라인차트 (예측 전체 + 실제 현재까지 + 현재시각 표시선)
        3. 시간별 정합성 테이블 (✅ 완료 / ⏳ 대기)
    """
    import altair as alt
    from district_v2_hourly import DistrictV2Hourly

    current_hour = datetime.now().hour

    with st.spinner('⏱️ 실시간 시간대 정합성 로딩 중...'):
        # 1. 시간별 예측 로딩
        predictor = DistrictV2Hourly(verbose=False)
        hourly_pred_df = predictor.to_hourly_estimate(target_date_str)

        if len(hourly_pred_df) == 0:
            st.warning("시간별 예측 데이터를 생성할 수 없습니다.")
            return

        # 2. 시간별 실제 라이딩 (BQ)
        client = get_bigquery_client()
        actual_query = f"""
        SELECT
            EXTRACT(HOUR FROM start_time) as hour,
            COUNT(*) as actual_rides
        FROM `service.rides`
        WHERE DATE(start_time) = '{target_date_str}'
        GROUP BY 1
        ORDER BY 1
        """
        try:
            actual_hourly = client.query(actual_query).to_dataframe()
        except Exception:
            actual_hourly = pd.DataFrame(columns=['hour', 'actual_rides'])

    # 3. 시간별 예측 집계 + 실제 병합
    pred_hourly = hourly_pred_df.groupby('hour')[
        'predicted_rides'].sum().reset_index()

    # 0~23시 전체 프레임 보장
    all_hours = pd.DataFrame({'hour': range(24)})
    merged = all_hours.merge(pred_hourly, on='hour', how='left')
    merged['predicted_rides'] = merged['predicted_rides'].fillna(0)
    merged = merged.merge(actual_hourly, on='hour', how='left')
    merged['actual_rides'] = merged['actual_rides'].fillna(0)

    # 상태 구분
    merged['status'] = merged['hour'].apply(
        lambda h: '✅' if h < current_hour else ('🔄' if h == current_hour else '⏳'))
    merged['hour_label'] = merged['hour'].apply(lambda h: f"{h:02d}시")

    # 완료된 시간만 오차 계산
    merged['error'] = np.where(
        merged['hour'] < current_hour,
        merged['predicted_rides'] - merged['actual_rides'],
        np.nan
    )
    merged['error_pct'] = np.where(
        (merged['hour'] < current_hour) & (merged['actual_rides'] > 0),
        ((merged['predicted_rides'] - merged['actual_rides'])
         / merged['actual_rides'] * 100),
        np.nan
    )

    # ── KPI ──
    completed = merged[merged['hour'] < current_hour]
    cum_pred = completed['predicted_rides'].sum()
    cum_actual = completed['actual_rides'].sum()
    cum_error_pct = ((cum_pred - cum_actual) / cum_actual * 100
                     if cum_actual > 0 else 0)

    valid_hours = completed[completed['actual_rides'] > 0]
    hour_mape = valid_hours['error_pct'].abs().mean() if len(valid_hours) > 0 else 0

    remaining_pred = merged[merged['hour'] >= current_hour]['predicted_rides'].sum()
    total_pred = merged['predicted_rides'].sum()

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("⏱️ 완료", f"{current_hour}/24시간")
    k2.metric("누적 실제", f"{cum_actual:,.0f}건")
    k3.metric("누적 예측", f"{cum_pred:,.0f}건",
              delta=f"{cum_error_pct:+.1f}%",
              delta_color="inverse" if cum_error_pct < 0 else "normal")
    k4.metric("시간 MAPE", f"{hour_mape:.1f}%")
    k5.metric("잔여 예측", f"{remaining_pred:,.0f}건",
              delta=f"총 {total_pred:,.0f}건")

    # ── 24시간 라인차트 ──
    chart_rows = []
    for _, row in merged.iterrows():
        h = int(row['hour'])
        chart_rows.append({
            'hour': h,
            'hour_label': row['hour_label'],
            'type': '예측',
            'rides': row['predicted_rides'],
        })
        if h < current_hour:
            chart_rows.append({
                'hour': h,
                'hour_label': row['hour_label'],
                'type': '실제',
                'rides': row['actual_rides'],
            })
        elif h == current_hour:
            # 진행 중 시간은 실제 표시 (불완전)
            chart_rows.append({
                'hour': h,
                'hour_label': row['hour_label'],
                'type': '실제 (진행중)',
                'rides': row['actual_rides'],
            })
    chart_data = pd.DataFrame(chart_rows)

    # 예측 라인 (24h 전체) + 실제 라인 (현재까지)
    line = alt.Chart(chart_data).mark_line(
        point=alt.OverlayMarkDef(size=30), strokeWidth=2
    ).encode(
        x=alt.X('hour:O', title='시간', sort=list(range(24)),
                axis=alt.Axis(labelAngle=0)),
        y=alt.Y('rides:Q', title='라이딩 건수'),
        color=alt.Color('type:N',
                        scale=alt.Scale(
                            domain=['실제', '실제 (진행중)', '예측'],
                            range=['#1976D2', '#90CAF9', '#FF9800']),
                        legend=alt.Legend(title='구분')),
        strokeDash=alt.StrokeDash('type:N',
                                   scale=alt.Scale(
                                       domain=['실제', '실제 (진행중)', '예측'],
                                       range=[[0], [4, 4], [0]]),
                                   legend=None),
        tooltip=[
            alt.Tooltip('hour_label:N', title='시간'),
            alt.Tooltip('type:N', title='구분'),
            alt.Tooltip('rides:Q', title='건수', format=',.0f'),
        ]
    ).properties(height=350)

    # 현재시각 표시선
    now_rule = alt.Chart(pd.DataFrame({
        'hour': [current_hour]
    })).mark_rule(
        color='#D32F2F', strokeDash=[6, 3], strokeWidth=2
    ).encode(x='hour:O')

    now_text = alt.Chart(pd.DataFrame({
        'hour': [current_hour], 'label': [f'현재 {current_hour:02d}시']
    })).mark_text(
        align='left', dx=5, dy=-10, fontSize=11, color='#D32F2F', fontWeight='bold'
    ).encode(
        x='hour:O',
        text='label:N'
    )

    st.altair_chart(line + now_rule + now_text, use_container_width=True)

    # ── 시간별 정합성 테이블 ──
    with st.expander("📋 시간별 상세 정합성", expanded=True):
        disp = merged[['hour_label', 'status', 'predicted_rides',
                        'actual_rides', 'error', 'error_pct']].copy()
        disp.columns = ['시간', '상태', '예측', '실제', '오차(건)', '오차(%)']
        disp['예측'] = disp['예측'].apply(lambda x: f"{x:,.0f}")
        disp['실제'] = disp.apply(
            lambda r: f"{float(r['실제']):,.0f}" if r['상태'] != '⏳' else '—', axis=1)
        disp['오차(건)'] = disp['오차(건)'].apply(
            lambda x: f"{x:+,.0f}" if pd.notna(x) else '—')
        disp['오차(%)'] = disp['오차(%)'].apply(
            lambda x: f"{x:+.1f}%" if pd.notna(x) else '—')
        st.dataframe(disp, use_container_width=True, hide_index=True, height=400)

    # ── District별 누적 정합성 Top/Bottom ──
    if current_hour > 0 and len(hourly_pred_df) > 0:
        completed_hours = list(range(current_hour))
        pred_cum = hourly_pred_df[hourly_pred_df['hour'].isin(completed_hours)].groupby(
            ['district', 'region', 'center'])['predicted_rides'].sum().reset_index()

        # district별 실제 (BQ)
        try:
            dist_actual_query = f"""
            SELECT
                h3_start_district_name as district,
                COUNT(*) as actual_rides
            FROM `service.rides`
            WHERE DATE(start_time) = '{target_date_str}'
                AND EXTRACT(HOUR FROM start_time) < {current_hour}
                AND h3_start_district_name IS NOT NULL
            GROUP BY 1
            """
            dist_actual = client.query(dist_actual_query).to_dataframe()
        except Exception:
            dist_actual = pd.DataFrame(columns=['district', 'actual_rides'])

        if len(dist_actual) > 0:
            dist_merged = pred_cum.merge(dist_actual, on='district', how='outer')
            dist_merged['predicted_rides'] = dist_merged['predicted_rides'].fillna(0)
            dist_merged['actual_rides'] = dist_merged['actual_rides'].fillna(0)
            dist_merged['error_pct'] = np.where(
                dist_merged['actual_rides'] > 5,
                ((dist_merged['predicted_rides'] - dist_merged['actual_rides'])
                 / dist_merged['actual_rides'] * 100),
                np.nan
            )

            valid_dist = dist_merged.dropna(subset=['error_pct'])
            if len(valid_dist) >= 5:
                with st.expander("🔴 과소예측 District Top 5 (현재까지)", expanded=False):
                    under = valid_dist.nsmallest(5, 'error_pct')[
                        ['district', 'center', 'predicted_rides', 'actual_rides', 'error_pct']].copy()
                    under.columns = ['District', '센터', '예측', '실제', '오차(%)']
                    under['오차(%)'] = under['오차(%)'].apply(lambda x: f"{x:+.1f}%")
                    under['예측'] = under['예측'].apply(lambda x: f"{x:,.0f}")
                    under['실제'] = under['실제'].apply(lambda x: f"{x:,.0f}")
                    st.dataframe(under, use_container_width=True, hide_index=True)

                with st.expander("🟢 과대예측 District Top 5 (현재까지)", expanded=False):
                    over = valid_dist.nlargest(5, 'error_pct')[
                        ['district', 'center', 'predicted_rides', 'actual_rides', 'error_pct']].copy()
                    over.columns = ['District', '센터', '예측', '실제', '오차(%)']
                    over['오차(%)'] = over['오차(%)'].apply(lambda x: f"{x:+.1f}%")
                    over['예측'] = over['예측'].apply(lambda x: f"{x:,.0f}")
                    over['실제'] = over['실제'].apply(lambda x: f"{x:,.0f}")
                    st.dataframe(over, use_container_width=True, hide_index=True)


def _render_hourly_prediction(st, base_date_str: str):
    """⏰ D+1 시간대 예측 탭 — 순수 예측 뷰 (실제 데이터 비교 없음)

    목적:
        내일(D+1) district × hour 수요를 운영 관점에서 보여주는 탭.
        검증(예측 vs 실제)은 Tab 1 '예측 검증'에서 수행.

    구성:
        1. KPI 카드 (총 예측, 피크시간, 피크 District)
        2. 시간별 수요 곡선 (area chart)
        3. 시간별 지도 (슬라이더)
        4. 센터 × 시간대 히트맵
        5. 운영 4구간 요약
        6. 데이터 다운로드
    """
    import altair as alt

    from district_v2_hourly import (
        DistrictV2Hourly, TIME_WINDOWS, OPS_SLOTS, hour_to_window
    )

    target_date = base_date_str

    st.caption(f"📅 예측 대상: **{target_date}** (내일)")

    # ── 데이터 로딩 ──
    with st.spinner('⏰ D+1 시간대별 예측 로딩 중...'):
        predictor = DistrictV2Hourly(verbose=False)
        pred_df = predictor.predict(target_date)

        if len(pred_df) == 0:
            st.error("시간대별 예측 데이터를 생성할 수 없습니다.")
            return

        # 시간별 추정치 (시계열 비율 분배 기반)
        hourly_pred_df = predictor.to_hourly_estimate(target_date)

        # Hex 공간 프로필 (시간별 지도의 H3 육각형 렌더링용)
        _hex_profiles = pd.DataFrame()
        try:
            from district_v2_hex import DistrictV2Hex
            _hex_predictor = DistrictV2Hex(verbose=False, top_n=15)
            _hex_profiles = _hex_predictor.get_hex_profiles(target_date)
        except Exception:
            pass

    # window별 예측 합계
    pred_window_total = pred_df.groupby('window')[
        'predicted_rides'].sum().reset_index()

    # window 순서 + 라벨
    window_order = list(TIME_WINDOWS.keys())
    window_labels = {k: v['label'] for k, v in TIME_WINDOWS.items()}
    pred_window_total['window_label'] = pred_window_total['window'].map(window_labels)
    pred_window_total['window_idx'] = pred_window_total['window'].map(
        {w: i for i, w in enumerate(window_order)})
    pred_window_total = pred_window_total.sort_values('window_idx')

    # ── 전체 합계 ──
    total_pred = pred_df['predicted_rides'].sum()

    # 피크 시간/district 계산
    if len(hourly_pred_df) > 0:
        hourly_total = hourly_pred_df.groupby('hour')[
            'predicted_rides'].sum().reset_index()
        peak_hour = int(hourly_total.loc[
            hourly_total['predicted_rides'].idxmax(), 'hour'])
        peak_hour_rides = hourly_total['predicted_rides'].max()

        top_district = hourly_pred_df.groupby('district')[
            'predicted_rides'].sum().sort_values(ascending=False)
        top_district_name = top_district.index[0] if len(top_district) > 0 else '-'
        top_district_rides = top_district.iloc[0] if len(top_district) > 0 else 0
    else:
        peak_hour, peak_hour_rides = 0, 0
        top_district_name, top_district_rides = '-', 0

    # ── 1. KPI 카드 ──
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📊 총 예측", f"{total_pred:,.0f}건")
    k2.metric("⏰ 피크 시간", f"{peak_hour:02d}시",
              delta=f"{peak_hour_rides:,.0f}건")
    k3.metric("📍 Top District", top_district_name,
              delta=f"{top_district_rides:,.0f}건")
    n_districts = pred_df['district'].nunique()
    k4.metric("🗺️ 대상 District", f"{n_districts}개")

    st.markdown("---")

    # ── 2. 시간별 수요 곡선 ──
    st.subheader("📈 시간별 수요 예측 (1시간 단위)")
    st.caption("과거 28일 시계열 비율 기반 분배")

    if len(hourly_pred_df) > 0:
        hourly_total['hour_label'] = hourly_total['hour'].apply(
            lambda h: f"{int(h):02d}시")

        h_area = alt.Chart(hourly_total).mark_area(
            opacity=0.6, color='#FF9800',
            line=alt.OverlayMarkDef(color='#E65100', strokeWidth=2)
        ).encode(
            x=alt.X('hour:O', title='시간', sort=list(range(24)),
                    axis=alt.Axis(labelAngle=0)),
            y=alt.Y('predicted_rides:Q', title='예측 라이딩 건수'),
            tooltip=[
                alt.Tooltip('hour_label:N', title='시간'),
                alt.Tooltip('predicted_rides:Q', title='예측 건수', format=',.0f'),
            ]
        ).properties(height=300)

        # 피크 시간 강조 룰라인
        peak_rule = alt.Chart(pd.DataFrame({
            'hour': [peak_hour], 'label': [f'피크 {peak_hour:02d}시']
        })).mark_rule(color='#D32F2F', strokeDash=[4, 4], strokeWidth=1.5).encode(
            x='hour:O'
        )

        st.altair_chart(h_area + peak_rule, use_container_width=True)

        # 시간대 Window 막대그래프 (예측만)
        sorted_labels = [window_labels[w] for w in window_order
                         if w in pred_window_total['window'].values]

        w_bar = alt.Chart(pred_window_total).mark_bar(
            cornerRadiusTopLeft=3, cornerRadiusTopRight=3,
            color='#FF9800'
        ).encode(
            x=alt.X('window_label:N', title='시간대',
                    sort=sorted_labels,
                    axis=alt.Axis(labelAngle=0)),
            y=alt.Y('predicted_rides:Q', title='예측 건수'),
            tooltip=[
                alt.Tooltip('window_label:N', title='시간대'),
                alt.Tooltip('predicted_rides:Q', title='예측 건수', format=',.0f'),
            ]
        ).properties(height=250)

        w_text = alt.Chart(pred_window_total).mark_text(
            dy=-12, fontSize=12, fontWeight='bold', color='#E65100'
        ).encode(
            x=alt.X('window_label:N', sort=sorted_labels),
            y=alt.Y('predicted_rides:Q'),
            text=alt.Text('predicted_rides:Q', format=',.0f'),
        )

        st.altair_chart(w_bar + w_text, use_container_width=True)

    # ── 2-2. 시간별 H3 수요 지도 (시간 슬라이더) ──
    if len(hourly_pred_df) > 0:
        st.markdown("---")
        st.subheader("🗺️ 시간별 수요 분포 지도")

        import folium as _folium

        _has_hex = len(_hex_profiles) > 0
        if _has_hex:
            st.caption("H3 육각형(~174m)으로 시간별 예측 수요를 표시합니다. "
                       "색이 진할수록 수요가 많습니다.")
        else:
            st.caption("슬라이더로 시간을 선택하면 해당 시간의 "
                       "district별 예측 수요를 지도에 표시합니다")

        # 시간 슬라이더
        selected_hour = st.slider(
            "시간 선택", 0, 23, 8, format="%d시",
            key='hourly_map_slider'
        )

        # 해당 시간의 데이터 필터
        hour_data = hourly_pred_df[hourly_pred_df['hour'] == selected_hour].copy()
        hour_data = hour_data[hour_data['predicted_rides'] > 0]

        if len(hour_data) == 0:
            st.info(f"{selected_hour:02d}시에 예측 건수가 있는 district가 없습니다")
        else:
            # KPI for selected hour
            h_total = hour_data['predicted_rides'].sum()
            h_n_districts = len(hour_data)

            hk1, hk2, hk3 = st.columns(3)
            hk1.metric(f"{selected_hour:02d}시 총 예측", f"{h_total:,.0f}건")
            hk2.metric("District 수", f"{h_n_districts}개")
            hk3.metric("일 전체 대비",
                        f"{h_total / total_pred * 100:.1f}%" if total_pred > 0 else "-")

            # 지도 생성
            valid_coords = hour_data.dropna(subset=['lat', 'lng'])
            if len(valid_coords) > 0:
                center_lat = valid_coords['lat'].mean()
                center_lng = valid_coords['lng'].mean()
            else:
                center_lat, center_lng = 37.52, 126.98

            hm = _folium.Map(
                location=[center_lat, center_lng],
                zoom_start=11,
                tiles='CartoDB positron'
            )

            if _has_hex:
                # ── H3 육각형 렌더링 ──
                # district 시간별 예측 × hex 비율 = hex 시간별 예측
                hour_hex = hour_data.merge(
                    _hex_profiles[['region', 'district', 'h3_index',
                                   'hex_ratio', 'hex_lat', 'hex_lng']],
                    on=['region', 'district'],
                    how='inner'
                )
                hour_hex['hex_rides'] = (hour_hex['predicted_rides']
                                         * hour_hex['hex_ratio'])
                hour_hex = hour_hex[hour_hex['hex_rides'] > 0.1]

                _sel_h = selected_hour  # closure용 복사

                def _h_tt(row):
                    return (f"{row.get('district', '')} | "
                            f"{row['hex_rides']:,.1f}건")

                def _h_popup(row):
                    return (
                        f"<div style='font-family:Arial;font-size:12px;"
                        f"min-width:170px;'>"
                        f"<b>{row.get('district', '')}</b><br>"
                        f"<span style='color:#666;'>"
                        f"{row.get('region', '')} "
                        f"({row.get('center', '')})</span>"
                        f"<hr style='margin:4px 0;'>"
                        f"⏰ {_sel_h:02d}시 예측: "
                        f"<b>{row['hex_rides']:,.1f}건</b><br>"
                        f"<span style='font-size:11px;color:#999;'>"
                        f"District 내 비중: "
                        f"{row['hex_ratio']*100:.1f}%</span>"
                        f"</div>"
                    )

                _add_h3_hexagons(
                    hm, hour_hex,
                    value_col='hex_rides',
                    opacity=0.6,
                    tooltip_fn=_h_tt,
                    popup_fn=_h_popup,
                )

                # 범례 HTML
                legend_html = """
                <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
                    background:white;padding:10px 14px;border-radius:6px;
                    box-shadow:0 2px 6px rgba(0,0,0,.25);font:12px Arial;">
                    <b>예측 수요</b><br>
                    <div style="display:flex;align-items:center;margin-top:4px;">
                        <span style="display:inline-block;width:18px;height:12px;
                            background:#FFFFB2;border:1px solid #ccc;"></span>
                        <span style="margin:0 8px 0 4px;">낮음</span>
                        <span style="display:inline-block;width:18px;height:12px;
                            background:#FD8D3C;border:1px solid #ccc;"></span>
                        <span style="margin:0 8px 0 4px;">중간</span>
                        <span style="display:inline-block;width:18px;height:12px;
                            background:#B10026;border:1px solid #ccc;"></span>
                        <span style="margin-left:4px;">높음</span>
                    </div>
                    <small style="color:#999;">H3 해상도 9 (~174m)</small>
                </div>
                """
                hm.get_root().html.add_child(_folium.Element(legend_html))

            else:
                # ── Fallback: CircleMarker (hex 데이터 없을 때) ──
                max_rides = hour_data['predicted_rides'].max()
                for _, row in hour_data.iterrows():
                    if pd.isna(row.get('lat')) or pd.isna(row.get('lng')):
                        continue
                    rides = row['predicted_rides']
                    radius = max(3, min(
                        np.sqrt(rides / max(max_rides, 1)) * 15, 18))

                    ratio = min(rides / max(max_rides, 1), 1.0)
                    r_c = int(255)
                    g_c = int(165 * (1 - ratio))
                    color = f'#{r_c:02x}{g_c:02x}00'

                    _folium.CircleMarker(
                        location=[row['lat'], row['lng']],
                        radius=radius,
                        color=color,
                        weight=1.5,
                        fill=True,
                        fill_color=color,
                        fill_opacity=0.55,
                        tooltip=f"{row.get('district', '')} | {rides:,.1f}건"
                    ).add_to(hm)

            try:
                from streamlit_folium import st_folium as _st_folium
                _st_folium(hm, width=None, height=500, returned_objects=[])
            except ImportError:
                import streamlit.components.v1 as _components
                _components.html(hm._repr_html_(), height=500)

            # Top 10 District 테이블
            with st.expander(f"📋 {selected_hour:02d}시 Top 10 District",
                             expanded=True):
                top10 = hour_data.nlargest(10, 'predicted_rides')[
                    ['district', 'region', 'center',
                     'predicted_rides']].copy()
                top10.columns = ['District', 'Area', '센터', '예측건수']
                top10['예측건수'] = top10['예측건수'].apply(
                    lambda x: f"{x:,.1f}")
                st.dataframe(top10, use_container_width=True,
                             hide_index=True)

    st.markdown("---")

    # ── 3. 센터별 × 시간대 히트맵 ──
    st.subheader("🏢 센터 × 시간대별 예측 수요")

    # 센터별 window 집계
    center_window = pred_df.groupby(['center', 'window'])[
        'predicted_rides'].sum().reset_index()
    center_window['window_label'] = center_window['window'].map(window_labels)
    center_window['window_idx'] = center_window['window'].map(
        {w: i for i, w in enumerate(window_order)})

    # 센터 정렬 (총 예측 높은 순)
    center_totals = center_window.groupby('center')[
        'predicted_rides'].sum().sort_values(ascending=False)
    center_order = center_totals.index.tolist()

    heat = alt.Chart(center_window).mark_rect(
        cornerRadius=3
    ).encode(
        x=alt.X('window_label:N', title='시간대',
                sort=sorted_labels,
                axis=alt.Axis(labelAngle=0)),
        y=alt.Y('center:N', title='센터', sort=center_order),
        color=alt.Color('predicted_rides:Q',
                        title='예측 건수',
                        scale=alt.Scale(scheme='oranges')),
        tooltip=[
            alt.Tooltip('center:N', title='센터'),
            alt.Tooltip('window_label:N', title='시간대'),
            alt.Tooltip('predicted_rides:Q', title='예측 건수', format=',.0f'),
        ]
    ).properties(height=max(300, len(center_order) * 35))

    text = alt.Chart(center_window).mark_text(
        fontSize=11, fontWeight='bold'
    ).encode(
        x=alt.X('window_label:N', sort=sorted_labels),
        y=alt.Y('center:N', sort=center_order),
        text=alt.Text('predicted_rides:Q', format=',.0f'),
        color=alt.condition(
            alt.datum.predicted_rides > center_window['predicted_rides'].quantile(0.7),
            alt.value('white'),
            alt.value('black')
        )
    )

    st.altair_chart(heat + text, use_container_width=True)

    # ── 4. 운영 4구간 요약 ──
    st.markdown("---")
    st.subheader("🔧 운영 4구간 요약")
    st.caption("야간준비 → 오전 피크 → 오후 피크 → 저녁~야간")

    ops_data = []
    for slot_name, slot_def in OPS_SLOTS.items():
        slot_windows = slot_def['windows']
        slot_pred = pred_df[pred_df['window'].isin(slot_windows)][
            'predicted_rides'].sum()
        ops_data.append({
            'slot': slot_def['label'],
            'predicted': slot_pred,
            'pct_of_day': slot_pred / total_pred * 100 if total_pred > 0 else 0,
        })

    ops_df = pd.DataFrame(ops_data)

    # 4구간 카드
    ops_cols = st.columns(4)
    for i, (_, row) in enumerate(ops_df.iterrows()):
        with ops_cols[i]:
            st.metric(row['slot'], f"{row['predicted']:,.0f}건")
            pct = row['pct_of_day']
            st.caption(f"일 전체의 {pct:.1f}%")

    # 운영구간별 센터 Top 5
    with st.expander("🏢 운영구간별 센터 수요 Top 5", expanded=False):
        for slot_name, slot_def in OPS_SLOTS.items():
            st.markdown(f"**{slot_def['label']}**")
            slot_windows = slot_def['windows']
            slot_center = pred_df[pred_df['window'].isin(slot_windows)].groupby(
                'center')['predicted_rides'].sum().sort_values(
                ascending=False).head(5)
            for rank, (center, rides) in enumerate(slot_center.items(), 1):
                st.markdown(f"  {rank}. **{center}** — {rides:,.0f}건")
            st.markdown("")

    # ── 5. Hex 단위 일 합산 수요 (기존 Hex 탭 통합) ──
    st.markdown("---")
    with st.expander("📍 Hex 단위 일 합산 수요 지도", expanded=False):
        _render_hex_map(st, target_date)

    # ── 6. 데이터 다운로드 ──
    st.markdown("---")
    with st.expander("📥 예측 데이터 다운로드"):
        dl_col1, dl_col2 = st.columns(2)

        with dl_col1:
            st.markdown("**3시간 Window 단위**")
            dl = pred_df[['date', 'region', 'district', 'window',
                          'window_label', 'predicted_rides', 'center']].copy()
            dl.columns = ['날짜', 'Area', 'District', 'Window',
                          '시간대', '예측건수', '센터']
            csv = dl.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "📥 Window별 CSV", csv,
                file_name=f"window_prediction_{target_date}.csv",
                mime='text/csv'
            )

        with dl_col2:
            st.markdown("**1시간 단위 (시계열 분배)**")
            if len(hourly_pred_df) > 0:
                dl_h = hourly_pred_df[['date', 'region', 'district', 'hour',
                                       'predicted_rides', 'center', 'window']].copy()
                dl_h.columns = ['날짜', 'Area', 'District', '시간',
                                '예측건수', '센터', 'Window']
                csv_h = dl_h.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    "📥 시간별 CSV", csv_h,
                    file_name=f"hourly_prediction_{target_date}.csv",
                    mime='text/csv'
                )
            else:
                st.info("시간별 데이터 없음")


# ============================================================
# CLI 모드 (HTML 파일 생성)
# ============================================================

def run_cli(target_date: str, use_full_model: bool = False, use_v2: bool = False):
    """CLI에서 특정 날짜 HTML 지도 생성"""
    model_label = "Production v2" if use_v2 else ("V7 풀모델" if use_full_model else "V7 경량")
    print(f"\n{'='*60}")
    print(f"🗺️ 예측 vs 실제 라이딩 지도 생성: {target_date} [{model_label}]")
    print(f"{'='*60}")

    client = get_bigquery_client()

    print("\n[1/5] 실제 라이딩 데이터 조회...")
    actual_district = get_actual_rides_district(client, target_date)
    actual_region = get_actual_rides_region(client, target_date)
    print(f"  district: {len(actual_district)}건, region: {len(actual_region)}건")
    print(f"  총 라이딩: {actual_region['ride_count'].sum():,}건")

    print("\n[2/5] 권역 중심점 조회...")
    region_centers = get_region_centers(client)
    print(f"  {len(region_centers)}개 권역")

    if use_v2:
        # ── Production v2: district 직접 예측 ──
        print("\n[3/5] Production v2 예측...")
        from production_v2_predictor import predict_district_rides as v2_predict
        pred_district, pred_df = v2_predict(target_date, verbose=True)
        total_pred = pred_df['adj_pred'].sum() if len(pred_df) > 0 else 0
        print(f"  예측 합계: {total_pred:,.0f}건 ({len(pred_district)} districts)")
    else:
        # ── V7: region 예측 → district 배분 ──
        print("\n[3/5] District 비율 조회...")
        district_ratios = get_district_ride_ratios(client)
        print(f"  {len(district_ratios)}개 district")

        print(f"\n[4/5] 예측 {'(풀 모델)' if use_full_model else '(경량)'}...")
        if use_full_model:
            pred_df = full_model_predict(client, target_date)
        else:
            pred_df = quick_predict(target_date)
        total_pred = pred_df['adj_pred'].sum() if len(pred_df) > 0 else 0
        print(f"  예측 합계: {total_pred:,.0f}건")

        pred_district = distribute_to_districts(pred_df, district_ratios)
        print(f"  → {len(pred_district)}개 district로 배분 완료")

    print("\n[5/5] 지도 생성...")
    m = create_prediction_map(
        actual_district, actual_region, pred_df,
        region_centers, target_date,
        pred_district_df=pred_district
    )

    output_path = os.path.join(OUTPUT_DIR, f'prediction_map_{target_date}.html')
    m.save(output_path)
    print(f"\n✅ 저장 완료: {output_path}")

    # 권역별 오차 요약
    if len(pred_df) > 0 and len(actual_region) > 0:
        merged = pred_df.merge(
            actual_region[['region', 'ride_count']].rename(columns={'ride_count': 'actual'}),
            on='region', how='left'
        )
        merged['actual'] = merged['actual'].fillna(0)

        total_actual = merged['actual'].sum()
        total_pred = merged['adj_pred'].sum()
        error = ((total_pred - total_actual) / total_actual * 100) if total_actual > 0 else 0

        print(f"\n{'='*60}")
        print(f"📊 요약: 실제 {total_actual:,.0f} / 예측 {total_pred:,.0f} / 오차 {error:+.1f}%")
        print(f"{'='*60}")

        # Top 과소/과대 권역
        merged['error_pct'] = np.where(
            merged['actual'] > 0,
            (merged['adj_pred'] - merged['actual']) / merged['actual'] * 100,
            0
        )
        under = merged[merged['error_pct'] < -15].sort_values('error_pct')
        over = merged[merged['error_pct'] > 15].sort_values('error_pct', ascending=False)

        if len(under) > 0:
            print(f"\n⚠️ 과소예측 권역 (>15%):")
            for _, r in under.head(5).iterrows():
                print(f"  {r['region']}: 예측 {r['adj_pred']:,.0f} / 실제 {r['actual']:,.0f} ({r['error_pct']:+.1f}%)")

        if len(over) > 0:
            print(f"\n⚠️ 과대예측 권역 (>15%):")
            for _, r in over.head(5).iterrows():
                print(f"  {r['region']}: 예측 {r['adj_pred']:,.0f} / 실제 {r['actual']:,.0f} ({r['error_pct']:+.1f}%)")


# ============================================================
# 메인
# ============================================================

if __name__ == '__main__':
    # Streamlit으로 실행되었는지 확인
    if 'streamlit' in sys.modules or os.environ.get('STREAMLIT_RUNTIME'):
        run_streamlit()
    else:
        # CLI 모드
        parser = argparse.ArgumentParser(description='예측 vs 실제 라이딩 지도')
        parser.add_argument('--date', type=str, default='2026-02-23',
                          help='대상 날짜 (YYYY-MM-DD)')
        parser.add_argument('--full-model', action='store_true',
                          help='V7 풀 ML 모델 사용')
        parser.add_argument('--model', type=str, default='v2',
                          choices=['v7', 'v2'],
                          help='예측 모델 (v2: district 직접 [기본], v7: region→district 배분 [보관])')
        args = parser.parse_args()

        run_cli(args.date, args.full_model, use_v2=(args.model == 'v2'))
