"""
District × Hour 수요 예측 엔진

region 일별 예측 (V7) × district 비율 × hour 비율 = district×hour 예측

Top-down 배분 방식:
1. quick_predict()로 region 일별 예측 (region_params.json 기반)
2. 최근 30일 라이딩 비율로 district 배분
3. app_open 시간대 패턴으로 hour 배분
→ 결과: district × hour × predicted_rides

검증: SUM(district×hour per region) == V7 region prediction (수학적 보장)

사용법:
    # 모듈 사용
    from district_hour_model import DistrictHourPredictor
    predictor = DistrictHourPredictor()
    result = predictor.predict('2026-02-25')

    # CLI
    python district_hour_model.py --date 2026-02-25
    python district_hour_model.py --date 2026-02-25 --validate
    python district_hour_model.py --date 2026-02-25 --time-slot morning
"""
import os
import sys
import json
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Authentication: set GOOGLE_APPLICATION_CREDENTIALS env var
SCRIPT_DIR_CREDS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'credentials', 'service-account.json')
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    if os.path.exists(SCRIPT_DIR_CREDS):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SCRIPT_DIR_CREDS

# ============================================================
# P5: 시간대별 기온 보정
# ============================================================

def estimate_hourly_temp(hour: int, temp_low: float, temp_high: float) -> float:
    """
    일 최저/최고 기온에서 시간별 기온 추정 (사인 곡선 근사)

    기상학적 가정:
    - 최저기온: 오전 6시 경 (일출 전)
    - 최고기온: 오후 14시 경 (일사 최대)
    - 나머지 시간: 사인 곡선 보간

    Args:
        hour: 0-23
        temp_low: 일 최저기온 (°C)
        temp_high: 일 최고기온 (°C)
    Returns:
        해당 시간 추정기온 (°C)
    """
    import math
    HOUR_MIN_TEMP = 6    # 최저기온 시각
    HOUR_MAX_TEMP = 14   # 최고기온 시각

    # 0~23 → 연속 phase (-1 ~ +1)
    # 14시에 cos=1 (최고), 6시에 cos=-1 (최저)가 되도록
    # period = 24시간, peak at 14시
    phase = 2 * math.pi * (hour - HOUR_MAX_TEMP) / 24.0
    t = (temp_high + temp_low) / 2 + (temp_high - temp_low) / 2 * math.cos(phase)
    return round(t, 1)


def get_hourly_temp_factors(
    temp_low: float, temp_high: float,
    is_weekday: bool = True,
    cold_threshold: float = -8, freeze_threshold: float = 0
) -> Dict[int, float]:
    """
    시간대별 기온 기반 수요 감쇠 팩터 (P5)

    핵심 개념: 수요의 기온 탄력성은 시간대마다 다름
    - 출퇴근 시간(7~9, 17~19): 낮은 탄력성 (기온 낮아도 이용)
    - 레저/여가(주말 낮, 평일 오후): 높은 탄력성 (추우면 안 탐)
    - 새벽/심야: 기본 수요가 적어 보정 의미 낮음

    elasticity (탄력성):
    - 0.0 = 기온 영향 없음 (출퇴근 등 필수 이동)
    - 1.0 = 기온 영향 100% (순수 레저)

    일 단위 기온 보정은 quick_predict에서 이미 적용됨.
    여기서는 시간대 간 **상대적 차이**만 보정 (총합 보존).

    Returns:
        {hour: factor} (24시간, 평균 ≈ 1.0)
    """
    # 시간대별 기온 탄력성 (평일/주말 구분)
    if is_weekday:
        # 평일: 출퇴근 시간 비탄력적
        ELASTICITY = {
            0: 0.1, 1: 0.1, 2: 0.1, 3: 0.1, 4: 0.1, 5: 0.1,  # 새벽: 거의 무영향
            6: 0.3, 7: 0.2, 8: 0.2, 9: 0.3,   # 출근: 비탄력적
            10: 0.5, 11: 0.6, 12: 0.7,          # 오전 후반: 중간
            13: 0.7, 14: 0.8, 15: 0.8, 16: 0.7, # 오후: 탄력적
            17: 0.3, 18: 0.2, 19: 0.3,          # 퇴근: 비탄력적
            20: 0.5, 21: 0.5,                    # 저녁: 중간
            22: 0.3, 23: 0.2,                    # 밤: 낮은 탄력성
        }
    else:
        # 주말: 대부분 레저 → 기온 탄력성 높음
        ELASTICITY = {
            0: 0.1, 1: 0.1, 2: 0.1, 3: 0.1, 4: 0.1, 5: 0.1,
            6: 0.3, 7: 0.5, 8: 0.6, 9: 0.7,
            10: 0.8, 11: 0.8, 12: 0.8,
            13: 0.8, 14: 0.9, 15: 0.9, 16: 0.8,
            17: 0.7, 18: 0.6, 19: 0.5,
            20: 0.4, 21: 0.3,
            22: 0.2, 23: 0.1,
        }

    raw_factors = {}
    for h in range(24):
        temp = estimate_hourly_temp(h, temp_low, temp_high)
        elasticity = ELASTICITY.get(h, 0.5)

        # 기온에 따른 원시 감쇠율
        if temp >= 10:
            raw_adj = 0.0  # 따뜻: 감쇠 없음
        elif temp >= freeze_threshold:
            # 0~10도: 약한 감쇠 (최대 -5%)
            raw_adj = -0.05 * (10 - temp) / (10 - freeze_threshold)
        elif temp >= cold_threshold:
            # -8~0도: 중간 감쇠 (최대 -15%)
            raw_adj = -0.05 - 0.10 * (freeze_threshold - temp) / (freeze_threshold - cold_threshold)
        else:
            # -8도 미만: 강한 감쇠 (최대 -20%)
            raw_adj = -0.15 - 0.05 * min(1, (cold_threshold - temp) / 10)

        # 탄력성 적용: 비탄력 시간대는 감쇠를 약화
        adj = raw_adj * elasticity
        raw_factors[h] = 1.0 + adj

    # 총합 보존: 팩터 정규화 (평균 = 1.0)
    # 일 전체 수요량은 변하지 않고, 시간대 간 비중만 조정
    avg_factor = sum(raw_factors.values()) / 24
    if avg_factor > 0:
        factors = {h: round(f / avg_factor, 4) for h, f in raw_factors.items()}
    else:
        factors = {h: 1.0 for h in range(24)}

    return factors


# 시간대 슬롯 정의 (route_optimization_v5 TIME_SLOTS 확장)
TIME_SLOTS = {
    'night_prep': {'hours': list(range(22, 24)) + list(range(0, 7)), 'desc': '야간준비 (22~06시, 다음날 오전 대비)'},
    'morning':    {'hours': list(range(7, 13)),  'desc': '오전 피크 (07~12시)'},
    'afternoon':  {'hours': list(range(13, 19)), 'desc': '오후 수요 (13~18시)'},
    'evening':    {'hours': list(range(19, 22)), 'desc': '저녁 + 야간준비 (19~21시)'},
}

# district hourly profile 최소 데이터 기준
MIN_APP_OPENS_FOR_DISTRICT_PROFILE = 50

# 보정 파라미터 경로
DH_PARAMS_PATH = os.path.join(SCRIPT_DIR, 'district_hour_params.json')


class DistrictHourPredictor:
    """
    District × Hour 수요 예측기

    Top-down 배분:
        region_daily_pred × district_ratio × hour_ratio

    자동 학습 연동:
        district_hour_params.json이 있으면 보정된 비율/계수를 자동 적용
    """

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self._client = None
        self._district_ratios = None
        self._hourly_profiles = None
        self._region_predictions = None
        self._tuned_params = self._load_tuned_params()

    def _load_tuned_params(self) -> Optional[Dict]:
        """district_hour_params.json (tuner 보정값) 로드"""
        if os.path.exists(DH_PARAMS_PATH):
            try:
                with open(DH_PARAMS_PATH, 'r') as f:
                    params = json.load(f)
                if self.verbose and params.get('last_updated'):
                    print(f"  📁 보정 파라미터 로드: {params['last_updated']}")
                return params
            except Exception:
                pass
        return None

    @property
    def client(self):
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client()
        return self._client

    # ================================================================
    # Step 1: Region 일별 예측 (quick_predict 재사용)
    # ================================================================

    def predict_region_daily(self, target_date: str) -> pd.DataFrame:
        """
        region 일별 예측 (region_params.json 기반 경량 예측)

        visualize_prediction_map.py:quick_predict() 와 동일한 로직.
        ML 모델 없이 즉시 예측 (<1초).

        Returns:
            DataFrame[region, center, adj_pred, avg_rides, factor, bias, desc]
        """
        # visualize_prediction_map의 quick_predict 재사용
        sys.path.insert(0, SCRIPT_DIR)
        from visualize_prediction_map import quick_predict

        pred_df = quick_predict(target_date)
        self._region_predictions = pred_df

        if self.verbose:
            total = pred_df['adj_pred'].sum()
            print(f"  [Step 1] Region 일별 예측: {len(pred_df)}개 region, "
                  f"총 {total:,.0f}건 예측")

        return pred_df

    # ================================================================
    # Step 2: District 비율 배분 (30일 라이딩 비율)
    # ================================================================

    def get_district_ratios(self, target_date: str = None) -> pd.DataFrame:
        """
        최근 30일 region 내 district별 라이딩 비율

        각 region 안에서 district가 차지하는 비율.
        이 비율 합계 = 1.0 (region 내).

        Returns:
            DataFrame[region, district, ride_count, lat, lng, region_total, ratio]
        """
        if self._district_ratios is not None:
            return self._district_ratios

        # target_date 기준 30일
        if target_date:
            end_date = (pd.Timestamp(target_date) - timedelta(days=1)).strftime('%Y-%m-%d')
            start_date = (pd.Timestamp(target_date) - timedelta(days=30)).strftime('%Y-%m-%d')
            date_filter = f"DATE(start_time) BETWEEN '{start_date}' AND '{end_date}'"
        else:
            date_filter = ("DATE(start_time) BETWEEN "
                          "DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) "
                          "AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)")

        query = f"""
        SELECT
            h3_start_area_name as region,
            h3_start_district_name as district,
            COUNT(*) as ride_count,
            AVG(ST_Y(start_location)) as lat,
            AVG(ST_X(start_location)) as lng
        FROM `bikeshare.service.rides`
        WHERE {date_filter}
            AND h3_start_area_name IS NOT NULL
            AND h3_start_district_name IS NOT NULL
            AND start_location IS NOT NULL
        GROUP BY 1, 2
        """
        df = self.client.query(query).to_dataframe()

        # region 내 비율 계산
        region_totals = df.groupby('region')['ride_count'].sum().reset_index()
        region_totals.columns = ['region', 'region_total']
        df = df.merge(region_totals, on='region')
        df['ratio'] = df['ride_count'] / df['region_total']

        self._district_ratios = df

        if self.verbose:
            n_regions = df['region'].nunique()
            n_districts = len(df)
            print(f"  [Step 2] District 비율: {n_regions}개 region → "
                  f"{n_districts}개 district")

        return df

    # ================================================================
    # Step 3: 시간대별 프로필 (app_open 패턴, 신규 쿼리)
    # ================================================================

    def get_hourly_profile(self, target_date: str) -> pd.DataFrame:
        """
        시간대별 비율 (app_open 패턴 기반)

        요일 타입별 (weekday/saturday/sunday+holiday) 시간대 분포 조회.
        district별 app_open 50건 미만이면 region 프로필로 fallback.

        Returns:
            DataFrame[region, district, hour, total_app_opens, hour_ratio]
            hour_ratio 합계 = 1.0 (district 내 24시간 합)
        """
        if self._hourly_profiles is not None:
            return self._hourly_profiles

        target = pd.Timestamp(target_date)
        dow = target.dayofweek  # 0=월 ~ 6=일

        # 공휴일 확인
        try:
            from korean_holidays import ADDITIONAL_HOLIDAYS
            is_holiday = target.date() in ADDITIONAL_HOLIDAYS
        except ImportError:
            is_holiday = False

        # 요일 타입 결정 → BigQuery DAYOFWEEK 필터
        # BigQuery DAYOFWEEK: 1=일, 2=월, 3=화, 4=수, 5=목, 6=금, 7=토
        if is_holiday or dow == 6:  # 일요일+공휴일
            day_type = 'sunday_holiday'
            day_filter = "EXTRACT(DAYOFWEEK FROM event_time) = 1"  # 일요일만
        elif dow == 5:  # 토요일
            day_type = 'saturday'
            day_filter = "EXTRACT(DAYOFWEEK FROM event_time) = 7"  # 토요일만
        else:  # 평일
            day_type = 'weekday'
            day_filter = "EXTRACT(DAYOFWEEK FROM event_time) IN (2,3,4,5,6)"

        # target_date 기준 28일 조회
        end_date = (target - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (target - timedelta(days=28)).strftime('%Y-%m-%d')

        query = f"""
        WITH district_hourly AS (
            SELECT
                h3_area_name as region,
                h3_district_name as district,
                EXTRACT(HOUR FROM event_time) as hour,
                COUNT(*) as app_opens
            FROM `bikeshare.service.app_accessibility`
            WHERE DATE(event_time) BETWEEN '{start_date}' AND '{end_date}'
                AND {day_filter}
                AND h3_area_name IS NOT NULL
                AND h3_district_name IS NOT NULL
            GROUP BY 1, 2, 3
        ),
        district_totals AS (
            SELECT
                region,
                district,
                SUM(app_opens) as total_opens
            FROM district_hourly
            GROUP BY 1, 2
        ),
        region_hourly AS (
            -- region 레벨 시간대 프로필 (fallback용)
            SELECT
                region,
                hour,
                SUM(app_opens) as app_opens
            FROM district_hourly
            GROUP BY 1, 2
        ),
        region_totals AS (
            SELECT
                region,
                SUM(app_opens) as total_opens
            FROM region_hourly
            GROUP BY 1
        )
        SELECT
            dh.region,
            dh.district,
            dh.hour,
            dh.app_opens as total_app_opens,
            -- district 데이터 충분하면 district 프로필, 아니면 region 프로필
            CASE
                WHEN dt.total_opens >= {MIN_APP_OPENS_FOR_DISTRICT_PROFILE}
                THEN SAFE_DIVIDE(dh.app_opens, dt.total_opens)
                ELSE SAFE_DIVIDE(rh.app_opens, rt.total_opens)
            END as hour_ratio,
            dt.total_opens as district_total_opens,
            CASE
                WHEN dt.total_opens >= {MIN_APP_OPENS_FOR_DISTRICT_PROFILE}
                THEN 'district'
                ELSE 'region_fallback'
            END as profile_source
        FROM district_hourly dh
        JOIN district_totals dt ON dh.region = dt.region AND dh.district = dt.district
        LEFT JOIN region_hourly rh ON dh.region = rh.region AND dh.hour = rh.hour
        LEFT JOIN region_totals rt ON dh.region = rt.region
        ORDER BY dh.region, dh.district, dh.hour
        """

        df = self.client.query(query).to_dataframe()
        self._hourly_profiles = df

        if self.verbose:
            n_districts = df[['region', 'district']].drop_duplicates().shape[0]
            n_fallback = df[df['profile_source'] == 'region_fallback'][
                ['region', 'district']].drop_duplicates().shape[0]
            print(f"  [Step 3] 시간대 프로필 ({day_type}): "
                  f"{n_districts}개 district, "
                  f"fallback {n_fallback}개")

        return df

    # ================================================================
    # Step 4: 전체 예측 오케스트레이션
    # ================================================================

    def predict(self, target_date: str) -> pd.DataFrame:
        """
        District × Hour 전체 예측

        region_daily × district_ratio × hour_ratio = district×hour 예측

        잠재수요 레이어:
            predicted_rides에 전환율 모델을 적용하여
            unconstrained_demand(잠재수요)를 역산한다.

        Returns:
            DataFrame[region, district, hour, predicted_rides, lat, lng, center,
                      day_type, avg_bike_count, conversion_rate,
                      unconstrained_demand, suppressed_demand]
        """
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"📊 District×Hour 수요 예측: {target_date}")
            print(f"{'='*60}")

        # 1. Region 일별 예측
        region_pred = self.predict_region_daily(target_date)

        # 2. District 비율
        district_ratios = self.get_district_ratios(target_date)

        # 3. 시간대 프로필
        hourly_profile = self.get_hourly_profile(target_date)

        # 4. 보정 파라미터 준비
        tuned_district_ratios = {}
        tuned_hourly_adj = {}
        if self._tuned_params:
            tuned_district_ratios = self._tuned_params.get('district_ratios', {})
            # 요일 타입 결정
            _target = pd.Timestamp(target_date)
            _dow = _target.dayofweek
            try:
                from korean_holidays import ADDITIONAL_HOLIDAYS
                _is_hol = _target.date() in ADDITIONAL_HOLIDAYS
            except ImportError:
                _is_hol = False
            if _is_hol or _dow == 6:
                _day_type = 'sunday_holiday'
            elif _dow == 5:
                _day_type = 'saturday'
            else:
                _day_type = 'weekday'
            tuned_hourly_adj = self._tuned_params.get(
                'hourly_profiles', {}).get(f'global_{_day_type}', {})

        n_tuned_districts = sum(len(v) for v in tuned_district_ratios.values())
        n_tuned_hours = len(tuned_hourly_adj)
        if self.verbose and (n_tuned_districts > 0 or n_tuned_hours > 0):
            print(f"  [Step 4] 보정 적용: {n_tuned_districts}개 district 비율, "
                  f"{n_tuned_hours}개 시간대")

        # 5. P5: 시간대별 기온 보정 팩터 준비
        # quick_predict에서 이미 적용된 날씨는 일 단위 보정
        # 여기서는 시간대 간 상대적 차이만 (총합 보존)
        try:
            from visualize_prediction_map import _get_weather_for_date
            from demand_model_v7 import load_weather_data
            weather_csv = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')
            weather_data = load_weather_data(weather_csv) if os.path.exists(weather_csv) else {}
            weather, weather_src = _get_weather_for_date(target_date, weather_data)
            temp_low = weather.get('temp_low', 5)
            temp_high = weather.get('temp_high', 10)
        except Exception:
            temp_low, temp_high = 5, 10
            weather_src = '기본값'

        # 평일/주말 구분
        _target_ts = pd.Timestamp(target_date)
        _is_weekday = _target_ts.dayofweek < 5
        try:
            from korean_holidays import ADDITIONAL_HOLIDAYS
            if _target_ts.date() in ADDITIONAL_HOLIDAYS:
                _is_weekday = False
        except ImportError:
            pass

        hourly_temp_factors = get_hourly_temp_factors(
            temp_low, temp_high, is_weekday=_is_weekday)

        # 기온 보정이 유의미한지 체크 (min/max 차이 2% 이상이면 적용)
        temp_factor_range = max(hourly_temp_factors.values()) - min(hourly_temp_factors.values())
        apply_temp_correction = temp_factor_range > 0.02

        if self.verbose:
            if apply_temp_correction:
                print(f"  [Step 5] 시간대별 기온 보정: "
                      f"최저 {temp_low}°C / 최고 {temp_high}°C "
                      f"({weather_src}), 팩터 범위 {temp_factor_range:.3f}")
            else:
                print(f"  [Step 5] 시간대별 기온 보정: 미적용 (기온 차이 미미)")

        # 6. 조합: region × district_ratio × hour_ratio × temp_factor
        if self.verbose:
            print(f"  [Step 6] 예측 조합 중...")

        results = []
        missing_districts = 0
        missing_hours = 0

        for _, pred_row in region_pred.iterrows():
            region = pred_row['region']
            region_daily = pred_row['adj_pred']
            center = pred_row.get('center', '')

            # 해당 region의 district들
            dists = district_ratios[district_ratios['region'] == region]
            if len(dists) == 0:
                missing_districts += 1
                continue

            # 보정된 district 비율이 있으면 적용
            region_tuned = tuned_district_ratios.get(region, {})

            for _, d_row in dists.iterrows():
                district = d_row['district']
                d_ratio = d_row['ratio']
                d_lat = d_row['lat']
                d_lng = d_row['lng']

                # 보정된 비율 블렌딩 (base × 0.7 + tuned × 0.3)
                if district in region_tuned:
                    tuned_r = region_tuned[district]
                    d_ratio = d_ratio * 0.7 + tuned_r * 0.3

                # district × day 예측
                district_daily = region_daily * d_ratio

                # 해당 district의 시간대 프로필
                hours = hourly_profile[
                    (hourly_profile['region'] == region) &
                    (hourly_profile['district'] == district)
                ]

                if len(hours) == 0:
                    # 시간대 프로필 없으면 region 프로필 사용
                    hours = hourly_profile[hourly_profile['region'] == region]
                    hours = hours.groupby('hour').agg({
                        'total_app_opens': 'sum',
                        'hour_ratio': 'mean'  # region 레벨 평균
                    }).reset_index()
                    hours['region'] = region
                    hours['district'] = district

                if len(hours) == 0:
                    missing_hours += 1
                    # 전역 시간대 프로필 fallback (과거 패턴 기반)
                    # 일반적 자전거 이용 시간대별 비율 (24시간 합 = 1.0)
                    _GLOBAL_HOUR_RATIOS = {
                        0: 0.005, 1: 0.003, 2: 0.002, 3: 0.002, 4: 0.003, 5: 0.008,
                        6: 0.025, 7: 0.055, 8: 0.065,
                        9: 0.050, 10: 0.050, 11: 0.055,
                        12: 0.065, 13: 0.060, 14: 0.055,
                        15: 0.060, 16: 0.070, 17: 0.075,
                        18: 0.075, 19: 0.065, 20: 0.050,
                        21: 0.040, 22: 0.035, 23: 0.027,
                    }
                    _ratio_sum = sum(_GLOBAL_HOUR_RATIOS.values())
                    for h in range(24):
                        ratio = _GLOBAL_HOUR_RATIOS[h] / _ratio_sum
                        pred_h = district_daily * ratio
                        if apply_temp_correction:
                            pred_h *= hourly_temp_factors.get(h, 1.0)
                        results.append({
                            'region': region,
                            'district': district,
                            'hour': h,
                            'predicted_rides': round(pred_h, 2),
                            'lat': d_lat,
                            'lng': d_lng,
                            'center': center,
                        })
                    continue

                # 시간대별 비율 정규화 (합계 = 1.0 보장)
                hours = hours.copy()

                # 시간대 보정값 적용 (tuner Level 1)
                if tuned_hourly_adj:
                    hours['hour_ratio'] = hours.apply(
                        lambda r: r['hour_ratio'] * 0.7 + tuned_hourly_adj.get(
                            str(int(r['hour'])), r['hour_ratio']) * 0.3
                        if str(int(r['hour'])) in tuned_hourly_adj
                        else r['hour_ratio'],
                        axis=1
                    )

                ratio_sum = hours['hour_ratio'].sum()
                if ratio_sum > 0:
                    hours['hour_ratio_norm'] = hours['hour_ratio'] / ratio_sum
                else:
                    # 전역 시간대 비율 fallback
                    _GLOBAL_HOUR_RATIOS = {
                        0: 0.005, 1: 0.003, 2: 0.002, 3: 0.002, 4: 0.003, 5: 0.008,
                        6: 0.025, 7: 0.055, 8: 0.065,
                        9: 0.050, 10: 0.050, 11: 0.055,
                        12: 0.065, 13: 0.060, 14: 0.055,
                        15: 0.060, 16: 0.070, 17: 0.075,
                        18: 0.075, 19: 0.065, 20: 0.050,
                        21: 0.040, 22: 0.035, 23: 0.027,
                    }
                    _gsum = sum(_GLOBAL_HOUR_RATIOS.get(int(h), 0.02) for h in hours['hour'])
                    if _gsum > 0:
                        hours['hour_ratio_norm'] = hours['hour'].apply(
                            lambda h: _GLOBAL_HOUR_RATIOS.get(int(h), 0.02) / _gsum
                        )
                    else:
                        # 전역 비율도 합이 0인 극단적 경우 → 전체 비율 재계산
                        _total = sum(_GLOBAL_HOUR_RATIOS.values())
                        hours['hour_ratio_norm'] = hours['hour'].apply(
                            lambda h: _GLOBAL_HOUR_RATIOS.get(int(h), 0.02) / _total
                        )

                for _, h_row in hours.iterrows():
                    h = int(h_row['hour'])
                    h_ratio = h_row['hour_ratio_norm']

                    predicted = district_daily * h_ratio

                    # P5: 시간대별 기온 보정
                    if apply_temp_correction:
                        predicted *= hourly_temp_factors.get(h, 1.0)

                    results.append({
                        'region': region,
                        'district': district,
                        'hour': h,
                        'predicted_rides': round(predicted, 2),
                        'lat': d_lat,
                        'lng': d_lng,
                        'center': center,
                    })

        result_df = pd.DataFrame(results)

        if len(result_df) > 0:
            # 요일 타입 추가
            target = pd.Timestamp(target_date)
            dow = target.dayofweek
            try:
                from korean_holidays import ADDITIONAL_HOLIDAYS
                is_holiday = target.date() in ADDITIONAL_HOLIDAYS
            except ImportError:
                is_holiday = False

            if is_holiday or dow == 6:
                result_df['day_type'] = 'sunday_holiday'
            elif dow == 5:
                result_df['day_type'] = 'saturday'
            else:
                result_df['day_type'] = 'weekday'

        # ================================================================
        # 잠재수요 레이어: 전환율 모델로 unconstrained demand 역산
        # ================================================================
        if len(result_df) > 0:
            result_df = self._add_unconstrained_demand(result_df)

        if self.verbose:
            if len(result_df) > 0:
                total_pred = result_df['predicted_rides'].sum()
                n_districts = result_df[['region', 'district']].drop_duplicates().shape[0]
                n_hours = result_df['hour'].nunique()
                print(f"\n  ✅ 결과: {n_districts}개 district × {n_hours}개 hour = "
                      f"{len(result_df):,}행")
                print(f"     총 예측 라이딩: {total_pred:,.0f}건")

                # 잠재수요 요약
                if 'unconstrained_demand' in result_df.columns:
                    total_unc = result_df['unconstrained_demand'].sum()
                    total_supp = result_df['suppressed_demand'].sum()
                    uplift_pct = (total_unc / total_pred - 1) * 100 if total_pred > 0 else 0
                    print(f"     잠재수요: {total_unc:,.0f}건 "
                          f"(실현 대비 +{uplift_pct:.1f}%, 억제 {total_supp:,.0f}건)")

                if missing_districts > 0:
                    print(f"     ⚠️ district 매핑 누락: {missing_districts}개 region")
                if missing_hours > 0:
                    print(f"     ⚠️ 시간대 프로필 누락 (전역 비율 적용): {missing_hours}개 district")
            else:
                print("  ❌ 결과 없음")

        return result_df

    # ================================================================
    # 잠재수요 역산 (전환율 모델 연동)
    # ================================================================

    def _add_unconstrained_demand(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        predicted_rides에 전환율 모델을 적용하여 잠재수요를 추가

        추가 컬럼:
            avg_bike_count: 해당 구역×시간 평균 가용기기수 (90일)
            conversion_rate: 현재 공급 수준의 전환율
            unconstrained_demand: 잠재수요
            suppressed_demand: 억제수요 (= unconstrained - predicted)
        """
        try:
            from conversion_model import ConversionModel
            conv_model = ConversionModel(verbose=False)
        except ImportError:
            if self.verbose:
                print("  ⚠️ conversion_model 로드 실패 → 잠재수요 미계산")
            result_df['avg_bike_count'] = 0.0
            result_df['conversion_rate'] = 0.0
            result_df['unconstrained_demand'] = result_df['predicted_rides']
            result_df['suppressed_demand'] = 0.0
            return result_df

        # 구역×시간별 평균 가용기기수 조회
        try:
            avg_bikes = conv_model.get_avg_bike_counts(lookback_days=90)
        except Exception as e:
            if self.verbose:
                print(f"  ⚠️ 평균 가용기기수 조회 실패: {e} → 전체 평균 사용")
            avg_bikes = pd.DataFrame()

        # 매칭
        if len(avg_bikes) > 0:
            result_df = result_df.merge(
                avg_bikes[['region', 'district', 'hour', 'avg_bike_count_100']],
                on=['region', 'district', 'hour'],
                how='left'
            )
            result_df['avg_bike_count'] = result_df['avg_bike_count_100'].fillna(
                avg_bikes['avg_bike_count_100'].mean())
            result_df.drop(columns=['avg_bike_count_100'], inplace=True, errors='ignore')
        else:
            result_df['avg_bike_count'] = 1.5  # 전체 평균 fallback

        # 전환율 계산
        result_df['conversion_rate'] = conv_model.predict_conversion_rate(
            result_df['avg_bike_count'].values, 'global').round(4)

        # 잠재수요 역산
        result_df['unconstrained_demand'] = conv_model.estimate_unconstrained_batch(
            result_df['predicted_rides'],
            result_df['avg_bike_count'],
            result_df['hour']
        )

        # 억제수요
        result_df['suppressed_demand'] = (
            result_df['unconstrained_demand'] - result_df['predicted_rides']
        ).clip(lower=0).round(2)

        return result_df

    # ================================================================
    # 시간대 슬롯 필터링
    # ================================================================

    def predict_time_slot(
        self, target_date: str, slot_name: str
    ) -> pd.DataFrame:
        """
        특정 시간대 슬롯의 예측을 집계

        Args:
            target_date: 대상 날짜
            slot_name: TIME_SLOTS 키 ('morning', 'afternoon', 'evening', 'night_prep')

        Returns:
            DataFrame[region, district, predicted_rides, lat, lng, center, time_slot]
            시간대 내 예측 합계
        """
        if slot_name not in TIME_SLOTS:
            raise ValueError(f"Unknown time slot: {slot_name}. "
                           f"Available: {list(TIME_SLOTS.keys())}")

        slot = TIME_SLOTS[slot_name]
        hours = slot['hours']

        # 전체 예측 생성 (캐시 활용)
        full_pred = self.predict(target_date)
        if len(full_pred) == 0:
            return pd.DataFrame()

        # 해당 시간대만 필터
        slot_df = full_pred[full_pred['hour'].isin(hours)].copy()

        # district별 합계
        agg = slot_df.groupby(['region', 'district']).agg({
            'predicted_rides': 'sum',
            'lat': 'first',
            'lng': 'first',
            'center': 'first',
            'day_type': 'first',
        }).reset_index()

        agg['time_slot'] = slot_name
        agg['time_slot_desc'] = slot['desc']
        agg['predicted_rides'] = agg['predicted_rides'].round(1)

        return agg.sort_values('predicted_rides', ascending=False)

    # ================================================================
    # 검증
    # ================================================================

    def validate(self, target_date: str) -> Dict:
        """
        예측 일관성 검증

        1. SUM(district×hour per region) == V7 region prediction (배분 일관성)
        2. 과거 14일 실제 시간대별 라이딩과 비교 (정확도)

        Returns:
            {'consistency': {...}, 'accuracy': {...}}
        """
        print(f"\n{'='*60}")
        print(f"🔍 District×Hour 예측 검증: {target_date}")
        print(f"{'='*60}")

        result = {}

        # === 1. 배분 일관성 ===
        print("\n[1/2] 배분 일관성 검증 (region 합계 == V7 예측)")
        full_pred = self.predict(target_date)
        region_pred = self._region_predictions

        if len(full_pred) == 0 or region_pred is None:
            print("  ❌ 예측 데이터 없음")
            return {'error': 'no predictions'}

        # district×hour 합계 → region별
        dh_region_sum = full_pred.groupby('region')['predicted_rides'].sum().reset_index()
        dh_region_sum.columns = ['region', 'dh_total']

        # V7 region 예측
        v7_pred = region_pred[['region', 'adj_pred']].copy()

        # 비교
        compare = v7_pred.merge(dh_region_sum, on='region', how='left')
        compare['dh_total'] = compare['dh_total'].fillna(0)
        compare['diff'] = compare['dh_total'] - compare['adj_pred']
        compare['diff_pct'] = np.where(
            compare['adj_pred'] > 0,
            (compare['diff'] / compare['adj_pred'] * 100).round(2),
            0
        )

        max_diff = compare['diff_pct'].abs().max()
        mean_diff = compare['diff_pct'].abs().mean()

        print(f"  V7 총 예측: {compare['adj_pred'].sum():,.0f}건")
        print(f"  D×H 총 합계: {compare['dh_total'].sum():,.0f}건")
        print(f"  최대 차이: {max_diff:.2f}%")
        print(f"  평균 차이: {mean_diff:.2f}%")

        if max_diff < 1.0:
            print("  ✅ 배분 일관성 OK (최대 차이 < 1%)")
        else:
            print("  ⚠️ 배분 불일치 감지!")
            issues = compare[compare['diff_pct'].abs() >= 1.0]
            for _, r in issues.iterrows():
                print(f"     {r['region']}: V7={r['adj_pred']:.0f}, "
                      f"D×H={r['dh_total']:.0f} ({r['diff_pct']:+.2f}%)")

        result['consistency'] = {
            'max_diff_pct': round(max_diff, 2),
            'mean_diff_pct': round(mean_diff, 2),
            'is_consistent': max_diff < 1.0,
            'details': compare.to_dict('records')
        }

        # === 2. 과거 실제 데이터와 비교 ===
        print("\n[2/2] 과거 실제 라이딩과 비교")

        target = pd.Timestamp(target_date)
        actual_query = f"""
        SELECT
            h3_start_area_name as region,
            h3_start_district_name as district,
            EXTRACT(HOUR FROM start_time) as hour,
            COUNT(*) as actual_rides
        FROM `bikeshare.service.rides`
        WHERE DATE(start_time) = '{target_date}'
            AND h3_start_area_name IS NOT NULL
            AND h3_start_district_name IS NOT NULL
        GROUP BY 1, 2, 3
        """

        try:
            actual_df = self.client.query(actual_query).to_dataframe()
        except Exception as e:
            print(f"  ⚠️ 실제 데이터 조회 실패: {e}")
            actual_df = pd.DataFrame()

        if len(actual_df) > 0:
            # 비교 - district×hour 레벨
            merged = full_pred.merge(
                actual_df,
                on=['region', 'district', 'hour'],
                how='outer'
            )
            merged['predicted_rides'] = merged['predicted_rides'].fillna(0)
            merged['actual_rides'] = merged['actual_rides'].fillna(0)

            # 전체 정확도
            total_pred = merged['predicted_rides'].sum()
            total_actual = merged['actual_rides'].sum()
            overall_error = ((total_pred - total_actual) / total_actual * 100
                           if total_actual > 0 else 0)

            # District 레벨 MAPE
            district_sum = merged.groupby(['region', 'district']).agg({
                'predicted_rides': 'sum',
                'actual_rides': 'sum'
            }).reset_index()
            district_sum = district_sum[district_sum['actual_rides'] > 0]
            if len(district_sum) > 0:
                district_sum['ape'] = (
                    (district_sum['predicted_rides'] - district_sum['actual_rides']).abs()
                    / district_sum['actual_rides'] * 100
                )
                district_mape = district_sum['ape'].mean()
            else:
                district_mape = None

            # 시간대 레벨 MAPE
            hour_sum = merged.groupby('hour').agg({
                'predicted_rides': 'sum',
                'actual_rides': 'sum'
            }).reset_index()
            hour_sum = hour_sum[hour_sum['actual_rides'] > 0]
            if len(hour_sum) > 0:
                hour_sum['ape'] = (
                    (hour_sum['predicted_rides'] - hour_sum['actual_rides']).abs()
                    / hour_sum['actual_rides'] * 100
                )
                hour_mape = hour_sum['ape'].mean()
            else:
                hour_mape = None

            print(f"  실제 총 라이딩: {total_actual:,.0f}건")
            print(f"  예측 총 라이딩: {total_pred:,.0f}건")
            print(f"  전체 오차: {overall_error:+.1f}%")
            if district_mape is not None:
                print(f"  District MAPE: {district_mape:.1f}%")
            if hour_mape is not None:
                print(f"  Hour MAPE: {hour_mape:.1f}%")

            # 시간대별 요약
            print(f"\n  시간대별 예측 vs 실제:")
            print(f"  {'시간':>4} {'예측':>8} {'실제':>8} {'오차':>8}")
            print(f"  {'-'*32}")
            for _, h in hour_sum.iterrows():
                err = ((h['predicted_rides'] - h['actual_rides'])
                       / h['actual_rides'] * 100 if h['actual_rides'] > 0 else 0)
                print(f"  {int(h['hour']):>4}시 {h['predicted_rides']:>8,.0f} "
                      f"{h['actual_rides']:>8,.0f} {err:>+7.1f}%")

            result['accuracy'] = {
                'total_actual': int(total_actual),
                'total_predicted': round(total_pred),
                'overall_error_pct': round(overall_error, 1),
                'district_mape': round(district_mape, 1) if district_mape else None,
                'hour_mape': round(hour_mape, 1) if hour_mape else None,
            }
        else:
            print(f"  ⚠️ {target_date}의 실제 데이터 없음 (미래 날짜이거나 데이터 미적재)")
            result['accuracy'] = None

        return result

    # ================================================================
    # 유틸리티
    # ================================================================

    def get_top_demand_districts(
        self, target_date: str, top_n: int = 20
    ) -> pd.DataFrame:
        """시간대별 수요 상위 district"""
        full_pred = self.predict(target_date)
        if len(full_pred) == 0:
            return pd.DataFrame()

        daily_demand = full_pred.groupby(['region', 'district']).agg({
            'predicted_rides': 'sum',
            'lat': 'first',
            'lng': 'first',
            'center': 'first',
        }).reset_index()

        return daily_demand.nlargest(top_n, 'predicted_rides')

    def get_peak_hours(
        self, target_date: str, district: str = None
    ) -> pd.DataFrame:
        """특정 district(또는 전체)의 피크 시간대"""
        full_pred = self.predict(target_date)
        if len(full_pred) == 0:
            return pd.DataFrame()

        if district:
            df = full_pred[full_pred['district'] == district]
        else:
            df = full_pred

        hourly = df.groupby('hour')['predicted_rides'].sum().reset_index()
        hourly = hourly.sort_values('predicted_rides', ascending=False)

        return hourly

    def to_region_summary(self, target_date: str) -> pd.DataFrame:
        """예측 결과를 region 레벨로 집계 (V7 비교용)"""
        full_pred = self.predict(target_date)
        if len(full_pred) == 0:
            return pd.DataFrame()

        region_sum = full_pred.groupby(['region', 'center']).agg({
            'predicted_rides': 'sum'
        }).reset_index()
        region_sum.columns = ['region', 'center', 'dh_predicted']
        region_sum['dh_predicted'] = region_sum['dh_predicted'].round(0)

        return region_sum.sort_values('dh_predicted', ascending=False)


# ================================================================
# CLI
# ================================================================

def run_cli():
    """CLI 실행"""
    parser = argparse.ArgumentParser(
        description='District × Hour 수요 예측',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
    python district_hour_model.py --date 2026-02-25
    python district_hour_model.py --date 2026-02-25 --validate
    python district_hour_model.py --date 2026-02-25 --time-slot morning
    python district_hour_model.py --date 2026-02-25 --top 10
        """
    )
    parser.add_argument('--date', type=str, default='2026-02-24',
                       help='대상 날짜 (YYYY-MM-DD)')
    parser.add_argument('--validate', action='store_true',
                       help='예측 검증 실행')
    parser.add_argument('--time-slot', type=str, default=None,
                       choices=list(TIME_SLOTS.keys()),
                       help='특정 시간대 슬롯 필터')
    parser.add_argument('--top', type=int, default=None,
                       help='수요 상위 N개 district 표시')
    parser.add_argument('--export-csv', type=str, default=None,
                       help='CSV 파일로 내보내기')

    args = parser.parse_args()

    predictor = DistrictHourPredictor(verbose=True)

    if args.validate:
        # 검증 모드
        result = predictor.validate(args.date)
        return

    if args.time_slot:
        # 시간대 슬롯 모드
        slot_pred = predictor.predict_time_slot(args.date, args.time_slot)
        slot_info = TIME_SLOTS[args.time_slot]

        print(f"\n📋 {slot_info['desc']} 예측")
        print(f"{'='*60}")

        if len(slot_pred) > 0:
            print(f"\n총 예측 라이딩: {slot_pred['predicted_rides'].sum():,.0f}건")
            print(f"대상 district: {len(slot_pred)}개")
            print(f"\n{'순위':>4} {'센터':<8} {'권역':<15} {'구역':<15} {'예측':>8}")
            print(f"{'-'*54}")

            for rank, (_, row) in enumerate(slot_pred.head(20).iterrows(), 1):
                print(f"{rank:>4} {row['center']:<8} {row['region']:<15} "
                      f"{row['district']:<15} {row['predicted_rides']:>8,.0f}")
        return

    if args.top:
        # 수요 상위 모드
        top_df = predictor.get_top_demand_districts(args.date, args.top)
        print(f"\n📋 수요 상위 {args.top}개 District")
        print(f"{'='*60}")

        if len(top_df) > 0:
            print(f"\n{'순위':>4} {'센터':<8} {'권역':<15} {'구역':<15} {'일 예측':>8}")
            print(f"{'-'*54}")

            for rank, (_, row) in enumerate(top_df.iterrows(), 1):
                print(f"{rank:>4} {row['center']:<8} {row['region']:<15} "
                      f"{row['district']:<15} {row['predicted_rides']:>8,.0f}")
        return

    # 기본 모드: 전체 예측
    pred_df = predictor.predict(args.date)

    if len(pred_df) > 0 and args.export_csv:
        pred_df.to_csv(args.export_csv, index=False, encoding='utf-8-sig')
        print(f"\n📁 CSV 내보내기: {args.export_csv}")
    elif len(pred_df) > 0:
        # 시간대별 요약
        print(f"\n📋 시간대별 수요 요약")
        print(f"{'='*40}")

        hourly = pred_df.groupby('hour')['predicted_rides'].sum().reset_index()
        hourly = hourly.sort_values('hour')

        print(f"\n{'시간':>4} {'예측 라이딩':>12} {'바':>6}")
        print(f"{'-'*26}")

        max_rides = hourly['predicted_rides'].max()
        for _, row in hourly.iterrows():
            bar_len = int(row['predicted_rides'] / max_rides * 20) if max_rides > 0 else 0
            bar = '█' * bar_len
            print(f"{int(row['hour']):>4}시 {row['predicted_rides']:>12,.0f} {bar}")

        # 센터별 요약
        print(f"\n📋 센터별 수요 요약")
        print(f"{'='*40}")

        center_sum = pred_df.groupby('center').agg({
            'predicted_rides': 'sum',
            'district': 'nunique',
            'region': 'nunique'
        }).reset_index()
        center_sum.columns = ['센터', '예측건수', 'district수', 'region수']
        center_sum = center_sum.sort_values('예측건수', ascending=False)

        print(f"\n{'센터':<12} {'예측건수':>10} {'district':>10} {'region':>8}")
        print(f"{'-'*44}")
        for _, row in center_sum.iterrows():
            print(f"{row['센터']:<12} {row['예측건수']:>10,.0f} "
                  f"{row['district수']:>10} {row['region수']:>8}")


if __name__ == '__main__':
    run_cli()
