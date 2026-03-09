"""
District × 3-Hour 시간대별 수요 예측 — Production v2 기반

아키텍처:
    1. production_v2_predictor로 district 일별 예측 (MAPE ~35%)
    2. 과거 28일 라이딩 시간대 분포로 3시간 window 배분
    3. 기온 보정으로 시간대 간 상대적 조정

3시간 Window:
    dawn    (00~05): 새벽
    morning (06~08): 출근
    midday  (09~11): 오전
    lunch   (12~14): 점심
    afternoon(15~17): 오후
    evening (18~20): 퇴근/저녁
    night   (21~23): 야간

사용법:
    from district_v2_hourly import DistrictV2Hourly
    predictor = DistrictV2Hourly()

    # 단일 날짜 예측
    result = predictor.predict('2026-02-27')

    # 기간 평가 (MAPE 계산)
    eval_result = predictor.evaluate_period('2026-02-16', '2026-02-26')

    # CLI
    python district_v2_hourly.py --date 2026-02-27
    python district_v2_hourly.py --evaluate 2026-02-16 2026-02-26
"""
import os
import sys
import json
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

# Authentication: set GOOGLE_APPLICATION_CREDENTIALS env var
SCRIPT_DIR_CREDS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'credentials', 'service-account.json')
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    if os.path.exists(SCRIPT_DIR_CREDS):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SCRIPT_DIR_CREDS

# ============================================================
# 3시간 Window 정의
# ============================================================

TIME_WINDOWS = {
    'dawn':      {'hours': [0, 1, 2, 3, 4, 5],   'label': '새벽 (00~05)'},
    'morning':   {'hours': [6, 7, 8],              'label': '출근 (06~08)'},
    'midday':    {'hours': [9, 10, 11],             'label': '오전 (09~11)'},
    'lunch':     {'hours': [12, 13, 14],            'label': '점심 (12~14)'},
    'afternoon': {'hours': [15, 16, 17],            'label': '오후 (15~17)'},
    'evening':   {'hours': [18, 19, 20],            'label': '퇴근 (18~20)'},
    'night':     {'hours': [21, 22, 23],            'label': '야간 (21~23)'},
}

# 운영 슬롯 (4구간 — 대시보드용)
OPS_SLOTS = {
    'night_prep': {'windows': ['dawn'],                 'label': '야간준비 (00~05)'},
    'am_peak':    {'windows': ['morning', 'midday'],    'label': '오전 피크 (06~11)'},
    'pm_peak':    {'windows': ['lunch', 'afternoon'],   'label': '오후 피크 (12~17)'},
    'evening':    {'windows': ['evening', 'night'],     'label': '저녁~야간 (18~23)'},
}

# 최소 데이터 기준
MIN_RIDES_FOR_PROFILE = 30   # district별 최소 라이딩 건수 (28일)


def hour_to_window(hour: int) -> str:
    """시간 → 3시간 window 매핑"""
    for wname, wdef in TIME_WINDOWS.items():
        if hour in wdef['hours']:
            return wname
    return 'dawn'  # fallback


# ============================================================
# 시간대별 기온 보정
# ============================================================

def _estimate_hourly_temp(hour: int, temp_low: float, temp_high: float) -> float:
    """일 최저/최고 기온에서 시간별 기온 추정 (사인 곡선)"""
    import math
    phase = 2 * math.pi * (hour - 14) / 24.0  # peak at 14시
    return (temp_high + temp_low) / 2 + (temp_high - temp_low) / 2 * math.cos(phase)


def get_window_temp_factors(
    temp_low: float, temp_high: float, is_weekday: bool = True
) -> Dict[str, float]:
    """
    3시간 window별 기온 보정 팩터 (총합 보존)

    출퇴근 시간은 기온에 덜 민감, 레저 시간은 민감
    """
    ELASTICITY_WD = {
        0: 0.1, 1: 0.1, 2: 0.1, 3: 0.1, 4: 0.1, 5: 0.1,
        6: 0.3, 7: 0.2, 8: 0.2,
        9: 0.3, 10: 0.5, 11: 0.6,
        12: 0.7, 13: 0.7, 14: 0.8,
        15: 0.8, 16: 0.7, 17: 0.3,
        18: 0.2, 19: 0.3, 20: 0.5,
        21: 0.5, 22: 0.3, 23: 0.2,
    }
    ELASTICITY_WE = {
        0: 0.1, 1: 0.1, 2: 0.1, 3: 0.1, 4: 0.1, 5: 0.1,
        6: 0.3, 7: 0.5, 8: 0.6,
        9: 0.7, 10: 0.8, 11: 0.8,
        12: 0.8, 13: 0.8, 14: 0.9,
        15: 0.9, 16: 0.8, 17: 0.7,
        18: 0.6, 19: 0.5, 20: 0.4,
        21: 0.3, 22: 0.2, 23: 0.1,
    }
    ELAST = ELASTICITY_WD if is_weekday else ELASTICITY_WE

    # 시간별 원시 팩터
    hourly_factors = {}
    for h in range(24):
        temp = _estimate_hourly_temp(h, temp_low, temp_high)
        elast = ELAST.get(h, 0.5)

        if temp >= 10:
            raw_adj = 0.0
        elif temp >= 0:
            raw_adj = -0.05 * (10 - temp) / 10
        elif temp >= -8:
            raw_adj = -0.05 - 0.10 * (-temp) / 8
        else:
            raw_adj = -0.15 - 0.05 * min(1, (-8 - temp) / 10)

        hourly_factors[h] = 1.0 + raw_adj * elast

    # 3시간 window로 집계 (시간수 가중)
    window_factors = {}
    for wname, wdef in TIME_WINDOWS.items():
        hours = wdef['hours']
        factor = np.mean([hourly_factors[h] for h in hours])
        window_factors[wname] = factor

    # 정규화 (총합 보존)
    total_hours = sum(len(w['hours']) for w in TIME_WINDOWS.values())
    weighted_sum = sum(
        window_factors[wn] * len(TIME_WINDOWS[wn]['hours'])
        for wn in window_factors
    )
    if weighted_sum > 0:
        scale = total_hours / weighted_sum  # 시간수 가중 평균 = 1.0
        # 실제로는 ratio에 곱하므로 개별 window 보정
        for wn in window_factors:
            window_factors[wn] = round(window_factors[wn] * scale, 4)

    return window_factors


# ============================================================
# 메인 클래스
# ============================================================

class DistrictV2Hourly:
    """
    Production v2 district 예측 → 3시간 window 배분

    핵심 로직:
        district_daily_pred (from v2) × window_ratio × temp_factor
                                        = district × window 예측

    window_ratio는 과거 28일 실제 라이딩 시간 분포에서 계산.
    district별 데이터 부족 시 region 프로필로 fallback.
    """

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self._client = None
        self._profile_cache = {}   # {(day_type, 기간키): profiles_df}
        self._hourly_raw_cache = {}  # {cache_key: raw_hourly_df} — 시간별 원본 데이터

    @property
    def client(self):
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client()
        return self._client

    # ================================================================
    # Step 1: Production v2 일별 district 예측
    # ================================================================

    def get_v2_daily_predictions(self, target_date: str) -> pd.DataFrame:
        """
        production_v2_predictor의 district 일별 예측 호출

        Returns:
            DataFrame[date, region, district, predicted_rides, lat, lng, center]
        """
        from production_v2_predictor import predict_district_rides

        district_df, region_df = predict_district_rides(target_date)

        if district_df is None or len(district_df) == 0:
            if self.verbose:
                print(f"  ⚠️ v2 예측 실패: {target_date}")
            return pd.DataFrame()

        # predict_district_rides 반환 컬럼:
        # region, district, adj_pred, pred_opens, pred_rpo, lat, lng, center, desc, ratio
        result = district_df[['region', 'district', 'adj_pred',
                              'lat', 'lng', 'center']].copy()
        result.columns = ['region', 'district', 'predicted_rides_daily',
                         'lat', 'lng', 'center']
        result['date'] = target_date

        return result

    # ================================================================
    # Step 2: 3시간 Window 프로필 (BQ에서 실제 라이딩 분포)
    # ================================================================

    def get_window_profiles(self, target_date: str) -> pd.DataFrame:
        """
        과거 28일 실제 라이딩 시간 분포 → 3시간 window 비율

        요일 타입(weekday/saturday/sunday)별로 분리.
        district별 MIN_RIDES 미만이면 region 프로필로 fallback.

        Returns:
            DataFrame[region, district, window, ratio, profile_source, total_rides]
        """
        target = pd.Timestamp(target_date)
        dow = target.dayofweek

        # 공휴일 확인
        try:
            from korean_holidays import ADDITIONAL_HOLIDAYS
            is_holiday = target.date() in ADDITIONAL_HOLIDAYS
        except ImportError:
            is_holiday = False

        # 요일 타입 → BigQuery DAYOFWEEK 필터
        if is_holiday or dow == 6:
            day_type = 'sunday_holiday'
            day_filter = "EXTRACT(DAYOFWEEK FROM start_time) = 1"  # 일요일
        elif dow == 5:
            day_type = 'saturday'
            day_filter = "EXTRACT(DAYOFWEEK FROM start_time) = 7"  # 토요일
        else:
            day_type = 'weekday'
            day_filter = "EXTRACT(DAYOFWEEK FROM start_time) IN (2,3,4,5,6)"

        # 캐시 키
        end_date = (target - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (target - timedelta(days=28)).strftime('%Y-%m-%d')
        cache_key = (day_type, start_date, end_date)

        if cache_key in self._profile_cache:
            return self._profile_cache[cache_key]

        if self.verbose:
            print(f"  📊 시간대 프로필 조회 ({day_type}, {start_date}~{end_date})")

        query = f"""
        WITH hourly_rides AS (
            SELECT
                h3_start_area_name as region,
                h3_start_district_name as district,
                EXTRACT(HOUR FROM start_time) as hour,
                COUNT(*) as rides
            FROM `bikeshare.service.rides`
            WHERE DATE(start_time) BETWEEN '{start_date}' AND '{end_date}'
                AND {day_filter}
                AND h3_start_area_name IS NOT NULL
                AND h3_start_district_name IS NOT NULL
            GROUP BY 1, 2, 3
        )
        SELECT region, district, hour, rides
        FROM hourly_rides
        ORDER BY region, district, hour
        """

        raw = self.client.query(query).to_dataframe()

        if len(raw) == 0:
            if self.verbose:
                print(f"  ⚠️ 시간대 프로필 데이터 없음")
            return pd.DataFrame()

        # hour → window 매핑
        raw['window'] = raw['hour'].apply(hour_to_window)

        # district × window 집계
        district_window = raw.groupby(['region', 'district', 'window'])[
            'rides'].sum().reset_index()

        # district별 총 라이딩
        district_total = district_window.groupby(['region', 'district'])[
            'rides'].sum().reset_index()
        district_total.columns = ['region', 'district', 'total_rides']

        district_window = district_window.merge(district_total, on=['region', 'district'])
        district_window['ratio'] = district_window['rides'] / district_window['total_rides']

        # ── Fallback: 데이터 부족 district → region 프로필 ──
        # region별 window 비율
        region_window = raw.groupby(['region', 'window'])['rides'].sum().reset_index()
        region_total = region_window.groupby('region')['rides'].sum().reset_index()
        region_total.columns = ['region', 'region_total']
        region_window = region_window.merge(region_total, on='region')
        region_window['region_ratio'] = region_window['rides'] / region_window['region_total']

        # 전역 프로필 (ultimate fallback)
        global_window = raw.groupby('window')['rides'].sum()
        global_total = global_window.sum()
        global_ratios = (global_window / global_total).to_dict()

        # 결과 구축
        results = []
        all_windows = list(TIME_WINDOWS.keys())

        districts = district_total[['region', 'district', 'total_rides']].drop_duplicates()

        for _, d_row in districts.iterrows():
            region = d_row['region']
            district = d_row['district']
            total = d_row['total_rides']

            use_district = total >= MIN_RIDES_FOR_PROFILE

            for wname in all_windows:
                if use_district:
                    mask = ((district_window['region'] == region) &
                            (district_window['district'] == district) &
                            (district_window['window'] == wname))
                    matched = district_window[mask]
                    if len(matched) > 0:
                        ratio = matched['ratio'].values[0]
                        source = 'district'
                    else:
                        ratio = 0.0
                        source = 'district'
                else:
                    # region fallback
                    reg_mask = ((region_window['region'] == region) &
                                (region_window['window'] == wname))
                    reg_matched = region_window[reg_mask]
                    if len(reg_matched) > 0:
                        ratio = reg_matched['region_ratio'].values[0]
                        source = 'region'
                    else:
                        _DEFAULT_W = {
                            'dawn': 0.03, 'morning': 0.15, 'midday': 0.15,
                            'lunch': 0.18, 'afternoon': 0.20,
                            'evening': 0.19, 'night': 0.10,
                        }
                        ratio = global_ratios.get(wname, _DEFAULT_W.get(wname, 0.14))
                        source = 'global'

                results.append({
                    'region': region,
                    'district': district,
                    'window': wname,
                    'ratio': ratio,
                    'profile_source': source,
                    'total_rides': total,
                })

        profiles = pd.DataFrame(results)

        # ratio 정규화 (district별 window 합 = 1.0)
        profile_sums = profiles.groupby(['region', 'district'])['ratio'].sum().reset_index()
        profile_sums.columns = ['region', 'district', 'ratio_sum']
        profiles = profiles.merge(profile_sums, on=['region', 'district'])
        _DEFAULT_W = {
            'dawn': 0.03, 'morning': 0.15, 'midday': 0.15,
            'lunch': 0.18, 'afternoon': 0.20, 'evening': 0.19, 'night': 0.10,
        }
        profiles['ratio'] = np.where(
            profiles['ratio_sum'] > 0,
            profiles['ratio'] / profiles['ratio_sum'],
            profiles['window'].map(_DEFAULT_W).fillna(0.14)
        )
        profiles.drop(columns=['ratio_sum'], inplace=True)

        if self.verbose:
            n_districts = profiles[['region', 'district']].drop_duplicates().shape[0]
            n_fallback = profiles[profiles['profile_source'] != 'district'][
                ['region', 'district']].drop_duplicates().shape[0]
            print(f"  → {n_districts}개 district, fallback {n_fallback}개")

        self._profile_cache[cache_key] = profiles
        self._hourly_raw_cache[cache_key] = raw  # 시간별 원본 데이터 보관
        return profiles

    # ================================================================
    # Step 3: 예측 조합 (daily × ratio × temp_factor)
    # ================================================================

    def predict(self, target_date: str) -> pd.DataFrame:
        """
        District × 3-Hour Window 예측

        Returns:
            DataFrame[date, region, district, window, window_label,
                      predicted_rides, lat, lng, center, day_type]
        """
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"📊 District × 3시간 수요 예측: {target_date}")
            print(f"{'='*60}")

        # 1. v2 일별 district 예측
        daily = self.get_v2_daily_predictions(target_date)
        if len(daily) == 0:
            return pd.DataFrame()

        if self.verbose:
            total_daily = daily['predicted_rides_daily'].sum()
            print(f"  [Step 1] v2 일별 예측: {len(daily)}개 district, "
                  f"총 {total_daily:,.0f}건")

        # 2. 3시간 window 프로필
        profiles = self.get_window_profiles(target_date)
        if len(profiles) == 0:
            # 프로필 없으면 전역 시계열 비율 시도 → 최종 fallback 균등 배분
            if self.verbose:
                print(f"  ⚠️ 프로필 없음 → 전역 기본 비율 적용")
            # 전역 기본 window 비율 (일반적인 자전거 이용 패턴)
            _DEFAULT_WINDOW_RATIOS = {
                'dawn': 0.03,      # 새벽 (00~05) ~3%
                'morning': 0.15,   # 출근 (06~08) ~15%
                'midday': 0.15,    # 오전 (09~11) ~15%
                'lunch': 0.18,     # 점심 (12~14) ~18%
                'afternoon': 0.20, # 오후 (15~17) ~20%
                'evening': 0.19,   # 퇴근 (18~20) ~19%
                'night': 0.10,     # 야간 (21~23) ~10%
            }
            results = []
            for _, row in daily.iterrows():
                for wname, wdef in TIME_WINDOWS.items():
                    weight = _DEFAULT_WINDOW_RATIOS.get(wname, 0.14)
                    results.append({
                        'date': target_date,
                        'region': row['region'],
                        'district': row['district'],
                        'window': wname,
                        'window_label': wdef['label'],
                        'predicted_rides': round(row['predicted_rides_daily'] * weight, 2),
                        'lat': row['lat'],
                        'lng': row['lng'],
                        'center': row.get('center', ''),
                    })
            return pd.DataFrame(results)

        # 3. 기온 보정 팩터
        target_ts = pd.Timestamp(target_date)
        is_weekday = target_ts.dayofweek < 5
        try:
            from korean_holidays import ADDITIONAL_HOLIDAYS
            if target_ts.date() in ADDITIONAL_HOLIDAYS:
                is_weekday = False
        except ImportError:
            pass

        try:
            weather_csv = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')
            if os.path.exists(weather_csv):
                from demand_model_v7 import load_weather_data
                weather_data = load_weather_data(weather_csv)
                from visualize_prediction_map import _get_weather_for_date
                weather, _ = _get_weather_for_date(target_date, weather_data)
                temp_low = weather.get('temp_low', 5)
                temp_high = weather.get('temp_high', 10)
            else:
                temp_low, temp_high = 5, 10
        except Exception:
            temp_low, temp_high = 5, 10

        temp_factors = get_window_temp_factors(temp_low, temp_high, is_weekday)
        temp_range = max(temp_factors.values()) - min(temp_factors.values())
        apply_temp = temp_range > 0.02

        if self.verbose and apply_temp:
            print(f"  [Step 3] 기온 보정: {temp_low}°C ~ {temp_high}°C, "
                  f"팩터 범위 {temp_range:.3f}")

        # 4. 조합
        results = []
        matched = 0
        fallback_uniform = 0

        for _, d_row in daily.iterrows():
            region = d_row['region']
            district = d_row['district']
            daily_pred = d_row['predicted_rides_daily']

            # 해당 district의 window 프로필
            p_mask = ((profiles['region'] == region) &
                      (profiles['district'] == district))
            d_profiles = profiles[p_mask]

            if len(d_profiles) == 0:
                # 프로필 없는 district → region 프로필 fallback
                fallback_uniform += 1
                region_profiles = profiles[profiles['region'] == region]
                if len(region_profiles) > 0:
                    # region 평균 비율 사용
                    region_avg = region_profiles.groupby('window')['ratio'].mean()
                    r_sum = region_avg.sum()
                    if r_sum > 0:
                        region_avg = region_avg / r_sum
                else:
                    region_avg = None

                _DEFAULT_W = {
                    'dawn': 0.03, 'morning': 0.15, 'midday': 0.15,
                    'lunch': 0.18, 'afternoon': 0.20,
                    'evening': 0.19, 'night': 0.10,
                }
                for wname, wdef in TIME_WINDOWS.items():
                    if region_avg is not None and wname in region_avg.index:
                        weight = region_avg[wname]
                    else:
                        weight = _DEFAULT_W.get(wname, 0.14)
                    pred = daily_pred * weight
                    if apply_temp:
                        pred *= temp_factors.get(wname, 1.0)
                    results.append({
                        'date': target_date,
                        'region': region,
                        'district': district,
                        'window': wname,
                        'window_label': wdef['label'],
                        'predicted_rides': round(pred, 2),
                        'lat': d_row['lat'],
                        'lng': d_row['lng'],
                        'center': d_row.get('center', ''),
                    })
            else:
                matched += 1
                for _, p_row in d_profiles.iterrows():
                    wname = p_row['window']
                    ratio = p_row['ratio']
                    pred = daily_pred * ratio
                    if apply_temp:
                        pred *= temp_factors.get(wname, 1.0)
                    results.append({
                        'date': target_date,
                        'region': region,
                        'district': district,
                        'window': wname,
                        'window_label': TIME_WINDOWS[wname]['label'],
                        'predicted_rides': round(pred, 2),
                        'lat': d_row['lat'],
                        'lng': d_row['lng'],
                        'center': d_row.get('center', ''),
                    })

        result_df = pd.DataFrame(results)

        # 요일 타입 추가
        dow = target_ts.dayofweek
        try:
            from korean_holidays import ADDITIONAL_HOLIDAYS
            is_hol = target_ts.date() in ADDITIONAL_HOLIDAYS
        except ImportError:
            is_hol = False

        if is_hol or dow == 6:
            result_df['day_type'] = 'sunday_holiday'
        elif dow == 5:
            result_df['day_type'] = 'saturday'
        else:
            result_df['day_type'] = 'weekday'

        if self.verbose:
            n_districts = result_df[['region', 'district']].drop_duplicates().shape[0]
            n_windows = result_df['window'].nunique()
            total_pred = result_df['predicted_rides'].sum()
            print(f"\n  ✅ 결과: {n_districts}개 district × {n_windows}개 window "
                  f"= {len(result_df):,}행")
            print(f"     총 예측: {total_pred:,.0f}건 "
                  f"(매칭 {matched}, 전역비율 {fallback_uniform})")

            # window별 요약
            w_sum = result_df.groupby('window')['predicted_rides'].sum()
            total = w_sum.sum()
            print(f"\n  {'Window':<12} {'예측건수':>10} {'비중':>8}")
            print(f"  {'-'*32}")
            for wname in TIME_WINDOWS:
                rides = w_sum.get(wname, 0)
                pct = rides / total * 100 if total > 0 else 0
                label = TIME_WINDOWS[wname]['label']
                print(f"  {label:<12} {rides:>10,.0f} {pct:>7.1f}%")

        return result_df

    # ================================================================
    # 평가: 예측 vs 실제
    # ================================================================

    def evaluate_date(self, target_date: str) -> Dict:
        """
        단일 날짜 예측 vs 실제 비교

        Returns:
            {date, daily_mape, window_mape, district_window_mape,
             window_errors: [{window, pred, actual, error_pct}]}
        """
        # 예측
        pred_df = self.predict(target_date)
        if len(pred_df) == 0:
            return {'date': target_date, 'error': 'no predictions'}

        # 실제 라이딩 (시간별 → window 집계)
        query = f"""
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
            actual_raw = self.client.query(query).to_dataframe()
        except Exception as e:
            return {'date': target_date, 'error': str(e)}

        if len(actual_raw) == 0:
            return {'date': target_date, 'error': 'no actual data'}

        # hour → window 매핑
        actual_raw['window'] = actual_raw['hour'].apply(hour_to_window)
        actual = actual_raw.groupby(['region', 'district', 'window'])[
            'actual_rides'].sum().reset_index()

        # 병합
        merged = pred_df.merge(
            actual, on=['region', 'district', 'window'], how='outer')
        merged['predicted_rides'] = merged['predicted_rides'].fillna(0)
        merged['actual_rides'] = merged['actual_rides'].fillna(0)

        # ── 1. 전체 합산 오차 ──
        total_pred = merged['predicted_rides'].sum()
        total_actual = merged['actual_rides'].sum()
        daily_error = ((total_pred - total_actual) / total_actual * 100
                       if total_actual > 0 else 0)

        # ── 2. Window별 MAPE ──
        w_agg = merged.groupby('window').agg({
            'predicted_rides': 'sum',
            'actual_rides': 'sum',
        }).reset_index()
        w_agg = w_agg[w_agg['actual_rides'] > 0]
        w_agg['ape'] = ((w_agg['predicted_rides'] - w_agg['actual_rides']).abs()
                        / w_agg['actual_rides'] * 100)
        w_agg['error_pct'] = ((w_agg['predicted_rides'] - w_agg['actual_rides'])
                              / w_agg['actual_rides'] * 100)
        window_mape = w_agg['ape'].mean() if len(w_agg) > 0 else None

        # ── 3. District×Window MAPE ──
        dw_agg = merged[(merged['actual_rides'] > 0)].copy()
        if len(dw_agg) > 0:
            dw_agg['ape'] = ((dw_agg['predicted_rides'] - dw_agg['actual_rides']).abs()
                             / dw_agg['actual_rides'] * 100)
            dw_mape = dw_agg['ape'].mean()
        else:
            dw_mape = None

        # ── 4. District 일별 MAPE (참고: v2 원래 MAPE) ──
        d_daily = merged.groupby(['region', 'district']).agg({
            'predicted_rides': 'sum',
            'actual_rides': 'sum',
        }).reset_index()
        d_daily = d_daily[d_daily['actual_rides'] > 0]
        if len(d_daily) > 0:
            d_daily['ape'] = ((d_daily['predicted_rides'] - d_daily['actual_rides']).abs()
                              / d_daily['actual_rides'] * 100)
            district_daily_mape = d_daily['ape'].mean()
        else:
            district_daily_mape = None

        result = {
            'date': target_date,
            'total_pred': round(total_pred),
            'total_actual': int(total_actual),
            'daily_error_pct': round(daily_error, 1),
            'district_daily_mape': round(district_daily_mape, 1) if district_daily_mape else None,
            'window_mape': round(window_mape, 1) if window_mape else None,
            'district_window_mape': round(dw_mape, 1) if dw_mape else None,
            'window_errors': w_agg.to_dict('records') if len(w_agg) > 0 else [],
        }

        if self.verbose:
            print(f"\n  📊 {target_date} 평가:")
            print(f"     총합: 예측 {total_pred:,.0f} / 실제 {total_actual:,.0f} "
                  f"({daily_error:+.1f}%)")
            if district_daily_mape:
                print(f"     District 일별 MAPE: {district_daily_mape:.1f}%")
            if window_mape:
                print(f"     Window 합산 MAPE: {window_mape:.1f}%")
            if dw_mape:
                print(f"     District×Window MAPE: {dw_mape:.1f}%")

            if len(w_agg) > 0:
                print(f"\n     {'Window':<12} {'예측':>8} {'실제':>8} {'오차':>8}")
                print(f"     {'-'*38}")
                for _, w in w_agg.iterrows():
                    wname = w['window']
                    label = TIME_WINDOWS.get(wname, {}).get('label', wname)
                    print(f"     {label:<12} {w['predicted_rides']:>8,.0f} "
                          f"{w['actual_rides']:>8,.0f} {w['error_pct']:>+7.1f}%")

        return result

    def evaluate_period(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        기간 평가: 일별 MAPE + Window별 MAPE 추이

        Returns:
            DataFrame with daily evaluation results
        """
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"📊 기간 평가: {start_date} ~ {end_date}")
            print(f"{'='*60}")

        dates = pd.date_range(start_date, end_date, freq='D')
        results = []

        for d in dates:
            date_str = d.strftime('%Y-%m-%d')
            try:
                eval_result = self.evaluate_date(date_str)
                results.append(eval_result)
            except Exception as e:
                if self.verbose:
                    print(f"  ⚠️ {date_str} 실패: {e}")
                results.append({'date': date_str, 'error': str(e)})

        # 요약
        valid = [r for r in results if 'error' not in r]

        if self.verbose and valid:
            print(f"\n{'='*60}")
            print(f"📋 기간 평가 요약 ({len(valid)}일)")
            print(f"{'='*60}")

            print(f"\n{'날짜':<12} {'일오차':>7} {'D-MAPE':>8} {'W-MAPE':>8} {'DW-MAPE':>8}")
            print(f"{'-'*47}")

            for r in valid:
                d_mape = r.get('district_daily_mape')
                w_mape = r.get('window_mape')
                dw_mape = r.get('district_window_mape')
                d_err = r.get('daily_error_pct', 0)

                d_str = f"{d_mape:.1f}%" if d_mape else '-'
                w_str = f"{w_mape:.1f}%" if w_mape else '-'
                dw_str = f"{dw_mape:.1f}%" if dw_mape else '-'

                print(f"{r['date']:<12} {d_err:>+6.1f}% {d_str:>8} "
                      f"{w_str:>8} {dw_str:>8}")

            # 평균
            avg_d = np.mean([r['district_daily_mape'] for r in valid
                             if r.get('district_daily_mape')])
            avg_w = np.mean([r['window_mape'] for r in valid
                             if r.get('window_mape')])
            avg_dw = np.mean([r['district_window_mape'] for r in valid
                              if r.get('district_window_mape')])
            avg_err = np.mean([r['daily_error_pct'] for r in valid])

            print(f"{'-'*47}")
            print(f"{'평균':<12} {avg_err:>+6.1f}% {avg_d:>7.1f}% "
                  f"{avg_w:>7.1f}% {avg_dw:>7.1f}%")

            # Window별 평균 오차
            print(f"\n📊 Window별 평균 오차:")
            for wname in TIME_WINDOWS:
                errors = []
                for r in valid:
                    for we in r.get('window_errors', []):
                        if we.get('window') == wname:
                            errors.append(we.get('error_pct', 0))
                if errors:
                    label = TIME_WINDOWS[wname]['label']
                    mean_err = np.mean(errors)
                    std_err = np.std(errors)
                    print(f"  {label:<15} {mean_err:>+6.1f}% ± {std_err:.1f}%")

        return pd.DataFrame(valid)

    # ================================================================
    # 유틸리티
    # ================================================================

    def get_ops_slot_summary(self, target_date: str) -> pd.DataFrame:
        """운영 4구간 요약 (대시보드용)"""
        full = self.predict(target_date)
        if len(full) == 0:
            return pd.DataFrame()

        results = []
        for slot_name, slot_def in OPS_SLOTS.items():
            windows = slot_def['windows']
            slot_data = full[full['window'].isin(windows)]
            if len(slot_data) == 0:
                continue

            slot_agg = slot_data.groupby(['region', 'district']).agg({
                'predicted_rides': 'sum',
                'lat': 'first',
                'lng': 'first',
                'center': 'first',
            }).reset_index()
            slot_agg['ops_slot'] = slot_name
            slot_agg['ops_label'] = slot_def['label']
            results.append(slot_agg)

        if results:
            return pd.concat(results, ignore_index=True)
        return pd.DataFrame()

    def _get_intra_window_ratios(self, target_date: str) -> Dict:
        """
        Window 내 시간별 비율 계산 (과거 28일 실제 라이딩 기반)

        get_window_profiles()에서 캐싱된 시간별 원본 데이터를 사용하여
        각 window 내에서 시간별 상대 비율을 계산.

        Returns:
            {
                (region, district): {window: {hour: ratio, ...}, ...},
                '__region__': {region: {window: {hour: ratio, ...}, ...}},
                '__global__': {window: {hour: ratio, ...}},
            }
        """
        # get_window_profiles()가 호출되어야 _hourly_raw_cache에 데이터가 있음
        # 가장 최근 캐시 키 사용
        target = pd.Timestamp(target_date)
        dow = target.dayofweek
        try:
            from korean_holidays import ADDITIONAL_HOLIDAYS
            is_holiday = target.date() in ADDITIONAL_HOLIDAYS
        except ImportError:
            is_holiday = False

        if is_holiday or dow == 6:
            day_type = 'sunday_holiday'
        elif dow == 5:
            day_type = 'saturday'
        else:
            day_type = 'weekday'

        end_date = (target - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (target - timedelta(days=28)).strftime('%Y-%m-%d')
        cache_key = (day_type, start_date, end_date)

        raw = self._hourly_raw_cache.get(cache_key)
        if raw is None or len(raw) == 0:
            return {}

        # raw: region, district, hour, rides (+ window 컬럼 이미 추가됨)
        if 'window' not in raw.columns:
            raw = raw.copy()
            raw['window'] = raw['hour'].apply(hour_to_window)

        ratios = {}

        # ── District-level intra-window ratios ──
        for (region, district), grp in raw.groupby(['region', 'district']):
            d_ratios = {}
            for wname, wdef in TIME_WINDOWS.items():
                w_hours = wdef['hours']
                w_data = grp[grp['hour'].isin(w_hours)]
                w_total = w_data['rides'].sum()
                if w_total > 0:
                    d_ratios[wname] = {
                        int(row['hour']): row['rides'] / w_total
                        for _, row in w_data.iterrows()
                    }
                # 빠진 시간에 0 채우기
                if wname in d_ratios:
                    for h in w_hours:
                        if h not in d_ratios[wname]:
                            d_ratios[wname][h] = 0.0
            ratios[(region, district)] = d_ratios

        # ── Region-level fallback ──
        region_ratios = {}
        for region, grp in raw.groupby('region'):
            r_ratios = {}
            for wname, wdef in TIME_WINDOWS.items():
                w_hours = wdef['hours']
                w_data = grp[grp['hour'].isin(w_hours)]
                w_total = w_data['rides'].sum()
                if w_total > 0:
                    hour_agg = w_data.groupby('hour')['rides'].sum()
                    r_ratios[wname] = {
                        int(h): rides / w_total
                        for h, rides in hour_agg.items()
                    }
                    for h in w_hours:
                        if h not in r_ratios[wname]:
                            r_ratios[wname][h] = 0.0
            region_ratios[region] = r_ratios
        ratios['__region__'] = region_ratios

        # ── Global fallback ──
        global_ratios = {}
        for wname, wdef in TIME_WINDOWS.items():
            w_hours = wdef['hours']
            w_data = raw[raw['hour'].isin(w_hours)]
            w_total = w_data['rides'].sum()
            if w_total > 0:
                hour_agg = w_data.groupby('hour')['rides'].sum()
                global_ratios[wname] = {
                    int(h): rides / w_total
                    for h, rides in hour_agg.items()
                }
                for h in w_hours:
                    if h not in global_ratios[wname]:
                        global_ratios[wname][h] = 0.0
        ratios['__global__'] = global_ratios

        return ratios

    def to_hourly_estimate(self, target_date: str) -> pd.DataFrame:
        """
        3시간 window → 1시간 추정치 (시계열 비율 분배)

        과거 28일 실제 라이딩의 시간별 분포를 기반으로
        window 내 각 시간의 비율을 계산하여 배분.
        데이터 부족 시 region → global → 전역 평균 비율 순 fallback.
        """
        full = self.predict(target_date)
        if len(full) == 0:
            return pd.DataFrame()

        # 시간별 intra-window 비율 조회
        intra_ratios = self._get_intra_window_ratios(target_date)
        region_ratios = intra_ratios.get('__region__', {})
        global_ratios = intra_ratios.get('__global__', {})

        results = []
        for _, row in full.iterrows():
            wname = row['window']
            hours = TIME_WINDOWS[wname]['hours']
            region = row['region']
            district = row['district']
            window_pred = row['predicted_rides']

            # 1순위: district-level 비율
            d_key = (region, district)
            hour_ratios = None
            if d_key in intra_ratios and wname in intra_ratios[d_key]:
                hour_ratios = intra_ratios[d_key][wname]

            # 2순위: region-level fallback
            if hour_ratios is None or len(hour_ratios) == 0:
                if region in region_ratios and wname in region_ratios[region]:
                    hour_ratios = region_ratios[region][wname]

            # 3순위: global fallback
            if hour_ratios is None or len(hour_ratios) == 0:
                if wname in global_ratios:
                    hour_ratios = global_ratios[wname]

            # 최종 fallback: 전역 시간대 평균 비율
            if hour_ratios is None or len(hour_ratios) == 0:
                _GHR = {
                    0: 0.005, 1: 0.003, 2: 0.002, 3: 0.002, 4: 0.003, 5: 0.008,
                    6: 0.025, 7: 0.055, 8: 0.065, 9: 0.050, 10: 0.050, 11: 0.055,
                    12: 0.065, 13: 0.060, 14: 0.055, 15: 0.060, 16: 0.070, 17: 0.075,
                    18: 0.075, 19: 0.065, 20: 0.050, 21: 0.040, 22: 0.035, 23: 0.027,
                }
                hour_ratios = {h: _GHR.get(h, 0.02) for h in hours}

            # 비율 합 정규화 (합 = 1.0 보장)
            ratio_sum = sum(hour_ratios.get(h, 0) for h in hours)
            if ratio_sum <= 0:
                _GHR = {
                    0: 0.005, 1: 0.003, 2: 0.002, 3: 0.002, 4: 0.003, 5: 0.008,
                    6: 0.025, 7: 0.055, 8: 0.065, 9: 0.050, 10: 0.050, 11: 0.055,
                    12: 0.065, 13: 0.060, 14: 0.055, 15: 0.060, 16: 0.070, 17: 0.075,
                    18: 0.075, 19: 0.065, 20: 0.050, 21: 0.040, 22: 0.035, 23: 0.027,
                }
                hour_ratios = {h: _GHR.get(h, 0.02) for h in hours}
                ratio_sum = sum(hour_ratios[h] for h in hours)

            for h in hours:
                ratio = hour_ratios.get(h, 0) / ratio_sum
                results.append({
                    'date': row.get('date', target_date),
                    'region': region,
                    'district': district,
                    'hour': h,
                    'predicted_rides': round(window_pred * ratio, 2),
                    'lat': row['lat'],
                    'lng': row['lng'],
                    'center': row.get('center', ''),
                    'window': wname,
                })

        return pd.DataFrame(results)


# ================================================================
# CLI
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description='District × 3시간 수요 예측 (Production v2 기반)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
    python district_v2_hourly.py --date 2026-02-27
    python district_v2_hourly.py --evaluate 2026-02-16 2026-02-26
    python district_v2_hourly.py --date 2026-02-27 --export result.csv
        """
    )
    parser.add_argument('--date', type=str, default=None,
                       help='예측 대상 날짜')
    parser.add_argument('--evaluate', nargs=2, metavar=('START', 'END'),
                       help='기간 평가 (start end)')
    parser.add_argument('--export', type=str, default=None,
                       help='CSV 내보내기 경로')

    args = parser.parse_args()
    predictor = DistrictV2Hourly(verbose=True)

    if args.evaluate:
        predictor.evaluate_period(args.evaluate[0], args.evaluate[1])
        return

    if args.date:
        pred = predictor.predict(args.date)
        if len(pred) > 0 and args.export:
            pred.to_csv(args.export, index=False, encoding='utf-8-sig')
            print(f"\n📁 CSV: {args.export}")
    else:
        # 기본: 내일 예측
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        predictor.predict(tomorrow)


if __name__ == '__main__':
    main()
