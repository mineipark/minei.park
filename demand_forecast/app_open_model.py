"""
앱 오픈 기반 수요 예측 모델 (V8)

기존 V7 파이프라인의 순환 문제를 해결:
    [V7] 과거 라이딩 → 라이딩 예측 (공급 제약이 내재 = 순환)
    [V8] 과거 앱오픈 → 앱오픈 예측 (공급 무관) × 전환율(기기수) → 라이딩 예측

⚠ 데이터 소스 주의:
    bike_accessibility_raw = **AOS(Android) 전용** 앱오픈 로그
    → iOS 앱오픈은 미포함 (전체 라이딩의 ~55%가 non-AOS 사용자)

    보정 계수 분해:
        rides_per_open (1.64) = aos_rides_per_open (0.74) × os_scale (2.21)
        ① AOS 순수 전환: 앱오픈 → 라이딩 (0.74, CV 2.3% 안정)
        ② OS 스케일: AOS 점유율 ~45% → 전체 라이딩 환산 (2.21, CV 2.9%)
        ③ region별 차이 큼: 마포 AOS 35% vs 대구수성 62% → region별 보정 필수

Pipeline:
    ① 앱오픈 예측 (region 일별) ← lag 블렌딩 + 날씨/요일 보정
    ② district 배분 ← 앱오픈 비율 (라이딩 비율이 아님!)
    ③ hour 배분 ← 앱오픈 시간대 패턴
    ④ 보정 계수 적용 ← region별 rides_per_open (AOS→전체 환산 내포)
    ⑤ 공급 효과 반영 ← cvr(현재공급) / cvr(평균공급)
    ⑥ 라이딩 예측 = 앱오픈 × rides_per_open × supply_adj

What-if 시뮬레이션:
    "이 구역에 기기 N대 추가하면 라이딩이 몇 건 늘어나는가?"
    → 앱오픈은 그대로, bike_count만 변경하여 전환율 재계산

사용법:
    from app_open_model import AppOpenPredictor
    predictor = AppOpenPredictor()
    result = predictor.predict('2026-02-25')

    # What-if: 구역에 기기 3대 추가
    sim = predictor.simulate_bike_change(result, {'서울강남_역삼': 3})

    # 보정 계수 진단
    predictor.diagnose_calibration('2026-02-25')

    # CLI
    python app_open_model.py --date 2026-02-25
    python app_open_model.py --date 2026-02-25 --compare    # V7 vs V8
    python app_open_model.py --date 2026-02-25 --validate   # 과거 검증
    python app_open_model.py --date 2026-02-25 --diagnose   # 보정 계수 진단
    python app_open_model.py --date 2026-02-25 --simulate "서울강남_역삼:+3"
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

# Region params (center 매핑 + 날씨 보정)
REGION_PARAMS_PATH = os.path.join(SCRIPT_DIR, 'region_params.json')

# ============================================================
# Lag 블렌딩 가중치 (quick_predict와 동일 구조)
# ============================================================
LAG_WEIGHTS = {
    'same_dow_avg': 0.35,    # 같은 요일 4주 평균
    'rolling_7d':   0.30,    # 최근 7일 평균
    'lag7d':        0.20,    # 지난주 같은 요일
    'lag1d':        0.10,    # 어제
    'historical':   0.05,    # 장기 평균
}

# 날씨 보정 감쇠 (앱오픈은 라이딩보다 날씨 영향 적음)
WEATHER_DAMPENING = 0.6

# 성장 팩터 범위
GROWTH_FACTOR_RANGE = (0.85, 1.25)

# District hourly profile 최소 데이터
MIN_APP_OPENS_FOR_DISTRICT_PROFILE = 50


class AppOpenPredictor:
    """
    앱 오픈 기반 수요 예측기 (V8)

    기존 DistrictHourPredictor(V7)의 순환 문제를 해결:
    - V7: 과거 라이딩 → 라이딩 예측 (공급 제약 내재)
    - V8: 과거 앱오픈 → 앱오픈 예측 × 전환율(기기수) → 라이딩 예측

    핵심 차이:
    1. region 예측: 라이딩 lag → 앱오픈 lag (공급 무관 신호)
    2. district 비율: 라이딩 비율 → 앱오픈 비율 (수요 위치 반영)
    3. 최종 출력: app_opens × conversion_rate = predicted_rides
    """

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self._client = None
        self._region_params = self._load_region_params()
        self._conv_model = None
        self._region_predictions = None
        self._district_ratios = None
        self._hourly_profiles = None

    def _load_region_params(self) -> Dict:
        """region_params.json 로드 (center 매핑 + 날씨 보정)"""
        if os.path.exists(REGION_PARAMS_PATH):
            try:
                with open(REGION_PARAMS_PATH, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    @property
    def client(self):
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client()
        return self._client

    @property
    def conv_model(self):
        if self._conv_model is None:
            from conversion_model import ConversionModel
            self._conv_model = ConversionModel(verbose=False)
        return self._conv_model

    # ================================================================
    # Step 1: Region 일별 앱오픈 예측
    # ================================================================

    def predict_region_daily(self, target_date: str) -> pd.DataFrame:
        """
        region 일별 앱오픈 예측 (lag 블렌딩 + 날씨/요일 보정)

        quick_predict()와 동일한 lag 블렌딩 구조이나,
        대상이 '라이딩'이 아닌 '앱오픈'이다.

        Returns:
            DataFrame[region, center, predicted_opens,
                      same_dow_avg, rolling_7d, lag7d, lag1d, correction]
        """
        if self._region_predictions is not None:
            return self._region_predictions

        target = pd.Timestamp(target_date)

        if self.verbose:
            print(f"  [Step 1] Region 일별 앱오픈 예측")

        # 35일 조회 (4주 + 7일 여유)
        end_date = (target - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (target - timedelta(days=35)).strftime('%Y-%m-%d')

        query = f"""
        SELECT
            h3_area_name as region,
            date,
            COUNT(*) as app_opens
        FROM `service.app_accessibility`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND h3_area_name IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1, 2
        """

        df = self.client.query(query).to_dataframe()

        if self.verbose:
            n_regions = df['region'].nunique()
            total_days = df['date'].nunique()
            print(f"    데이터: {n_regions}개 region × {total_days}일")

        # Lag features per region
        target_dow = target.dayofweek  # 0=월 ~ 6=일

        results = []
        for region in df['region'].unique():
            rdf = df[df['region'] == region].sort_values('date')
            rdf = rdf.copy()
            rdf['date'] = pd.to_datetime(rdf['date'])
            rdf = rdf.set_index('date')['app_opens']

            # --- Lag features ---
            # lag1d: 어제
            lag_1d = float(rdf.iloc[-1]) if len(rdf) >= 1 else None

            # lag7d: 지난주 같은 요일
            lag_7d = float(rdf.iloc[-7]) if len(rdf) >= 7 else None

            # same_dow_avg: 같은 요일 최근 4주 평균
            same_dow = rdf[rdf.index.dayofweek == target_dow]
            same_dow_avg = float(same_dow.tail(4).mean()) if len(same_dow) >= 1 else None

            # rolling_7d: 최근 7일 평균
            rolling_7d = float(rdf.tail(7).mean()) if len(rdf) >= 7 else None

            # historical: 전체 기간 평균
            historical_avg = float(rdf.mean())

            # --- 가중 블렌딩 ---
            components = {
                'same_dow_avg': same_dow_avg,
                'rolling_7d': rolling_7d,
                'lag7d': lag_7d,
                'lag1d': lag_1d,
                'historical': historical_avg,
            }

            predicted = 0.0
            total_weight = 0.0
            for comp_name, comp_val in components.items():
                if comp_val is not None and not np.isnan(comp_val):
                    w = LAG_WEIGHTS[comp_name]
                    predicted += w * comp_val
                    total_weight += w

            if total_weight > 0:
                predicted /= total_weight
            else:
                predicted = historical_avg if historical_avg else 0

            # --- 성장 팩터 (최근 추세 반영) ---
            if rolling_7d and historical_avg and historical_avg > 0:
                growth = rolling_7d / historical_avg
                growth = np.clip(growth, *GROWTH_FACTOR_RANGE)
                predicted *= growth

            # --- 날씨/요일 보정 ---
            correction = self._get_day_weather_correction(region, target_date)
            predicted *= correction

            # Center 매핑
            center = self._region_params.get(region, {}).get('center', '')

            results.append({
                'region': region,
                'center': center,
                'predicted_opens': round(predicted, 1),
                'same_dow_avg': round(same_dow_avg, 1) if same_dow_avg is not None else None,
                'rolling_7d': round(rolling_7d, 1) if rolling_7d is not None else None,
                'lag7d': lag_7d,
                'lag1d': lag_1d,
                'correction': round(correction, 3),
            })

        result_df = pd.DataFrame(results)
        self._region_predictions = result_df

        if self.verbose:
            total = result_df['predicted_opens'].sum()
            print(f"    -> {len(result_df)}개 region, "
                  f"총 {total:,.0f}건 앱오픈 예측")

        return result_df

    def _get_day_weather_correction(
        self, region: str, target_date: str
    ) -> float:
        """
        요일 + 날씨 보정 팩터

        region_params.json의 보정값을 재활용하되,
        앱오픈은 라이딩보다 날씨 영향이 적으므로 WEATHER_DAMPENING 적용.
        """
        rp = self._region_params.get(region, {})
        if not rp:
            return 1.0

        target = pd.Timestamp(target_date)
        dow = target.dayofweek  # 0=월 ~ 6=일

        # --- 요일 보정 ---
        day_adj = 0.0
        is_holiday = False
        try:
            from korean_holidays import ADDITIONAL_HOLIDAYS
            is_holiday = target.date() in ADDITIONAL_HOLIDAYS
        except ImportError:
            pass

        if is_holiday:
            day_adj = rp.get('sun', -0.1)
        elif dow == 5:  # 토요일
            day_adj = rp.get('sat', 0)
        elif dow == 6:  # 일요일
            day_adj = rp.get('sun', 0)
        elif dow == 0:  # 월요일
            day_adj = rp.get('mon', 0)

        # --- 날씨 보정 ---
        weather_adj = 0.0
        try:
            sys.path.insert(0, SCRIPT_DIR)
            from visualize_prediction_map import _get_weather_for_date
            from demand_model_v7 import load_weather_data

            weather_csv = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')
            weather_data = (
                load_weather_data(weather_csv)
                if os.path.exists(weather_csv) else {}
            )
            weather, _ = _get_weather_for_date(target_date, weather_data)

            temp_low = weather.get('temp_low', 5)
            snow_cm = weather.get('snow_cm', 0)

            if snow_cm >= 1:
                weather_adj = rp.get('snow', -0.3)
            elif temp_low <= -8:
                weather_adj = rp.get('cold', -0.08)
            elif temp_low <= 0:
                weather_adj = rp.get('freeze', -0.15)
        except Exception:
            pass

        # 앱오픈은 날씨 영향 적음
        weather_adj *= WEATHER_DAMPENING

        return 1.0 + day_adj + weather_adj

    # ================================================================
    # Step 2: District 비율 (앱오픈 기반 — V7과의 핵심 차이)
    # ================================================================

    def get_district_ratios(self, target_date: str) -> pd.DataFrame:
        """
        앱오픈 기반 district 비율 (30일)

        V7과의 핵심 차이:
        - V7: tf_riding에서 라이딩 비율 → 기기 없으면 라이딩 0 → 비율 0
        - V8: bike_accessibility_raw에서 앱오픈 비율 → 기기 없어도 앱 열면 반영됨

        Returns:
            DataFrame[region, district, app_opens, lat, lng, region_total, ratio]
        """
        if self._district_ratios is not None:
            return self._district_ratios

        target = pd.Timestamp(target_date)
        end_date = (target - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (target - timedelta(days=30)).strftime('%Y-%m-%d')

        if self.verbose:
            print(f"  [Step 2] District 앱오픈 비율 ({start_date}~{end_date})")

        query = f"""
        SELECT
            h3_area_name as region,
            h3_district_name as district,
            COUNT(*) as app_opens,
            AVG(ST_Y(location)) as lat,
            AVG(ST_X(location)) as lng
        FROM `service.app_accessibility`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND h3_area_name IS NOT NULL
            AND h3_district_name IS NOT NULL
        GROUP BY 1, 2
        """

        df = self.client.query(query).to_dataframe()

        # Region 내 비율 계산
        region_totals = df.groupby('region')['app_opens'].sum().reset_index()
        region_totals.columns = ['region', 'region_total']
        df = df.merge(region_totals, on='region')
        df['ratio'] = df['app_opens'] / df['region_total']

        self._district_ratios = df

        if self.verbose:
            n_regions = df['region'].nunique()
            n_districts = len(df)
            print(f"    -> {n_regions}개 region, {n_districts}개 district")

        return df

    # ================================================================
    # Step 3: 시간대 프로필 (앱오픈 시간대 패턴)
    # ================================================================

    def get_hourly_profile(self, target_date: str) -> pd.DataFrame:
        """
        시간대별 앱오픈 비율

        DistrictHourPredictor와 동일한 로직 (이미 앱오픈 기반).
        요일 타입별 패턴 조회 + district fallback.

        Returns:
            DataFrame[region, district, hour, total_app_opens,
                      hour_ratio, profile_source]
        """
        if self._hourly_profiles is not None:
            return self._hourly_profiles

        target = pd.Timestamp(target_date)
        dow = target.dayofweek

        # 공휴일 체크
        try:
            from korean_holidays import ADDITIONAL_HOLIDAYS
            is_holiday = target.date() in ADDITIONAL_HOLIDAYS
        except ImportError:
            is_holiday = False

        # 요일 타입 → BigQuery DAYOFWEEK 필터
        if is_holiday or dow == 6:
            day_type = 'sunday_holiday'
            day_filter = "EXTRACT(DAYOFWEEK FROM event_time) = 1"
        elif dow == 5:
            day_type = 'saturday'
            day_filter = "EXTRACT(DAYOFWEEK FROM event_time) = 7"
        else:
            day_type = 'weekday'
            day_filter = "EXTRACT(DAYOFWEEK FROM event_time) IN (2,3,4,5,6)"

        end_date = (target - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (target - timedelta(days=28)).strftime('%Y-%m-%d')

        if self.verbose:
            print(f"  [Step 3] 시간대 프로필 ({day_type})")

        query = f"""
        WITH district_hourly AS (
            SELECT
                h3_area_name as region,
                h3_district_name as district,
                EXTRACT(HOUR FROM event_time) as hour,
                COUNT(*) as app_opens
            FROM `service.app_accessibility`
            WHERE DATE(event_time) BETWEEN '{start_date}' AND '{end_date}'
                AND {day_filter}
                AND h3_area_name IS NOT NULL
                AND h3_district_name IS NOT NULL
            GROUP BY 1, 2, 3
        ),
        district_totals AS (
            SELECT region, district, SUM(app_opens) as total_opens
            FROM district_hourly
            GROUP BY 1, 2
        ),
        region_hourly AS (
            SELECT region, hour, SUM(app_opens) as app_opens
            FROM district_hourly
            GROUP BY 1, 2
        ),
        region_totals AS (
            SELECT region, SUM(app_opens) as total_opens
            FROM region_hourly
            GROUP BY 1
        )
        SELECT
            dh.region,
            dh.district,
            dh.hour,
            dh.app_opens as total_app_opens,
            CASE
                WHEN dt.total_opens >= {MIN_APP_OPENS_FOR_DISTRICT_PROFILE}
                THEN SAFE_DIVIDE(dh.app_opens, dt.total_opens)
                ELSE SAFE_DIVIDE(rh.app_opens, rt.total_opens)
            END as hour_ratio,
            CASE
                WHEN dt.total_opens >= {MIN_APP_OPENS_FOR_DISTRICT_PROFILE}
                THEN 'district'
                ELSE 'region_fallback'
            END as profile_source
        FROM district_hourly dh
        JOIN district_totals dt
            ON dh.region = dt.region AND dh.district = dt.district
        LEFT JOIN region_hourly rh
            ON dh.region = rh.region AND dh.hour = rh.hour
        LEFT JOIN region_totals rt
            ON dh.region = rt.region
        ORDER BY dh.region, dh.district, dh.hour
        """

        df = self.client.query(query).to_dataframe()
        self._hourly_profiles = df

        if self.verbose:
            n_districts = df[['region', 'district']].drop_duplicates().shape[0]
            n_fallback = df[df['profile_source'] == 'region_fallback'][
                ['region', 'district']].drop_duplicates().shape[0]
            print(f"    -> {n_districts}개 district, fallback {n_fallback}개")

        return df

    # ================================================================
    # 앱오픈 → 라이딩 보정 계수
    # ================================================================

    def _fetch_calibration(
        self, target_date: str, lookback_days: int = 30
    ) -> pd.DataFrame:
        """
        Region별 앱오픈→라이딩 보정 계수 (AOS→전체 환산 내포)

        bike_accessibility_raw = AOS(Android) 전용 앱오픈 로그이므로,
        전체 라이딩(AOS+iOS)과 비교 시 rides_per_open > 1.0이 됨.

        분해:
            rides_per_open = (aos_rides / aos_opens) × (total_rides / aos_rides)
                           = 순수AOS전환(0.74) × OS스케일(2.21)
                           ≈ 1.64

        Region별 차이:
            - 서울마포(iOS 多): rides_per_open ≈ 2.16
            - 대구수성(AOS 多): rides_per_open ≈ 1.37

        수식:
            rides_per_open = sum(total_rides) / sum(aos_opens)  (per region)

        최종 예측:
            predicted_rides = aos_opens × rides_per_open
                              × cvr(current_supply) / cvr(avg_supply)

        Returns:
            DataFrame[region, app_opens_30d, rides_30d, rides_per_open,
                      avg_bike_count]
        """
        target = pd.Timestamp(target_date)
        end_date = (target - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (target - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        query = f"""
        WITH daily_opens AS (
            SELECT
                h3_area_name as region,
                date,
                COUNT(*) as app_opens,
                AVG(bike_count_100) as avg_bike_count
            FROM `service.app_accessibility`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
                AND h3_area_name IS NOT NULL
            GROUP BY 1, 2
        ),
        daily_rides AS (
            SELECT
                h3_start_area_name as region,
                DATE(start_time) as date,
                COUNT(*) as rides
            FROM `service.rides`
            WHERE DATE(start_time) BETWEEN '{start_date}' AND '{end_date}'
                AND h3_start_area_name IS NOT NULL
            GROUP BY 1, 2
        )
        SELECT
            o.region,
            SUM(o.app_opens) as app_opens_30d,
            SUM(COALESCE(r.rides, 0)) as rides_30d,
            SAFE_DIVIDE(SUM(COALESCE(r.rides, 0)), SUM(o.app_opens))
                as rides_per_open,
            AVG(o.avg_bike_count) as avg_bike_count
        FROM daily_opens o
        LEFT JOIN daily_rides r ON o.region = r.region AND o.date = r.date
        GROUP BY 1
        HAVING SUM(o.app_opens) >= 100
        """

        if self.verbose:
            print(f"  [Step 4a] 보정 계수 조회 (AOS앱오픈 → 전체라이딩)")

        df = self.client.query(query).to_dataframe()

        if self.verbose and len(df) > 0:
            total_opens = df['app_opens_30d'].sum()
            total_rides = df['rides_30d'].sum()
            global_rpo = total_rides / total_opens if total_opens > 0 else 0
            rpo_std = df['rides_per_open'].std()
            rpo_cv = rpo_std / df['rides_per_open'].mean() * 100 if df['rides_per_open'].mean() > 0 else 0
            print(f"    30일: AOS앱오픈 {total_opens:,.0f} → 전체라이딩 {total_rides:,.0f}")
            print(f"    보정계수: 전체평균 {global_rpo:.3f}, "
                  f"region별 CV {rpo_cv:.0f}% "
                  f"(min {df['rides_per_open'].min():.2f} ~ "
                  f"max {df['rides_per_open'].max():.2f})")

        return df

    # ================================================================
    # 보정 계수 진단 (AOS/OS 분해)
    # ================================================================

    def diagnose_calibration(
        self, target_date: str, lookback_days: int = 30, top_n: int = 20
    ) -> Dict:
        """
        보정 계수(rides_per_open)를 AOS 전환율 × OS 스케일로 분해 진단

        bike_accessibility_raw의 user_id로 AOS 사용자를 식별하고,
        tf_riding에서 해당 사용자의 라이딩을 분리하여 분해.

        분해:
            rides_per_open = aos_rides/aos_opens × total_rides/aos_rides
            (1.64)         = (0.74)              × (2.21)
                           = 순수AOS전환         × OS스케일(1/AOS점유율)

        Returns:
            dict with global + region-level decomposition
        """
        target = pd.Timestamp(target_date)
        end_date = (target - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (target - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        print(f"\n{'='*65}")
        print(f"  보정 계수 진단 (AOS/OS 분해): {start_date} ~ {end_date}")
        print(f"{'='*65}")

        query = f"""
        WITH aos_users AS (
            SELECT DISTINCT user_id
            FROM `service.app_accessibility`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
                AND user_id IS NOT NULL
        ),
        region_opens AS (
            SELECT h3_area_name as region, COUNT(*) as aos_opens
            FROM `service.app_accessibility`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
                AND h3_area_name IS NOT NULL
            GROUP BY 1
        ),
        region_rides AS (
            SELECT
                r.h3_start_area_name as region,
                COUNTIF(a.user_id IS NOT NULL) as aos_rides,
                COUNTIF(a.user_id IS NULL) as non_aos_rides,
                COUNT(*) as total_rides
            FROM `service.rides` r
            LEFT JOIN aos_users a ON r.user_id = a.user_id
            WHERE DATE(r.start_time) BETWEEN '{start_date}' AND '{end_date}'
                AND r.h3_start_area_name IS NOT NULL
            GROUP BY 1
        )
        SELECT
            COALESCE(o.region, rd.region) as region,
            COALESCE(o.aos_opens, 0) as aos_opens,
            COALESCE(rd.aos_rides, 0) as aos_rides,
            COALESCE(rd.non_aos_rides, 0) as non_aos_rides,
            COALESCE(rd.total_rides, 0) as total_rides,
            SAFE_DIVIDE(rd.aos_rides, rd.total_rides) as aos_ride_share,
            SAFE_DIVIDE(rd.aos_rides, o.aos_opens) as aos_rides_per_open,
            SAFE_DIVIDE(rd.total_rides, o.aos_opens) as rides_per_open,
            SAFE_DIVIDE(rd.total_rides, rd.aos_rides) as os_scale
        FROM region_opens o
        FULL OUTER JOIN region_rides rd ON o.region = rd.region
        WHERE COALESCE(o.aos_opens, 0) >= 100
        ORDER BY total_rides DESC
        """

        df = self.client.query(query).to_dataframe()

        # Global summary
        total_aos_opens = df['aos_opens'].sum()
        total_aos_rides = df['aos_rides'].sum()
        total_non_aos = df['non_aos_rides'].sum()
        total_rides = df['total_rides'].sum()

        g_aos_cvr = total_aos_rides / total_aos_opens if total_aos_opens > 0 else 0
        g_os_scale = total_rides / total_aos_rides if total_aos_rides > 0 else 1
        g_rpo = total_rides / total_aos_opens if total_aos_opens > 0 else 0
        g_aos_share = total_aos_rides / total_rides if total_rides > 0 else 0

        print(f"\n  [전체 요약]")
        print(f"    AOS 앱오픈:      {total_aos_opens:>12,}건")
        print(f"    AOS 라이딩:      {total_aos_rides:>12,}건")
        print(f"    non-AOS 라이딩:  {total_non_aos:>12,}건")
        print(f"    전체 라이딩:     {total_rides:>12,}건")
        print()
        print(f"  [보정 계수 분해]")
        print(f"    rides_per_open = aos_cvr × os_scale")
        print(f"    {g_rpo:.4f}        = {g_aos_cvr:.4f}  × {g_os_scale:.4f}")
        print(f"    AOS 라이딩 점유율: {g_aos_share*100:.1f}%")
        print(f"    non-AOS 점유율:    {(1-g_aos_share)*100:.1f}%")

        # Stability
        shares = df['aos_ride_share'].dropna()
        rpos = df['rides_per_open'].dropna()
        print(f"\n  [region별 편차]")
        print(f"    AOS 점유율  — "
              f"평균 {shares.mean()*100:.1f}%, "
              f"CV {shares.std()/shares.mean()*100:.0f}%, "
              f"범위 {shares.min()*100:.0f}~{shares.max()*100:.0f}%")
        print(f"    rides_per_open — "
              f"평균 {rpos.mean():.3f}, "
              f"CV {rpos.std()/rpos.mean()*100:.0f}%, "
              f"범위 {rpos.min():.2f}~{rpos.max():.2f}")

        # Region detail
        print(f"\n  [Region별 상세 (상위 {top_n})]")
        print(f"  {'region':>18} {'AOS열림':>8} {'AOS탑승':>8} {'non-AOS':>8} "
              f"{'전체':>8} {'AOS%':>6} {'AOS전환':>8} {'OS배율':>7} {'보정계수':>8}")
        print(f"  {'-'*95}")

        for _, row in df.head(top_n).iterrows():
            print(f"  {row['region']:>18} "
                  f"{row['aos_opens']:>8,.0f} "
                  f"{row['aos_rides']:>8,.0f} "
                  f"{row['non_aos_rides']:>8,.0f} "
                  f"{row['total_rides']:>8,.0f} "
                  f"{row['aos_ride_share']*100:>5.1f}% "
                  f"{row.get('aos_rides_per_open', 0):>8.3f} "
                  f"{row.get('os_scale', 0):>7.3f} "
                  f"{row.get('rides_per_open', 0):>8.3f}")

        return {
            'global': {
                'aos_opens': int(total_aos_opens),
                'aos_rides': int(total_aos_rides),
                'non_aos_rides': int(total_non_aos),
                'total_rides': int(total_rides),
                'aos_conversion': round(g_aos_cvr, 4),
                'os_scale': round(g_os_scale, 4),
                'rides_per_open': round(g_rpo, 4),
                'aos_ride_share': round(g_aos_share, 4),
            },
            'region_df': df,
        }

    # ================================================================
    # Step 4: 전체 예측 오케스트레이션
    # ================================================================

    def predict(self, target_date: str) -> pd.DataFrame:
        """
        앱오픈 기반 District x Hour 수요 예측

        수식:
            predicted_rides = app_opens × rides_per_open
                              × cvr(current_bikes) / cvr(avg_bikes)

        - rides_per_open: 실제 라이딩/앱오픈 비율 (보정 계수)
        - cvr 비율: 현재 기기수 vs 과거 평균의 전환율 차이 (공급 효과)

        Returns:
            DataFrame columns:
                region, district, hour,
                app_opens,              # 앱오픈 예측
                avg_bike_count,         # 평균 가용기기수
                conversion_rate,        # 전환율
                predicted_rides,        # 예상 라이딩 (보정 적용)
                unconstrained_demand,   # 잠재수요 (최대 전환율)
                suppressed_demand,      # 억제수요
                lat, lng, center, day_type
        """
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"  V8 앱오픈 기반 수요 예측: {target_date}")
            print(f"{'='*60}")

        # 1. Region 일별 앱오픈
        region_pred = self.predict_region_daily(target_date)

        # 2. District 비율 (앱오픈 기반)
        district_ratios = self.get_district_ratios(target_date)

        # 3. 시간대 프로필
        hourly_profile = self.get_hourly_profile(target_date)

        # 4. 보정 계수 + 가용기기수
        calibration = self._fetch_calibration(target_date)

        if self.verbose:
            print(f"  [Step 4b] 가용기기수 + 전환율")

        try:
            avg_bikes = self.conv_model.get_avg_bike_counts(lookback_days=90)
        except Exception as e:
            if self.verbose:
                print(f"    !! 가용기기수 조회 실패: {e}")
            avg_bikes = pd.DataFrame()

        # P5: 시간대별 기온 보정
        hourly_temp_factors, apply_temp = self._get_temp_factors(target_date)

        # 5. 조합
        if self.verbose:
            print(f"  [Step 5] 예측 조합")

        results = []
        missing_districts = 0

        for _, pred_row in region_pred.iterrows():
            region = pred_row['region']
            region_opens = pred_row['predicted_opens']
            center = pred_row['center']

            # 해당 region의 district들
            dists = district_ratios[district_ratios['region'] == region]
            if len(dists) == 0:
                missing_districts += 1
                continue

            for _, d_row in dists.iterrows():
                district = d_row['district']
                d_ratio = d_row['ratio']
                d_lat = d_row['lat']
                d_lng = d_row['lng']

                district_opens = region_opens * d_ratio

                # 시간대 프로필
                hours = hourly_profile[
                    (hourly_profile['region'] == region) &
                    (hourly_profile['district'] == district)
                ]

                if len(hours) == 0:
                    # Region fallback
                    hours = hourly_profile[
                        hourly_profile['region'] == region
                    ]
                    hours = hours.groupby('hour').agg({
                        'total_app_opens': 'sum',
                        'hour_ratio': 'mean',
                    }).reset_index()

                if len(hours) == 0:
                    # 전역 시간대 프로필 fallback (과거 패턴 기반)
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
                        h_opens = district_opens * ratio
                        if apply_temp:
                            h_opens *= hourly_temp_factors.get(h, 1.0)
                        results.append({
                            'region': region, 'district': district,
                            'hour': h, 'app_opens': round(h_opens, 2),
                            'lat': d_lat, 'lng': d_lng, 'center': center,
                        })
                    continue

                # 시간대 비율 정규화 (합 = 1.0)
                hours = hours.copy()
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
                        _total = sum(_GLOBAL_HOUR_RATIOS.values())
                        hours['hour_ratio_norm'] = hours['hour'].apply(
                            lambda h: _GLOBAL_HOUR_RATIOS.get(int(h), 0.02) / _total
                        )

                for _, h_row in hours.iterrows():
                    h = int(h_row['hour'])
                    h_ratio = h_row['hour_ratio_norm']

                    h_opens = district_opens * h_ratio
                    if apply_temp:
                        h_opens *= hourly_temp_factors.get(h, 1.0)

                    results.append({
                        'region': region, 'district': district,
                        'hour': h, 'app_opens': round(h_opens, 2),
                        'lat': d_lat, 'lng': d_lng, 'center': center,
                    })

        result_df = pd.DataFrame(results)

        if len(result_df) == 0:
            if self.verbose:
                print("  !! 결과 없음")
            return result_df

        # Day type
        target_ts = pd.Timestamp(target_date)
        dow = target_ts.dayofweek
        try:
            from korean_holidays import ADDITIONAL_HOLIDAYS
            is_holiday = target_ts.date() in ADDITIONAL_HOLIDAYS
        except ImportError:
            is_holiday = False

        if is_holiday or dow == 6:
            result_df['day_type'] = 'sunday_holiday'
        elif dow == 5:
            result_df['day_type'] = 'saturday'
        else:
            result_df['day_type'] = 'weekday'

        # 6. 전환율 적용 + 보정 → 라이딩 예측
        result_df = self._apply_conversion(result_df, avg_bikes, calibration)

        if self.verbose:
            total_opens = result_df['app_opens'].sum()
            total_rides = result_df['predicted_rides'].sum()
            total_unc = result_df['unconstrained_demand'].sum()
            total_supp = result_df['suppressed_demand'].sum()
            n_districts = result_df[['region', 'district']].drop_duplicates().shape[0]
            avg_cvr = (total_rides / total_opens * 100) if total_opens > 0 else 0

            print(f"\n  [결과] {n_districts}개 district "
                  f"x {result_df['hour'].nunique()}개 hour "
                  f"= {len(result_df):,}행")
            print(f"     앱오픈 예측: {total_opens:,.0f}건")
            print(f"     라이딩 예측: {total_rides:,.0f}건 "
                  f"(평균 전환율 {avg_cvr:.1f}%)")
            print(f"     잠재수요:   {total_unc:,.0f}건 "
                  f"(억제 {total_supp:,.0f}건)")
            if missing_districts > 0:
                print(f"     !! district 누락: {missing_districts}개 region")

        return result_df

    def _get_temp_factors(
        self, target_date: str
    ) -> Tuple[Dict[int, float], bool]:
        """시간대별 기온 보정 팩터 (P5 재사용)"""
        try:
            from district_hour_model import get_hourly_temp_factors
            from visualize_prediction_map import _get_weather_for_date
            from demand_model_v7 import load_weather_data

            weather_csv = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')
            weather_data = (
                load_weather_data(weather_csv)
                if os.path.exists(weather_csv) else {}
            )
            weather, _ = _get_weather_for_date(target_date, weather_data)
            temp_low = weather.get('temp_low', 5)
            temp_high = weather.get('temp_high', 10)

            target_ts = pd.Timestamp(target_date)
            is_weekday = target_ts.dayofweek < 5
            try:
                from korean_holidays import ADDITIONAL_HOLIDAYS
                if target_ts.date() in ADDITIONAL_HOLIDAYS:
                    is_weekday = False
            except ImportError:
                pass

            factors = get_hourly_temp_factors(
                temp_low, temp_high, is_weekday=is_weekday)
            f_range = max(factors.values()) - min(factors.values())
            apply = f_range > 0.02
            return factors, apply

        except Exception:
            return {h: 1.0 for h in range(24)}, False

    # ================================================================
    # 전환율 적용 (앱오픈 → 라이딩)
    # ================================================================

    def _apply_conversion(
        self,
        result_df: pd.DataFrame,
        avg_bikes: pd.DataFrame,
        calibration: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        전환율 모델 + 보정 계수로 앱오픈 → 라이딩 변환

        수식:
            base_rides = app_opens × rides_per_open  (보정된 기본 예측)
            supply_adj = cvr(current_bikes) / cvr(region_avg_bikes)
            predicted_rides = base_rides × supply_adj

        - rides_per_open: 앱오픈 대비 실제 라이딩 비율 (데이터 소스 간 보정)
        - supply_adj: 현재 기기수의 전환율 / 해당 region 과거 평균 기기수의 전환율
          → 기기가 평균보다 많으면 >1, 적으면 <1
        """
        # --- 보정 계수 매칭 ---
        if len(calibration) > 0:
            result_df = result_df.merge(
                calibration[['region', 'rides_per_open', 'avg_bike_count']].rename(
                    columns={'avg_bike_count': '_cal_avg_bikes'}),
                on='region', how='left'
            )
            # fallback: 전체 평균 보정 계수
            global_rpo = (
                calibration['rides_30d'].sum()
                / calibration['app_opens_30d'].sum()
            ) if calibration['app_opens_30d'].sum() > 0 else 1.0
            result_df['rides_per_open'] = result_df['rides_per_open'].fillna(global_rpo)
            result_df['_cal_avg_bikes'] = result_df['_cal_avg_bikes'].fillna(
                calibration['avg_bike_count'].mean())
        else:
            result_df['rides_per_open'] = 1.0
            result_df['_cal_avg_bikes'] = 1.5

        # --- 가용기기수 매칭 (district×hour 레벨) ---
        if len(avg_bikes) > 0:
            result_df = result_df.merge(
                avg_bikes[['region', 'district', 'hour', 'avg_bike_count_100']],
                on=['region', 'district', 'hour'],
                how='left'
            )
            global_avg_bikes = avg_bikes['avg_bike_count_100'].mean()
            result_df['avg_bike_count'] = result_df[
                'avg_bike_count_100'].fillna(global_avg_bikes)
            result_df.drop(
                columns=['avg_bike_count_100'], inplace=True, errors='ignore')
        else:
            result_df['avg_bike_count'] = 1.5

        # --- 전환율 계산 (region별, 시간대별) ---
        result_df['conversion_rate'] = 0.0
        result_df['_max_rate'] = 0.0
        result_df['_avg_cvr'] = 0.0  # region 과거 평균 기기수의 전환율

        for region in result_df['region'].unique():
            mask = result_df['region'] == region
            bikes = result_df.loc[mask, 'avg_bike_count'].values
            hours = result_df.loc[mask, 'hour'].values
            is_commute = np.isin(hours, [7, 8, 9, 17, 18, 19])

            # region의 과거 평균 기기수
            cal_avg = result_df.loc[mask, '_cal_avg_bikes'].iloc[0]

            rates = np.zeros(mask.sum())
            max_r = np.zeros(mask.sum())
            avg_r = np.zeros(mask.sum())

            for seg_mask, seg in [
                (is_commute, 'commute'),
                (~is_commute, 'leisure'),
            ]:
                if seg_mask.any():
                    rates[seg_mask] = self.conv_model.predict_conversion_rate(
                        bikes[seg_mask], seg, region)
                    max_r[seg_mask] = self.conv_model.get_max_conversion_rate(
                        seg, region)
                    avg_r[seg_mask] = float(
                        self.conv_model.predict_conversion_rate(
                            cal_avg, seg, region))

            result_df.loc[mask, 'conversion_rate'] = np.round(rates, 4)
            result_df.loc[mask, '_max_rate'] = max_r
            result_df.loc[mask, '_avg_cvr'] = avg_r

        # --- 라이딩 예측 (보정 적용) ---
        # base = app_opens × rides_per_open (보정된 기본값)
        # supply_adj = cvr(current) / cvr(avg) (공급 효과)
        safe_avg_cvr = np.where(
            result_df['_avg_cvr'] > 0, result_df['_avg_cvr'], 1.0)
        supply_adj = result_df['conversion_rate'].values / safe_avg_cvr

        result_df['predicted_rides'] = (
            result_df['app_opens']
            * result_df['rides_per_open']
            * supply_adj
        ).round(2)

        # --- 잠재수요 (최대 전환율 적용) ---
        max_supply_adj = result_df['_max_rate'].values / safe_avg_cvr
        result_df['unconstrained_demand'] = (
            result_df['app_opens']
            * result_df['rides_per_open']
            * max_supply_adj
        ).round(2)

        # --- 억제수요 ---
        result_df['suppressed_demand'] = (
            result_df['unconstrained_demand'] - result_df['predicted_rides']
        ).clip(lower=0).round(2)

        # 정리
        result_df.drop(
            columns=['_max_rate', '_avg_cvr', '_cal_avg_bikes'],
            inplace=True, errors='ignore')

        return result_df

    # ================================================================
    # What-if 시뮬레이션
    # ================================================================

    def simulate_bike_change(
        self,
        pred_df: pd.DataFrame,
        changes: Dict[str, float],
    ) -> pd.DataFrame:
        """
        기기 추가/제거 시뮬레이션

        앱오픈은 그대로, bike_count만 변경하여 전환율 재계산.
        "이 구역에 기기 3대 추가하면 라이딩이 몇 건 늘어나는가?"

        Args:
            pred_df: predict() 결과 DataFrame
            changes: {district_name: delta_bikes}
                     양수 = 기기 추가, 음수 = 기기 제거

        Returns:
            DataFrame with additional columns:
                new_bike_count, new_conversion_rate,
                new_predicted_rides, delta_rides
        """
        sim_df = pred_df.copy()
        sim_df['new_bike_count'] = sim_df['avg_bike_count']

        for district, delta in changes.items():
            mask = sim_df['district'] == district
            sim_df.loc[mask, 'new_bike_count'] = (
                sim_df.loc[mask, 'avg_bike_count'] + delta
            ).clip(lower=0)

        # 새 전환율 계산 + 보정 적용
        sim_df['new_conversion_rate'] = 0.0

        for region in sim_df['region'].unique():
            mask = sim_df['region'] == region
            bikes = sim_df.loc[mask, 'new_bike_count'].values
            hours = sim_df.loc[mask, 'hour'].values
            is_commute = np.isin(hours, [7, 8, 9, 17, 18, 19])

            rates = np.zeros(mask.sum())
            for seg_mask, seg in [
                (is_commute, 'commute'),
                (~is_commute, 'leisure'),
            ]:
                if seg_mask.any():
                    rates[seg_mask] = self.conv_model.predict_conversion_rate(
                        bikes[seg_mask], seg, region)

            sim_df.loc[mask, 'new_conversion_rate'] = np.round(rates, 4)

        # predicted_rides는 이미 rides_per_open 보정이 적용되어 있으므로
        # 새 라이딩은 기존 예측 × (새 전환율 / 기존 전환율) 비율로 조정
        safe_cvr = np.where(
            sim_df['conversion_rate'] > 0,
            sim_df['conversion_rate'], 1.0)
        sim_df['new_predicted_rides'] = (
            sim_df['predicted_rides']
            * sim_df['new_conversion_rate'] / safe_cvr
        ).round(2)

        sim_df['delta_rides'] = (
            sim_df['new_predicted_rides'] - sim_df['predicted_rides']
        ).round(2)

        return sim_df

    # ================================================================
    # V7 vs V8 비교
    # ================================================================

    def compare_with_v7(self, target_date: str) -> pd.DataFrame:
        """
        V7 (라이딩 기반) vs V8 (앱오픈 기반) 예측 비교

        공급 제약 지역에서 V8이 더 높은 예측을 내는지 확인.
        """
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"  V7 vs V8 비교: {target_date}")
            print(f"{'='*60}")

        # V8 예측
        v8 = self.predict(target_date)
        v8_district = v8.groupby(['region', 'district']).agg({
            'app_opens': 'sum',
            'predicted_rides': 'sum',
            'unconstrained_demand': 'sum',
            'suppressed_demand': 'sum',
            'avg_bike_count': 'mean',
            'conversion_rate': 'mean',
            'lat': 'first',
            'lng': 'first',
            'center': 'first',
        }).reset_index()
        v8_district.columns = [
            'region', 'district', 'v8_opens', 'v8_rides',
            'v8_unconstrained', 'v8_suppressed', 'avg_bikes', 'avg_cvr',
            'lat', 'lng', 'center',
        ]

        # V7 예측
        from district_hour_model import DistrictHourPredictor
        v7_pred = DistrictHourPredictor(verbose=False)
        v7 = v7_pred.predict(target_date)
        v7_district = v7.groupby(['region', 'district']).agg({
            'predicted_rides': 'sum',
        }).reset_index()
        v7_district.columns = ['region', 'district', 'v7_rides']

        # 병합
        compare = v8_district.merge(
            v7_district, on=['region', 'district'], how='outer')
        compare['v7_rides'] = compare['v7_rides'].fillna(0)
        compare['v8_rides'] = compare['v8_rides'].fillna(0)
        compare['diff'] = compare['v8_rides'] - compare['v7_rides']
        compare['diff_pct'] = np.where(
            compare['v7_rides'] > 0,
            (compare['diff'] / compare['v7_rides'] * 100).round(1),
            0,
        )

        if self.verbose:
            v7_total = compare['v7_rides'].sum()
            v8_total = compare['v8_rides'].sum()
            diff_total = v8_total - v7_total

            print(f"\n  V7 총 예측: {v7_total:,.0f}건")
            print(f"  V8 총 예측: {v8_total:,.0f}건")
            print(f"  차이: {diff_total:+,.0f}건 "
                  f"({diff_total / v7_total * 100:+.1f}%)"
                  if v7_total > 0 else "")

            # V8 > V7 (공급 제약으로 V7이 과소추정)
            higher = compare[compare['diff'] > 1].sort_values(
                'diff', ascending=False)
            if len(higher) > 0:
                print(f"\n  V8 > V7 상위 (공급 제약 의심):")
                print(f"  {'권역':>14} {'구역':>14} "
                      f"{'V7':>7} {'V8':>7} {'차이':>7} {'억제':>7} {'기기':>5}")
                print(f"  {'-'*72}")
                for _, row in higher.head(15).iterrows():
                    print(f"  {row['region']:>14} {row['district']:>14} "
                          f"{row['v7_rides']:>7,.0f} {row['v8_rides']:>7,.0f} "
                          f"{row['diff']:>+7,.0f} "
                          f"{row.get('v8_suppressed', 0):>7,.0f} "
                          f"{row.get('avg_bikes', 0):>4.1f}대")

            # V7 > V8 (V7이 과대추정)
            lower = compare[compare['diff'] < -1].sort_values('diff')
            if len(lower) > 0:
                print(f"\n  V7 > V8 상위 (V7 과대추정 가능):")
                for _, row in lower.head(10).iterrows():
                    print(f"  {row['region']:>14} {row['district']:>14} "
                          f"{row['v7_rides']:>7,.0f} {row['v8_rides']:>7,.0f} "
                          f"{row['diff']:>+7,.0f}")

        return compare

    # ================================================================
    # 검증 (과거 실제와 비교)
    # ================================================================

    def validate(self, target_date: str) -> Dict:
        """과거 날짜로 예측 → 실제 라이딩과 비교"""
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"  V8 예측 검증: {target_date}")
            print(f"{'='*60}")

        pred_df = self.predict(target_date)

        # 실제 라이딩
        query = f"""
        SELECT
            h3_start_area_name as region,
            h3_start_district_name as district,
            EXTRACT(HOUR FROM start_time) as hour,
            COUNT(*) as actual_rides
        FROM `service.rides`
        WHERE DATE(start_time) = '{target_date}'
            AND h3_start_area_name IS NOT NULL
            AND h3_start_district_name IS NOT NULL
        GROUP BY 1, 2, 3
        """

        try:
            actual_df = self.client.query(query).to_dataframe()
        except Exception as e:
            print(f"  !! 실제 데이터 조회 실패: {e}")
            return {'error': str(e)}

        if len(actual_df) == 0:
            print(f"  !! {target_date} 실제 데이터 없음")
            return {'error': 'no actual data'}

        # 비교
        merged = pred_df.merge(
            actual_df, on=['region', 'district', 'hour'], how='outer')
        merged['predicted_rides'] = merged['predicted_rides'].fillna(0)
        merged['actual_rides'] = merged['actual_rides'].fillna(0)

        total_pred = merged['predicted_rides'].sum()
        total_actual = merged['actual_rides'].sum()
        overall_error = (
            (total_pred / total_actual - 1) * 100
            if total_actual > 0 else 0
        )

        # District MAPE
        d_sum = merged.groupby(['region', 'district']).agg({
            'predicted_rides': 'sum', 'actual_rides': 'sum',
        }).reset_index()
        d_sum = d_sum[d_sum['actual_rides'] > 0]
        d_mape = None
        if len(d_sum) > 0:
            d_sum['ape'] = (
                (d_sum['predicted_rides'] - d_sum['actual_rides']).abs()
                / d_sum['actual_rides'] * 100
            )
            d_mape = d_sum['ape'].mean()

        # Hour MAPE
        h_sum = merged.groupby('hour').agg({
            'predicted_rides': 'sum', 'actual_rides': 'sum',
        }).reset_index()
        h_sum = h_sum[h_sum['actual_rides'] > 0]
        h_mape = None
        if len(h_sum) > 0:
            h_sum['ape'] = (
                (h_sum['predicted_rides'] - h_sum['actual_rides']).abs()
                / h_sum['actual_rides'] * 100
            )
            h_mape = h_sum['ape'].mean()

        if self.verbose:
            print(f"\n  실제: {total_actual:,.0f}건  "
                  f"예측: {total_pred:,.0f}건  "
                  f"오차: {overall_error:+.1f}%")
            if d_mape is not None:
                print(f"  District MAPE: {d_mape:.1f}%")
            if h_mape is not None:
                print(f"  Hour MAPE: {h_mape:.1f}%")

            print(f"\n  {'시간':>4} {'예측':>8} {'실제':>8} {'오차':>8}")
            print(f"  {'-'*32}")
            for _, h in h_sum.sort_values('hour').iterrows():
                err = (
                    (h['predicted_rides'] - h['actual_rides'])
                    / h['actual_rides'] * 100
                    if h['actual_rides'] > 0 else 0
                )
                print(f"  {int(h['hour']):>4}시 "
                      f"{h['predicted_rides']:>8,.0f} "
                      f"{h['actual_rides']:>8,.0f} "
                      f"{err:>+7.1f}%")

        return {
            'total_actual': int(total_actual),
            'total_predicted': round(total_pred),
            'overall_error_pct': round(overall_error, 1),
            'district_mape': round(d_mape, 1) if d_mape else None,
            'hour_mape': round(h_mape, 1) if h_mape else None,
        }


# ================================================================
# CLI
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description='앱 오픈 기반 수요 예측 (V8)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
    python app_open_model.py --date 2026-02-25
    python app_open_model.py --date 2026-02-25 --validate
    python app_open_model.py --date 2026-02-25 --compare
    python app_open_model.py --date 2026-02-25 --simulate "서울강남_역삼:+3"
    python app_open_model.py --date 2026-02-25 --top 20
        """
    )
    parser.add_argument('--date', type=str, default='2026-02-25',
                        help='대상 날짜 (YYYY-MM-DD)')
    parser.add_argument('--validate', action='store_true',
                        help='과거 날짜 검증')
    parser.add_argument('--compare', action='store_true',
                        help='V7 vs V8 비교')
    parser.add_argument('--diagnose', action='store_true',
                        help='보정 계수 진단 (AOS/OS 분해)')
    parser.add_argument('--simulate', type=str, default=None,
                        help='기기 추가 시뮬레이션 ("구역:+N,구역:+N")')
    parser.add_argument('--export-csv', type=str, default=None,
                        help='CSV 내보내기')
    parser.add_argument('--top', type=int, default=None,
                        help='수요 상위 N개 district')

    args = parser.parse_args()

    predictor = AppOpenPredictor(verbose=True)

    if args.validate:
        predictor.validate(args.date)
        return

    if args.compare:
        predictor.compare_with_v7(args.date)
        return

    if args.diagnose:
        predictor.diagnose_calibration(args.date)
        return

    # 기본 예측
    pred_df = predictor.predict(args.date)

    if len(pred_df) == 0:
        return

    # --- 시뮬레이션 ---
    if args.simulate:
        changes = {}
        for item in args.simulate.split(','):
            parts = item.strip().split(':')
            if len(parts) == 2:
                changes[parts[0].strip()] = float(parts[1])

        if changes:
            sim_df = predictor.simulate_bike_change(pred_df, changes)

            print(f"\n{'='*60}")
            print(f"  기기 변경 시뮬레이션")
            print(f"{'='*60}")

            for district, delta in changes.items():
                d_sim = sim_df[sim_df['district'] == district]
                if len(d_sim) > 0:
                    old_rides = d_sim['predicted_rides'].sum()
                    new_rides = d_sim['new_predicted_rides'].sum()
                    delta_r = new_rides - old_rides
                    old_bikes = d_sim['avg_bike_count'].mean()
                    new_bikes = d_sim['new_bike_count'].mean()

                    print(f"\n  {district}")
                    print(f"    기기: {old_bikes:.1f} -> {new_bikes:.1f} "
                          f"({delta:+.0f}대)")
                    print(f"    라이딩: {old_rides:.0f} -> {new_rides:.0f} "
                          f"({delta_r:+.0f}건)")
                else:
                    print(f"\n  {district}: 데이터 없음")
        return

    # --- CSV ---
    if args.export_csv:
        pred_df.to_csv(args.export_csv, index=False, encoding='utf-8-sig')
        print(f"\n  CSV: {args.export_csv}")
        return

    # --- 상위 district ---
    if args.top:
        daily = pred_df.groupby(['region', 'district', 'center']).agg({
            'app_opens': 'sum',
            'predicted_rides': 'sum',
            'suppressed_demand': 'sum',
            'avg_bike_count': 'mean',
        }).reset_index().sort_values('predicted_rides', ascending=False)

        print(f"\n  수요 상위 {args.top}개 District (V8)")
        print(f"{'='*80}")
        print(f"{'순위':>4} {'센터':<8} {'권역':<14} {'구역':<16} "
              f"{'앱오픈':>8} {'라이딩':>8} {'억제':>8} {'기기':>6}")
        print(f"{'-'*78}")

        for rank, (_, row) in enumerate(
            daily.head(args.top).iterrows(), 1
        ):
            print(f"{rank:>4} {row['center']:<8} {row['region']:<14} "
                  f"{row['district']:<16} {row['app_opens']:>8,.0f} "
                  f"{row['predicted_rides']:>8,.0f} "
                  f"{row['suppressed_demand']:>8,.0f} "
                  f"{row['avg_bike_count']:>5.1f}대")
        return

    # --- 기본 출력: 시간대별 요약 ---
    print(f"\n  시간대별 수요 요약 (V8)")
    print(f"{'='*50}")

    hourly = pred_df.groupby('hour').agg({
        'app_opens': 'sum',
        'predicted_rides': 'sum',
    }).reset_index().sort_values('hour')

    max_rides = hourly['predicted_rides'].max()
    print(f"\n{'시간':>4} {'앱오픈':>10} {'라이딩':>10} {'전환율':>8}")
    print(f"{'-'*36}")

    for _, row in hourly.iterrows():
        cvr = (row['predicted_rides'] / row['app_opens'] * 100
               if row['app_opens'] > 0 else 0)
        bar_len = (
            int(row['predicted_rides'] / max_rides * 20)
            if max_rides > 0 else 0
        )
        bar = '#' * bar_len
        print(f"{int(row['hour']):>4}시 {row['app_opens']:>10,.0f} "
              f"{row['predicted_rides']:>10,.0f} {cvr:>7.1f}% {bar}")

    # 센터별 요약
    print(f"\n  센터별 수요 요약 (V8)")
    print(f"{'='*60}")

    center_sum = pred_df.groupby('center').agg({
        'app_opens': 'sum',
        'predicted_rides': 'sum',
        'suppressed_demand': 'sum',
        'district': 'nunique',
    }).reset_index().sort_values('predicted_rides', ascending=False)

    print(f"\n{'센터':<12} {'앱오픈':>10} {'라이딩':>10} "
          f"{'억제':>10} {'district':>8}")
    print(f"{'-'*54}")
    for _, row in center_sum.iterrows():
        print(f"{row['center']:<12} {row['app_opens']:>10,.0f} "
              f"{row['predicted_rides']:>10,.0f} "
              f"{row['suppressed_demand']:>10,.0f} "
              f"{row['district']:>8}")


if __name__ == '__main__':
    main()
