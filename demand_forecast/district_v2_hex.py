"""
District → H3 Hex 공간 분포 예측 — Production v2 기반

아키텍처:
    1. production_v2_predictor로 district 일별 예측
    2. 과거 28일 라이딩 위치 데이터로 district 내 hex별 비율 계산
    3. district_pred × hex_ratio = hex별 예측 라이딩

옵션으로 시간대(window)별 배분도 결합 가능:
    district_pred × window_ratio × hex_ratio_within_window
    = hex × window 예측

사용법:
    from district_v2_hex import DistrictV2Hex
    predictor = DistrictV2Hex()

    # 단일 날짜 예측 (district → hex 배분)
    result = predictor.predict('2026-02-27')

    # 시간대×hex 결합 예측
    result = predictor.predict('2026-02-27', with_window=True)

    # 기간 평가
    eval_df = predictor.evaluate_period('2026-02-16', '2026-02-22')

    # CLI
    python district_v2_hex.py --date 2026-02-27
    python district_v2_hex.py --evaluate 2026-02-16 2026-02-22
"""
import os
import sys
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
# 상수
# ============================================================

MIN_RIDES_FOR_HEX_PROFILE = 20   # district 내 최소 라이딩 (28일)
MIN_HEX_RIDES = 3                # hex별 최소 건수 (이하면 제외)
TOP_N_HEX_DEFAULT = 20           # 기본 반환 hex 수 (per district)
H3_RESOLUTION = 9                # H3 해상도 (~174m 반경)


# ============================================================
# 메인 클래스
# ============================================================

class DistrictV2Hex:
    """
    Production v2 district 예측 → H3 hex 공간 배분

    핵심 로직:
        district_daily_pred × hex_share_within_district
                              = hex별 예측 라이딩

    hex_share는 과거 28일 실제 라이딩 위치에서 계산.
    district별 데이터 부족 시 균등 배분으로 fallback.
    """

    def __init__(self, verbose: bool = True, top_n: int = TOP_N_HEX_DEFAULT):
        self.verbose = verbose
        self.top_n = top_n
        self._client = None
        self._profile_cache = {}

    @property
    def client(self):
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client()
        return self._client

    # ================================================================
    # Step 1: Production v2 일별 district 예측 (hourly와 동일)
    # ================================================================

    def get_v2_daily_predictions(self, target_date: str) -> pd.DataFrame:
        """production_v2_predictor의 district 일별 예측 호출"""
        from production_v2_predictor import predict_district_rides

        district_df, region_df = predict_district_rides(target_date)

        if district_df is None or len(district_df) == 0:
            if self.verbose:
                print(f"  ⚠️ v2 예측 실패: {target_date}")
            return pd.DataFrame()

        result = district_df[['region', 'district', 'adj_pred',
                              'lat', 'lng', 'center']].copy()
        result.columns = ['region', 'district', 'predicted_rides_daily',
                          'lat', 'lng', 'center']
        result['date'] = target_date
        return result

    # ================================================================
    # Step 2: Hex 공간 프로필 (BQ에서 실제 라이딩 위치 분포)
    # ================================================================

    def get_hex_profiles(self, target_date: str) -> pd.DataFrame:
        """
        과거 28일 라이딩 위치 → district 내 hex별 비율

        요일 타입(weekday/saturday/sunday)별로 분리.
        district 내 MIN_RIDES 미만이면 전체 요일 프로필로 fallback.

        Returns:
            DataFrame[region, district, h3_index, hex_ratio,
                      hex_rides, district_total, hex_lat, hex_lng,
                      profile_source, hex_rank]
        """
        target = pd.Timestamp(target_date)
        dow = target.dayofweek

        # 공휴일 확인
        try:
            from korean_holidays import ADDITIONAL_HOLIDAYS
            is_holiday = target.date() in ADDITIONAL_HOLIDAYS
        except ImportError:
            is_holiday = False

        # 요일 타입
        if is_holiday or dow == 6:
            day_type = 'sunday_holiday'
            day_filter = "EXTRACT(DAYOFWEEK FROM start_time) = 1"
        elif dow == 5:
            day_type = 'saturday'
            day_filter = "EXTRACT(DAYOFWEEK FROM start_time) = 7"
        else:
            day_type = 'weekday'
            day_filter = "EXTRACT(DAYOFWEEK FROM start_time) IN (2,3,4,5,6)"

        # 기간
        end_date = (target - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (target - timedelta(days=28)).strftime('%Y-%m-%d')
        cache_key = (day_type, start_date, end_date)

        if cache_key in self._profile_cache:
            return self._profile_cache[cache_key]

        if self.verbose:
            print(f"  📍 Hex 프로필 조회 ({day_type}, {start_date}~{end_date})")

        # ── BQ 쿼리: district × hex별 라이딩 수 + 좌표 ──
        query = f"""
        WITH hex_rides AS (
            SELECT
                h3_start_area_name as region,
                h3_start_district_name as district,
                udf.geo_to_h3(
                    ST_Y(start_location),
                    ST_X(start_location),
                    {H3_RESOLUTION}
                ) as h3_index,
                AVG(ST_Y(start_location)) as hex_lat,
                AVG(ST_X(start_location)) as hex_lng,
                COUNT(*) as rides
            FROM `service.rides`
            WHERE DATE(start_time) BETWEEN '{start_date}' AND '{end_date}'
                AND {day_filter}
                AND h3_start_area_name IS NOT NULL
                AND h3_start_district_name IS NOT NULL
                AND start_location IS NOT NULL
            GROUP BY 1, 2, 3
        ),
        district_totals AS (
            SELECT region, district, SUM(rides) as district_total
            FROM hex_rides
            GROUP BY 1, 2
        )
        SELECT
            h.region, h.district, h.h3_index,
            h.hex_lat, h.hex_lng,
            h.rides as hex_rides,
            d.district_total,
            SAFE_DIVIDE(h.rides, d.district_total) as hex_ratio
        FROM hex_rides h
        JOIN district_totals d USING (region, district)
        WHERE h.rides >= {MIN_HEX_RIDES}
        ORDER BY h.region, h.district, h.rides DESC
        """

        raw = self.client.query(query).to_dataframe()

        if len(raw) == 0:
            if self.verbose:
                print(f"  ⚠️ Hex 프로필 데이터 없음")
            return pd.DataFrame()

        # ── Fallback: 요일별 데이터 부족 → 전체 요일 프로필 ──
        # district별 충분한 데이터가 있는지 확인
        insufficient = raw.groupby(['region', 'district'])['district_total'].first()
        need_fallback = insufficient[insufficient < MIN_RIDES_FOR_HEX_PROFILE].index

        if len(need_fallback) > 0 and day_filter != 'TRUE':
            if self.verbose:
                print(f"  ↩️ {len(need_fallback)}개 district → 전체요일 fallback")

            fallback_query = f"""
            WITH hex_rides AS (
                SELECT
                    h3_start_area_name as region,
                    h3_start_district_name as district,
                    udf.geo_to_h3(
                        ST_Y(start_location),
                        ST_X(start_location),
                        {H3_RESOLUTION}
                    ) as h3_index,
                    AVG(ST_Y(start_location)) as hex_lat,
                    AVG(ST_X(start_location)) as hex_lng,
                    COUNT(*) as rides
                FROM `service.rides`
                WHERE DATE(start_time) BETWEEN '{start_date}' AND '{end_date}'
                    AND h3_start_area_name IS NOT NULL
                    AND h3_start_district_name IS NOT NULL
                    AND start_location IS NOT NULL
                GROUP BY 1, 2, 3
            ),
            district_totals AS (
                SELECT region, district, SUM(rides) as district_total
                FROM hex_rides
                GROUP BY 1, 2
            )
            SELECT
                h.region, h.district, h.h3_index,
                h.hex_lat, h.hex_lng,
                h.rides as hex_rides,
                d.district_total,
                SAFE_DIVIDE(h.rides, d.district_total) as hex_ratio
            FROM hex_rides h
            JOIN district_totals d USING (region, district)
            WHERE h.rides >= {MIN_HEX_RIDES}
            ORDER BY h.region, h.district, h.rides DESC
            """
            fallback_raw = self.client.query(fallback_query).to_dataframe()

            if len(fallback_raw) > 0:
                # 부족한 district만 fallback 데이터로 교체
                fb_districts = set(f"{r[0]}|{r[1]}" for r in need_fallback)
                raw_sufficient = raw[~raw.apply(
                    lambda r: f"{r['region']}|{r['district']}" in fb_districts, axis=1)]
                fallback_filtered = fallback_raw[fallback_raw.apply(
                    lambda r: f"{r['region']}|{r['district']}" in fb_districts, axis=1)]
                fallback_filtered = fallback_filtered.copy()
                fallback_filtered['_source'] = 'all_days'
                raw_sufficient = raw_sufficient.copy()
                raw_sufficient['_source'] = day_type
                raw = pd.concat([raw_sufficient, fallback_filtered], ignore_index=True)
            else:
                raw['_source'] = day_type
        else:
            raw['_source'] = day_type

        # ── District별 hex 정규화 + 랭킹 ──
        profiles = []
        for (region, district), grp in raw.groupby(['region', 'district']):
            grp = grp.sort_values('hex_rides', ascending=False).copy()

            # 비율 재정규화 (top_n 적용 후에도 합 = 1.0이 되도록)
            grp['hex_rank'] = range(1, len(grp) + 1)
            ratio_sum = grp['hex_ratio'].sum()
            if ratio_sum > 0:
                grp['hex_ratio'] = grp['hex_ratio'] / ratio_sum

            grp['profile_source'] = grp.get('_source', day_type)
            profiles.append(grp)

        if profiles:
            result = pd.concat(profiles, ignore_index=True)
        else:
            result = pd.DataFrame()

        if '_source' in result.columns:
            result.drop(columns=['_source'], inplace=True)

        self._profile_cache[cache_key] = result

        if self.verbose:
            n_districts = result[['region', 'district']].drop_duplicates().shape[0]
            n_hexes = result['h3_index'].nunique()
            print(f"  → {n_districts}개 district, {n_hexes:,}개 hex")
            # 상위 hex 집중도
            top5 = result[result['hex_rank'] <= 5].groupby(
                ['region', 'district'])['hex_ratio'].sum().mean()
            print(f"  → Top 5 hex 평균 점유율: {top5*100:.1f}%")

        return result

    # ================================================================
    # Step 3: 예측 — District daily × hex ratio
    # ================================================================

    def predict(self, target_date: str,
                with_window: bool = False,
                top_n: Optional[int] = None) -> pd.DataFrame:
        """
        District → Hex 공간 배분 예측

        Args:
            target_date: 예측 날짜
            with_window: True면 시간대(window)별 × hex 결합 예측
            top_n: district당 반환 hex 수 (None=self.top_n)

        Returns:
            DataFrame[date, region, district, h3_index,
                      predicted_rides, hex_ratio, hex_rank,
                      hex_lat, hex_lng, center,
                      (+ window, window_label if with_window)]
        """
        top_n = top_n or self.top_n

        if self.verbose:
            print(f"\n{'='*60}")
            mode = "Hex × Window" if with_window else "Hex"
            print(f"📍 District → {mode} 수요 예측: {target_date}")
            print(f"{'='*60}")

        # 1. v2 일별 district 예측
        daily = self.get_v2_daily_predictions(target_date)
        if len(daily) == 0:
            return pd.DataFrame()

        if self.verbose:
            total = daily['predicted_rides_daily'].sum()
            print(f"  [Step 1] v2 일별 예측: {len(daily)}개 district, 총 {total:,.0f}건")

        # 2. Hex 프로필
        hex_profiles = self.get_hex_profiles(target_date)
        if len(hex_profiles) == 0:
            if self.verbose:
                print(f"  ⚠️ Hex 프로필 없음")
            return pd.DataFrame()

        # 3. (옵션) Window 프로필
        window_profiles = None
        if with_window:
            from district_v2_hourly import DistrictV2Hourly, TIME_WINDOWS
            hourly = DistrictV2Hourly(verbose=False)
            window_profiles = hourly.get_window_profiles(target_date)
            if self.verbose:
                if len(window_profiles) > 0:
                    print(f"  [Step 3] Window 프로필 로드 완료")
                else:
                    print(f"  ⚠️ Window 프로필 없음 → 일별만 배분")
                    with_window = False

        # 4. 조합
        results = []
        matched = 0
        no_hex = 0

        for _, d_row in daily.iterrows():
            region = d_row['region']
            district = d_row['district']
            daily_pred = d_row['predicted_rides_daily']

            # 해당 district의 hex 프로필
            h_mask = ((hex_profiles['region'] == region) &
                      (hex_profiles['district'] == district))
            d_hexes = hex_profiles[h_mask].copy()

            if len(d_hexes) == 0:
                no_hex += 1
                # hex 프로필 없으면 district 중심점에 전체 배정
                if with_window:
                    from district_v2_hourly import TIME_WINDOWS
                    w_mask = ((window_profiles['region'] == region) &
                              (window_profiles['district'] == district))
                    d_windows = window_profiles[w_mask]
                    if len(d_windows) > 0:
                        for _, w_row in d_windows.iterrows():
                            results.append({
                                'date': target_date,
                                'region': region,
                                'district': district,
                                'h3_index': 'unknown',
                                'predicted_rides': round(daily_pred * w_row['ratio'], 2),
                                'hex_ratio': 1.0,
                                'hex_rank': 1,
                                'hex_lat': d_row['lat'],
                                'hex_lng': d_row['lng'],
                                'center': d_row.get('center', ''),
                                'window': w_row['window'],
                                'window_label': '',
                            })
                    else:
                        results.append({
                            'date': target_date,
                            'region': region,
                            'district': district,
                            'h3_index': 'unknown',
                            'predicted_rides': round(daily_pred, 2),
                            'hex_ratio': 1.0,
                            'hex_rank': 1,
                            'hex_lat': d_row['lat'],
                            'hex_lng': d_row['lng'],
                            'center': d_row.get('center', ''),
                        })
                else:
                    results.append({
                        'date': target_date,
                        'region': region,
                        'district': district,
                        'h3_index': 'unknown',
                        'predicted_rides': round(daily_pred, 2),
                        'hex_ratio': 1.0,
                        'hex_rank': 1,
                        'hex_lat': d_row['lat'],
                        'hex_lng': d_row['lng'],
                        'center': d_row.get('center', ''),
                    })
                continue

            matched += 1

            # top_n 적용
            d_hexes_top = d_hexes[d_hexes['hex_rank'] <= top_n].copy()

            # 나머지 hex는 "기타"로 합산
            other_ratio = d_hexes[d_hexes['hex_rank'] > top_n]['hex_ratio'].sum()

            # top_n 내에서 비율 재정규화 (top_n + other = 1.0)
            top_ratio_sum = d_hexes_top['hex_ratio'].sum()

            if with_window and window_profiles is not None:
                from district_v2_hourly import TIME_WINDOWS
                w_mask = ((window_profiles['region'] == region) &
                          (window_profiles['district'] == district))
                d_windows = window_profiles[w_mask]

                if len(d_windows) == 0:
                    # window 프로필 없으면 균등 배분
                    from district_v2_hourly import TIME_WINDOWS as TW
                    for _, h_row in d_hexes_top.iterrows():
                        for wname, wdef in TW.items():
                            w_ratio = len(wdef['hours']) / 24.0
                            pred = daily_pred * h_row['hex_ratio'] * w_ratio
                            results.append({
                                'date': target_date,
                                'region': region,
                                'district': district,
                                'h3_index': h_row['h3_index'],
                                'predicted_rides': round(pred, 2),
                                'hex_ratio': h_row['hex_ratio'],
                                'hex_rank': h_row['hex_rank'],
                                'hex_lat': h_row['hex_lat'],
                                'hex_lng': h_row['hex_lng'],
                                'center': d_row.get('center', ''),
                                'window': wname,
                                'window_label': wdef['label'],
                            })
                else:
                    for _, h_row in d_hexes_top.iterrows():
                        for _, w_row in d_windows.iterrows():
                            pred = daily_pred * h_row['hex_ratio'] * w_row['ratio']
                            results.append({
                                'date': target_date,
                                'region': region,
                                'district': district,
                                'h3_index': h_row['h3_index'],
                                'predicted_rides': round(pred, 2),
                                'hex_ratio': h_row['hex_ratio'],
                                'hex_rank': h_row['hex_rank'],
                                'hex_lat': h_row['hex_lat'],
                                'hex_lng': h_row['hex_lng'],
                                'center': d_row.get('center', ''),
                                'window': w_row['window'],
                                'window_label': '',
                            })
            else:
                for _, h_row in d_hexes_top.iterrows():
                    pred = daily_pred * h_row['hex_ratio']
                    results.append({
                        'date': target_date,
                        'region': region,
                        'district': district,
                        'h3_index': h_row['h3_index'],
                        'predicted_rides': round(pred, 2),
                        'hex_ratio': h_row['hex_ratio'],
                        'hex_rank': h_row['hex_rank'],
                        'hex_lat': h_row['hex_lat'],
                        'hex_lng': h_row['hex_lng'],
                        'center': d_row.get('center', ''),
                    })

                # "기타" hex 합산 행
                if other_ratio > 0:
                    pred = daily_pred * other_ratio
                    # 기타 hex의 평균 좌표
                    other_hexes = d_hexes[d_hexes['hex_rank'] > top_n]
                    results.append({
                        'date': target_date,
                        'region': region,
                        'district': district,
                        'h3_index': 'other',
                        'predicted_rides': round(pred, 2),
                        'hex_ratio': other_ratio,
                        'hex_rank': top_n + 1,
                        'hex_lat': other_hexes['hex_lat'].mean(),
                        'hex_lng': other_hexes['hex_lng'].mean(),
                        'center': d_row.get('center', ''),
                    })

        result_df = pd.DataFrame(results)

        if self.verbose and len(result_df) > 0:
            n_dist = result_df[['region', 'district']].drop_duplicates().shape[0]
            n_hex = result_df[result_df['h3_index'] != 'other']['h3_index'].nunique()
            total_pred = result_df['predicted_rides'].sum()
            print(f"\n  ✅ 결과: {n_dist}개 district → {n_hex:,}개 hex")
            print(f"     총 예측: {total_pred:,.0f}건 "
                  f"(매칭 {matched}, hex없음 {no_hex})")

            # Top hex 예측 비중
            top5_pred = result_df[result_df['hex_rank'] <= 5]['predicted_rides'].sum()
            print(f"     Top 5 hex 예측 비중: {top5_pred/total_pred*100:.1f}%")

        return result_df

    # ================================================================
    # 평가
    # ================================================================

    def evaluate_date(self, target_date: str, top_n: Optional[int] = None) -> Dict:
        """
        단일 날짜 hex 예측 vs 실제 비교

        Returns:
            {date, total_pred, total_actual, daily_error_pct,
             hex_mape, hex_coverage, district_count, hex_count}
        """
        top_n = top_n or self.top_n
        pred_df = self.predict(target_date, top_n=top_n)
        if len(pred_df) == 0:
            return {'date': target_date, 'error': 'no predictions'}

        # 실제 hex 라이딩
        query = f"""
        SELECT
            h3_start_area_name as region,
            h3_start_district_name as district,
            udf.geo_to_h3(
                ST_Y(start_location),
                ST_X(start_location),
                {H3_RESOLUTION}
            ) as h3_index,
            COUNT(*) as actual_rides
        FROM `service.rides`
        WHERE DATE(start_time) = '{target_date}'
            AND h3_start_area_name IS NOT NULL
            AND h3_start_district_name IS NOT NULL
            AND start_location IS NOT NULL
        GROUP BY 1, 2, 3
        """

        try:
            actual = self.client.query(query).to_dataframe()
        except Exception as e:
            return {'date': target_date, 'error': str(e)}

        if len(actual) == 0:
            return {'date': target_date, 'error': 'no actual data'}

        # ── 1. District 수준 정확도 (기존 모델 참고) ──
        actual_district = actual.groupby(['region', 'district'])[
            'actual_rides'].sum().reset_index()
        pred_district = pred_df.groupby(['region', 'district'])[
            'predicted_rides'].sum().reset_index()

        d_merged = pred_district.merge(actual_district, on=['region', 'district'], how='inner')
        d_merged = d_merged[d_merged['actual_rides'] > 0]
        d_merged['ape'] = (d_merged['predicted_rides'] - d_merged['actual_rides']).abs() / d_merged['actual_rides'] * 100
        district_mape = d_merged['ape'].mean() if len(d_merged) > 0 else None

        # ── 2. Hex 수준 정확도 ──
        pred_hex = pred_df[~pred_df['h3_index'].isin(['unknown', 'other'])].copy()
        merged = pred_hex.merge(
            actual, on=['region', 'district', 'h3_index'], how='inner')
        merged = merged[merged['actual_rides'] > 0]

        if len(merged) > 0:
            merged['ape'] = ((merged['predicted_rides'] - merged['actual_rides']).abs()
                             / merged['actual_rides'] * 100)
            hex_mape = merged['ape'].mean()

            # 가중 MAPE (라이딩 수 가중)
            merged['weighted_ape'] = merged['ape'] * merged['actual_rides']
            hex_wmape = merged['weighted_ape'].sum() / merged['actual_rides'].sum()
        else:
            hex_mape = None
            hex_wmape = None

        # ── 3. Coverage (예측한 hex가 실제의 몇 %를 커버하는지) ──
        total_actual = actual['actual_rides'].sum()
        covered_actual = merged['actual_rides'].sum() if len(merged) > 0 else 0
        coverage = covered_actual / total_actual * 100 if total_actual > 0 else 0

        # ── 4. 규모별 MAPE ──
        size_mapes = {}
        if len(merged) > 0:
            bins = [0, 5, 10, 20, 50, float('inf')]
            labels = ['1-5', '6-10', '11-20', '21-50', '50+']
            merged['size_group'] = pd.cut(merged['actual_rides'], bins=bins, labels=labels)
            for grp_name, grp_data in merged.groupby('size_group', observed=True):
                if len(grp_data) > 0:
                    size_mapes[str(grp_name)] = {
                        'count': len(grp_data),
                        'mape': round(grp_data['ape'].mean(), 1),
                    }

        total_pred = pred_df['predicted_rides'].sum()

        result = {
            'date': target_date,
            'total_pred': round(total_pred),
            'total_actual': int(total_actual),
            'daily_error_pct': round((total_pred - total_actual) / total_actual * 100, 1)
                               if total_actual > 0 else 0,
            'district_mape': round(district_mape, 1) if district_mape else None,
            'hex_mape': round(hex_mape, 1) if hex_mape else None,
            'hex_wmape': round(hex_wmape, 1) if hex_wmape else None,
            'hex_coverage': round(coverage, 1),
            'matched_hexes': len(merged),
            'total_pred_hexes': len(pred_hex),
            'total_actual_hexes': actual['h3_index'].nunique(),
            'size_mapes': size_mapes,
        }

        if self.verbose:
            print(f"\n  📊 {target_date} Hex 평가:")
            print(f"     총합: 예측 {total_pred:,.0f} / 실제 {total_actual:,.0f} "
                  f"({result['daily_error_pct']:+.1f}%)")
            if district_mape:
                print(f"     District MAPE: {district_mape:.1f}%")
            if hex_mape:
                print(f"     Hex MAPE: {hex_mape:.1f}% (가중: {hex_wmape:.1f}%)")
            print(f"     Coverage: {coverage:.1f}% "
                  f"({len(merged)}/{actual['h3_index'].nunique()} hex)")
            if size_mapes:
                print(f"\n     규모별 MAPE:")
                for grp, info in size_mapes.items():
                    print(f"       {grp}건: {info['mape']:.1f}% ({info['count']}개)")

        return result

    def evaluate_period(self, start_date: str, end_date: str) -> pd.DataFrame:
        """기간 평가"""
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"📍 Hex 기간 평가: {start_date} ~ {end_date}")
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

        valid = [r for r in results if 'error' not in r]

        if self.verbose and valid:
            print(f"\n{'='*60}")
            print(f"📋 Hex 기간 평가 요약 ({len(valid)}일)")
            print(f"{'='*60}")

            print(f"\n{'날짜':<12} {'일오차':>7} {'D-MAPE':>8} "
                  f"{'H-MAPE':>8} {'H-wMAPE':>8} {'커버리지':>8}")
            print(f"{'-'*55}")

            for r in valid:
                d_mape = r.get('district_mape')
                h_mape = r.get('hex_mape')
                hw_mape = r.get('hex_wmape')
                cov = r.get('hex_coverage', 0)
                d_err = r.get('daily_error_pct', 0)

                d_str = f"{d_mape:.1f}%" if d_mape else '-'
                h_str = f"{h_mape:.1f}%" if h_mape else '-'
                hw_str = f"{hw_mape:.1f}%" if hw_mape else '-'

                print(f"{r['date']:<12} {d_err:>+6.1f}% {d_str:>8} "
                      f"{h_str:>8} {hw_str:>8} {cov:>7.1f}%")

            avg_d = np.mean([r['district_mape'] for r in valid
                             if r.get('district_mape')])
            avg_h = np.mean([r['hex_mape'] for r in valid
                             if r.get('hex_mape')])
            avg_hw = np.mean([r['hex_wmape'] for r in valid
                              if r.get('hex_wmape')])
            avg_cov = np.mean([r['hex_coverage'] for r in valid])
            avg_err = np.mean([r['daily_error_pct'] for r in valid])

            print(f"{'-'*55}")
            print(f"{'평균':<12} {avg_err:>+6.1f}% {avg_d:>7.1f}% "
                  f"{avg_h:>7.1f}% {avg_hw:>7.1f}% {avg_cov:>7.1f}%")

        return pd.DataFrame(valid)

    # ================================================================
    # 유틸리티
    # ================================================================

    def get_district_hex_summary(self, target_date: str,
                                 region: str = None,
                                 district: str = None) -> pd.DataFrame:
        """
        특정 district의 hex 분포 상세 (대시보드/디버깅용)
        """
        pred = self.predict(target_date)
        if len(pred) == 0:
            return pd.DataFrame()

        if district:
            pred = pred[pred['district'] == district]
        if region:
            pred = pred[pred['region'] == region]

        return pred.sort_values(['district', 'hex_rank'])


# ================================================================
# CLI
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description='District → H3 Hex 공간 수요 예측',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
    python district_v2_hex.py --date 2026-02-27
    python district_v2_hex.py --date 2026-02-27 --top 10
    python district_v2_hex.py --evaluate 2026-02-20 2026-02-26
    python district_v2_hex.py --date 2026-02-27 --export hex_pred.csv
        """
    )
    parser.add_argument('--date', type=str, default=None,
                        help='예측 대상 날짜')
    parser.add_argument('--evaluate', nargs=2, metavar=('START', 'END'),
                        help='기간 평가')
    parser.add_argument('--top', type=int, default=TOP_N_HEX_DEFAULT,
                        help=f'district당 top hex 수 (default: {TOP_N_HEX_DEFAULT})')
    parser.add_argument('--export', type=str, default=None,
                        help='CSV 내보내기 경로')
    parser.add_argument('--with-window', action='store_true',
                        help='시간대별 결합 예측')

    args = parser.parse_args()
    predictor = DistrictV2Hex(verbose=True, top_n=args.top)

    if args.evaluate:
        eval_df = predictor.evaluate_period(args.evaluate[0], args.evaluate[1])
        if args.export and len(eval_df) > 0:
            eval_df.to_csv(args.export, index=False)
            print(f"\n💾 평가 결과 저장: {args.export}")
    elif args.date:
        result = predictor.predict(args.date, with_window=args.with_window)
        if len(result) > 0:
            print(f"\n📊 샘플 (상위 20행):")
            cols = ['district', 'h3_index', 'predicted_rides', 'hex_ratio', 'hex_rank']
            if 'window' in result.columns:
                cols.insert(2, 'window')
            print(result[cols].head(20).to_string(index=False))

            if args.export:
                result.to_csv(args.export, index=False)
                print(f"\n💾 저장: {args.export} ({len(result):,}행)")
    else:
        # 기본: 오늘 예측
        today = datetime.now().strftime('%Y-%m-%d')
        result = predictor.predict(today)
        if len(result) > 0:
            print(f"\n📊 샘플 (상위 20행):")
            cols = ['district', 'h3_index', 'predicted_rides', 'hex_ratio', 'hex_rank']
            print(result[cols].head(20).to_string(index=False))


if __name__ == '__main__':
    main()
