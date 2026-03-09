"""
전환율 모델 (Conversion Model)

bike_accessibility_raw에서 가용기기수 → 전환율 관계를 학습하고,
실현 라이딩으로부터 잠재(비제약) 수요를 역산한다.

핵심 수식:
    전환율 = base_rate + max_gain × (1 - e^(-decay × bike_count))
    잠재수요 = 실현라이딩 / 전환율(현재공급) × 최대전환율

세분화 계층:
    region (h3_area_name) → global (전체 평균)
    각 레벨에서 시간대 세분화: commute (7-9,17-19) / leisure (나머지)

사용법:
    from conversion_model import ConversionModel
    model = ConversionModel()
    model.fit()  # 90일 데이터로 학습 (global + region)
    model.predict_conversion_rate(3, region='서울강남권역')
    model.estimate_unconstrained(80, 2.5, region='서울강남권역')

    # CLI
    python conversion_model.py --fit
    python conversion_model.py --fit-region
    python conversion_model.py --test
    python conversion_model.py --drift  # 월별 파라미터 변화 분석
"""
import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from scipy.optimize import curve_fit

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARAMS_PATH = os.path.join(SCRIPT_DIR, 'conversion_params.json')

# Authentication: set GOOGLE_APPLICATION_CREDENTIALS env var
SCRIPT_DIR_CREDS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'credentials', 'service-account.json')
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    if os.path.exists(SCRIPT_DIR_CREDS):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SCRIPT_DIR_CREDS

# 기본 파라미터 (fit 전 fallback)
DEFAULT_PARAMS = {
    'base_rate': 0.363,
    'max_gain': 0.444,
    'decay': 0.3,
}

# 피팅 최소 샘플 수
MIN_SAMPLES_GLOBAL = 100
MIN_SAMPLES_REGION = 500
MIN_POINTS = 3  # 최소 bike_count 포인트 수


class ConversionModel:
    """
    가용기기수 → 전환율 모델

    bike_accessibility_raw 데이터에서:
    - bike_count_100 (100m 반경 가용기기수) → is_converted (전환 여부)
    관계를 학습한다.

    전환율 함수:
        f(n) = base_rate + max_gain × (1 - e^(-decay × n))
        - n=0: f(0) = base_rate (기기 0대여도 기본 전환 — 걸어가서 타는 경우 등)
        - n→∞: f(∞) = base_rate + max_gain (최대 전환율)

    파라미터 계층:
        region (h3_area_name별) → global (전체)
        predict 시 region이 있으면 우선 사용, 없으면 global fallback
    """

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self._client = None
        self.params = {}           # {segment: {base_rate, max_gain, decay}}
        self.region_params = {}    # {region_name: {segment: {base_rate, max_gain, decay}}}
        self.fitted = False
        self._load_params()

    def _load_params(self):
        """저장된 파라미터 로드"""
        if os.path.exists(PARAMS_PATH):
            try:
                with open(PARAMS_PATH, 'r') as f:
                    saved = json.load(f)

                # global/commute/leisure 파라미터
                for seg in ('global', 'commute', 'leisure'):
                    if seg in saved and isinstance(saved[seg], dict) and 'base_rate' in saved[seg]:
                        self.params[seg] = {
                            k: saved[seg][k]
                            for k in ('base_rate', 'max_gain', 'decay')
                            if k in saved[seg]
                        }

                # region 파라미터
                if 'regions' in saved and isinstance(saved['regions'], dict):
                    for region_name, region_data in saved['regions'].items():
                        self.region_params[region_name] = {}
                        for seg in ('global', 'commute', 'leisure'):
                            if seg in region_data and isinstance(region_data[seg], dict):
                                self.region_params[region_name][seg] = {
                                    k: region_data[seg][k]
                                    for k in ('base_rate', 'max_gain', 'decay')
                                    if k in region_data[seg]
                                }

                self.fitted = bool(self.params)
                if self.verbose and self.fitted:
                    fitted_at = saved.get('fitted_at', '?')
                    n_regions = len(self.region_params)
                    region_info = f", {n_regions}개 지역" if n_regions else ""
                    print(f"  📁 전환율 파라미터 로드 (fitted: {fitted_at}{region_info})")
            except Exception:
                self.params = {}
                self.region_params = {}
        if not self.params:
            self.params = {'global': DEFAULT_PARAMS.copy()}

    @property
    def client(self):
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client()
        return self._client

    # ================================================================
    # 전환율 함수
    # ================================================================

    @staticmethod
    def _conversion_func(bike_count, base_rate, max_gain, decay):
        """전환율 함수: base_rate + max_gain × (1 - e^(-decay × n))"""
        return base_rate + max_gain * (1 - np.exp(-decay * np.asarray(bike_count, dtype=float)))

    def _get_params(self, segment: str = 'global', region: str = None) -> dict:
        """
        파라미터 조회 (fallback 계층)
        region → global segment → DEFAULT_PARAMS
        """
        if region and region in self.region_params:
            rp = self.region_params[region]
            if segment in rp:
                return rp[segment]
            if 'global' in rp:
                return rp['global']
        return self.params.get(segment, self.params.get('global', DEFAULT_PARAMS))

    def predict_conversion_rate(
        self, bike_count, segment: str = 'global', region: str = None
    ) -> np.ndarray:
        """
        가용기기수 → 전환율 예측

        Args:
            bike_count: 가용기기수 (scalar 또는 array)
            segment: 'global', 'commute', 'leisure'
            region: h3_area_name (지역별 파라미터 사용)

        Returns:
            전환율 (0~1)
        """
        p = self._get_params(segment, region)
        return self._conversion_func(
            bike_count, p['base_rate'], p['max_gain'], p['decay'])

    def get_max_conversion_rate(self, segment: str = 'global', region: str = None) -> float:
        """이론적 최대 전환율 (기기 충분할 때)"""
        p = self._get_params(segment, region)
        return p['base_rate'] + p['max_gain']

    # ================================================================
    # 잠재수요 역산
    # ================================================================

    def estimate_unconstrained(
        self,
        realized_rides: float,
        avg_bike_count: float,
        segment: str = 'global',
        region: str = None
    ) -> float:
        """
        실현 라이딩 → 잠재(비제약) 수요 역산

        수식:
            앱오픈 추정 = realized / conversion_rate(current_supply)
            잠재수요 = 앱오픈 × max_conversion_rate

        Args:
            realized_rides: 실현(예측) 라이딩 수
            avg_bike_count: 해당 지점의 평균 가용기기수
            segment: 'global', 'commute', 'leisure'
            region: h3_area_name (지역별 파라미터)

        Returns:
            잠재수요 (realized 이상)
        """
        if realized_rides <= 0:
            return 0.0

        current_rate = float(self.predict_conversion_rate(avg_bike_count, segment, region))
        max_rate = self.get_max_conversion_rate(segment, region)

        if current_rate <= 0:
            return realized_rides

        unconstrained = realized_rides / current_rate * max_rate
        return max(unconstrained, realized_rides)

    def estimate_unconstrained_batch(
        self,
        realized_series: pd.Series,
        bike_count_series: pd.Series,
        hour_series: pd.Series = None,
        region_series: pd.Series = None
    ) -> pd.Series:
        """
        배치 잠재수요 역산 (DataFrame 컬럼 단위)

        region_series가 있으면 지역별 파라미터 적용.
        hour_series가 있으면 출퇴근/레저 세분화 적용.
        """
        result = pd.Series(0.0, index=realized_series.index)

        # region별 처리가 있는 경우
        if region_series is not None and self.region_params:
            for region_name in region_series.unique():
                mask = region_series == region_name
                if not mask.any():
                    continue
                sub_realized = realized_series[mask]
                sub_bikes = bike_count_series[mask]
                sub_hours = hour_series[mask] if hour_series is not None else None
                result[mask] = self._batch_for_segment(
                    sub_realized, sub_bikes, sub_hours, region_name)
        else:
            result = self._batch_for_segment(
                realized_series, bike_count_series, hour_series, None)

        return result.round(2)

    def _batch_for_segment(
        self,
        realized: pd.Series,
        bikes: pd.Series,
        hours: pd.Series,
        region: str
    ) -> pd.Series:
        """단일 region에 대한 배치 역산"""
        result = pd.Series(0.0, index=realized.index)

        if hours is not None and ('commute' in self.params or
                (region and region in self.region_params)):
            commute_mask = hours.isin([7, 8, 9, 17, 18, 19])
            for mask, seg in [(commute_mask, 'commute'), (~commute_mask, 'leisure')]:
                if mask.any():
                    rates = self.predict_conversion_rate(
                        bikes[mask].values, seg, region)
                    max_rate = self.get_max_conversion_rate(seg, region)
                    safe_rates = np.where(rates > 0, rates, 1.0)
                    unc = realized[mask].values / safe_rates * max_rate
                    result[mask] = np.maximum(unc, realized[mask].values)
        else:
            rates = self.predict_conversion_rate(bikes.values, 'global', region)
            max_rate = self.get_max_conversion_rate('global', region)
            safe_rates = np.where(rates > 0, rates, 1.0)
            unc = realized.values / safe_rates * max_rate
            result = pd.Series(
                np.maximum(unc, realized.values), index=realized.index)

        return result

    # ================================================================
    # 학습 (fit)
    # ================================================================

    def fit(self, lookback_days: int = 90, fit_regions: bool = False) -> Dict:
        """
        bike_accessibility_raw에서 전환율 커브 학습

        Args:
            lookback_days: 학습 데이터 기간 (기본 90일)
            fit_regions: True면 지역별 파라미터도 학습

        Returns:
            {segment: {base_rate, max_gain, decay, max_rate, r_squared, sample_size}}
        """
        if self.verbose:
            print(f"\n{'='*60}")
            region_label = " + 지역별" if fit_regions else ""
            print(f"📈 전환율 모델 학습{region_label} (최근 {lookback_days}일)")
            print(f"{'='*60}")

        # 데이터 조회
        data = self._fetch_conversion_data(lookback_days, include_region=fit_regions)
        if data is None or len(data) == 0:
            print("  ❌ 학습 데이터 없음")
            return {}

        # === Global fit ===
        results = self._fit_global(data)

        # === Region fit ===
        region_results = {}
        if fit_regions and 'h3_area_name' in data.columns:
            region_results = self._fit_all_regions(data)

        # 파라미터 업데이트
        for seg, res in results.items():
            self.params[seg] = {
                'base_rate': res['base_rate'],
                'max_gain': res['max_gain'],
                'decay': res['decay'],
            }

        self.region_params = {}
        for region_name, region_segs in region_results.items():
            self.region_params[region_name] = {}
            for seg, res in region_segs.items():
                self.region_params[region_name][seg] = {
                    'base_rate': res['base_rate'],
                    'max_gain': res['max_gain'],
                    'decay': res['decay'],
                }

        self.fitted = True

        # 저장
        self.save_params(results, region_results)

        return results

    def _fit_global(self, data: pd.DataFrame) -> Dict:
        """전체(global) + 시간대별 피팅"""
        results = {}

        # 전환율 = 전환건수 / 전체 앱오픈
        global_data = data.groupby('bike_count_100').agg(
            total=('total_events', 'sum'),
            converted=('converted_events', 'sum'),
        ).reset_index()
        global_data['conversion_rate'] = (
            global_data['converted'] / global_data['total'].clip(lower=1))
        global_data = global_data[global_data['total'] >= MIN_SAMPLES_GLOBAL]

        global_result = self._fit_segment(global_data, 'global')
        if global_result:
            results['global'] = global_result

        # Commute vs Leisure
        for segment, hour_filter in [
            ('commute', [7, 8, 9, 17, 18, 19]),
            ('leisure', list(set(range(24)) - {7, 8, 9, 17, 18, 19})),
        ]:
            seg_data = data[data['hour_group'].isin(hour_filter)].groupby(
                'bike_count_100').agg(
                total=('total_events', 'sum'),
                converted=('converted_events', 'sum'),
            ).reset_index()
            seg_data['conversion_rate'] = (
                seg_data['converted'] / seg_data['total'].clip(lower=1))
            seg_data = seg_data[seg_data['total'] >= 50]

            seg_result = self._fit_segment(seg_data, segment)
            if seg_result:
                results[segment] = seg_result

        return results

    def _fit_all_regions(self, data: pd.DataFrame) -> Dict:
        """지역별 피팅"""
        region_results = {}
        regions = data['h3_area_name'].dropna().unique()

        if self.verbose:
            print(f"\n  🌍 지역별 피팅 ({len(regions)}개 지역)")

        fitted_count = 0
        skipped_count = 0

        for region_name in sorted(regions):
            rdata = data[data['h3_area_name'] == region_name]
            region_total = rdata['total_events'].sum()

            if region_total < MIN_SAMPLES_REGION:
                skipped_count += 1
                continue

            # region global fit
            rg = rdata.groupby('bike_count_100').agg(
                total=('total_events', 'sum'),
                converted=('converted_events', 'sum'),
            ).reset_index()
            rg['conversion_rate'] = rg['converted'] / rg['total'].clip(lower=1)
            rg = rg[rg['total'] >= 30]

            rg_result = self._fit_segment(rg, f'region:{region_name}', quiet=True)
            if not rg_result:
                skipped_count += 1
                continue

            region_results[region_name] = {'global': rg_result}
            fitted_count += 1

        if self.verbose:
            print(f"  ✅ {fitted_count}개 지역 피팅 완료, {skipped_count}개 스킵 (데이터 부족)")

        return region_results

    def _fetch_conversion_data(
        self, lookback_days: int, include_region: bool = False
    ) -> Optional[pd.DataFrame]:
        """bike_accessibility_raw에서 전환율 학습 데이터 조회"""
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        region_col = ",\n            h3_area_name" if include_region else ""
        region_group = ", h3_area_name" if include_region else ""

        query = f"""
        SELECT
            bike_count_100,
            EXTRACT(HOUR FROM event_time) as hour_group{region_col},
            COUNT(*) as total_events,
            SUM(CASE WHEN is_accessible THEN 1 ELSE 0 END) as accessible_events,
            SUM(CASE WHEN is_converted THEN 1 ELSE 0 END) as converted_events
        FROM `bikeshare.service.app_accessibility`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND h3_area_name IS NOT NULL
        GROUP BY bike_count_100, hour_group{region_group}
        """

        if self.verbose:
            print(f"  📡 데이터 조회: {start_date} ~ {end_date}")

        try:
            df = self.client.query(query).to_dataframe()
            if self.verbose:
                total = df['total_events'].sum()
                print(f"  데이터: {len(df):,}행, 총 {total:,}건 앱오픈")
            return df
        except Exception as e:
            print(f"  ❌ 데이터 조회 실패: {e}")
            return None

    def _fit_segment(
        self, data: pd.DataFrame, segment: str, quiet: bool = False
    ) -> Optional[Dict]:
        """단일 세그먼트 커브 피팅"""
        if len(data) < MIN_POINTS:
            if self.verbose and not quiet:
                print(f"  [{segment}] 데이터 부족 ({len(data)}행)")
            return None

        x = data['bike_count_100'].values.astype(float)
        y = data['conversion_rate'].values.astype(float)
        weights = np.sqrt(data['total'].values.astype(float))

        try:
            params, _ = curve_fit(
                self._conversion_func,
                x, y,
                p0=[0.36, 0.44, 0.9],
                bounds=([0, 0, 0.01], [1, 1, 10]),
                sigma=1.0 / weights,
                maxfev=5000,
            )
            base_rate, max_gain, decay = params
            max_rate = base_rate + max_gain

            # R² 계산
            y_pred = self._conversion_func(x, *params)
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0

            result = {
                'base_rate': round(float(base_rate), 4),
                'max_gain': round(float(max_gain), 4),
                'decay': round(float(decay), 4),
                'max_rate': round(float(max_rate), 4),
                'r_squared': round(float(r_squared), 4),
                'sample_size': int(data['total'].sum()),
            }

            if self.verbose and not quiet:
                print(f"  [{segment}] base={base_rate:.3f}, "
                      f"max_gain={max_gain:.3f}, decay={decay:.3f} "
                      f"→ max_rate={max_rate:.3f} (R²={r_squared:.3f}, "
                      f"n={result['sample_size']:,})")

            return result

        except Exception as e:
            if self.verbose and not quiet:
                print(f"  [{segment}] 피팅 실패: {e} → 기본값 사용")
            return None

    # ================================================================
    # 파라미터 drift 분석 (시간별 변화)
    # ================================================================

    def analyze_drift(self, months: int = 6, top_regions: int = 10) -> pd.DataFrame:
        """
        월별 파라미터 변화 분석

        Args:
            months: 분석할 개월 수 (기본 6)
            top_regions: 상위 N개 지역만 분석

        Returns:
            DataFrame[region, month, base_rate, max_gain, decay, ...]
        """
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"📊 전환율 파라미터 drift 분석 (최근 {months}개월)")
            print(f"{'='*60}")

        # 월별 데이터 조회
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=months * 30)).strftime('%Y-%m-%d')

        query = f"""
        SELECT
            h3_area_name,
            bike_count_100,
            FORMAT_DATE('%Y-%m', date) as month,
            COUNT(*) as total_events,
            SUM(CASE WHEN is_converted THEN 1 ELSE 0 END) as converted_events
        FROM `bikeshare.service.app_accessibility`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND h3_area_name IS NOT NULL
        GROUP BY 1, 2, 3
        """

        if self.verbose:
            print(f"  📡 데이터 조회: {start_date} ~ {end_date}")

        try:
            df = self.client.query(query).to_dataframe()
        except Exception as e:
            print(f"  ❌ 조회 실패: {e}")
            return pd.DataFrame()

        if self.verbose:
            print(f"  데이터: {len(df):,}행, 총 {df['total_events'].sum():,}건")

        # 상위 지역 선정 (샘플 많은 순)
        region_totals = df.groupby('h3_area_name')['total_events'].sum()
        top_region_names = region_totals.nlargest(top_regions).index.tolist()

        # 전체(global) + 상위 지역별 월별 피팅
        all_months = sorted(df['month'].unique())
        drift_rows = []

        for month in all_months:
            mdata = df[df['month'] == month]

            # Global
            mg = mdata.groupby('bike_count_100').agg(
                total=('total_events', 'sum'),
                converted=('converted_events', 'sum'),
            ).reset_index()
            mg['conversion_rate'] = mg['converted'] / mg['total'].clip(lower=1)
            mg = mg[mg['total'] >= 50]

            g_result = self._fit_segment(mg, f'global:{month}', quiet=True)
            if g_result:
                drift_rows.append({
                    'region': '_global',
                    'month': month,
                    **g_result,
                })

            # 지역별
            for region_name in top_region_names:
                rdata = mdata[mdata['h3_area_name'] == region_name]
                rg = rdata.groupby('bike_count_100').agg(
                    total=('total_events', 'sum'),
                    converted=('converted_events', 'sum'),
                ).reset_index()
                rg['conversion_rate'] = rg['converted'] / rg['total'].clip(lower=1)
                rg = rg[rg['total'] >= 20]

                r_result = self._fit_segment(rg, f'{region_name}:{month}', quiet=True)
                if r_result:
                    drift_rows.append({
                        'region': region_name,
                        'month': month,
                        **r_result,
                    })

        drift_df = pd.DataFrame(drift_rows)

        if self.verbose and len(drift_df) > 0:
            self._print_drift_summary(drift_df)

        return drift_df

    def _print_drift_summary(self, drift_df: pd.DataFrame):
        """drift 분석 결과 출력"""
        print(f"\n  === 월별 파라미터 변화 ===")

        for region in drift_df['region'].unique():
            rdf = drift_df[drift_df['region'] == region].sort_values('month')
            if len(rdf) < 2:
                continue

            label = '🌐 전체' if region == '_global' else f'📍 {region}'
            print(f"\n  {label}")
            print(f"    {'월':>7} | {'base':>5} {'gain':>5} {'decay':>5} | {'max_rate':>8} {'R²':>5} | {'샘플':>8}")
            print(f"    {'-'*55}")

            for _, row in rdf.iterrows():
                print(f"    {row['month']:>7} | {row['base_rate']:.3f} {row['max_gain']:.3f} "
                      f"{row['decay']:.2f} | {row['max_rate']:>7.1%} {row['r_squared']:>5.2f} "
                      f"| {row['sample_size']:>7,}건")

            # 변동폭
            for col in ('base_rate', 'max_gain', 'decay'):
                vals = rdf[col].values
                std = np.std(vals)
                mean = np.mean(vals)
                cv = std / mean * 100 if mean > 0 else 0
                print(f"    {col:>12} 변동계수(CV): {cv:.1f}%"
                      f"  {'⚠️ 불안정' if cv > 20 else '✅ 안정'}")

    # ================================================================
    # 구역×시간 평균 가용기기수 조회
    # ================================================================

    def get_avg_bike_counts(self, lookback_days: int = 90) -> pd.DataFrame:
        """
        구역×시간대별 평균 가용기기수 (잠재수요 역산 시 필요)

        Returns:
            DataFrame[region, district, hour, avg_bike_count_100]
        """
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        query = f"""
        SELECT
            h3_area_name as region,
            h3_district_name as district,
            EXTRACT(HOUR FROM event_time) as hour,
            AVG(bike_count_100) as avg_bike_count_100
        FROM `bikeshare.service.app_accessibility`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND h3_area_name IS NOT NULL
            AND h3_district_name IS NOT NULL
        GROUP BY 1, 2, 3
        """

        if self.verbose:
            print(f"  📡 평균 가용기기수 조회 ({start_date} ~ {end_date})")

        df = self.client.query(query).to_dataframe()

        if self.verbose:
            n_districts = df[['region', 'district']].drop_duplicates().shape[0]
            avg_overall = df['avg_bike_count_100'].mean()
            print(f"  {n_districts}개 구역, 전체 평균 기기수: {avg_overall:.1f}대")

        return df

    # ================================================================
    # 저장/로드
    # ================================================================

    def save_params(self, results: Dict = None, region_results: Dict = None):
        """파라미터 JSON 저장"""
        save_data = {}

        # global 세그먼트별 파라미터
        for seg in ('global', 'commute', 'leisure'):
            if results and seg in results:
                save_data[seg] = results[seg]
            elif seg in self.params:
                save_data[seg] = self.params[seg]

        # region 파라미터
        if region_results:
            save_data['regions'] = region_results
        elif self.region_params:
            # 기존 region params 유지
            save_data['regions'] = {}
            for rname, rsegs in self.region_params.items():
                save_data['regions'][rname] = {}
                for seg, p in rsegs.items():
                    save_data['regions'][rname][seg] = p

        save_data['fitted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M')
        save_data['lookback_days'] = 90
        save_data['n_regions'] = len(save_data.get('regions', {}))

        with open(PARAMS_PATH, 'w') as f:
            json.dump(save_data, f, indent=2, ensure_ascii=False)

        if self.verbose:
            print(f"  📁 저장: {PARAMS_PATH}")

    # ================================================================
    # 진단/테스트
    # ================================================================

    def diagnose(self, show_regions: bool = False):
        """현재 모델 상태 진단"""
        print(f"\n{'='*60}")
        print(f"🔍 전환율 모델 진단")
        print(f"{'='*60}")

        for seg in ('global', 'commute', 'leisure'):
            p = self.params.get(seg)
            if not p:
                continue

            max_rate = p['base_rate'] + p['max_gain']
            print(f"\n  [{seg}]")
            print(f"    base_rate: {p['base_rate']:.3f} (기기 0대 전환율)")
            print(f"    max_rate:  {max_rate:.3f} (최대 전환율)")
            print(f"    decay:     {p['decay']:.3f} (감쇠 속도)")

            print(f"\n    기기수  전환율")
            print(f"    {'─'*18}")
            for n in [0, 1, 2, 3, 5, 7, 10]:
                rate = float(self.predict_conversion_rate(n, seg))
                bar = '█' * int(rate * 30)
                print(f"    {n:>4}대  {rate:.1%} {bar}")

        # 잠재수요 역산 예시
        print(f"\n  📊 잠재수요 역산 예시 (실현 100건 기준):")
        print(f"    {'평균기기수':>8}  {'전환율':>6}  {'잠재수요':>8}  {'억제수요':>8}")
        print(f"    {'─'*36}")
        for n in [0.5, 1, 2, 3, 5, 10]:
            unc = self.estimate_unconstrained(100, n)
            rate = float(self.predict_conversion_rate(n))
            supp = unc - 100
            print(f"    {n:>6.1f}대  {rate:>5.1%}  {unc:>7.0f}건  {supp:>+7.0f}건")

        # Region 요약
        if self.region_params:
            print(f"\n  🌍 지역별 파라미터 ({len(self.region_params)}개)")
            if show_regions:
                print(f"    {'지역':>16} | {'base':>5} {'gain':>5} {'decay':>5} | {'max_rate':>8} {'R²':>5}")
                print(f"    {'-'*55}")
                for rname in sorted(self.region_params.keys()):
                    p = self.region_params[rname].get('global', {})
                    if not p:
                        continue
                    mr = p['base_rate'] + p['max_gain']
                    r2 = self.region_params[rname].get('global', {}).get('r_squared', '?')
                    print(f"    {rname:>16} | {p['base_rate']:.3f} {p['max_gain']:.3f} "
                          f"{p['decay']:.2f} | {mr:>7.1%}")
            else:
                bases = [rp['global']['base_rate'] for rp in self.region_params.values()
                         if 'global' in rp]
                decays = [rp['global']['decay'] for rp in self.region_params.values()
                          if 'global' in rp]
                if bases:
                    print(f"    base_rate: {min(bases):.3f} ~ {max(bases):.3f}")
                    print(f"    decay:     {min(decays):.2f} ~ {max(decays):.2f}")
                    print(f"    (--test --regions 로 전체 목록 확인)")


# ================================================================
# CLI
# ================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description='전환율 모델')
    parser.add_argument('--fit', action='store_true', help='global 학습')
    parser.add_argument('--fit-region', action='store_true', help='global + 지역별 학습')
    parser.add_argument('--test', action='store_true', help='모델 진단')
    parser.add_argument('--drift', action='store_true', help='월별 파라미터 drift 분석')
    parser.add_argument('--regions', action='store_true', help='지역별 상세 출력')
    parser.add_argument('--days', type=int, default=90, help='학습 기간 (일)')
    parser.add_argument('--months', type=int, default=6, help='drift 분석 기간 (월)')
    args = parser.parse_args()

    model = ConversionModel(verbose=True)

    if args.fit_region:
        model.fit(lookback_days=args.days, fit_regions=True)
        model.diagnose(show_regions=args.regions)
    elif args.fit:
        model.fit(lookback_days=args.days, fit_regions=False)
        model.diagnose()
    elif args.drift:
        model.analyze_drift(months=args.months)
    elif args.test:
        model.diagnose(show_regions=args.regions)
    else:
        model.diagnose(show_regions=args.regions)


if __name__ == '__main__':
    main()
