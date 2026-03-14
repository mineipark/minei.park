"""
수요 예측 모델 v7: 권역별 보정 파라미터

각 권역의 이용 패턴을 회귀분석하여 도출한 보정 계수 적용
- 도심 권역: 주말 효과 강함 (출퇴근 패턴)
- 외곽/신도시: 날씨 민감도 높음 (레저 패턴)

사용법:
    from demand_model_v7 import DemandForecastModelV7

    model = DemandForecastModelV7()
    predictions = model.predict('2026-01-25', weather={'temp_low': -5, 'temp_high': 2})
"""
import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from sklearn.ensemble import GradientBoostingRegressor
import warnings
warnings.filterwarnings('ignore')


# 타겟 센터 목록 (하위 호환용 - 실제 필터링은 region_params.json 기반)
TARGET_CENTERS = None  # deprecated: region_params.json에서 동적 로드

# 스케일 파라미터 (최적화 결과)
DAY_SCALE = 0.25      # 요일 효과 25% (0.30에서 하향 - 주말 과보정 완화)
WEATHER_SCALE = 0.45  # 날씨 효과 45%

# Bias 보정 (일관된 과소예측 편향 해소)
# region_params.json에 권역별 bias 값이 있으면 사용, 없으면 기본값 적용
BIAS_CORRECTION_DEFAULT = 0.05

# 기본 권역 파라미터 (전체 평균)
DEFAULT_REGION_PARAMS = {
    'sat': -0.20, 'sun': -0.28, 'mon': -0.08,
    'cold': -0.32, 'freeze': -0.10, 'snow': -0.35
}


class RegionWeatherCorrection:
    """권역별 날씨 + 요일 보정"""

    COLD_THRESHOLD = -8   # 한파 기준 (최저기온)
    FREEZE_THRESHOLD = 0  # 영하 기준 (최고기온)
    SNOW_THRESHOLD = 1.0  # 적설 기준 (cm)
    MIN_FACTOR = 0.25     # 최소 보정 팩터

    def __init__(self, params_path: Optional[str] = None):
        """권역별 파라미터 로드"""
        if params_path is None:
            params_path = os.path.join(os.path.dirname(__file__), 'region_params.json')

        if os.path.exists(params_path):
            with open(params_path, 'r') as f:
                self.region_params = json.load(f)
        else:
            self.region_params = {}

    def calculate(self, region: str, temp_low: float, temp_high: float,
                  is_saturday: bool, is_sunday: bool, is_monday: bool,
                  snow_depth: float, is_holiday: bool = False) -> Tuple[float, str]:
        """
        권역별 보정 팩터 계산

        Log 회귀계수를 multiplicative factor로 변환하여 적용

        Returns:
            (factor, description)
        """
        params = self.region_params.get(region, DEFAULT_REGION_PARAMS)
        factor = 1.0
        descriptions = []

        # 1. 요일 보정 (공휴일은 일요일과 유사한 패턴 적용)
        day_adj = 0
        day_descs = []
        if is_holiday and not is_saturday and not is_sunday:
            day_adj += params.get('sun', -0.28)
            day_descs.append("공휴일")
        if is_saturday:
            day_adj += params.get('sat', -0.20)
            day_descs.append("토")
        if is_sunday:
            day_adj += params.get('sun', -0.28)
            day_descs.append("일")
        if is_monday and not is_holiday:
            day_adj += params.get('mon', -0.08)
            day_descs.append("월")

        if day_adj != 0:
            day_effect = (np.exp(day_adj) - 1) * DAY_SCALE
            day_effect = max(day_effect, -0.30)  # 최대 -30%
            factor *= (1 + day_effect)
            descriptions.append(f"{'+'.join(day_descs)}:{day_effect*100:+.0f}%")

        # 2. 날씨 보정
        weather_adj = 0
        weather_descs = []

        if temp_low < self.COLD_THRESHOLD:
            weather_adj += params.get('cold', -0.32)
            weather_descs.append("한파")

        if temp_high < self.FREEZE_THRESHOLD:
            weather_adj += params.get('freeze', -0.10)
            weather_descs.append("영하")

        if snow_depth >= self.SNOW_THRESHOLD:
            weather_adj += params.get('snow', -0.35)
            weather_descs.append("적설")

        if weather_adj != 0:
            weather_effect = (np.exp(weather_adj) - 1) * WEATHER_SCALE
            weather_effect = max(weather_effect, -0.50)  # 최대 -50%
            factor *= (1 + weather_effect)
            descriptions.append(f"{'+'.join(weather_descs)}:{weather_effect*100:+.0f}%")

        factor = max(factor, self.MIN_FACTOR)
        desc = ", ".join(descriptions) if descriptions else "보정 없음"

        return factor, desc

    def get_bias(self, region: str) -> float:
        """권역별 bias 보정값 반환 (region_params에 없으면 기본값)"""
        params = self.region_params.get(region, {})
        return params.get('bias', BIAS_CORRECTION_DEFAULT)


class DemandForecastModelV7:
    """수요 예측 모델 v7: 권역별 보정 파라미터 적용"""

    def __init__(self, credentials_path: Optional[str] = None,
                 target_centers: Optional[List[str]] = None):
        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        elif not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
            default_path = os.path.join(os.path.dirname(__file__), '..', 'credentials', 'service-account.json')
            if os.path.exists(default_path):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = default_path

        from google.cloud import bigquery
        self.client = bigquery.Client()
        self.model = None
        self.feature_cols = None
        self.holidays = None
        self.top_regions = None
        # region_params.json 기반으로 대상 권역 결정 (센터 제한 제거)
        self.correction = RegionWeatherCorrection()
        self.target_regions = list(self.correction.region_params.keys()) if self.correction.region_params else []
        # 하위 호환: target_centers 인자가 전달되면 해당 센터 권역만 필터
        if target_centers:
            self.target_regions = [
                r for r, p in self.correction.region_params.items()
                if p.get('center', '') in target_centers
            ]

    def _load_holidays(self):
        if self.holidays is None:
            query = "SELECT date FROM `reference.public_holidays`"
            self.holidays = set(self.client.query(query).to_dataframe()['date'].tolist())
            # BigQuery에 누락된 공휴일 보충 (설, 추석 등 음력 기반)
            from korean_holidays import ADDITIONAL_HOLIDAYS
            self.holidays |= ADDITIONAL_HOLIDAYS
        return self.holidays

    def fetch_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """운영 중 전체 권역 라이딩 데이터 조회 (region_params.json 기반)"""
        regions_str = ','.join([f"'{r}'" for r in self.target_regions])

        query = f"""
        SELECT
            DATE(r.start_time) as date,
            EXTRACT(DAYOFWEEK FROM r.start_time) as day_of_week,
            EXTRACT(MONTH FROM r.start_time) as month,
            COALESCE(c.name, 'unknown') as center_name,
            r.h3_start_area_name as region,
            COUNT(*) as ride_count
        FROM `service.rides` r
        LEFT JOIN `service.geo_area` a ON r.h3_start_area_name = a.name
        LEFT JOIN `service.service_center` c ON a.center_id = c.id
        WHERE r.start_time BETWEEN '{start_date}' AND '{end_date} 23:59:59'
            AND r.h3_start_area_name IN ({regions_str})
        GROUP BY 1, 2, 3, 4, 5
        """
        return self.client.query(query).to_dataframe()

    def prepare_features(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
        """피처 준비"""
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])

        holidays = self._load_holidays()

        df['is_holiday'] = df['date'].dt.date.isin(holidays).astype(int)
        df['is_weekend'] = df['day_of_week'].isin([1, 7]).astype(int)
        df['is_saturday'] = (df['day_of_week'] == 7).astype(int)
        df['is_sunday'] = (df['day_of_week'] == 1).astype(int)
        df['is_winter'] = df['month'].isin([12, 1, 2]).astype(int)
        df['winter_weekend'] = df['is_winter'] * df['is_weekend']

        if self.top_regions is None:
            self.top_regions = df.groupby('region')['ride_count'].sum().nlargest(15).index.tolist()

        df['region_top'] = df['region'].apply(lambda x: x if x in self.top_regions else 'other')
        region_dummies = pd.get_dummies(df['region_top'], prefix='reg')
        df = pd.concat([df, region_dummies], axis=1)

        center_dummies = pd.get_dummies(df['center_name'], prefix='center')
        df = pd.concat([df, center_dummies], axis=1)

        df = df.sort_values(['region', 'date'])
        df['ride_count'] = df['ride_count'].astype(float)
        df['lag1'] = df.groupby('region')['ride_count'].shift(1)
        df['lag7'] = df.groupby('region')['ride_count'].shift(7)

        mean_rides = df['ride_count'].mean()
        df['lag1'] = df['lag1'].fillna(mean_rides)
        df['lag7'] = df['lag7'].fillna(mean_rides)

        feature_cols = [
            'day_of_week', 'month', 'is_weekend', 'is_saturday', 'is_sunday',
            'is_winter', 'is_holiday', 'winter_weekend', 'lag1', 'lag7'
        ] + [c for c in region_dummies.columns] + [c for c in center_dummies.columns]

        return df, feature_cols

    def train(self, train_df: pd.DataFrame):
        """모델 학습"""
        df, feature_cols = self.prepare_features(train_df)
        self.feature_cols = feature_cols

        self.model = GradientBoostingRegressor(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.1,
            random_state=42
        )
        self.model.fit(df[feature_cols], df['ride_count'])

        return self

    def predict(
        self,
        target_date: str,
        weather: Optional[Dict[str, float]] = None,
        historical_data: Optional[pd.DataFrame] = None
    ) -> Dict:
        """특정 날짜 수요 예측"""
        target = pd.Timestamp(target_date)

        if historical_data is None:
            start_date = (target - timedelta(days=400)).strftime('%Y-%m-%d')
            end_date = target.strftime('%Y-%m-%d')
            historical_data = self.fetch_data(start_date, end_date)

        cutoff = target - timedelta(days=1)
        train_data = historical_data[pd.to_datetime(historical_data['date']) <= cutoff]
        self.train(train_data)

        test_data = historical_data[pd.to_datetime(historical_data['date']) == target]

        if len(test_data) == 0:
            return {'error': f'{target_date} 데이터 없음'}

        df, _ = self.prepare_features(historical_data)
        test_df = df[df['date'] == target]

        # 요일 정보
        dow = int(test_df['day_of_week'].iloc[0])
        is_saturday = (dow == 7)
        is_sunday = (dow == 1)
        is_monday = (dow == 2)

        # 공휴일 여부 확인
        holidays = self._load_holidays()
        is_holiday = target.date() in holidays

        temp_low = weather.get('temp_low', 0) if weather else 0
        temp_high = weather.get('temp_high', 10) if weather else 10
        snow_depth = weather.get('snow_depth', 0) if weather else 0

        total_base_pred = 0
        total_adj_pred = 0
        total_actual = 0
        region_details = []

        # 권역별 예측 및 보정
        for region in test_df['region'].unique():
            region_df = test_df[test_df['region'] == region]

            base_pred = self.model.predict(region_df[self.feature_cols]).sum()
            actual = region_df['ride_count'].sum()
            center = region_df['center_name'].iloc[0]

            if weather:
                factor, desc = self.correction.calculate(
                    region, temp_low, temp_high, is_saturday, is_sunday, is_monday,
                    snow_depth, is_holiday=is_holiday
                )
            else:
                factor, desc = 1.0, "날씨 정보 없음"

            region_bias = self.correction.get_bias(region)
            adj_pred = base_pred * factor * (1 + region_bias)

            total_base_pred += base_pred
            total_adj_pred += adj_pred
            total_actual += actual

            region_details.append({
                'region': region,
                'center': center,
                'actual': actual,
                'base_pred': base_pred,
                'factor': factor,
                'adj_pred': adj_pred,
                'desc': desc
            })

        dow_names = {1: '일', 2: '월', 3: '화', 4: '수', 5: '목', 6: '금', 7: '토'}

        return {
            'date': target_date,
            'day_of_week': dow_names[dow],
            'is_weekend': is_saturday or is_sunday,
            'actual': total_actual,
            'base_prediction': round(total_base_pred),
            'adjusted_prediction': round(total_adj_pred),
            'error_pct': round((total_adj_pred - total_actual) / total_actual * 100, 1) if total_actual > 0 else None,
            'region_details': region_details
        }

    def backtest(
        self,
        test_dates: list,
        weather_data: Dict[str, Dict],
        historical_data: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """백테스트 실행"""
        results = []

        for date in test_dates:
            date_str = date if isinstance(date, str) else date.strftime('%Y-%m-%d')
            weather = weather_data.get(date_str, {})

            result = self.predict(date_str, weather, historical_data)
            results.append(result)

        return pd.DataFrame(results)


def load_weather_data(csv_path: str) -> Dict[str, Dict]:
    """날씨 데이터 CSV 로드"""
    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['date'])

    weather_data = {}
    for _, row in df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        weather_data[date_str] = {
            'temp_low': float(row['temp_low']) if pd.notna(row['temp_low']) else 0,
            'temp_high': float(row['temp_high']) if pd.notna(row['temp_high']) else 10,
            'snow_depth': float(row['snow_depth']) if pd.notna(row['snow_depth']) else 0
        }
    return weather_data


def main():
    """테스트 실행"""
    print("="*70)
    print("수요 예측 모델 v7: 권역별 보정 파라미터 적용")
    print("="*70)

    model = DemandForecastModelV7()
    print(f"대상 권역: {len(model.target_regions)}개 (region_params.json 기반)")
    print(f"DAY_SCALE: {DAY_SCALE}, WEATHER_SCALE: {WEATHER_SCALE}, BIAS_DEFAULT: {BIAS_CORRECTION_DEFAULT}")

    weather_csv = os.path.join(os.path.dirname(__file__), 'weather_2025_202601.csv')
    if os.path.exists(weather_csv):
        weather_data = load_weather_data(weather_csv)
        print(f"날씨 데이터: {len(weather_data)}일")
    else:
        weather_data = {}
        print("날씨 데이터 없음")

    print("\n데이터 로딩 중...")
    data = model.fetch_data('2025-01-01', '2026-01-25')
    print(f"로드 완료: {len(data):,}건")

    print("\n" + "="*70)
    print("백테스트: 2026년 1월 19일 ~ 25일")
    print("="*70)

    test_dates = pd.date_range('2026-01-19', '2026-01-25')
    results = model.backtest(test_dates, weather_data, data)

    print(f"\n{'날짜':<12} {'요일':<3} {'실제':>10} {'기본예측':>10} {'보정예측':>10} {'오차':>8}")
    print("-"*60)

    for _, r in results.iterrows():
        print(f"{r['date']:<12} {r['day_of_week']:<3} {r['actual']:>10,.0f} {r['base_prediction']:>10,.0f} {r['adjusted_prediction']:>10,.0f} {r['error_pct']:>+7.1f}%")

    mape = results['error_pct'].abs().mean()
    print("-"*60)
    print(f"\nMAPE: {mape:.1f}%")


if __name__ == "__main__":
    main()
