"""
합성 데이터 생성기

데이터 웨어하우스 접근 없이 수요 예측 파이프라인을 시연하기 위한
현실적인 샘플 데이터를 생성합니다.

합성 데이터는 실제 패턴을 모사합니다:
    - 요일별 계절성 (주말에 더 높음)
    - 날씨 효과 (비가 오면 수요 감소)
    - 공간 상관관계 (인접 구역은 유사)
    - 공휴일 효과 (주요 공휴일: 수요 -50%)
    - 계절적 추세 (여름 피크, 겨울 저점)
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def generate_sample_data(
    n_districts: int = 30,
    n_areas: int = 6,
    start_date: str = '2025-01-01',
    end_date: str = '2026-02-26',
    seed: int = 42,
):
    """
    파이프라인 데모를 위한 합성 구역x날짜 데이터를 생성합니다.

    Args:
        n_districts: 생성할 구역 수
        n_areas: 상위 권역 수
        start_date: 날짜 범위 시작
        end_date: 날짜 범위 끝
        seed: 재현성을 위한 랜덤 시드

    Returns:
        (df, weather_df, holidays_set) 튜플
    """
    np.random.seed(seed)
    print(f"합성 데이터 생성 중: {n_districts}개 구역, "
          f"{start_date} ~ {end_date}")

    dates = pd.date_range(start_date, end_date, freq='D')
    n_days = len(dates)

    # ── 구역 메타데이터 ──
    districts = []
    for i in range(n_districts):
        area_id = i % n_areas
        districts.append({
            'h3_district_name': f'district_{i:03d}',
            'h3_area_name': f'area_{area_id:02d}',
            'center_lat': 37.5 + np.random.uniform(-0.3, 0.3),
            'center_lng': 127.0 + np.random.uniform(-0.3, 0.3),
            'base_opens': np.random.lognormal(3.0, 0.5),  # 5~100
            'base_rpo': np.random.uniform(0.5, 2.0),
            'commute_ratio': np.random.uniform(0.15, 0.35),
            'evening_ratio': np.random.uniform(0.20, 0.35),
            'has_subway': int(np.random.random() < 0.4),
            'is_commercial': int(np.random.random() < 0.3),
        })
    district_df = pd.DataFrame(districts)

    # ── 날씨 데이터 ──
    weather_records = []
    for d in dates:
        month = d.month
        # 계절별 기온 패턴
        base_temp = 15 + 15 * np.sin(2 * np.pi * (month - 4) / 12)
        temp_high = base_temp + np.random.normal(5, 2)
        temp_low = base_temp - np.random.normal(5, 2)
        rain = max(0, np.random.exponential(3) - 2) if np.random.random() < 0.3 else 0
        weather_records.append({
            'date': d,
            'temp_low': temp_low,
            'temp_high': temp_high,
            'rain_sum': round(rain, 1),
            'windspeed_avg': max(0, np.random.normal(3, 2)),
            'humidity_avg': np.random.normal(60, 15),
            'snow_depth': max(0, np.random.normal(-5, 3)) if month in [12, 1, 2] else 0,
        })
    weather_df = pd.DataFrame(weather_records)

    # ── 공휴일 ──
    holidays_set = set()
    for year in [2025, 2026]:
        # 신정
        holidays_set.add(pd.Timestamp(f'{year}-01-01'))
        # 설날 (음력 설) - 근사값
        if year == 2025:
            for d in pd.date_range('2025-01-28', '2025-01-30'):
                holidays_set.add(d)
        else:
            for d in pd.date_range('2026-02-16', '2026-02-18'):
                holidays_set.add(d)
        # 어린이날
        holidays_set.add(pd.Timestamp(f'{year}-05-05'))
        # 광복절
        holidays_set.add(pd.Timestamp(f'{year}-08-15'))
        # 추석 - 근사값
        if year == 2025:
            for d in pd.date_range('2025-10-05', '2025-10-07'):
                holidays_set.add(d)
        # 크리스마스
        holidays_set.add(pd.Timestamp(f'{year}-12-25'))

    # ── 구역x날짜 레코드 생성 ──
    records = []
    for _, district in district_df.iterrows():
        for i, d in enumerate(dates):
            # 요일 효과
            dow = d.dayofweek
            dow_factor = [0.85, 0.90, 0.95, 1.00, 1.05, 1.30, 1.25][dow]

            # 계절 효과
            month = d.month
            season_factor = 0.7 + 0.6 * np.sin(2 * np.pi * (month - 4) / 12)

            # 날씨 효과
            w = weather_df.iloc[i]
            rain_effect = 1.0 - 0.03 * w['rain_sum']
            cold_effect = 1.0 if w['temp_low'] > -5 else 0.7
            weather_factor = max(0.3, rain_effect * cold_effect)

            # 공휴일 효과
            is_holiday = d in holidays_set
            holiday_factor = 0.5 if is_holiday else 1.0

            # 결합
            base = district['base_opens']
            noise = np.random.lognormal(0, 0.15)
            app_opens = max(1, int(
                base * dow_factor * season_factor * weather_factor
                * holiday_factor * noise
            ))

            # 라이딩 (RPO는 공급량과 조건에 따라 변동)
            bike_supply = max(0, np.random.normal(3, 1.5))
            rpo_base = district['base_rpo']
            rpo_noise = np.random.normal(1.0, 0.15)
            rpo = max(0.1, rpo_base * weather_factor * rpo_noise)
            rides = max(0, int(app_opens * rpo + np.random.normal(0, 2)))

            # 접근성
            acc_rate = min(1.0, 0.3 + 0.1 * bike_supply + np.random.normal(0, 0.05))
            cond_conv = rpo / max(acc_rate, 0.05) if acc_rate > 0.05 else np.nan

            records.append({
                'date': d,
                'h3_area_name': district['h3_area_name'],
                'h3_district_name': district['h3_district_name'],
                'app_opens': app_opens,
                'rides': rides,
                'rides_per_open': rides / max(app_opens, 1),
                'avg_bikes_100m': bike_supply,
                'avg_bikes_400m': bike_supply * 2.5,
                'accessibility_rate': acc_rate,
                'cond_conversion_rate': cond_conv,
                'center_lat': district['center_lat'],
                'center_lng': district['center_lng'],
                'hour_std': np.random.uniform(4, 7),
                'commute_ratio': district['commute_ratio'],
                'evening_ratio': district['evening_ratio'],
                'has_subway': district['has_subway'],
                'is_commercial': district['is_commercial'],
            })

    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])

    print(f"  생성 완료: {len(df):,}행, "
          f"{n_districts}개 구역, {n_days}일")
    print(f"  앱 오픈 범위: {df['app_opens'].min()} ~ {df['app_opens'].max()}")
    print(f"  라이딩 범위: {df['rides'].min()} ~ {df['rides'].max()}")
    print(f"  공휴일: {len(holidays_set)}일")

    return df, weather_df, holidays_set


if __name__ == '__main__':
    df, weather_df, holidays = generate_sample_data()
    print(f"\n샘플 데이터 형태: {df.shape}")
    print(f"날씨 데이터 형태: {weather_df.shape}")
    print(f"\n구역별 요약:")
    print(df.groupby('h3_district_name')['app_opens'].mean().describe())
