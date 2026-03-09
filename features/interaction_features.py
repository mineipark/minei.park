"""
Stage 7: 교차 피처

트리 모델이 피처를 독립적으로 고려할 때 놓칠 수 있는
비선형 효과를 포착하는 피처 간 조합.

핵심 교차 효과:
- 비 x 주말: 비 오는 주말이 비 오는 평일보다 수요 타격이 큼
  (여가 라이딩은 감소, 통근 라이딩은 유지)
- 한파 x 휴일: 유사한 복합 효과
- 주요 명절 x 인접일: 여행 패턴이 수요에 영향
- POI x 캘린더: 지하철역은 평일에 더 중요
- 날씨 x RPO 모멘텀: 비는 높은 모멘텀 구역에 다르게 영향

생성 피처:
    날씨x캘린더: rain_off, cold_off, rain_weekend,
                 severe_weather_off, heavy_rain_off
    명절: is_major_holiday, major_holiday_adj
    POIx캘린더: subway_x_off, commercial_x_off
    POIx시간프로필: subway_x_commute, commercial_x_evening
    날씨xRPO: rain_x_rpo_momentum, temp_deviation_x_rpo
"""

import numpy as np
import pandas as pd


def create_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    피처 간 교차항 계산.

    트리 기반 모델은 교차 효과를 암묵적으로 학습할 수 있지만,
    명시적으로 제공하면 다음 경우에 도움이 된다:
    1. 다른 분기의 피처 간 교차 효과
    2. 발견에 2회 이상 분할이 필요한 교차 효과
    3. 희귀 조합에 대한 학습 데이터가 부족한 경우

    Args:
        df: 캘린더, 날씨, POI, 모멘텀 피처를 가진 DataFrame

    Returns:
        교차 피처가 추가된 DataFrame
    """
    print("[Stage 7] 교차 피처...")

    df['month'] = df['date'].dt.month

    # ── 날씨 x 캘린더 ──
    df['rain_off'] = df.get('is_rain', 0) * df.get('is_off', 0)
    df['cold_off'] = df.get('is_cold', 0) * df.get('is_off', 0)
    df['rain_weekend'] = df.get('is_rain', 0) * df.get('is_weekend', 0)
    df['severe_weather_off'] = df.get('is_severe_weather', 0) * df.get('is_off', 0)
    df['heavy_rain_off'] = df.get('is_heavy_rain', 0) * df.get('is_off', 0)

    # ── 주요 명절 감지 ──
    # 3일 이상 연속 공휴일 = 주요 명절 (설날, 추석)
    df['_hol_block'] = df.groupby('h3_district_name')['is_holiday'].transform(
        lambda x: x.rolling(3, min_periods=1, center=True).sum()
    )
    df['is_major_holiday'] = (df['_hol_block'] >= 2).astype(int)

    # 인접일 (여행 효과: 출발/귀경)
    g = df.groupby('h3_district_name')
    df['major_holiday_adj'] = (
        (df['is_major_holiday'] == 1)
        | (g['is_major_holiday'].shift(1) == 1)
        | (g['is_major_holiday'].shift(-1) == 1)
    ).astype(int)
    df = df.drop(columns=['_hol_block'])

    # ── POI x 캘린더 ──
    df['subway_x_off'] = df.get('has_subway', 0) * df.get('is_off', 0)
    df['commercial_x_off'] = df.get('is_commercial', 0) * df.get('is_off', 0)

    # ── POI x 시간 프로필 ──
    df['subway_x_commute'] = df.get('has_subway', 0) * df.get('commute_ratio', 0)
    df['commercial_x_evening'] = df.get('is_commercial', 0) * df.get('evening_ratio', 0)

    # ── 날씨 x RPO 모멘텀 ──
    # 비는 전환율을 억제하지만, 효과는 구역의 최근 모멘텀에 따라 다름
    rpo_momentum = df.get('rpo_momentum', pd.Series(1, index=df.index)).fillna(1)
    df['rain_x_rpo_momentum'] = df['rain_sum'].fillna(0) * rpo_momentum

    # 월평균 대비 기온 편차 x RPO 모멘텀
    if 'temp_avg' in df.columns:
        monthly_temp_avg = df.groupby('month')['temp_avg'].transform('mean')
        df['temp_deviation'] = df['temp_avg'] - monthly_temp_avg
        df['temp_deviation_x_rpo'] = df['temp_deviation'].fillna(0) * rpo_momentum
    else:
        df['temp_deviation'] = 0
        df['temp_deviation_x_rpo'] = 0

    n_major = df.get('is_major_holiday', pd.Series(0)).sum()
    print(f"  교차 피처 완료 "
          f"(주요 명절: {n_major} 구역-일)")
    return df
