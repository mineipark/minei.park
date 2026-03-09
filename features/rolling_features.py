"""
Stage 1: 셀프 롤링 피처

구역별로 14일 롤링 통계를 계산하여 각 구역의 최근 행동 패턴을 포착한다.

생성 피처:
    - {col}_rolling: 14일 롤링 평균 (누출 방지를 위해 1일 시프트)
    - opens_cv_rolling: 변동계수 (변동성 지표)
"""

import pandas as pd
from config import ROLLING_WINDOW


def create_self_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    구역별 14일 롤링 통계 계산.

    모든 롤링 윈도우는 데이터 누출 방지를 위해 1일 시프트된다
    (즉, 오늘 예측에는 어제까지의 데이터만 사용).

    Args:
        df: [h3_district_name, date, app_opens, rides,
            rides_per_open, avg_bikes_400m, accessibility_rate, hour_std,
            cond_conversion_rate] 컬럼을 가진 DataFrame

    Returns:
        롤링 피처가 추가된 DataFrame
    """
    print("[Stage 1] 셀프 롤링 피처...")

    df = df.sort_values(['h3_district_name', 'date']).copy()
    g = df.groupby('h3_district_name')

    # 주요 지표의 롤링 평균 (shift(1) = 과거 데이터만 사용)
    rolling_cols = [
        'app_opens', 'avg_bikes_400m', 'accessibility_rate',
        'rides', 'rides_per_open', 'hour_std',
        'cond_conversion_rate'
    ]

    for col in rolling_cols:
        if col in df.columns:
            df[f'{col}_rolling'] = g[col].transform(
                lambda x: x.shift(1).rolling(
                    ROLLING_WINDOW, min_periods=3
                ).mean()
            )

    # 변동계수: 수요 변동성 포착
    # CV가 높은 구역은 예측이 어려움 → 모델이 이를 학습
    df['opens_cv_rolling'] = g['app_opens'].transform(
        lambda x: (
            x.shift(1).rolling(ROLLING_WINDOW, min_periods=3).std()
            / x.shift(1).rolling(ROLLING_WINDOW, min_periods=3).mean()
        )
    )

    n_features = sum(1 for c in df.columns if c.endswith('_rolling'))
    print(f"  롤링 피처 {n_features}개 생성 (윈도우={ROLLING_WINDOW}일)")
    return df
