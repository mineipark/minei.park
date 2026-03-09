"""
Stage 3: 권역 수준 피처

상위 권역(h3_area_name) 수준으로 지표를 집계하여,
개별 구역이 속한 지리적 권역의 거시적 수요 패턴을 포착한다.

생성 피처:
    - area_total_opens_prev: 권역 내 전일 총 앱 오픈
    - area_avg_rpo_prev: 권역 내 전일 평균 RPO
    - area_avg_access_prev: 권역 내 전일 평균 접근성
    - area_district_count: 해당 권역의 구역 수
    - area_opens_lag7/ma7: 권역 수준 주간 패턴
    - district_area_share: 권역 앱 오픈 중 해당 구역 비중
"""

import pandas as pd


def create_area_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    권역 수준 집계 피처 계산.

    각 구역은 상위 권역에 속한다. 권역 수준 피처는
    전반적인 수요 환경을 포착하며, 구역 자체 이력이 부족할 때 유용하다.

    Args:
        df: [date, h3_area_name, h3_district_name,
            app_opens, rides, rides_per_open, accessibility_rate,
            app_opens_rolling] 컬럼을 가진 DataFrame

    Returns:
        권역 피처가 추가된 DataFrame
    """
    print("[Stage 3] 권역 수준 피처...")

    df = df.sort_values(['date', 'h3_area_name', 'h3_district_name']).copy()

    # 일별 권역 집계
    area_daily = df.groupby(['date', 'h3_area_name']).agg(
        area_total_opens=('app_opens', 'sum'),
        area_total_rides=('rides', 'sum'),
        area_avg_rpo=('rides_per_open', 'mean'),
        area_avg_access=('accessibility_rate', 'mean'),
        area_district_count=('h3_district_name', 'nunique'),
    ).reset_index()

    area_daily = area_daily.sort_values(['h3_area_name', 'date'])
    ag = area_daily.groupby('h3_area_name')

    # 전일 값 (1일 시프트)
    for col in ['area_total_opens', 'area_total_rides', 'area_avg_rpo', 'area_avg_access']:
        area_daily[f'{col}_prev'] = ag[col].shift(1)

    # 주간 래그 및 7일 이동평균 (앱 오픈 모델용)
    area_daily['area_opens_lag7'] = ag['area_total_opens'].shift(7)
    area_daily['area_opens_ma7'] = ag['area_total_opens'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=3).mean()
    )

    # 구역 수준으로 다시 병합
    merge_cols = [
        'date', 'h3_area_name', 'area_district_count',
        'area_total_opens_prev', 'area_total_rides_prev',
        'area_avg_rpo_prev', 'area_avg_access_prev',
        'area_opens_lag7', 'area_opens_ma7'
    ]
    df = df.merge(area_daily[merge_cols], on=['date', 'h3_area_name'], how='left')

    # 권역 내 구역 비중
    # 비중이 높은 구역은 권역 행동을 주도하고, 낮은 구역은 따라감
    df['district_area_share'] = (
        df['app_opens_rolling']
        / df.groupby(['date', 'h3_area_name'])['app_opens_rolling'].transform('sum')
    )

    n_areas = df['h3_area_name'].nunique()
    print(f"  권역 피처 완료 ({n_areas}개 권역)")
    return df
