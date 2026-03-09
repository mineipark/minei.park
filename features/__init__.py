"""
피처 엔지니어링 파이프라인 (7단계)

구역×날짜 원시 데이터를 체계적인 7단계 파이프라인을 통해
ML 학습용 피처로 변환한다.

    Stage 1: 셀프 롤링       - 구역별 14일 롤링 통계
    Stage 2: 공간 피처       - 2km 이웃 집계 + 허브 기준점
    Stage 3: 권역 피처       - 상위 권역 수준 집계
    Stage 4: 래그 & 모멘텀   - 시간 이동 피처 + 추세 감지
    Stage 5: 캘린더          - 공휴일, 주말, 연속 휴일
    Stage 6: 날씨            - 기온, 비, 바람, 눈
    Stage 7: 교차 피처       - 피처 간 조합

사용법:
    from features import build_all_features
    df = build_all_features(raw_df, weather_df, holidays_set)
"""

from features.rolling_features import create_self_features
from features.spatial_features import create_neighbor_hub_features
from features.area_features import create_area_features
from features.lag_features import create_lag_features
from features.calendar_features import create_calendar_features
from features.weather_features import create_weather_features
from features.interaction_features import create_interaction_features


def build_all_features(df, weather_df=None, holidays_set=None):
    """
    전체 7단계 피처 엔지니어링 파이프라인 실행.

    Args:
        df: [date, h3_area_name, h3_district_name,
            app_opens, rides, rides_per_open, avg_bikes_100m, avg_bikes_400m,
            accessibility_rate, center_lat, center_lng, hour_std, ...] 컬럼을 가진 DataFrame
        weather_df: [date, temp_low, temp_high, rain_sum, ...] 컬럼을 가진 DataFrame
        holidays_set: 공휴일 날짜 Set

    Returns:
        모든 엔지니어링 피처가 추가된 DataFrame
    """
    print("=" * 60)
    print("피처 엔지니어링 파이프라인 (7단계)")
    print("=" * 60)

    df = create_self_features(df)           # Stage 1
    df = create_neighbor_hub_features(df)   # Stage 2
    df = create_area_features(df)           # Stage 3
    df = create_lag_features(df)            # Stage 4
    df = create_calendar_features(df, holidays_set)  # Stage 5
    df = create_weather_features(df, weather_df)     # Stage 6
    df = create_interaction_features(df)    # Stage 7

    print(f"\n피처 엔지니어링 완료: {df.shape[1]}개 컬럼")
    return df
