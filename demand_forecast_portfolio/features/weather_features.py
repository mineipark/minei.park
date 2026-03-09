"""
Stage 6: 날씨 피처

날씨는 전기자전거 수요에 강한 영향을 미친다:
- 비: 라이딩 -30~50%
- 한파 (<-8도): 라이딩 -20~40%
- 악천후 (폭우 + 바람 + 한파): 라이딩 -50~70%

생성 피처:
    연속형: temp_low/high/avg, rain_sum, windspeed_avg,
            humidity_avg, snow_depth, temp_range
    이진형: is_cold, is_freeze, is_rain, is_heavy_rain,
            is_windy, is_snow, is_severe_weather
"""

import pandas as pd


def create_weather_features(df: pd.DataFrame, weather_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    날씨 피처 병합 및 파생.

    Args:
        df: [date] 컬럼을 가진 DataFrame
        weather_df: 일별 날씨 데이터 DataFrame:
            [date, temp_low, temp_high, rain_sum, windspeed_avg,
             humidity_avg, snow_depth]
            None이면 모든 날씨 피처가 0으로 기본 설정된다.

    Returns:
        날씨 피처가 추가된 DataFrame
    """
    print("[Stage 6] 날씨 피처...")

    if weather_df is not None and len(weather_df) > 0:
        weather = weather_df.copy()
        weather['date'] = pd.to_datetime(weather['date'])

        # 결측값 처리
        weather['rain_sum'] = weather['rain_sum'].fillna(0)
        weather['windspeed_avg'] = weather.get('windspeed_avg', pd.Series(dtype=float)).fillna(0)
        weather['humidity_avg'] = weather.get('humidity_avg', pd.Series(dtype=float)).fillna(50)
        weather['snow_depth'] = weather.get('snow_depth', pd.Series(dtype=float)).fillna(0)

        # 파생 연속형 피처
        weather['temp_avg'] = (weather['temp_low'] + weather['temp_high']) / 2
        weather['temp_range'] = weather['temp_high'] - weather['temp_low']

        # 이진 임계값 피처
        weather['is_cold'] = (weather['temp_low'] <= -8).astype(int)
        weather['is_freeze'] = (weather['temp_high'] <= 0).astype(int)
        weather['is_rain'] = (weather['rain_sum'] > 0).astype(int)
        weather['is_heavy_rain'] = (weather['rain_sum'] >= 10).astype(int)
        weather['is_windy'] = (weather['windspeed_avg'] >= 8).astype(int)
        weather['is_snow'] = (weather['snow_depth'] > 0).astype(int)

        # 복합 악천후 지표
        weather['is_severe_weather'] = (
            (weather['rain_sum'] >= 10)
            | (weather['windspeed_avg'] >= 7)
            | ((weather['temp_high'] <= 2) & (weather['rain_sum'] > 0))
        ).astype(int)

        # 병합
        merge_cols = [c for c in weather.columns if c in [
            'date', 'temp_low', 'temp_high', 'temp_avg', 'rain_sum',
            'windspeed_avg', 'humidity_avg', 'snow_depth', 'temp_range',
            'is_cold', 'is_freeze', 'is_rain', 'is_heavy_rain',
            'is_windy', 'is_snow', 'is_severe_weather'
        ]]
        df = df.merge(weather[merge_cols], on='date', how='left')

        # 결측 구간 전방 채움 및 나머지 NaN 채움
        continuous = ['temp_low', 'temp_high', 'temp_avg', 'rain_sum',
                      'windspeed_avg', 'humidity_avg', 'snow_depth', 'temp_range']
        for col in continuous:
            if col in df.columns:
                df[col] = df[col].ffill().fillna(0)

        binary = ['is_cold', 'is_freeze', 'is_rain', 'is_heavy_rain',
                   'is_windy', 'is_snow', 'is_severe_weather']
        for col in binary:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)

        print(f"  날씨 데이터 병합 완료 ({len(weather)}일)")
    else:
        print("  날씨 데이터 미제공 — 기본값 사용")
        for col in ['temp_low', 'temp_high', 'temp_avg', 'rain_sum',
                     'is_cold', 'is_freeze', 'is_rain', 'is_heavy_rain',
                     'windspeed_avg', 'humidity_avg', 'snow_depth',
                     'is_windy', 'is_snow', 'is_severe_weather', 'temp_range']:
            df[col] = 0

    return df
