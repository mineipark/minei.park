"""
Stage 4: 래그 & 모멘텀 피처

시간 패턴을 포착하는 시간 이동 피처:
- 직접 래그: 전일, 전주
- 동일 요일 평균: 요일별 4주 롤링
- 상대 변화율: 롤링 평균 대비 편차
- 모멘텀: 단기 vs 중기 추세 비교
- 선형 추세 기울기: 7일 피팅 방향

생성 피처:
    래그: opens_lag1/7, opens_ma7, opens_same_dow_avg
    변화율: opens_ratio_to_avg, opens_wow_change, rpo_ratio_to_avg
    스케일: log_opens_rolling, rpo_x_opens
    모멘텀: opens_trend_7d, opens_momentum, rpo_momentum
"""

import numpy as np
import pandas as pd


def create_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    래그, 상대 변화율, 모멘텀 피처 계산.

    설계 원칙:
    - 모든 래그는 데이터 누출 방지를 위해 shift(1+) 사용
    - 모멘텀 = 단기 MA / 장기 MA (>1 = 상승 추세)
    - 상대 비율은 "어제가 얼마나 이례적이었는지" 신호를 포착
    - 스케일 피처(로그, 곱)는 RPO 모델이 물량과 전환율 간
      역관계를 학습하도록 지원

    Args:
        df: [h3_district_name, date, app_opens, rides,
            rides_per_open, app_opens_rolling, rides_per_open_rolling,
            accessibility_rate, accessibility_rate_rolling,
            cond_conversion_rate, cond_conversion_rate_rolling] 컬럼을 가진 DataFrame

    Returns:
        래그 및 모멘텀 피처가 추가된 DataFrame
    """
    print("[Stage 4] 래그 & 모멘텀 피처...")

    df = df.sort_values(['h3_district_name', 'date']).copy()
    g = df.groupby('h3_district_name')

    # ── 직접 래그 ──
    df['opens_lag1'] = g['app_opens'].shift(1)
    df['opens_lag7'] = g['app_opens'].shift(7)
    df['opens_ma7'] = g['app_opens'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=3).mean())

    # 동일 요일 평균 (4주 과거 데이터)
    for lag in [14, 21, 28]:
        df[f'_opens_lag{lag}'] = g['app_opens'].shift(lag)
    df['opens_same_dow_avg'] = df[
        ['opens_lag7', '_opens_lag14', '_opens_lag21', '_opens_lag28']
    ].mean(axis=1)
    df = df.drop(columns=['_opens_lag14', '_opens_lag21', '_opens_lag28'])

    # 라이딩 래그
    if 'rides' in df.columns:
        df['rides_lag1'] = g['rides'].shift(1)
        df['rides_lag7'] = g['rides'].shift(7)
        df['rides_ma7'] = g['rides'].transform(
            lambda x: x.shift(1).rolling(7, min_periods=3).mean())

    # ── 상대 변화율 ──
    # "어제가 14일 평균보다 높았는가, 낮았는가?"
    df['opens_ratio_to_avg'] = (
        df['opens_lag1'] / df['app_opens_rolling'].clip(lower=1)
    )
    # 전주 대비 변화율
    df['opens_wow_change'] = (
        (df['opens_lag1'] - df['opens_lag7']) / df['opens_lag7'].clip(lower=1)
    )

    # 롤링 평균 대비 RPO
    rpo_lag1 = g['rides_per_open'].shift(1)
    df['rpo_ratio_to_avg'] = (
        rpo_lag1 / df['rides_per_open_rolling'].clip(lower=0.01)
    )

    # ── 스케일 인식 피처 ──
    # 앱 오픈 증가 → RPO 감소 (희석 효과). 이 피처들은
    # 모델이 이 역관계를 학습하도록 지원한다.
    df['log_opens_rolling'] = np.log1p(df['app_opens_rolling'].fillna(0))
    df['rpo_x_opens'] = (
        df['rides_per_open_rolling'].fillna(0) * df['app_opens_rolling'].fillna(0)
    )

    # ── 모멘텀 피처 ──
    # 7일 선형 기울기: 양수 = 수요 상승 중
    df['opens_trend_7d'] = g['app_opens'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=4).apply(
            lambda w: np.polyfit(range(len(w)), w, 1)[0] if len(w) >= 4 else 0,
            raw=False
        )
    )

    # 단기(7일) vs 중기(28일) 이동평균 비율
    # >1이면 최근 수요가 장기 추세보다 높음
    _opens_ma28 = g['app_opens'].transform(
        lambda x: x.shift(1).rolling(28, min_periods=7).mean())
    df['opens_momentum'] = df['opens_ma7'] / _opens_ma28.clip(lower=1)

    # RPO 모멘텀
    _rpo_ma7 = g['rides_per_open'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=3).mean())
    _rpo_ma28 = g['rides_per_open'].transform(
        lambda x: x.shift(1).rolling(28, min_periods=7).mean())
    df['rpo_momentum'] = _rpo_ma7 / _rpo_ma28.clip(lower=0.01)

    # ── 접근성 모멘텀 ──
    df['acc_rate_lag1'] = g['accessibility_rate'].shift(1)
    df['acc_rate_lag7'] = g['accessibility_rate'].shift(7)
    _acc_ma7 = g['accessibility_rate'].transform(
        lambda x: x.shift(1).rolling(7, min_periods=3).mean())
    _acc_ma28 = g['accessibility_rate'].transform(
        lambda x: x.shift(1).rolling(28, min_periods=7).mean())
    df['acc_momentum'] = _acc_ma7 / _acc_ma28.clip(lower=0.01)

    # ── 조건부 전환율 모멘텀 ──
    if 'cond_conversion_rate' in df.columns:
        df['cond_conv_lag1'] = g['cond_conversion_rate'].shift(1)
        df['cond_conv_lag7'] = g['cond_conversion_rate'].shift(7)
        _cconv_ma7 = g['cond_conversion_rate'].transform(
            lambda x: x.shift(1).rolling(7, min_periods=3).mean())
        _cconv_ma28 = g['cond_conversion_rate'].transform(
            lambda x: x.shift(1).rolling(28, min_periods=7).mean())
        df['cond_conv_momentum'] = _cconv_ma7 / _cconv_ma28.clip(lower=0.01)

    # ── RPO 래그 직접 값 ──
    df['rpo_lag1'] = g['rides_per_open'].shift(1)
    df['rpo_lag7'] = g['rides_per_open'].shift(7)

    print(f"  래그, 비율, 스케일, 모멘텀 피처 생성 완료")
    return df
