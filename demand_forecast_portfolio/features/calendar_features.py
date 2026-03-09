"""
Stage 5: 캘린더 피처

시간적 패턴 인코딩: 요일, 공휴일, 연속 휴일,
주요 명절 기간, 명절 후 회복기.

핵심 인사이트: 전기자전거 수요는 강한 요일 패턴을 보이며
(주말 +30%), 주요 명절 기간에 급감한다
(설날, 추석: -50~70%).

생성 피처:
    기본: dow, is_weekend, is_holiday, is_off
    인접: is_holiday_eve, near_holiday, days_to_holiday
    연속: is_consecutive_off
    회복: days_since_major_holiday, is_recovery_phase
"""

import pandas as pd


def create_calendar_features(df: pd.DataFrame, holidays_set: set = None) -> pd.DataFrame:
    """
    캘린더 및 공휴일 피처 계산.

    회복기 피처가 특히 중요하다: 연휴(설날, 추석) 후
    수요는 즉시 정상으로 돌아오지 않고 약 7일에 걸쳐 회복된다.

    Args:
        df: [date, h3_district_name] 컬럼을 가진 DataFrame
        holidays_set: 공휴일인 pd.Timestamp 날짜의 Set.
            None이면 주말 피처만 생성된다.

    Returns:
        캘린더 피처가 추가된 DataFrame
    """
    print("[Stage 5] 캘린더 피처...")

    if holidays_set is None:
        holidays_set = set()

    df['dow'] = df['date'].dt.dayofweek
    df['is_weekend'] = (df['dow'] >= 5).astype(int)
    df['is_holiday'] = df['date'].isin(holidays_set).astype(int)
    df['is_off'] = ((df['is_weekend'] == 1) | (df['is_holiday'] == 1)).astype(int)

    # 공휴일 전날: 공휴일 하루 전 (저녁 수요 증가)
    holiday_eves = {h - pd.Timedelta(days=1) for h in holidays_set} - holidays_set
    df['is_holiday_eve'] = df['date'].isin(holiday_eves).astype(int)

    # 가장 가까운 공휴일까지 거리
    holidays_sorted = sorted(holidays_set) if holidays_set else []
    if holidays_sorted:
        df['days_to_holiday'] = df['date'].apply(
            lambda dt: min([(h - dt).days for h in holidays_sorted], key=abs))
        df['near_holiday'] = (df['days_to_holiday'].abs() <= 2).astype(int)
    else:
        df['days_to_holiday'] = 99
        df['near_holiday'] = 0

    # 연속 휴일 (복합 여가 효과)
    df = df.sort_values(['h3_district_name', 'date'])
    df['prev_is_off'] = df.groupby('h3_district_name')['is_off'].shift(1)
    df['is_consecutive_off'] = (
        (df['is_off'] == 1) & (df['prev_is_off'] == 1)
    ).astype(int)
    df = df.drop(columns=['prev_is_off'])

    # ── 주요 명절 감지 및 회복기 ──
    # 주요 명절 = 3일 이상 연속 공휴일 (설날, 추석)
    df['_hol_block'] = df.groupby('h3_district_name')['is_holiday'].transform(
        lambda x: x.rolling(3, min_periods=1, center=True).sum()
    )
    df['_is_major_holiday'] = (df['_hol_block'] >= 2).astype(int)

    def _days_since_major_holiday_end(group):
        """마지막 주요 명절 종료 이후 경과일 추적."""
        result = pd.Series(30, index=group.index)
        last_end = None
        for idx, row in group.iterrows():
            if row['_is_major_holiday'] == 1:
                last_end = row['date']
            if last_end is not None:
                days = (row['date'] - last_end).days
                result[idx] = min(days, 30)
        return result

    df['days_since_major_holiday'] = df.groupby(
        'h3_district_name', group_keys=False
    ).apply(_days_since_major_holiday_end, include_groups=False).reset_index(level=0, drop=True)

    # 회복기: 주요 명절 후 1-7일
    df['is_recovery_phase'] = (
        (df['days_since_major_holiday'] > 0)
        & (df['days_since_major_holiday'] <= 7)
    ).astype(int)

    df = df.drop(columns=['_hol_block', '_is_major_holiday'])

    n_holidays = df['is_holiday'].sum()
    print(f"  캘린더 완료 (공휴일 {len(holidays_set)}일, "
          f"공휴일-구역 행 {n_holidays}개)")
    return df
