"""
후처리 파이프라인

원시 모델 예측값을 보정하여 운영 환경에 적합한 예측치로 변환하는
일련의 조정 단계:

1. RPO 수축: 모델 예측값을 롤링 평균 방향으로 블렌딩
2. 소규모 구역 클리핑: 저물량 구역의 RPO 상한 설정
3. 구역별 보정: 구역 단위 편향 보정
4. 요일별 보정: 요일 단위 편향 보정
5. 공휴일 감쇠: 주요 공휴일 수요 억제

각 단계는 특정 실패 모드를 해결한다:
- 수축 → 소표본 구역에서 RPO 급변동 방지
- 클리핑 → 데이터가 희소할 때 과대 추정 방지
- 보정 → 구역별/요일별 체계적 편향 수정
- 감쇠 → 학습 데이터에 없는 주요 공휴일 수요 감소 반영
"""

import numpy as np
import pandas as pd
from typing import Dict, Optional

from config import (
    RPO_SHRINKAGE_ALPHA,
    SMALL_DISTRICT_THRESHOLD,
    SMALL_DISTRICT_RPO_MULT,
    CALIBRATION_RANGE,
    DOW_CALIBRATION_RANGE,
    CALIBRATION_WINDOW,
)


def apply_rpo_shrinkage(
    pred_rpo: np.ndarray,
    rolling_rpo: np.ndarray,
    alpha: float = RPO_SHRINKAGE_ALPHA,
) -> np.ndarray:
    """
    모델 RPO 예측값을 롤링 평균 방향으로 수축한다.

    동기: 일일 오픈 수가 적은 소규모 구역은 노이즈가 큰 RPO 추정치를
    산출한다. 롤링 평균으로 블렌딩하면 모델이 이탈을 포착하는 능력을
    유지하면서도 예측을 안정화할 수 있다.

    수식: final_rpo = alpha x model_rpo + (1 - alpha) x rolling_rpo

    Args:
        pred_rpo: 원시 모델 예측값
        rolling_rpo: 구역별 14일 롤링 평균 RPO
        alpha: 모델 블렌딩 가중치 (기본값: 0.6)

    Returns:
        수축 조정된 RPO 예측값
    """
    rolling_safe = np.nan_to_num(rolling_rpo, nan=np.nanmean(pred_rpo))
    return alpha * pred_rpo + (1 - alpha) * rolling_safe


def clip_small_district_rpo(
    pred_rpo: np.ndarray,
    opens_rolling: np.ndarray,
    area_names: np.ndarray,
    area_rpo_median: Dict[str, float],
    threshold: float = SMALL_DISTRICT_THRESHOLD,
    multiplier: float = SMALL_DISTRICT_RPO_MULT,
) -> np.ndarray:
    """
    소규모 구역의 RPO를 권역 중앙값 x 배수로 상한 제한한다.

    소규모 구역(평균 오픈 수 < threshold)은 RPO 분산이 크다.
    해당 구역의 RPO는 권역 수준 중앙값을 크게 초과하면 안 된다.
    극단적 값을 정당화할 만큼 충분한 데이터가 없기 때문이다.

    Args:
        pred_rpo: 현재 RPO 예측값
        opens_rolling: 구역별 롤링 평균 앱 오픈 수
        area_names: 행별 권역명
        area_rpo_median: 권역명 → 중앙값 RPO 매핑 Dict
        threshold: 오픈 수가 이 값 미만이면 클리핑 적용
        multiplier: 상한 = 권역_중앙값 x 배수

    Returns:
        클리핑된 RPO 예측값
    """
    clipped = pred_rpo.copy()
    small_mask = opens_rolling < threshold
    n_clipped = 0

    for i in np.where(small_mask)[0]:
        area_med = area_rpo_median.get(area_names[i], 1.5)
        cap = area_med * multiplier
        if clipped[i] > cap:
            clipped[i] = cap
            n_clipped += 1

    if small_mask.sum() > 0:
        pct = n_clipped / small_mask.sum() * 100
        print(f"  [RPO 클리핑] 소규모(<{threshold}): "
              f"{n_clipped}/{small_mask.sum()} 클리핑됨 ({pct:.1f}%)")
    return clipped


def apply_district_calibration(
    pred_rides: np.ndarray,
    districts: np.ndarray,
    calibration: Dict[str, float],
) -> np.ndarray:
    """
    구역별 편향 보정 계수를 적용한다.

    보정 계수는 최근 N일 예측 오차로부터 학습된다:
    계수 = mean(실제값 / 예측값).
    극단적 보정을 방지하기 위해 [0.5, 1.5]로 제한한다.

    Args:
        pred_rides: 현재 라이딩 예측값
        districts: 행별 구역명
        calibration: 구역명 → 보정 계수 매핑 Dict

    Returns:
        보정된 라이딩 예측값
    """
    calibrated = pred_rides.copy()
    n_cal = 0

    for i in range(len(calibrated)):
        factor = calibration.get(districts[i], 1.0)
        factor = np.clip(factor, *CALIBRATION_RANGE)
        if abs(factor - 1.0) > 0.05:
            calibrated[i] *= factor
            n_cal += 1

    print(f"  [구역 보정] {n_cal}/{len(calibrated)} 조정됨")
    return calibrated


def apply_dow_calibration(
    pred_rides: np.ndarray,
    dows: np.ndarray,
    dow_calibration: Dict[int, float],
) -> np.ndarray:
    """
    요일별 편향 보정을 적용한다.

    일부 요일은 지속적으로 과대/과소 예측된다. 이 함수는
    체계적인 요일별 편향을 수정한다 (예: 월요일 과소 예측).

    Args:
        pred_rides: 현재 예측값
        dows: 요일 값 (0=월, 6=일)
        dow_calibration: 요일 → 보정 계수 매핑 Dict

    Returns:
        요일 보정된 예측값
    """
    calibrated = pred_rides.copy()
    n_cal = 0

    for i in range(len(calibrated)):
        factor = dow_calibration.get(int(dows[i]), 1.0)
        factor = np.clip(factor, *DOW_CALIBRATION_RANGE)
        if abs(factor - 1.0) > 0.03:
            calibrated[i] *= factor
            n_cal += 1

    print(f"  [요일 보정] {n_cal}/{len(calibrated)} 조정됨")
    return calibrated


def apply_holiday_dampening(
    pred_rides: np.ndarray,
    is_major_holiday: np.ndarray,
    dampening_factor: float = 0.7,
) -> np.ndarray:
    """
    주요 공휴일에 대한 추가 수요 억제를 적용한다.

    주요 공휴일(설날, 추석)은 50-70% 수요 감소를 유발하는데,
    제한된 학습 사례로는 학습이 어렵다.
    명시적 감쇠 계수가 안전장치 역할을 한다.

    Args:
        pred_rides: 현재 예측값
        is_major_holiday: 행별 바이너리 지표
        dampening_factor: 공휴일에 예측값에 곱할 계수

    Returns:
        공휴일 감쇠된 예측값
    """
    result = pred_rides.copy()
    mask = is_major_holiday == 1

    if mask.any():
        result[mask] *= dampening_factor
        print(f"  [공휴일 감쇠] {mask.sum()}행 x {dampening_factor:.2f}")

    return result


def compute_calibration_factors(
    actuals_df: pd.DataFrame,
    predictions_df: pd.DataFrame,
    window_days: int = CALIBRATION_WINDOW,
) -> Dict[str, float]:
    """
    최근 오차로부터 구역별 보정 계수를 학습한다.

    Args:
        actuals_df: [date, h3_district_name, rides] 컬럼이 있는 DataFrame
        predictions_df: [date, h3_district_name, pred_rides] 컬럼이 있는 DataFrame
        window_days: 사용할 최근 일수

    Returns:
        구역명 → 보정 계수 매핑 Dict
    """
    merged = actuals_df.merge(predictions_df, on=['date', 'h3_district_name'])
    merged = merged.sort_values('date')

    # 최근 N일 데이터 사용
    cutoff = merged['date'].max() - pd.Timedelta(days=window_days)
    recent = merged[merged['date'] > cutoff]

    calibration = {}
    for district, group in recent.groupby('h3_district_name'):
        if len(group) >= 3 and group['pred_rides'].sum() > 0:
            factor = group['rides'].sum() / group['pred_rides'].sum()
            calibration[district] = np.clip(factor, *CALIBRATION_RANGE)

    return calibration


def full_postprocessing(
    pred_opens: np.ndarray,
    pred_rpo: np.ndarray,
    df: pd.DataFrame,
    area_rpo_median: Dict[str, float] = None,
    district_calibration: Dict[str, float] = None,
    dow_calibration: Dict[int, float] = None,
    holiday_dampening: float = 1.0,
) -> np.ndarray:
    """
    전체 후처리 파이프라인을 실행한다.

    Args:
        pred_opens: 예측된 앱 오픈 수
        pred_rpo: 예측된 오픈당 라이딩 비율
        df: 메타데이터 컬럼이 포함된 DataFrame
        area_rpo_median: 클리핑용 권역 수준 RPO 중앙값
        district_calibration: 구역별 편향 보정 계수
        dow_calibration: 요일별 편향 보정 계수
        holiday_dampening: 주요 공휴일 감쇠 계수

    Returns:
        최종 보정된 라이딩 예측값
    """
    print("\n--- 후처리 파이프라인 ---")

    # 1단계: RPO 수축
    if 'rides_per_open_rolling' in df.columns:
        pred_rpo = apply_rpo_shrinkage(
            pred_rpo, df['rides_per_open_rolling'].values
        )

    # 2단계: 소규모 구역 RPO 클리핑
    if area_rpo_median and 'app_opens_rolling' in df.columns:
        pred_rpo = clip_small_district_rpo(
            pred_rpo,
            df['app_opens_rolling'].values,
            df['h3_area_name'].values,
            area_rpo_median,
        )

    # 결합: 라이딩 = 오픈 x RPO
    pred_rides = pred_opens * pred_rpo

    # 3단계: 구역별 보정
    if district_calibration:
        pred_rides = apply_district_calibration(
            pred_rides, df['h3_district_name'].values, district_calibration
        )

    # 4단계: 요일별 보정
    if dow_calibration and 'dow' in df.columns:
        pred_rides = apply_dow_calibration(
            pred_rides, df['dow'].values, dow_calibration
        )

    # 5단계: 공휴일 감쇠
    if holiday_dampening < 1.0 and 'is_major_holiday' in df.columns:
        pred_rides = apply_holiday_dampening(
            pred_rides, df['is_major_holiday'].values, holiday_dampening
        )

    return np.maximum(pred_rides, 0)
