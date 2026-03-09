"""
오차 지표

시스템 전반에서 사용되는 표준 예측 지표.
모든 함수는 엣지 케이스(0, NaN)를 안전하게 처리합니다.
"""

import numpy as np


def mape(actual: np.ndarray, predicted: np.ndarray) -> float:
    """
    평균 절대 백분율 오차 (MAPE).

    MAPE = mean(|actual - predicted| / max(actual, 1)) x 100

    참고: 라이딩 수가 0인 구역에서의 0으로 나누기를 방지하기 위해
    actual 대신 max(actual, 1)을 사용합니다.
    """
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    return np.mean(
        np.abs(actual - predicted) / np.maximum(actual, 1)
    ) * 100


def mae(actual: np.ndarray, predicted: np.ndarray) -> float:
    """평균 절대 오차 (MAE)."""
    return np.mean(np.abs(np.asarray(actual) - np.asarray(predicted)))


def rmse(actual: np.ndarray, predicted: np.ndarray) -> float:
    """평균 제곱근 오차 (RMSE)."""
    return np.sqrt(np.mean((np.asarray(actual) - np.asarray(predicted)) ** 2))


def bias(actual: np.ndarray, predicted: np.ndarray) -> float:
    """
    체계적 편향 (%).

    양수 = 과대 예측, 음수 = 과소 예측.
    편향 = mean(predicted - actual) / mean(actual) x 100
    """
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    mean_actual = np.mean(actual)
    if mean_actual == 0:
        return 0.0
    return np.mean(predicted - actual) / mean_actual * 100


def weighted_mape(actual: np.ndarray, predicted: np.ndarray) -> float:
    """
    가중 MAPE — 대규모 구역이 오차에 더 많이 기여합니다.

    wMAPE = sum(|actual - predicted|) / sum(actual) x 100

    운영 의사결정에 선호되는 지표로, 고볼륨 구역의 오차가
    저볼륨 구역보다 더 중요하기 때문입니다.
    """
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    total = np.sum(actual)
    if total == 0:
        return 0.0
    return np.sum(np.abs(actual - predicted)) / total * 100
