"""
평가 모듈

종합적인 모델 평가 항목:
- 구역 수준 지표 (MAPE, MAE, RMSE, 편향)
- 일별 집계 분석
- 규모별 성능 분석
- 베이스라인 비교 (lag-7 단순 모델)
- 오라클 분석 (실제 앱오픈 x 예측 RPO)
"""

import numpy as np
import pandas as pd
from typing import Dict, Optional

from utils.metrics import mape, mae, rmse, bias


def evaluate_predictions(
    actual_opens: np.ndarray,
    actual_rides: np.ndarray,
    pred_opens: np.ndarray,
    pred_rides: np.ndarray,
    dates: np.ndarray = None,
    baseline: np.ndarray = None,
    opens_rolling: np.ndarray = None,
) -> Dict:
    """
    예측 성능에 대한 종합 평가.

    Args:
        actual_opens: 실제 앱 오픈 수
        actual_rides: 실제 라이딩 수
        pred_opens: 예측 앱 오픈 수
        pred_rides: 예측 라이딩 수
        dates: 일별 집계를 위한 날짜 배열
        baseline: 비교를 위한 베이스라인 예측값 (예: lag-7)
        opens_rolling: 규모 분류를 위한 이동평균 앱 오픈 수

    Returns:
        모든 평가 지표를 포함하는 Dict
    """
    print("\n" + "=" * 70)
    print("  평가 결과")
    print("=" * 70)

    # ── 전체 지표 ──
    _mape = mape(actual_rides, pred_rides)
    _mae = mae(actual_rides, pred_rides)
    _rmse = rmse(actual_rides, pred_rides)
    _bias = bias(actual_rides, pred_rides)

    print(f"\n  전체 (구역 수준):")
    print(f"    MAPE: {_mape:.1f}%")
    print(f"    MAE:  {_mae:.1f}")
    print(f"    RMSE: {_rmse:.1f}")
    print(f"    편향: {_bias:+.1f}%")

    result = {
        'mape': _mape, 'mae': _mae, 'rmse': _rmse, 'bias': _bias,
    }

    # ── 베이스라인 비교 ──
    if baseline is not None:
        bl_safe = np.nan_to_num(baseline, nan=np.nanmean(actual_rides))
        bl_mape = mape(actual_rides, bl_safe)
        improvement = bl_mape - _mape
        print(f"\n  vs 베이스라인 (lag-7): {bl_mape:.1f}% → {_mape:.1f}% "
              f"({improvement:+.1f}%p 개선)")
        result['baseline_mape'] = bl_mape

    # ── 오라클 분석 ──
    # 완벽한 앱오픈 예측이 있다면, RPO만으로 얼마나 좋은 성능을 낼 수 있는가?
    pred_rpo_implicit = np.where(pred_opens > 0, pred_rides / pred_opens, 0)
    oracle_rides = actual_opens * pred_rpo_implicit
    oracle_mape = mape(actual_rides, oracle_rides)
    opens_cost = _mape - oracle_mape
    print(f"\n  오라클 (실제_앱오픈 x 예측_RPO): {oracle_mape:.1f}%")
    print(f"  앱오픈 예측 비용: +{opens_cost:.1f}%p")
    result['oracle_mape'] = oracle_mape

    # ── 일별 집계 ──
    if dates is not None:
        print(f"\n  --- 일별 상세 ---")
        print(f"  {'날짜':>10} | {'실제':>8} {'예측':>8} {'오차%':>7} |")
        print(f"  {'-' * 42}")

        unique_dates = sorted(np.unique(dates))
        daily_actuals, daily_preds = [], []

        for d in unique_dates:
            mask = dates == d
            a_sum = actual_rides[mask].sum()
            p_sum = pred_rides[mask].sum()
            err = (p_sum / a_sum - 1) * 100 if a_sum > 0 else 0

            daily_actuals.append(a_sum)
            daily_preds.append(p_sum)

            d_str = pd.Timestamp(d).strftime('%m/%d(%a)')
            print(f"  {d_str:>10} | {a_sum:8.0f} {p_sum:8.0f} {err:+6.1f}% |")

        daily_actuals = np.array(daily_actuals)
        daily_preds = np.array(daily_preds)

        total_a = daily_actuals.sum()
        total_p = daily_preds.sum()
        print(f"  {'-' * 42}")
        print(f"  {'합계':>10} | {total_a:8.0f} {total_p:8.0f} "
              f"{(total_p / total_a - 1) * 100:+6.1f}% |")

        daily_mape = np.mean(np.abs(daily_actuals - daily_preds) / daily_actuals) * 100
        print(f"\n  일별 집계 MAPE: {daily_mape:.1f}%")
        result['daily_mape'] = daily_mape

    # ── 규모별 분석 ──
    if opens_rolling is not None:
        print(f"\n  --- 구역 규모별 성능 ---")
        for label, lo, hi in [
            ('소형 (8-15)', 8, 15),
            ('중형 (15-30)', 15, 30),
            ('대형 (30-60)', 30, 60),
            ('초대형 (60+)', 60, 999),
        ]:
            mask = (opens_rolling >= lo) & (opens_rolling < hi)
            if mask.sum() > 0:
                s_mape = mape(actual_rides[mask], pred_rides[mask])
                s_bias = bias(actual_rides[mask], pred_rides[mask])
                print(f"    {label:20s}: MAPE {s_mape:5.1f}%, "
                      f"편향 {s_bias:+5.1f}% ({mask.sum():,}행)")

    return result
