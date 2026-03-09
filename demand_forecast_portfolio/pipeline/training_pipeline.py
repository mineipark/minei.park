"""
학습 파이프라인

엔드투엔드 워크플로우:
    단계 1: 데이터 로드 (DW 또는 샘플)
    단계 2: 피처 엔지니어링 (7단계)
    단계 3: 학습/테스트 분할 및 A그룹 필터링
    단계 4: 앱오픈 모델 학습
    단계 5: RPO 모델 학습
    단계 6: 보정 계수 계산
    단계 7: 결합 예측 평가
    단계 8: 모델 번들 저장

사용법:
    python -m pipeline.training_pipeline
    python -m pipeline.training_pipeline --use-sample  # 합성 데이터
"""

import os
import pickle
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, Tuple

from config import (
    MODEL_START, TRAIN_END, A_GROUP_THRESHOLD,
    B2B_EXCLUDE, WEIGHT_HALF_LIFE_DAYS, CALIBRATION_WINDOW,
)
from features import build_all_features
from models.opens_model import OpensModel
from models.rpo_model import RPOModel
from models.postprocessing import full_postprocessing, compute_calibration_factors
from pipeline.evaluation import evaluate_predictions


def compute_time_weights(dates: pd.Series, half_life_days: int = WEIGHT_HALF_LIFE_DAYS) -> np.ndarray:
    """
    지수 감쇠 가중치: 최근 데이터에 더 높은 가중치를 부여합니다.

    half_life=90일 경우:
        90일 전  → 가중치 0.50
        180일 전 → 가중치 0.25
        360일 전 → 가중치 0.06

    최근 패턴에 우선순위를 두면서도 과거 데이터(계절성, 공휴일)에서
    학습할 수 있도록 합니다.
    """
    max_date = dates.max()
    days_ago = (max_date - dates).dt.days.values.astype(float)
    weights = np.exp(-np.log(2) / half_life_days * days_ago)
    return np.maximum(weights, 0.01)


def prepare_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, list]:
    """
    A그룹 구역 필터링 및 학습/테스트 분할.

    A그룹: 일평균 앱 오픈 수가 임계값을 초과하는 구역
    B2B 제외: 일반적이지 않은 수요 패턴을 보이는 산업 지역
    """
    print("\n[단계 3] 데이터 분할...")

    df_model = df[df['date'] >= MODEL_START].copy()

    # B2B 구역 제외
    df_model = df_model[~df_model['h3_district_name'].isin(B2B_EXCLUDE)]

    # A그룹 필터: 충분한 데이터가 있는 구역만 포함
    district_avg = df_model.groupby('h3_district_name')['app_opens'].mean()
    a_group = district_avg[district_avg > A_GROUP_THRESHOLD].index
    df_model = df_model[df_model['h3_district_name'].isin(a_group)]

    # 피처 이력이 부족한 행 제거
    df_clean = df_model.dropna(subset=['opens_lag7', 'app_opens_rolling'])

    # 시간 기반 분할
    train = df_clean[df_clean['date'] <= TRAIN_END].copy()
    test = df_clean[df_clean['date'] > TRAIN_END].copy()

    print(f"  A그룹: {len(a_group)}개 구역 (임계값: {A_GROUP_THRESHOLD})")
    print(f"  학습: {len(train):,}행 (~{TRAIN_END})")
    print(f"  테스트: {len(test):,}행 ({TRAIN_END}~)")

    return train, test, list(a_group)


def run_training_pipeline(df: pd.DataFrame, weather_df=None, holidays_set=None) -> Dict:
    """
    전체 학습 파이프라인을 실행합니다.

    Args:
        df: 구역×날짜 원시 데이터
        weather_df: 일별 날씨 데이터
        holidays_set: 공휴일 날짜 집합

    Returns:
        학습된 모델, 보정 계수, 평가 결과를 포함하는 Dict
    """
    print("=" * 70)
    print("  수요 예측 학습 파이프라인")
    print("=" * 70)

    # 단계 2: 피처 엔지니어링
    df = build_all_features(df, weather_df, holidays_set)

    # 단계 3: 데이터 분할
    train, test, a_group = prepare_data(df)

    # 시간 가중치 계산 (최근 데이터에 높은 가중치)
    train_weights = compute_time_weights(train['date'])

    # 단계 4: 앱오픈 모델 학습
    print("\n[단계 4] 앱오픈 모델 학습 중...")
    opens_model = OpensModel()
    opens_result = opens_model.train(train, test, sample_weights=train_weights)

    # 단계 5: RPO 모델 학습
    print("\n[단계 5] RPO 모델 학습 중...")
    rpo_model = RPOModel()
    rpo_result = rpo_model.train(train, test, sample_weights=train_weights)

    # 단계 6: 보정 계수 계산
    print("\n[단계 6] 보정 계수 계산 중...")

    # 권역 수준 RPO 중앙값 (소규모 구역 클리핑용)
    area_rpo_median = {}
    recent_train = train[train['date'] > (train['date'].max() - pd.Timedelta(days=30))]
    for area, grp in recent_train.groupby('h3_area_name'):
        area_rpo_median[area] = grp['rides_per_open'].median()

    # 학습 잔차를 이용한 구역별 보정
    train_pred_opens = opens_model.predict(train)
    train_pred_rpo = rpo_model.predict(train)
    train_pred_rides = train_pred_opens * train_pred_rpo
    train_with_pred = train[['date', 'h3_district_name', 'rides']].copy()
    train_with_pred['pred_rides'] = train_pred_rides
    district_calibration = compute_calibration_factors(
        train[['date', 'h3_district_name', 'rides']],
        train_with_pred[['date', 'h3_district_name', 'pred_rides']],
        window_days=CALIBRATION_WINDOW,
    )
    print(f"  구역별 보정: {len(district_calibration)}개 구역")

    # 요일별 보정
    dow_calibration = {}
    for dow in range(7):
        mask = train_with_pred['date'].dt.dayofweek == dow
        sub = train_with_pred[mask]
        if len(sub) > 0 and sub['pred_rides'].sum() > 0:
            dow_calibration[dow] = sub['rides'].sum() / sub['pred_rides'].sum()
    print(f"  요일별 보정: {dow_calibration}")

    # 단계 7: 평가
    print("\n[단계 7] 평가 중...")
    pred_opens = opens_model.predict(test)
    pred_rpo = rpo_model.predict(test)

    pred_rides = full_postprocessing(
        pred_opens, pred_rpo, test,
        area_rpo_median=area_rpo_median,
        district_calibration=district_calibration,
        dow_calibration=dow_calibration,
    )

    eval_result = evaluate_predictions(
        actual_opens=test['app_opens'].values,
        actual_rides=test['rides'].values,
        pred_opens=pred_opens,
        pred_rides=pred_rides,
        dates=test['date'].values,
        baseline=test['rides_lag7'].fillna(test['rides_ma7']).values,
    )

    # 단계 8: 결과 패키징
    bundle = {
        'opens_model': opens_model,
        'rpo_model': rpo_model,
        'a_group_districts': a_group,
        'area_rpo_median': area_rpo_median,
        'district_calibration': district_calibration,
        'dow_calibration': dow_calibration,
        'eval': eval_result,
        'trained_at': datetime.now().isoformat(),
    }

    print("\n" + "=" * 70)
    print(f"  학습 완료. 구역 MAPE: {eval_result['mape']:.1f}%")
    print("=" * 70)

    return bundle


def save_model_bundle(bundle: Dict, path: str):
    """학습된 모델 번들을 디스크에 저장합니다."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        pickle.dump(bundle, f)
    print(f"모델 번들 저장 완료: {path}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-sample', action='store_true',
                        help='합성 샘플 데이터 사용')
    args = parser.parse_args()

    if args.use_sample:
        from data.generate_sample import generate_sample_data
        df, weather_df, holidays = generate_sample_data()
    else:
        raise NotImplementedError(
            "데이터 웨어하우스 접근에는 인증 정보가 필요합니다. "
            "데모를 위해 --use-sample을 사용하세요."
        )

    bundle = run_training_pipeline(df, weather_df, holidays)
    save_model_bundle(bundle, 'models/production_v2.pkl')
