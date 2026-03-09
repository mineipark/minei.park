#!/usr/bin/env python3
"""
수요 예측 시스템 — 엔드투엔드 데모

합성 데이터를 사용하여 전체 파이프라인을 시연합니다:
    1. 현실적인 샘플 데이터 생성
    2. 7단계 피처 엔지니어링 실행
    3. 앱오픈 및 RPO 모델 학습
    4. 후처리 적용 (축소, 보정)
    5. 예측 평가
    6. 전환율 모델 시연

실행:
    python run_demo.py
"""

import sys
import os

# 프로젝트 루트를 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd


def main():
    print("=" * 70)
    print("  전기자전거 수요 예측 시스템 — 포트폴리오 데모")
    print("=" * 70)

    # ── 단계 1: 데이터 생성 ──
    print("\n[1/5] 합성 데이터 생성 중...")
    from data.generate_sample import generate_sample_data
    df, weather_df, holidays = generate_sample_data(
        n_districts=25, n_areas=5, seed=42
    )

    # ── 단계 2: 피처 엔지니어링 ──
    print("\n[2/5] 피처 엔지니어링 파이프라인 실행 중...")
    from features import build_all_features
    df = build_all_features(df, weather_df, holidays)

    # ── 단계 3: 모델 학습 ──
    print("\n[3/5] 모델 학습 중...")
    from config import MODEL_START, TRAIN_END, A_GROUP_THRESHOLD
    from models.opens_model import OpensModel
    from models.rpo_model import RPOModel
    from models.postprocessing import full_postprocessing
    from pipeline.evaluation import evaluate_predictions

    # 데이터 분할
    df_model = df[df['date'] >= MODEL_START].copy()
    district_avg = df_model.groupby('h3_district_name')['app_opens'].mean()
    a_group = district_avg[district_avg > A_GROUP_THRESHOLD].index
    df_model = df_model[df_model['h3_district_name'].isin(a_group)]
    df_clean = df_model.dropna(subset=['opens_lag7', 'app_opens_rolling'])

    train = df_clean[df_clean['date'] <= TRAIN_END].copy()
    test = df_clean[df_clean['date'] > TRAIN_END].copy()

    print(f"\n  A그룹: {len(a_group)}개 구역")
    print(f"  학습: {len(train):,}행")
    print(f"  테스트: {len(test):,}행")

    if len(test) == 0:
        print("\n  경고: 테스트 데이터 없음 — 분할 조정 중...")
        split_date = df_clean['date'].quantile(0.85)
        train = df_clean[df_clean['date'] <= split_date].copy()
        test = df_clean[df_clean['date'] > split_date].copy()
        print(f"  조정 완료 — 학습: {len(train):,}, 테스트: {len(test):,}")

    # 앱오픈 모델 학습
    opens_model = OpensModel()
    opens_result = opens_model.train(train, test)

    # RPO 모델 학습
    rpo_model = RPOModel()
    rpo_result = rpo_model.train(train, test)

    # ── 단계 4: 후처리 및 평가 ──
    print("\n[4/5] 후처리 및 평가 중...")

    pred_opens = opens_model.predict(test)
    pred_rpo = rpo_model.predict(test)

    # 클리핑을 위한 권역 RPO 중앙값
    area_rpo_median = {}
    for area, grp in train.groupby('h3_area_name'):
        area_rpo_median[area] = grp['rides_per_open'].median()

    pred_rides = full_postprocessing(
        pred_opens, pred_rpo, test,
        area_rpo_median=area_rpo_median,
    )

    baseline = test['rides_lag7'].fillna(test['rides_ma7']).values
    baseline = np.nan_to_num(baseline, nan=np.nanmean(test['rides'].values))

    eval_result = evaluate_predictions(
        actual_opens=test['app_opens'].values,
        actual_rides=test['rides'].values,
        pred_opens=pred_opens,
        pred_rides=pred_rides,
        dates=test['date'].values,
        baseline=baseline,
        opens_rolling=test['app_opens_rolling'].values if 'app_opens_rolling' in test.columns else None,
    )

    # ── 단계 5: 전환율 모델 데모 ──
    print("\n[5/5] 전환율 모델 데모...")
    from conversion.conversion_model import ConversionModel

    conv_model = ConversionModel()

    print(f"\n  기기 공급량 → 전환율:")
    for n in [0, 1, 2, 3, 5, 10]:
        cvr = conv_model.predict_conversion_rate(n)
        print(f"    100m 이내 기기 {n}대 → 전환율 {cvr:.1%}")

    print(f"\n  목표 전환율을 위한 필요 기기 수:")
    for target in [0.5, 0.6, 0.7]:
        bikes = conv_model.inverse_conversion_rate(target)
        print(f"    목표 {target:.0%} → 평균 {bikes:.1f}대/100m 필요")

    result = conv_model.estimate_unconstrained(
        realized_rides=1000, avg_bike_count=2.0
    )
    print(f"\n  비제약 수요 (1000건 라이딩, 100m당 기기 2대):")
    print(f"    현재 전환율: {result['current_cvr']:.1%}")
    print(f"    최대 전환율: {result['max_cvr']:.1%}")
    print(f"    비제약 라이딩: {result['unconstrained_rides']:.0f}건")
    print(f"    억제된 라이딩: {result['suppressed_rides']:.0f}건 "
          f"({result['gap_pct']:.1f}% 갭)")

    # ── 요약 ──
    print("\n" + "=" * 70)
    print("  데모 완료")
    print("=" * 70)
    print(f"\n  주요 결과:")
    print(f"    구역 수준 MAPE: {eval_result['mape']:.1f}%")
    if 'daily_mape' in eval_result:
        print(f"    일별 집계 MAPE: {eval_result['daily_mape']:.1f}%")
    if 'baseline_mape' in eval_result:
        improvement = eval_result['baseline_mape'] - eval_result['mape']
        print(f"    vs 베이스라인: {improvement:+.1f}%p 개선")
    print(f"    앱오픈 모델 피처 수: {len(opens_model.features)}")
    print(f"    RPO 모델 피처 수: {len(rpo_model.features)}")
    print(f"\n  아키텍처: 예측_라이딩 = 예측_앱오픈 x 예측_RPO")
    print(f"  후처리: RPO 축소 + 보정 + 공휴일 감쇠")


if __name__ == '__main__':
    main()
