"""주말 vs 평일 오차 분석 및 개선 시뮬레이션
- 현재 dow_calibration 값 확인
- 주말/평일 오차 비중 정량화
- 클리핑 해제 시 개선 효과 시뮬레이션
"""
import os, sys, pickle
import numpy as np
import pandas as pd
from datetime import timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Authentication: set GOOGLE_APPLICATION_CREDENTIALS env var
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    _default_cred = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'credentials', 'service-account.json')
    if os.path.exists(_default_cred):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _default_cred

DOW_NAMES = ['월', '화', '수', '목', '금', '토', '일']

# ── 1. 현재 모델의 dow_calibration 확인 ──
print("=" * 60)
print("1️⃣  현재 모델의 요일별 보정값 (dow_calibration)")
print("=" * 60)

model_path = os.path.join(SCRIPT_DIR, 'models', 'production_v2.pkl')
with open(model_path, 'rb') as f:
    # SECURITY: 로컬에서 직접 학습한 모델만 로드 (외부 파일 사용 금지)
    bundle = pickle.load(f)

dow_cal = bundle.get('dow_calibration', {})
print(f"\n  저장된 dow_calibration:")
for d in range(7):
    v = dow_cal.get(d, 1.0)
    marker = " ← 캡에 걸림!" if abs(v) >= 1.29 or abs(v) <= 0.71 else ""
    print(f"    {DOW_NAMES[d]}(dow={d}): {v:.4f}  (예측 × {v:.3f}){marker}")

# ── 2. 최근 14일 평가 (주말/평일 분리) ──
print("\n" + "=" * 60)
print("2️⃣  최근 14일 주말 vs 평일 오차 분석")
print("=" * 60)

from production_v2_predictor import evaluate_period

end_date = '2026-02-26'
start_date = '2026-02-13'
eval_df = evaluate_period(start_date, end_date, verbose=False)

if eval_df is not None and len(eval_df) > 0:
    daily = eval_df.groupby('date').agg(
        actual=('rides', 'sum'),
        predicted=('pred_rides', 'sum')
    ).reset_index()
    daily['dow'] = pd.to_datetime(daily['date']).dt.dayofweek
    daily['day_name'] = daily['dow'].map(lambda d: DOW_NAMES[d])
    daily['is_weekend'] = daily['dow'].isin([5, 6])  # 토, 일
    daily['error_pct'] = ((daily['predicted'] - daily['actual']) / daily['actual'] * 100).round(1)
    daily['abs_error'] = daily['error_pct'].abs()

    print(f"\n  전체 {len(daily)}일 분석:")
    print(f"  {'날짜':12s} {'요일':4s} {'실제':>8s} {'예측':>8s} {'오차':>7s}")
    print("  " + "-" * 45)
    for _, r in daily.sort_values('date').iterrows():
        tag = "🔴" if r['is_weekend'] else "  "
        print(f"  {tag}{str(r['date'])[:10]:10s} {r['day_name']:4s} "
              f"{r['actual']:8,.0f} {r['predicted']:8,.0f} {r['error_pct']:+6.1f}%")

    # 평일/주말 분리 집계
    weekday = daily[~daily['is_weekend']]
    weekend = daily[daily['is_weekend']]

    print(f"\n  ┌─────────────────────────────────────────────┐")
    print(f"  │ 구분     │ 일수 │ 평균 MAPE │ 평균 Bias   │")
    print(f"  ├─────────────────────────────────────────────┤")
    wd_mape = weekday['abs_error'].mean()
    wd_bias = weekday['error_pct'].mean()
    we_mape = weekend['abs_error'].mean()
    we_bias = weekend['error_pct'].mean()
    all_mape = daily['abs_error'].mean()
    all_bias = daily['error_pct'].mean()
    print(f"  │ 평일     │ {len(weekday):3d}  │ {wd_mape:8.1f}% │ {wd_bias:+8.1f}%  │")
    print(f"  │ 주말     │ {len(weekend):3d}  │ {we_mape:8.1f}% │ {we_bias:+8.1f}%  │")
    print(f"  │ 전체     │ {len(daily):3d}  │ {all_mape:8.1f}% │ {all_bias:+8.1f}%  │")
    print(f"  └─────────────────────────────────────────────┘")

    # 주말이 전체 오차에 기여하는 비중
    weekend_abs_sum = weekend['abs_error'].sum()
    total_abs_sum = daily['abs_error'].sum()
    weekend_days_pct = len(weekend) / len(daily) * 100
    weekend_err_pct = weekend_abs_sum / total_abs_sum * 100
    print(f"\n  📊 주말 비중: 일수 {weekend_days_pct:.0f}% → 오차 기여 {weekend_err_pct:.0f}%")

    # ── 3. 주말 보정 시뮬레이션 ──
    print("\n" + "=" * 60)
    print("3️⃣  주말 보정 시뮬레이션 (dow별 실제 bias로 교정)")
    print("=" * 60)

    # 각 요일별 실제 bias 계산
    dow_daily = eval_df.copy()
    dow_daily['dow'] = pd.to_datetime(dow_daily['date']).dt.dayofweek
    dow_agg = dow_daily.groupby('dow').agg(
        actual=('rides', 'sum'),
        predicted=('pred_rides', 'sum'),
    ).reset_index()
    dow_agg['bias_ratio'] = dow_agg['predicted'] / dow_agg['actual']
    dow_agg['needed_cal'] = 1.0 / dow_agg['bias_ratio']  # 이걸 곱해야 보정됨
    dow_agg['current_cal'] = dow_agg['dow'].map(lambda d: dow_cal.get(d, 1.0))

    print(f"\n  {'요일':4s} {'실제':>10s} {'현재예측':>10s} {'bias':>7s} {'필요보정':>8s} {'현재보정':>8s} {'차이':>7s}")
    print("  " + "-" * 60)
    for _, r in dow_agg.sort_values('dow').iterrows():
        d = int(r['dow'])
        gap = r['needed_cal'] - r['current_cal']
        marker = " ← 미흡!" if abs(gap) > 0.05 else ""
        print(f"  {DOW_NAMES[d]:4s} {r['actual']:10,.0f} {r['predicted']:10,.0f} "
              f"{r['bias_ratio']:6.3f} {r['needed_cal']:8.3f} {r['current_cal']:8.3f} "
              f"{gap:+6.3f}{marker}")

    # 시뮬레이션: 새 보정값으로 전체 MAPE 재계산
    print(f"\n  ── 시뮬레이션: 새 보정값 적용 시 ──")
    new_dow_cal = {}
    for _, r in dow_agg.iterrows():
        d = int(r['dow'])
        # 클리핑 없이 적용 (0.5~2.0 넓은 범위)
        new_dow_cal[d] = np.clip(r['needed_cal'], 0.5, 2.0)

    # 시뮬레이션: eval_df에 새 보정값 적용
    sim_df = eval_df.copy()
    sim_df['dow'] = pd.to_datetime(sim_df['date']).dt.dayofweek

    # 현재 보정값 제거 후 새 보정값 적용
    for i in range(len(sim_df)):
        d = int(sim_df.iloc[i]['dow'])
        old_cal = dow_cal.get(d, 1.0)
        new_cal = new_dow_cal.get(d, 1.0)
        # 현재값 / 기존보정 * 새보정
        if abs(old_cal - 1.0) > 0.03:
            sim_df.iloc[i, sim_df.columns.get_loc('pred_rides')] = (
                sim_df.iloc[i]['pred_rides'] / old_cal * new_cal
            )
        elif abs(new_cal - 1.0) > 0.03:
            sim_df.iloc[i, sim_df.columns.get_loc('pred_rides')] *= new_cal

    sim_daily = sim_df.groupby('date').agg(
        actual=('rides', 'sum'),
        predicted=('pred_rides', 'sum')
    ).reset_index()
    sim_daily['dow'] = pd.to_datetime(sim_daily['date']).dt.dayofweek
    sim_daily['is_weekend'] = sim_daily['dow'].isin([5, 6])
    sim_daily['error_pct'] = ((sim_daily['predicted'] - sim_daily['actual']) / sim_daily['actual'] * 100).round(1)
    sim_daily['abs_error'] = sim_daily['error_pct'].abs()

    sim_wd = sim_daily[~sim_daily['is_weekend']]
    sim_we = sim_daily[sim_daily['is_weekend']]

    print(f"\n  ┌───────────────────────────────────────────────────────┐")
    print(f"  │ 구분     │ 현재 MAPE │ 보정 후 MAPE │ 개선       │")
    print(f"  ├───────────────────────────────────────────────────────┤")
    new_wd_mape = sim_wd['abs_error'].mean()
    new_we_mape = sim_we['abs_error'].mean()
    new_all_mape = sim_daily['abs_error'].mean()
    print(f"  │ 평일     │ {wd_mape:8.1f}%  │ {new_wd_mape:10.1f}%  │ {new_wd_mape - wd_mape:+6.1f}%p │")
    print(f"  │ 주말     │ {we_mape:8.1f}%  │ {new_we_mape:10.1f}%  │ {new_we_mape - we_mape:+6.1f}%p │")
    print(f"  │ 전체     │ {all_mape:8.1f}%  │ {new_all_mape:10.1f}%  │ {new_all_mape - all_mape:+6.1f}%p │")
    print(f"  └───────────────────────────────────────────────────────┘")

    # ── 4. 추천 액션 ──
    print(f"\n" + "=" * 60)
    print("4️⃣  추천 액션")
    print("=" * 60)
    print(f"\n  현재 dow_calibration 클리핑: [0.7, 1.3]")
    print(f"  제안: 클리핑 범위를 [0.5, 1.8] 로 확대")
    print(f"\n  새 dow_calibration 값:")
    for d in range(7):
        old = dow_cal.get(d, 1.0)
        new = new_dow_cal.get(d, 1.0)
        if abs(new - old) > 0.03:
            print(f"    {DOW_NAMES[d]}: {old:.3f} → {new:.3f} ({(new-old)/old*100:+.1f}%)")
        else:
            print(f"    {DOW_NAMES[d]}: {old:.3f} (변동 없음)")

else:
    print("  평가 데이터 없음")
