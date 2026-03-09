"""
District × Hour 예측 자동 학습 시스템

3단계 학습:
  Level 1 (매일): district 비율 + hour 비율 자동 보정
  Level 2 (주 1회): 조건별 계수 학습 (날씨/요일)
  Level 3 (주 1회): LightGBM 직접 ML 예측 + 자동 롤백

사용법:
    python district_hour_tuner.py                  # 일일 파이프라인
    python district_hour_tuner.py evaluate          # 전날 평가만
    python district_hour_tuner.py tune              # 비율 보정만
    python district_hour_tuner.py --full            # Level 1-3 전체
"""
import os
import sys
import json
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

# Authentication: set GOOGLE_APPLICATION_CREDENTIALS env var
SCRIPT_DIR_CREDS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'credentials', 'service-account.json')
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    if os.path.exists(SCRIPT_DIR_CREDS):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SCRIPT_DIR_CREDS

# 파일 경로
DH_PARAMS_PATH = os.path.join(SCRIPT_DIR, 'district_hour_params.json')
DH_PERF_LOG_PATH = os.path.join(SCRIPT_DIR, 'district_hour_performance_log.json')

# 학습 설정
LEARNING_RATE = 0.15        # 비율 보정 속도
BIAS_THRESHOLD = 0.05       # 5% 이상 bias일 때만 보정
MIN_SAMPLES = 3             # 최소 데이터 포인트
MAX_ADJUSTMENT = 0.10       # 최대 1회 조정폭 (비율)
MAPE_ALERT_THRESHOLD = 20   # MAPE 20% 초과 시 경고


class DistrictHourTuner:
    """District × Hour 예측 자동 학습"""

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self._client = None
        self.params = self._load_params()
        self.perf_log = self._load_perf_log()

    @property
    def client(self):
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client()
        return self._client

    # ================================================================
    # 파라미터 저장/로드
    # ================================================================

    def _load_params(self) -> Dict:
        """district_hour_params.json 로드"""
        if os.path.exists(DH_PARAMS_PATH):
            with open(DH_PARAMS_PATH, 'r') as f:
                return json.load(f)
        return {
            'district_ratios': {},      # {region: {district: ratio}}
            'hourly_profiles': {},       # {region_district_daytype: {hour: ratio}}
            'condition_coefficients': {},  # {district: {cold_adj, snow_adj, ...}}
            'ml_model_active': False,
            'last_updated': None,
        }

    def _save_params(self):
        """district_hour_params.json 저장"""
        self.params['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
        with open(DH_PARAMS_PATH, 'w') as f:
            json.dump(self.params, f, indent=2, ensure_ascii=False)
        if self.verbose:
            print(f"  💾 파라미터 저장: {DH_PARAMS_PATH}")

    def _load_perf_log(self) -> Dict:
        """성능 로그 로드"""
        if os.path.exists(DH_PERF_LOG_PATH):
            with open(DH_PERF_LOG_PATH, 'r') as f:
                return json.load(f)
        return {
            'daily': [],           # [{date, mape, bias, ...}]
            'corrections': [],     # [{date, type, adjustments}]
            'model_comparison': [],  # [{date, ratio_mape, ml_mape}]
        }

    def _save_perf_log(self):
        """성능 로그 저장"""
        # 최근 90일만 유지
        cutoff = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        self.perf_log['daily'] = [
            d for d in self.perf_log['daily'] if d.get('date', '') >= cutoff]
        self.perf_log['corrections'] = [
            c for c in self.perf_log['corrections'] if c.get('date', '') >= cutoff]

        with open(DH_PERF_LOG_PATH, 'w') as f:
            json.dump(self.perf_log, f, indent=2, ensure_ascii=False)

    # ================================================================
    # 전날 평가 (예측 vs 실제)
    # ================================================================

    def evaluate_yesterday(self, target_date: str = None) -> Dict:
        """
        전날 예측 vs 실제 비교

        Returns:
            {date, total_mape, district_mape, hour_mape, bias,
             district_errors: [{region, district, pred, actual, error}],
             hour_errors: [{hour, pred, actual, error}]}
        """
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        if self.verbose:
            print(f"\n📊 전날 평가: {target_date}")

        # 예측 생성
        from district_hour_model import DistrictHourPredictor
        predictor = DistrictHourPredictor(verbose=False)
        pred_df = predictor.predict(target_date)

        if len(pred_df) == 0:
            return {'date': target_date, 'error': 'no predictions'}

        # 실제 데이터
        actual_query = f"""
        SELECT
            h3_start_area_name as region,
            h3_start_district_name as district,
            EXTRACT(HOUR FROM start_time) as hour,
            COUNT(*) as actual_rides
        FROM `bikeshare.service.rides`
        WHERE DATE(start_time) = '{target_date}'
            AND h3_start_area_name IS NOT NULL
            AND h3_start_district_name IS NOT NULL
        GROUP BY 1, 2, 3
        """

        try:
            actual_df = self.client.query(actual_query).to_dataframe()
        except Exception as e:
            return {'date': target_date, 'error': str(e)}

        if len(actual_df) == 0:
            return {'date': target_date, 'error': 'no actual data'}

        # 병합
        merged = pred_df.merge(
            actual_df, on=['region', 'district', 'hour'], how='outer')
        merged['predicted_rides'] = merged['predicted_rides'].fillna(0)
        merged['actual_rides'] = merged['actual_rides'].fillna(0)

        # 전체 통계
        total_pred = merged['predicted_rides'].sum()
        total_actual = merged['actual_rides'].sum()
        overall_bias = ((total_pred - total_actual) / total_actual * 100
                       if total_actual > 0 else 0)

        # District 레벨 MAPE
        d_agg = merged.groupby(['region', 'district']).agg({
            'predicted_rides': 'sum',
            'actual_rides': 'sum'
        }).reset_index()
        d_agg = d_agg[d_agg['actual_rides'] > 0]
        d_agg['ape'] = ((d_agg['predicted_rides'] - d_agg['actual_rides']).abs()
                        / d_agg['actual_rides'] * 100)
        d_agg['error_pct'] = ((d_agg['predicted_rides'] - d_agg['actual_rides'])
                              / d_agg['actual_rides'] * 100)
        district_mape = d_agg['ape'].mean() if len(d_agg) > 0 else None

        # District별 비율 오차 (비율 보정용)
        d_agg['pred_ratio'] = (d_agg['predicted_rides']
                               / d_agg.groupby('region')['predicted_rides'].transform('sum'))
        d_agg['actual_ratio'] = (d_agg['actual_rides']
                                 / d_agg.groupby('region')['actual_rides'].transform('sum'))
        d_agg['ratio_diff'] = d_agg['actual_ratio'] - d_agg['pred_ratio']

        # Hour 레벨 MAPE
        h_agg = merged.groupby('hour').agg({
            'predicted_rides': 'sum',
            'actual_rides': 'sum'
        }).reset_index()
        h_agg = h_agg[h_agg['actual_rides'] > 0]
        h_agg['ape'] = ((h_agg['predicted_rides'] - h_agg['actual_rides']).abs()
                        / h_agg['actual_rides'] * 100)
        hour_mape = h_agg['ape'].mean() if len(h_agg) > 0 else None

        result = {
            'date': target_date,
            'total_pred': round(total_pred),
            'total_actual': int(total_actual),
            'overall_bias': round(overall_bias, 1),
            'district_mape': round(district_mape, 1) if district_mape else None,
            'hour_mape': round(hour_mape, 1) if hour_mape else None,
            'district_errors': d_agg.to_dict('records'),
            'hour_errors': h_agg.to_dict('records'),
        }

        # 로그 기록 (중복 방지: 같은 날짜 있으면 덮어쓰기)
        existing_dates = {d['date'] for d in self.perf_log['daily']}
        new_entry = {
            'date': target_date,
            'total_pred': result['total_pred'],
            'total_actual': result['total_actual'],
            'overall_bias': result['overall_bias'],
            'district_mape': result['district_mape'],
            'hour_mape': result['hour_mape'],
        }
        if target_date in existing_dates:
            self.perf_log['daily'] = [
                new_entry if d['date'] == target_date else d
                for d in self.perf_log['daily']
            ]
        else:
            self.perf_log['daily'].append(new_entry)
        self._save_perf_log()

        if self.verbose:
            print(f"  총 예측: {total_pred:,.0f} / 실제: {total_actual:,.0f} "
                  f"(bias: {overall_bias:+.1f}%)")
            if district_mape:
                print(f"  District MAPE: {district_mape:.1f}%")
            if hour_mape:
                print(f"  Hour MAPE: {hour_mape:.1f}%")
            if district_mape and district_mape > MAPE_ALERT_THRESHOLD:
                print(f"  ⚠️ MAPE {district_mape:.1f}% > {MAPE_ALERT_THRESHOLD}% 경고!")

        return result

    # ================================================================
    # Level 1: 비율 자동 보정 (매일)
    # ================================================================

    def tune_district_ratios(self, eval_result: Dict) -> int:
        """
        District 비율 보정

        실제 ratio vs 예측 ratio 비교 → bias 감지 시 비율 조정
        """
        if self.verbose:
            print(f"\n🔧 [Level 1] District 비율 보정")

        d_errors = eval_result.get('district_errors', [])
        if not d_errors:
            if self.verbose:
                print("  데이터 없음, 스킵")
            return 0

        adjustments = 0

        for d in d_errors:
            region = d.get('region', '')
            district = d.get('district', '')
            ratio_diff = d.get('ratio_diff')

            # NaN/None 체크 (outer join으로 예측/실제 불일치 시)
            if ratio_diff is None or (isinstance(ratio_diff, float) and np.isnan(ratio_diff)):
                continue

            if abs(ratio_diff) < BIAS_THRESHOLD:
                continue  # 5% 미만 차이는 무시 (노이즈)

            # 보정 적용
            if region not in self.params['district_ratios']:
                self.params['district_ratios'][region] = {}

            old_ratio = self.params['district_ratios'][region].get(district)

            if old_ratio is None:
                # 처음이면 현재 pred_ratio를 base로
                pred_ratio = d.get('pred_ratio', 0)
                if pred_ratio is None or (isinstance(pred_ratio, float) and np.isnan(pred_ratio)):
                    pred_ratio = 0
                if pred_ratio > 0:
                    new_ratio = pred_ratio + ratio_diff * LEARNING_RATE
                    new_ratio = max(0.01, min(0.99, new_ratio))
                    self.params['district_ratios'][region][district] = round(new_ratio, 4)
                    adjustments += 1
            else:
                # 기존 값 보정
                adjustment = ratio_diff * LEARNING_RATE
                adjustment = max(-MAX_ADJUSTMENT, min(MAX_ADJUSTMENT, adjustment))
                new_ratio = old_ratio + adjustment
                new_ratio = max(0.01, min(0.99, new_ratio))

                if abs(new_ratio - old_ratio) > 0.001:
                    self.params['district_ratios'][region][district] = round(new_ratio, 4)
                    adjustments += 1

        if adjustments > 0:
            self._save_params()
            self.perf_log['corrections'].append({
                'date': eval_result.get('date', ''),
                'type': 'district_ratio',
                'count': adjustments,
            })
            self._save_perf_log()

        if self.verbose:
            print(f"  보정: {adjustments}개 district")

        return adjustments

    def tune_hourly_profiles(self, eval_result: Dict) -> int:
        """
        시간대 비율 보정

        실제 hour 분포 vs 예측 hour 분포 비교 → 비율 조정
        """
        if self.verbose:
            print(f"  🔧 [Level 1] 시간대 비율 보정")

        h_errors = eval_result.get('hour_errors', [])
        if not h_errors:
            if self.verbose:
                print("  데이터 없음, 스킵")
            return 0

        # 전체 레벨에서 시간대 비율 비교
        total_pred = sum(h.get('predicted_rides', 0) for h in h_errors)
        total_actual = sum(h.get('actual_rides', 0) for h in h_errors)

        if total_pred == 0 or total_actual == 0:
            return 0

        adjustments = 0
        target_date = eval_result.get('date', '')

        # 요일 타입 결정
        target = pd.Timestamp(target_date)
        dow = target.dayofweek
        try:
            from korean_holidays import ADDITIONAL_HOLIDAYS
            is_holiday = target.date() in ADDITIONAL_HOLIDAYS
        except ImportError:
            is_holiday = False

        if is_holiday or dow == 6:
            day_type = 'sunday_holiday'
        elif dow == 5:
            day_type = 'saturday'
        else:
            day_type = 'weekday'

        profile_key = f"global_{day_type}"

        if profile_key not in self.params['hourly_profiles']:
            self.params['hourly_profiles'][profile_key] = {}

        for h in h_errors:
            hour = str(int(h.get('hour', 0)))
            pred_ratio = h.get('predicted_rides', 0) / total_pred if total_pred > 0 else 0
            actual_ratio = h.get('actual_rides', 0) / total_actual if total_actual > 0 else 0

            ratio_diff = actual_ratio - pred_ratio

            if abs(ratio_diff) < 0.005:  # 0.5% 미만 차이 무시
                continue

            old = self.params['hourly_profiles'][profile_key].get(hour, pred_ratio)
            adjustment = ratio_diff * LEARNING_RATE
            new = old + adjustment
            new = max(0.001, new)

            if abs(new - old) > 0.001:
                self.params['hourly_profiles'][profile_key][hour] = round(new, 4)
                adjustments += 1

        if adjustments > 0:
            self._save_params()
            self.perf_log['corrections'].append({
                'date': target_date,
                'type': 'hourly_profile',
                'count': adjustments,
                'day_type': day_type,
            })
            self._save_perf_log()

        if self.verbose:
            print(f"  보정: {adjustments}개 시간대 ({day_type})")

        return adjustments

    # ================================================================
    # Level 2: 조건별 계수 학습 (주 1회)
    # ================================================================

    def tune_condition_coefficients(self, days: int = 28) -> int:
        """
        District별 날씨/요일 반응 계수 학습

        최근 28일 데이터를 분석하여 district별로
        region 평균 대비 추가 조정 계수를 학습.
        """
        if self.verbose:
            print(f"\n🔧 [Level 2] 조건별 계수 학습 (최근 {days}일)")

        # 성능 로그에서 최근 데이터 수집
        recent = [d for d in self.perf_log['daily']
                  if d.get('date', '') >= (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')]

        if len(recent) < 7:
            if self.verbose:
                print(f"  데이터 부족 ({len(recent)}일), 최소 7일 필요")
            return 0

        # 전체 bias 추세 분석
        biases = [d['overall_bias'] for d in recent if d.get('overall_bias') is not None]
        if biases:
            mean_bias = np.mean(biases)
            if self.verbose:
                print(f"  전체 평균 bias: {mean_bias:+.1f}%")

        # 요일별 bias 패턴
        adjustments = 0
        weekday_biases = []
        saturday_biases = []
        sunday_biases = []

        for d in recent:
            dt = pd.Timestamp(d['date'])
            dow = dt.dayofweek
            bias = d.get('overall_bias', 0)

            if dow < 5:
                weekday_biases.append(bias)
            elif dow == 5:
                saturday_biases.append(bias)
            else:
                sunday_biases.append(bias)

        # 요일별 보정 저장
        for day_type, biases_list in [
            ('weekday', weekday_biases),
            ('saturday', saturday_biases),
            ('sunday', sunday_biases),
        ]:
            if len(biases_list) >= MIN_SAMPLES:
                mean_b = np.mean(biases_list)
                if abs(mean_b) > 5:  # 5% 이상 편향
                    self.params['condition_coefficients'][f'global_{day_type}_bias'] = round(mean_b, 2)
                    adjustments += 1
                    if self.verbose:
                        print(f"  {day_type} bias: {mean_b:+.1f}%")

        if adjustments > 0:
            self._save_params()
            self.perf_log['corrections'].append({
                'date': datetime.now().strftime('%Y-%m-%d'),
                'type': 'condition_coefficients',
                'count': adjustments,
            })
            self._save_perf_log()

        if self.verbose:
            print(f"  보정: {adjustments}개 계수")

        return adjustments

    # ================================================================
    # Level 3: ML 모델 (주 1회, 향후 확장)
    # ================================================================

    def check_ml_readiness(self) -> bool:
        """ML 모델 학습에 충분한 데이터가 있는지 확인"""
        n_days = len(self.perf_log['daily'])
        return n_days >= 14

    def retrain_ml_model(self) -> Optional[Dict]:
        """
        LightGBM 직접 ML 예측 모델 재학습

        충분한 데이터 축적 후 district×hour 직접 예측.
        비율 배분 MAPE와 비교 → 더 나으면 전환, 아니면 롤백.

        (Phase 7 - 데이터 축적 후 활성화)
        """
        if not self.check_ml_readiness():
            if self.verbose:
                print(f"\n📊 [Level 3] ML 모델: 데이터 부족 "
                      f"({len(self.perf_log['daily'])}일 / 최소 14일)")
            return None

        if self.verbose:
            print(f"\n📊 [Level 3] ML 모델 재학습 준비됨 (Phase 7에서 구현)")
            print(f"  축적 데이터: {len(self.perf_log['daily'])}일")
            print(f"  → LightGBM features: lag1d, lag7d, app_open, supply, "
                  f"weather, dow, hour, accessibility/conversion")

        return {'status': 'ready', 'data_days': len(self.perf_log['daily'])}

    # ================================================================
    # 일일 파이프라인
    # ================================================================

    def run_daily_pipeline(self, target_date: str = None) -> Dict:
        """
        매일 자동 실행 파이프라인

        1. 전날 예측 vs 실제 비교
        2. Level 1: 비율 보정 (매일)
        3. Level 2: 조건별 계수 (월요일만)
        4. Level 3: ML 재학습 (월요일, 데이터 충분 시)
        5. 성능 로그 기록
        """
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        print(f"\n{'='*60}")
        print(f"🔄 District×Hour 자동 학습 파이프라인")
        print(f"   평가 대상: {target_date}")
        print(f"{'='*60}")

        result = {
            'date': target_date,
            'evaluation': None,
            'level1_adjustments': 0,
            'level2_adjustments': 0,
            'level3_status': None,
        }

        # 1. 전날 평가
        eval_result = self.evaluate_yesterday(target_date)
        result['evaluation'] = {
            'overall_bias': eval_result.get('overall_bias'),
            'district_mape': eval_result.get('district_mape'),
            'hour_mape': eval_result.get('hour_mape'),
        }

        if 'error' in eval_result:
            print(f"  ⚠️ 평가 실패: {eval_result['error']}")
            return result

        # 2. Level 1: 비율 보정 (매일)
        adj1 = self.tune_district_ratios(eval_result)
        adj2 = self.tune_hourly_profiles(eval_result)
        result['level1_adjustments'] = adj1 + adj2

        # 3. Level 2: 조건별 계수 (월요일만)
        today = datetime.now()
        if today.weekday() == 0:  # 월요일
            adj3 = self.tune_condition_coefficients()
            result['level2_adjustments'] = adj3

        # 4. Level 3: ML (월요일, 데이터 충분 시)
        if today.weekday() == 0:
            ml_result = self.retrain_ml_model()
            result['level3_status'] = ml_result

        # 5. 요약
        print(f"\n{'='*60}")
        print(f"📋 파이프라인 완료")
        print(f"  Level 1 보정: {result['level1_adjustments']}건")
        if result['level2_adjustments'] > 0:
            print(f"  Level 2 보정: {result['level2_adjustments']}건")
        if result['level3_status']:
            print(f"  Level 3: {result['level3_status'].get('status', 'N/A')}")
        print(f"{'='*60}")

        return result

    # ================================================================
    # 성능 추적 리포트
    # ================================================================

    def print_performance_report(self, days: int = 14):
        """최근 성능 추이 리포트"""
        recent = self.perf_log['daily'][-days:]

        if not recent:
            print("성능 데이터 없음")
            return

        print(f"\n{'='*60}")
        print(f"📊 성능 추이 리포트 (최근 {len(recent)}일)")
        print(f"{'='*60}")

        print(f"\n{'날짜':<12} {'예측':>8} {'실제':>8} {'Bias':>7} {'D-MAPE':>7} {'H-MAPE':>7}")
        print(f"{'-'*55}")

        for d in recent:
            date_str = d.get('date', '')
            pred = d.get('total_pred', 0)
            actual = d.get('total_actual', 0)
            bias = d.get('overall_bias', 0)
            d_mape = d.get('district_mape', '-')
            h_mape = d.get('hour_mape', '-')

            d_mape_str = f"{d_mape:.1f}%" if isinstance(d_mape, (int, float)) else d_mape
            h_mape_str = f"{h_mape:.1f}%" if isinstance(h_mape, (int, float)) else h_mape

            print(f"{date_str:<12} {pred:>8,} {actual:>8,} "
                  f"{bias:>+6.1f}% {d_mape_str:>7} {h_mape_str:>7}")

        # 평균
        biases = [d['overall_bias'] for d in recent if d.get('overall_bias') is not None]
        d_mapes = [d['district_mape'] for d in recent
                   if d.get('district_mape') is not None]
        h_mapes = [d['hour_mape'] for d in recent
                   if d.get('hour_mape') is not None]

        print(f"{'-'*55}")
        avg_bias = np.mean(biases) if biases else 0
        avg_d = np.mean(d_mapes) if d_mapes else 0
        avg_h = np.mean(h_mapes) if h_mapes else 0
        print(f"{'평균':<12} {'':<8} {'':<8} {avg_bias:>+6.1f}% "
              f"{avg_d:>6.1f}% {avg_h:>6.1f}%")

        # 보정 이력
        corrections = self.perf_log.get('corrections', [])[-10:]
        if corrections:
            print(f"\n📝 최근 보정 이력:")
            for c in corrections:
                print(f"  {c.get('date', '')} | {c.get('type', '')} | "
                      f"{c.get('count', 0)}건")


# ================================================================
# CLI
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description='District×Hour 예측 자동 학습',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('action', nargs='?', default='pipeline',
                       choices=['pipeline', 'evaluate', 'tune', 'report'],
                       help='실행 액션')
    parser.add_argument('--date', type=str, default=None,
                       help='평가 대상 날짜')
    parser.add_argument('--full', action='store_true',
                       help='Level 1-3 전체 실행')
    parser.add_argument('--days', type=int, default=14,
                       help='리포트 기간')

    args = parser.parse_args()

    tuner = DistrictHourTuner(verbose=True)

    if args.action == 'evaluate':
        tuner.evaluate_yesterday(args.date)

    elif args.action == 'tune':
        eval_result = tuner.evaluate_yesterday(args.date)
        if 'error' not in eval_result:
            tuner.tune_district_ratios(eval_result)
            tuner.tune_hourly_profiles(eval_result)

    elif args.action == 'report':
        tuner.print_performance_report(args.days)

    else:  # pipeline
        tuner.run_daily_pipeline(args.date)


if __name__ == '__main__':
    main()
