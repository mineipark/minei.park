#!/usr/bin/env python
"""
자동 모델 개선 시스템

매일 실행되어:
1. 실제 데이터와 예측 비교
2. 권역별/조건별 오차 분석
3. region_params.json 파라미터 자동 조정
4. 성능 로그 기록

사용법:
    python auto_improve.py          # 전체 프로세스
    python auto_improve.py analyze  # 분석만
    python auto_improve.py tune     # 튜닝만
"""
import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REGION_PARAMS_PATH = os.path.join(SCRIPT_DIR, 'region_params.json')
PERFORMANCE_LOG_PATH = os.path.join(SCRIPT_DIR, 'performance_log.json')
WEATHER_CSV_PATH = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')

# 튜닝 설정
LEARNING_RATE = 0.1  # 파라미터 조정 속도
MIN_SAMPLES = 2      # 최소 샘플 수 (14일 분석 → 토/일 각 2회)
MAX_ADJUSTMENT = 0.2 # 최대 조정폭


class ModelAnalyzer:
    """모델 성능 분석"""

    def __init__(self):
        self.load_region_params()
        self.load_performance_log()

    def load_region_params(self):
        """권역 파라미터 로드"""
        if os.path.exists(REGION_PARAMS_PATH):
            with open(REGION_PARAMS_PATH, 'r') as f:
                self.region_params = json.load(f)
        else:
            self.region_params = {}

    def load_performance_log(self):
        """성능 로그 로드"""
        if os.path.exists(PERFORMANCE_LOG_PATH):
            with open(PERFORMANCE_LOG_PATH, 'r') as f:
                self.performance_log = json.load(f)
        else:
            self.performance_log = {
                'daily': [],
                'weekly_mape': [],
                'region_errors': {},
                'condition_errors': {},
                'param_history': []
            }

    def save_performance_log(self):
        """성능 로그 저장"""
        with open(PERFORMANCE_LOG_PATH, 'w') as f:
            json.dump(self.performance_log, f, indent=2, ensure_ascii=False)

    def save_region_params(self):
        """권역 파라미터 저장"""
        with open(REGION_PARAMS_PATH, 'w') as f:
            json.dump(self.region_params, f, indent=2, ensure_ascii=False)

    def analyze_recent_performance(self, days: int = 7) -> Dict:
        """최근 N일 성능 분석"""
        from demand_model_v7 import DemandForecastModelV7, load_weather_data

        print(f"\n[분석] 최근 {days}일 성능 분석")

        model = DemandForecastModelV7()
        weather_data = load_weather_data(WEATHER_CSV_PATH)

        today = datetime.now()
        start_date = (today - timedelta(days=days+400)).strftime('%Y-%m-%d')
        end_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')

        historical_data = model.fetch_data(start_date, end_date)

        # 분석 기간
        analysis_start = today - timedelta(days=days)
        analysis_dates = pd.date_range(analysis_start, today - timedelta(days=1))

        results = {
            'period': f"{analysis_start.strftime('%Y-%m-%d')} ~ {(today - timedelta(days=1)).strftime('%Y-%m-%d')}",
            'daily_errors': [],
            'region_errors': {},
            'condition_errors': {
                'weekday': [], 'saturday': [], 'sunday': [],
                'cold': [], 'normal': [], 'warm': [],
                'snow': [], 'no_snow': []
            }
        }

        for date in analysis_dates:
            date_str = date.strftime('%Y-%m-%d')
            weather = weather_data.get(date_str, {})

            if not weather:
                continue

            try:
                result = model.predict(date_str, weather, historical_data)

                if 'error' in result:
                    continue

                actual = result['actual']
                pred = result['adjusted_prediction']
                error_pct = (pred - actual) / actual * 100 if actual > 0 else 0

                dow = date.weekday()
                temp_low = weather.get('temp_low', 0)
                snow = weather.get('snow_depth', 0)

                # 일별 오차
                results['daily_errors'].append({
                    'date': date_str,
                    'actual': actual,
                    'pred': pred,
                    'error_pct': error_pct,
                    'dow': dow,
                    'temp_low': temp_low,
                    'snow': snow
                })

                # 조건별 분류
                if dow < 5:
                    results['condition_errors']['weekday'].append(error_pct)
                elif dow == 5:
                    results['condition_errors']['saturday'].append(error_pct)
                else:
                    results['condition_errors']['sunday'].append(error_pct)

                if temp_low < -8:
                    results['condition_errors']['cold'].append(error_pct)
                elif temp_low > 5:
                    results['condition_errors']['warm'].append(error_pct)
                else:
                    results['condition_errors']['normal'].append(error_pct)

                if snow > 0:
                    results['condition_errors']['snow'].append(error_pct)
                else:
                    results['condition_errors']['no_snow'].append(error_pct)

                # 권역별 오차
                for detail in result.get('region_details', []):
                    region = detail['region']
                    r_actual = detail['actual']
                    r_pred = detail['adj_pred']

                    if r_actual > 50:  # 최소 이용량
                        r_error = (r_pred - r_actual) / r_actual * 100

                        if region not in results['region_errors']:
                            results['region_errors'][region] = []
                        results['region_errors'][region].append({
                            'date': date_str,
                            'error_pct': r_error,
                            'dow': dow,
                            'temp_low': temp_low
                        })

            except Exception as e:
                print(f"  {date_str} 분석 실패: {e}")

        return results

    def calculate_bias(self, errors: List[float]) -> Tuple[float, str]:
        """편향 계산 (과대/과소 예측)"""
        if len(errors) < MIN_SAMPLES:
            return 0, "샘플 부족"

        mean_error = np.mean(errors)
        std_error = np.std(errors)

        if mean_error > std_error:
            return mean_error, "과대예측"
        elif mean_error < -std_error:
            return mean_error, "과소예측"
        else:
            return mean_error, "적정"


class ModelTuner:
    """모델 파라미터 자동 튜닝"""

    def __init__(self, analyzer: ModelAnalyzer):
        self.analyzer = analyzer

    def tune_region_params(self, analysis: Dict) -> Dict:
        """권역별 파라미터 튜닝"""
        print("\n[튜닝] 권역별 파라미터 조정")

        adjustments = {}

        for region, errors in analysis['region_errors'].items():
            if len(errors) < MIN_SAMPLES:
                continue

            if region not in self.analyzer.region_params:
                continue

            params = self.analyzer.region_params[region]

            # 요일별 편향 분석
            weekday_errors = [e['error_pct'] for e in errors if e['dow'] < 5]
            saturday_errors = [e['error_pct'] for e in errors if e['dow'] == 5]
            sunday_errors = [e['error_pct'] for e in errors if e['dow'] == 6]

            # 날씨별 편향 분석
            cold_errors = [e['error_pct'] for e in errors if e['temp_low'] < -8]
            normal_errors = [e['error_pct'] for e in errors if -8 <= e['temp_low'] <= 5]

            region_adj = {}

            # 토요일 보정 조정
            if len(saturday_errors) >= 2:
                sat_bias, _ = self.analyzer.calculate_bias(saturday_errors)
                if abs(sat_bias) > 5:  # 5% 이상 편향
                    old_sat = params.get('sat', -0.20)
                    # 과대예측이면 sat를 더 낮게 (더 많이 감소)
                    adjustment = -sat_bias * LEARNING_RATE / 100
                    adjustment = max(-MAX_ADJUSTMENT, min(MAX_ADJUSTMENT, adjustment))
                    new_sat = old_sat + adjustment
                    new_sat = max(-1.0, min(0, new_sat))  # 범위 제한

                    if abs(new_sat - old_sat) > 0.01:
                        params['sat'] = round(new_sat, 4)
                        region_adj['sat'] = {'old': old_sat, 'new': new_sat, 'bias': sat_bias}

            # 일요일 보정 조정
            if len(sunday_errors) >= 2:
                sun_bias, _ = self.analyzer.calculate_bias(sunday_errors)
                if abs(sun_bias) > 5:
                    old_sun = params.get('sun', -0.28)
                    adjustment = -sun_bias * LEARNING_RATE / 100
                    adjustment = max(-MAX_ADJUSTMENT, min(MAX_ADJUSTMENT, adjustment))
                    new_sun = old_sun + adjustment
                    new_sun = max(-1.0, min(0, new_sun))

                    if abs(new_sun - old_sun) > 0.01:
                        params['sun'] = round(new_sun, 4)
                        region_adj['sun'] = {'old': old_sun, 'new': new_sun, 'bias': sun_bias}

            # 한파 보정 조정
            if len(cold_errors) >= 2:
                cold_bias, _ = self.analyzer.calculate_bias(cold_errors)
                if abs(cold_bias) > 5:
                    old_cold = params.get('cold', -0.32)
                    adjustment = -cold_bias * LEARNING_RATE / 100
                    adjustment = max(-MAX_ADJUSTMENT, min(MAX_ADJUSTMENT, adjustment))
                    new_cold = old_cold + adjustment
                    new_cold = max(-1.5, min(0, new_cold))

                    if abs(new_cold - old_cold) > 0.01:
                        params['cold'] = round(new_cold, 4)
                        region_adj['cold'] = {'old': old_cold, 'new': new_cold, 'bias': cold_bias}

            if region_adj:
                adjustments[region] = region_adj
                self.analyzer.region_params[region] = params

        return adjustments

    def tune_region_bias(self, analysis: Dict) -> Dict:
        """권역별 bias 보정값 튜닝 (과소/과대예측 편향 해소)"""
        print("\n[튜닝] 권역별 bias 조정")

        BIAS_LR = 0.15          # bias 학습률 (좀 더 빠르게 수렴)
        BIAS_MIN = -0.10        # 최대 -10% 하향
        BIAS_MAX = 0.20         # 최대 +20% 상향
        BIAS_THRESHOLD = 3.0    # 3% 이상 편향만 조정

        bias_adjustments = {}

        for region, errors in analysis['region_errors'].items():
            if len(errors) < MIN_SAMPLES:
                continue

            if region not in self.analyzer.region_params:
                continue

            # 전체 오차의 평균 (방향성 포함, MAPE 아님)
            all_errors = [e['error_pct'] for e in errors]
            mean_bias = np.mean(all_errors)  # + = 과대예측, - = 과소예측

            if abs(mean_bias) < BIAS_THRESHOLD:
                continue

            params = self.analyzer.region_params[region]
            old_bias = params.get('bias', 0.05)

            # 과소예측(-) → bias 올리기, 과대예측(+) → bias 내리기
            adjustment = -mean_bias * BIAS_LR / 100
            new_bias = old_bias + adjustment
            new_bias = max(BIAS_MIN, min(BIAS_MAX, new_bias))
            new_bias = round(new_bias, 4)

            if abs(new_bias - old_bias) > 0.005:
                params['bias'] = new_bias
                self.analyzer.region_params[region] = params
                bias_adjustments[region] = {
                    'old': old_bias, 'new': new_bias,
                    'mean_bias': round(mean_bias, 1),
                    'samples': len(errors)
                }

        if bias_adjustments:
            print(f"  {len(bias_adjustments)}개 권역 bias 조정:")
            for region, adj in list(bias_adjustments.items())[:10]:
                print(f"    {region}: {adj['old']:.3f}→{adj['new']:.3f} (편향 {adj['mean_bias']:+.1f}%, {adj['samples']}일)")
            if len(bias_adjustments) > 10:
                print(f"    ... 외 {len(bias_adjustments) - 10}개")
        else:
            print("  bias 조정 없음")

        return bias_adjustments

    def update_avg_rides(self) -> Dict:
        """avg_rides를 최근 30일 rolling average로 갱신

        기존: 전체 기간 평균 → 계절성 무시 (겨울에 과대예측)
        개선: 최근 30일 평균 → 현재 시즌 반영
        """
        print("\n[갱신] avg_rides → 최근 30일 rolling average")

        # BigQuery 인증
        cred_candidates = [
            os.path.join(SCRIPT_DIR, '..', 'credentials', 'service-account.json'),
            'os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'credentials/service-account.json')',
            os.path.expanduser('~/Downloads/service-account.json'),
        ]
        for cred in cred_candidates:
            if os.path.exists(cred):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cred
                break

        from google.cloud import bigquery
        client = bigquery.Client()

        # region_params.json에 등록된 권역만 대상
        target_regions = list(self.analyzer.region_params.keys())
        regions_str = ','.join([f"'{r}'" for r in target_regions])

        MIN_TOTAL_RIDES = 100  # 30일 합계 100건 미만이면 신뢰 부족 → 갱신 스킵

        query = f"""
        SELECT
            h3_start_area_name as region,
            COUNT(*) as total_rides,
            COUNT(DISTINCT DATE(start_time)) as active_days,
            COUNT(*) * 1.0 / COUNT(DISTINCT DATE(start_time)) as avg_daily_rides
        FROM `bikeshare.service.rides`
        WHERE DATE(start_time)
              BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND h3_start_area_name IN ({regions_str})
        GROUP BY 1
        """

        df = client.query(query).to_dataframe()

        updates = {}
        skipped = 0
        for _, row in df.iterrows():
            region = row['region']
            total_rides = int(row['total_rides'])
            new_avg = round(float(row['avg_daily_rides']), 2)

            # 30일간 100건 미만: 서비스 중단/축소된 권역 → 갱신 스킵
            if total_rides < MIN_TOTAL_RIDES:
                skipped += 1
                continue

            if region in self.analyzer.region_params:
                old_avg = self.analyzer.region_params[region].get('avg_rides', 0)
                pct_change = abs(new_avg - old_avg) / max(old_avg, 1) * 100

                # 5% 이상 변화가 있을 때만 갱신 (노이즈 방지)
                if pct_change > 5:
                    self.analyzer.region_params[region]['avg_rides'] = new_avg
                    updates[region] = {
                        'old': round(old_avg, 1),
                        'new': new_avg,
                        'change_pct': round(pct_change, 1)
                    }

        if skipped > 0:
            print(f"  ⏭️ {skipped}개 권역 스킵 (30일 {MIN_TOTAL_RIDES}건 미만 → 서비스 축소)")

        if updates:
            print(f"  {len(updates)}개 권역 avg_rides 갱신:")
            for region, u in sorted(updates.items(), key=lambda x: -abs(x[1]['change_pct']))[:10]:
                direction = '↓' if u['new'] < u['old'] else '↑'
                print(f"    {region}: {u['old']:.0f}→{u['new']:.0f} ({direction}{u['change_pct']:.0f}%)")
            if len(updates) > 10:
                print(f"    ... 외 {len(updates) - 10}개")
        else:
            print("  avg_rides 변화 없음 (5% 미만)")

        return updates

    def validate_tuning(self, adjustments: Dict) -> bool:
        """튜닝 결과 검증"""
        if not adjustments:
            print("  조정 없음")
            return False

        print(f"\n  {len(adjustments)}개 권역 파라미터 조정:")
        for region, adj in list(adjustments.items())[:10]:
            adj_str = ', '.join([f"{k}: {v['old']:.3f}→{v['new']:.3f}" for k, v in adj.items()])
            print(f"    {region}: {adj_str}")

        if len(adjustments) > 10:
            print(f"    ... 외 {len(adjustments) - 10}개")

        return True


def _refit_conversion_model():
    """전환율 모델 주간 리핏 (월요일 또는 강제 실행)"""
    try:
        from conversion_model import ConversionModel, PARAMS_PATH
        import os

        # 마지막 피팅 시점 확인
        should_refit = False
        if not os.path.exists(PARAMS_PATH):
            should_refit = True
        else:
            with open(PARAMS_PATH, 'r') as f:
                saved = json.load(f)
            fitted_at = saved.get('fitted_at', '')
            if fitted_at:
                try:
                    last_fit = datetime.strptime(fitted_at[:10], '%Y-%m-%d')
                    days_since = (datetime.now() - last_fit).days
                    if days_since >= 7:  # 7일 이상 경과
                        should_refit = True
                except ValueError:
                    should_refit = True
            else:
                should_refit = True

        if not should_refit:
            print(f"\n[전환율] 리핏 불필요 (마지막: {fitted_at})")
            return False

        print(f"\n[전환율] 주간 리핏 실행")
        model = ConversionModel(verbose=True)
        results = model.fit(lookback_days=90)

        if results:
            global_r = results.get('global', {})
            print(f"  ✅ 전환율 모델 리핏 완료: "
                  f"base={global_r.get('base_rate', '?')}, "
                  f"max={global_r.get('max_rate', '?')}, "
                  f"R²={global_r.get('r_squared', '?')}")
            return True
        return False

    except ImportError:
        print(f"\n[전환율] conversion_model.py 미설치 → 스킵")
        return False
    except Exception as e:
        print(f"\n[전환율] 리핏 실패: {e}")
        return False


def run_daily_improvement():
    """일일 자동 개선 실행"""
    print("="*60)
    print(f"🔧 자동 모델 개선 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)

    analyzer = ModelAnalyzer()
    tuner = ModelTuner(analyzer)

    # 0. avg_rides를 최근 30일 rolling average로 갱신
    try:
        avg_updates = tuner.update_avg_rides()
    except Exception as e:
        avg_updates = {}
        print(f"  ⚠️ avg_rides 갱신 실패: {e}")

    # 0-1. 전환율 모델 주간 리핏
    conversion_refitted = _refit_conversion_model()

    # 1. 최근 14일 분석 (토/일 각 2회 이상 확보)
    analysis = analyzer.analyze_recent_performance(14)

    # 2. 전체 성능 출력
    if analysis['daily_errors']:
        errors = [e['error_pct'] for e in analysis['daily_errors']]
        mape = np.mean(np.abs(errors))
        bias = np.mean(errors)

        print(f"\n[성능 요약]")
        print(f"  기간: {analysis['period']}")
        print(f"  MAPE: {mape:.1f}%")
        print(f"  평균 편향: {bias:+.1f}% ({'과대예측' if bias > 0 else '과소예측'})")

        # 조건별 성능
        print(f"\n[조건별 성능]")
        for condition, errs in analysis['condition_errors'].items():
            if errs:
                c_mape = np.mean(np.abs(errs))
                c_bias = np.mean(errs)
                print(f"  {condition}: MAPE {c_mape:.1f}%, 편향 {c_bias:+.1f}%")

    # 3. 파라미터 튜닝 (요일/날씨)
    adjustments = tuner.tune_region_params(analysis)

    # 4. 권역별 bias 튜닝
    bias_adjustments = tuner.tune_region_bias(analysis)

    # 5. 저장
    if adjustments or bias_adjustments or avg_updates:
        if adjustments:
            tuner.validate_tuning(adjustments)
        analyzer.save_region_params()
        print("\n✅ region_params.json 업데이트됨")

    # 6. 성능 로그 기록
    if analysis['daily_errors']:
        log_entry = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'mape': round(mape, 2),
            'bias': round(bias, 2),
            'adjustments': len(adjustments),
            'bias_adjustments': len(bias_adjustments),
            'avg_rides_updates': len(avg_updates),
            'conversion_refitted': conversion_refitted,
        }
        analyzer.performance_log['daily'].append(log_entry)

        # 최근 30일만 유지
        analyzer.performance_log['daily'] = analyzer.performance_log['daily'][-30:]
        analyzer.save_performance_log()

    print("\n" + "="*60)


def show_performance_history():
    """성능 이력 표시"""
    analyzer = ModelAnalyzer()

    if not analyzer.performance_log['daily']:
        print("성능 이력 없음")
        return

    print("="*60)
    print("📊 모델 성능 이력")
    print("="*60)

    print(f"\n{'날짜':<12} {'MAPE':>8} {'편향':>8} {'조정':>6}")
    print("-"*40)

    for entry in analyzer.performance_log['daily'][-14:]:
        print(f"{entry['date']:<12} {entry['mape']:>7.1f}% {entry['bias']:>+7.1f}% {entry['adjustments']:>6}")


def main():
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == 'analyze':
            analyzer = ModelAnalyzer()
            analysis = analyzer.analyze_recent_performance(7)
            # 결과 출력만
        elif cmd == 'history':
            show_performance_history()
        else:
            run_daily_improvement()
    else:
        run_daily_improvement()


if __name__ == "__main__":
    main()
