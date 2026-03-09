"""
전환율 모델

포화 지수 함수를 사용하여 기기 공급량과 라이딩 전환율 간의 관계를 모델링합니다:

    전환율 = base_rate + max_gain x (1 - e^(-decay x bike_count))

이 함수는 기기 추가에 따른 수확 체감 효과를 포착합니다:
    - 기기 0대: base_rate (~36%) — 사용자가 근처 기기까지 도보 이동
    - 기기 1대: ~54% — 대부분의 사용자가 전환
    - 기기 3대 이상: ~72% — 포화 근접

활용 사례:
    - 순방향: 기기 수 → 예상 전환율
    - 역방향: 목표 전환율 → 필요 기기 수
    - 비제약 수요: 실현 라이딩 → 잠재 수요 추정
    - What-if 분석: "구역 X에 기기 N대를 추가하면?"

파라미터는 집계된 접근성 로그 데이터에서
scipy.optimize.curve_fit을 사용하여 지역별, 세그먼트별(통근 vs 여가 시간대)로 적합됩니다.
"""

import json
import numpy as np
from typing import Dict, Optional, Tuple
from scipy.optimize import curve_fit


# 기본 파라미터 (운영 데이터에서 추정)
DEFAULT_PARAMS = {
    'base_rate': 0.363,
    'max_gain': 0.444,
    'decay': 0.3,
}


class ConversionModel:
    """
    기기 공급량 → 전환율 모델.

    가용 기기 수(100m 이내)와 라이딩 전환 확률 간의 관계에
    포화 지수 함수를 적합합니다.

    계층 구조:
        지역별 파라미터 → 전역 파라미터 → 기본값

    세그먼트:
        global: 전체 시간대
        commute: 오전 7-9시, 오후 5-7시 (날씨 영향 적음)
        leisure: 기타 시간대 (날씨 영향 큼)
    """

    def __init__(self):
        self.params: Dict[str, Dict] = {'global': DEFAULT_PARAMS.copy()}
        self.region_params: Dict[str, Dict] = {}
        self.fitted = False

    @staticmethod
    def _conversion_func(bike_count, base_rate, max_gain, decay):
        """포화 지수 함수: f(n) = b + m x (1 - e^(-d x n))"""
        return base_rate + max_gain * (1 - np.exp(-decay * np.asarray(bike_count, dtype=float)))

    def _get_params(self, segment: str = 'global', region: str = None) -> Dict:
        """폴백 계층 구조를 통한 파라미터 조회."""
        if region and region in self.region_params:
            rp = self.region_params[region]
            if segment in rp:
                return rp[segment]
            if 'global' in rp:
                return rp['global']
        return self.params.get(segment, self.params.get('global', DEFAULT_PARAMS))

    # ── 순방향: 기기 수 → 전환율 ──

    def predict_conversion_rate(
        self, bike_count, segment: str = 'global', region: str = None
    ) -> np.ndarray:
        """
        가용 기기 수에 따른 전환율을 예측합니다.

        Args:
            bike_count: 100m 이내 기기 수 (스칼라 또는 배열)
            segment: 'global', 'commute', 또는 'leisure'
            region: 지역별 파라미터를 위한 지역명

        Returns:
            전환율 (0 ~ base_rate + max_gain)
        """
        p = self._get_params(segment, region)
        return self._conversion_func(bike_count, p['base_rate'], p['max_gain'], p['decay'])

    def get_max_conversion_rate(self, segment: str = 'global', region: str = None) -> float:
        """이론적 최대 전환율 (기기 수 무한대)."""
        p = self._get_params(segment, region)
        return p['base_rate'] + p['max_gain']

    # ── 역방향: 목표 전환율 → 필요 기기 수 ──

    def inverse_conversion_rate(
        self, target_rate: float,
        segment: str = 'global', region: str = None,
        max_return: float = 15.0,
    ) -> float:
        """
        목표 전환율을 달성하기 위한 필요 기기 수를 계산합니다.

        역함수:
            n = -ln(1 - (rate - base) / max_gain) / decay

        Args:
            target_rate: 원하는 전환율 (0 ~ max_rate)
            segment: 시간대 세그먼트
            region: 지역명
            max_return: 목표가 최대값을 초과할 때의 반환값

        Returns:
            필요한 평균 bike_count_100m
        """
        p = self._get_params(segment, region)
        max_rate = p['base_rate'] + p['max_gain']

        if target_rate <= p['base_rate']:
            return 0.0
        if target_rate >= max_rate * 0.99:
            return max_return

        ratio = (target_rate - p['base_rate']) / p['max_gain']
        ratio = min(ratio, 0.99)  # 수치 안정성
        return -np.log(1 - ratio) / p['decay']

    # ── 비제약 수요 추정 ──

    def estimate_unconstrained(
        self, realized_rides: float,
        avg_bike_count: float,
        segment: str = 'global',
        region: str = None,
    ) -> Dict:
        """
        실현 라이딩으로부터 잠재(비제약) 수요를 추정합니다.

        모든 곳에 최대 기기 공급이 있다면, 얼마나 많은 라이딩이
        발생할 것인가? 이를 통해 억제된 수요를 파악합니다.

        Args:
            realized_rides: 실제 관측 라이딩 수
            avg_bike_count: 100m 이내 평균 기기 수
            segment: 시간대 세그먼트
            region: 지역명

        Returns:
            current_cvr, max_cvr, unconstrained_rides, gap을 포함하는 Dict
        """
        current_cvr = self.predict_conversion_rate(avg_bike_count, segment, region)
        max_cvr = self.get_max_conversion_rate(segment, region)

        if current_cvr > 0:
            unconstrained = realized_rides * (max_cvr / current_cvr)
        else:
            unconstrained = realized_rides

        return {
            'current_cvr': float(current_cvr),
            'max_cvr': float(max_cvr),
            'realized_rides': realized_rides,
            'unconstrained_rides': float(unconstrained),
            'suppressed_rides': float(unconstrained - realized_rides),
            'gap_pct': float((unconstrained / max(realized_rides, 1) - 1) * 100),
        }

    # ── 적합 ──

    def fit(self, bike_counts: np.ndarray, conversion_rates: np.ndarray,
            segment: str = 'global') -> Dict:
        """
        관측 데이터로부터 모델 파라미터를 적합합니다.

        Args:
            bike_counts: 기기 수 배열 (예: bike_count_100)
            conversion_rates: 관측 전환율 배열
            segment: 적합할 세그먼트

        Returns:
            적합된 파라미터 Dict
        """
        try:
            popt, _ = curve_fit(
                self._conversion_func, bike_counts, conversion_rates,
                p0=[0.3, 0.5, 0.3],
                bounds=([0, 0, 0.01], [1, 1, 5]),
                maxfev=5000,
            )
            params = {
                'base_rate': float(popt[0]),
                'max_gain': float(popt[1]),
                'decay': float(popt[2]),
            }
            self.params[segment] = params
            self.fitted = True
            print(f"  적합 완료 ({segment}): base={params['base_rate']:.3f}, "
                  f"max_gain={params['max_gain']:.3f}, decay={params['decay']:.3f}")
            return params
        except Exception as e:
            print(f"  적합 실패 ({segment}): {e}")
            return DEFAULT_PARAMS.copy()

    def fit_region(self, bike_counts: np.ndarray, conversion_rates: np.ndarray,
                   region: str, segment: str = 'global') -> Dict:
        """지역별 파라미터를 적합합니다."""
        if region not in self.region_params:
            self.region_params[region] = {}

        try:
            popt, _ = curve_fit(
                self._conversion_func, bike_counts, conversion_rates,
                p0=[0.3, 0.5, 0.3],
                bounds=([0, 0, 0.01], [1, 1, 5]),
                maxfev=5000,
            )
            params = {
                'base_rate': float(popt[0]),
                'max_gain': float(popt[1]),
                'decay': float(popt[2]),
            }
            self.region_params[region][segment] = params
            return params
        except Exception:
            return self._get_params(segment)

    # ── 저장/로드 ──

    def save_params(self, path: str):
        """파라미터를 JSON으로 저장합니다."""
        data = {
            **{k: v for k, v in self.params.items()},
            'regions': self.region_params,
        }
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)

    def load_params(self, path: str):
        """JSON에서 파라미터를 로드합니다."""
        with open(path, 'r') as f:
            data = json.load(f)
        for seg in ('global', 'commute', 'leisure'):
            if seg in data and isinstance(data[seg], dict) and 'base_rate' in data[seg]:
                self.params[seg] = data[seg]
        if 'regions' in data:
            self.region_params = data['regions']
        self.fitted = True


# ── 데모 ──

def demo():
    """전환율 모델의 간단한 데모."""
    model = ConversionModel()

    print("전환율 모델 데모")
    print("=" * 50)
    print(f"\n파라미터: {model.params['global']}")
    print(f"\n기기 수 → 전환율:")
    for n in [0, 1, 2, 3, 5, 10]:
        cvr = model.predict_conversion_rate(n)
        print(f"  기기 {n}대 → {cvr:.1%}")

    print(f"\n목표 전환율 → 필요 기기 수:")
    for target in [0.4, 0.5, 0.6, 0.7]:
        bikes = model.inverse_conversion_rate(target)
        print(f"  전환율 {target:.0%} → 기기 {bikes:.1f}대 필요")

    print(f"\n비제약 수요 추정:")
    result = model.estimate_unconstrained(
        realized_rides=100, avg_bike_count=2.0
    )
    print(f"  실현 라이딩: {result['realized_rides']}건")
    print(f"  비제약 라이딩: {result['unconstrained_rides']:.0f}건")
    print(f"  억제된 라이딩: {result['suppressed_rides']:.0f}건 ({result['gap_pct']:.1f}%)")


if __name__ == '__main__':
    demo()
