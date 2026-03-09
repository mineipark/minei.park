"""
오픈당 라이딩 (RPO) 모델 (LightGBM + Huber Loss)

예측: "앱을 연 사용자 중 실제로 라이딩하는 비율은?"

전환 측 모델 — 자전거 공급, 공간적 접근성, 기상 조건이 앱 오픈을
실제 라이딩으로 전환하는 확률에 미치는 영향을 포착한다.

설계 선택:
    - Huber 손실: 이상치 RPO 값에 강건함 (이벤트, 데이터 오류 등)
    - 보수적 리프 수 (15): 소규모 구역에서 과적합 방지
    - RPO 클리핑 [0, 3.5]: 비현실적 예측값 제한
    - 롤링 평균 방향 수축: 소표본 예측 안정화

주요 피처 (중요도 순):
    1. rides_per_open_rolling: 14일 롤링 RPO (기본 수준)
    2. area_avg_rpo_prev: 권역 수준 전환율 (거시 환경)
    3. neighbor_weighted_rpo: 공간 파급 신호
    4. rpo_x_opens: 물량-전환율 상호작용
    5. avg_bikes_400m_rolling: 지역 자전거 공급량
"""

import numpy as np
import pandas as pd
from typing import Dict

try:
    import lightgbm as lgb
    HAS_LGB = True
except (ImportError, OSError):
    HAS_LGB = False
    from sklearn.ensemble import GradientBoostingRegressor

from config import RPO_LGB_PARAMS, RPO_FEATURES, RPO_CLIP_MAX


class RPOModel:
    """오픈당 라이딩(RPO) 예측을 위한 LightGBM 모델."""

    def __init__(self, params: dict = None, features: list = None):
        self.params = params or RPO_LGB_PARAMS
        self.features = features or RPO_FEATURES
        self.model = None
        self.importance = None
        self.best_iteration = None

    def train(
        self,
        train_df: pd.DataFrame,
        test_df: pd.DataFrame,
        sample_weights: np.ndarray = None,
    ) -> Dict:
        """
        RPO 모델을 학습한다.

        전처리:
        - RPO = 0인 행 제외 (라이딩 없음 = 전환 학습에 비정보적)
        - RPO > 3.5 클리핑 (극단적 이상치가 Huber 손실을 왜곡)

        Args:
            train_df: 'rides_per_open' 타겟이 포함된 학습 데이터
            test_df: 검증 데이터
            sample_weights: 선택적 시간 감쇠 가중치

        Returns:
            예측값과 지표를 담은 Dict
        """
        # 학습 타겟 필터링 및 클리핑
        train_clean = train_df[train_df['rides_per_open'] > 0].copy()
        train_clean['rides_per_open'] = train_clean['rides_per_open'].clip(
            upper=RPO_CLIP_MAX
        )

        avail = [c for c in self.features if c in train_clean.columns]
        missing = [c for c in self.features if c not in train_clean.columns]
        if missing:
            print(f"  경고: 피처 {len(missing)}개 누락: {missing[:5]}...")

        X_train = train_clean[avail].astype(float).fillna(0).values
        y_train = train_clean['rides_per_open'].astype(float).values
        X_test = test_df[avail].astype(float).fillna(0).values
        y_test = test_df['rides_per_open'].astype(float).values

        # 필터링된 행에 대한 가중치 조정
        weights = None
        if sample_weights is not None:
            mask = train_df['rides_per_open'] > 0
            weights = sample_weights[mask.values]

        if HAS_LGB:
            dtrain = lgb.Dataset(
                X_train, label=y_train,
                feature_name=avail, weight=weights
            )
            dvalid = lgb.Dataset(
                X_test, label=y_test,
                feature_name=avail, reference=dtrain
            )
            self.model = lgb.train(
                self.params, dtrain,
                num_boost_round=1000,
                valid_sets=[dtrain, dvalid],
                valid_names=['train', 'valid'],
                callbacks=[lgb.log_evaluation(200), lgb.early_stopping(50)],
            )
            self.best_iteration = self.model.best_iteration
            y_pred = np.maximum(self.model.predict(X_test), 0)
            self.importance = pd.DataFrame({
                'feature': avail,
                'importance': self.model.feature_importance(importance_type='gain'),
            }).sort_values('importance', ascending=False)
        else:
            print("    (sklearn GradientBoosting 사용 중 — 운영 환경에서는 LightGBM 설치 필요)")
            self.model = GradientBoostingRegressor(
                n_estimators=200, max_depth=4, learning_rate=0.05,
                subsample=0.7, random_state=42,
            )
            self.model.fit(X_train, y_train, sample_weight=weights)
            self.best_iteration = self.model.n_estimators
            y_pred = np.maximum(self.model.predict(X_test), 0)
            self.importance = pd.DataFrame({
                'feature': avail,
                'importance': self.model.feature_importances_,
            }).sort_values('importance', ascending=False)

        self.features = avail
        self._use_lgb = HAS_LGB

        mape = np.mean(np.abs(y_test - y_pred) / np.maximum(y_test, 0.01)) * 100
        mae = np.mean(np.abs(y_test - y_pred))

        print(f"\n  RPO 모델 결과:")
        print(f"    MAPE: {mape:.1f}%, MAE: {mae:.2f}")
        print(f"    피처 수: {len(avail)}, 최적 반복: {self.best_iteration}")
        print(f"    상위 5개: {', '.join(self.importance.head(5)['feature'].tolist())}")

        return {
            'predictions': y_pred,
            'mape': mape,
            'mae': mae,
        }

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """새로운 데이터에 대한 RPO 예측을 생성한다."""
        if self.model is None:
            raise ValueError("모델이 학습되지 않았습니다. 먼저 train()을 호출하세요.")

        avail = [c for c in self.features if c in df.columns]
        X = df[avail].astype(float).fillna(0).values
        return np.clip(self.model.predict(X), 0, RPO_CLIP_MAX)
