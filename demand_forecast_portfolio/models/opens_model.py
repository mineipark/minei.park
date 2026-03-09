"""
앱 오픈 모델 (LightGBM)

예측: "내일 이 구역에서 몇 명이 앱을 열 것인가?"

수요 측 모델 — 자전거 공급과 대체로 무관한 인구 수준의 행동 패턴을 포착한다
(앱을 여는 데 근처에 자전거가 있을 필요가 없음).

주요 피처 (중요도 순):
    1. opens_ma7: 7일 이동평균 (강한 주간 패턴)
    2. opens_same_dow_avg: 동일 요일 평균 (요일 효과)
    3. opens_lag1: 전일 오픈 수 (자기상관)
    4. app_opens_rolling: 14일 롤링 평균 (기본 수준)
    5. is_off: 주말/공휴일 바이너리 (수요 변동)
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple

try:
    import lightgbm as lgb
    HAS_LGB = True
except (ImportError, OSError):
    HAS_LGB = False
    from sklearn.ensemble import GradientBoostingRegressor

from config import OPENS_LGB_PARAMS, OPENS_FEATURES


class OpensModel:
    """앱 오픈 예측을 위한 LightGBM 모델."""

    def __init__(self, params: dict = None, features: list = None):
        self.params = params or OPENS_LGB_PARAMS
        self.features = features or OPENS_FEATURES
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
        조기 종료를 적용하여 Opens 모델을 학습한다.

        Args:
            train_df: 피처 컬럼과 'app_opens' 타겟이 포함된 학습 데이터
            test_df: 조기 종료를 위한 검증 데이터
            sample_weights: 선택적 시간 감쇠 가중치

        Returns:
            모델 지표 및 진단 정보를 담은 Dict
        """
        avail = [c for c in self.features if c in train_df.columns]
        missing = [c for c in self.features if c not in train_df.columns]
        if missing:
            print(f"  경고: 피처 {len(missing)}개 누락: {missing[:5]}...")

        X_train = train_df[avail].astype(float).fillna(0).values
        y_train = train_df['app_opens'].astype(float).values
        X_test = test_df[avail].astype(float).fillna(0).values
        y_test = test_df['app_opens'].astype(float).values

        if HAS_LGB:
            dtrain = lgb.Dataset(
                X_train, label=y_train,
                feature_name=avail, weight=sample_weights
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
                n_estimators=200, max_depth=5, learning_rate=0.05,
                subsample=0.7, random_state=42,
            )
            self.model.fit(X_train, y_train, sample_weight=sample_weights)
            self.best_iteration = self.model.n_estimators
            y_pred = np.maximum(self.model.predict(X_test), 0)
            self.importance = pd.DataFrame({
                'feature': avail,
                'importance': self.model.feature_importances_,
            }).sort_values('importance', ascending=False)

        self.features = avail  # 실제 사용된 피처로 업데이트
        self._use_lgb = HAS_LGB

        # 평가 지표
        mape = np.mean(np.abs(y_test - y_pred) / np.maximum(y_test, 0.01)) * 100
        mae = np.mean(np.abs(y_test - y_pred))

        print(f"\n  Opens 모델 결과:")
        print(f"    MAPE: {mape:.1f}%, MAE: {mae:.2f}")
        print(f"    피처 수: {len(avail)}, 최적 반복: {self.best_iteration}")
        print(f"    상위 5개: {', '.join(self.importance.head(5)['feature'].tolist())}")

        return {
            'predictions': y_pred,
            'mape': mape,
            'mae': mae,
        }

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """새로운 데이터에 대한 예측을 생성한다."""
        if self.model is None:
            raise ValueError("모델이 학습되지 않았습니다. 먼저 train()을 호출하세요.")

        avail = [c for c in self.features if c in df.columns]
        X = df[avail].astype(float).fillna(0).values
        return np.maximum(self.model.predict(X), 0)
