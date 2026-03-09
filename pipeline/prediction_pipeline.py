"""
예측 파이프라인

학습된 모델을 로드하고 새로운 데이터에 대한 예측을 생성합니다.
추론 시 피처 엔지니어링을 처리하고 모든 후처리 단계를 적용합니다.

사용법:
    pipeline = PredictionPipeline.load('models/')
    predictions = pipeline.predict(new_data, weather_df, holidays)
"""

import numpy as np
import pandas as pd
from typing import Dict, Optional, Set

from features import build_all_features
from models.opens_model import OpensModel
from models.rpo_model import RPOModel
from models.postprocessing import full_postprocessing


class PredictionPipeline:
    """
    엔드투엔드 예측 파이프라인.

    피처 엔지니어링 → 모델 추론 → 후처리를
    하나의 predict() 호출로 래핑합니다.
    """

    def __init__(
        self,
        opens_model: OpensModel,
        rpo_model: RPOModel,
        area_rpo_median: Dict[str, float],
        calibration_factors: Optional[Dict] = None,
    ):
        self.opens_model = opens_model
        self.rpo_model = rpo_model
        self.area_rpo_median = area_rpo_median
        self.calibration_factors = calibration_factors or {}

    def predict(
        self,
        df: pd.DataFrame,
        weather_df: pd.DataFrame,
        holidays: Set[str],
    ) -> pd.DataFrame:
        """
        입력 데이터에 대한 라이딩 예측을 생성합니다.

        Args:
            df: 일별 구역 수준 원시 데이터
            weather_df: 날씨 데이터 [date, temp_low, temp_high, ...]
            holidays: 공휴일 날짜 문자열 집합

        Returns:
            예측값이 추가된 DataFrame:
                pred_opens, pred_rpo, pred_rides
        """
        # 단계 1: 피처 엔지니어링
        df = build_all_features(df, weather_df, holidays)

        # 단계 2: 모델 추론
        pred_opens = self.opens_model.predict(df)
        pred_rpo = self.rpo_model.predict(df)

        # 단계 3: 후처리
        pred_rides = full_postprocessing(
            pred_opens, pred_rpo, df,
            area_rpo_median=self.area_rpo_median,
            calibration_factors=self.calibration_factors,
        )

        # 예측값 추가
        df = df.copy()
        df['pred_opens'] = pred_opens
        df['pred_rpo'] = pred_rpo
        df['pred_rides'] = pred_rides

        return df

    def predict_district(
        self,
        df: pd.DataFrame,
        weather_df: pd.DataFrame,
        holidays: Set[str],
        district: str,
    ) -> pd.DataFrame:
        """단일 구역에 대해 예측합니다."""
        district_df = df[df['h3_district_name'] == district].copy()
        return self.predict(district_df, weather_df, holidays)

    @classmethod
    def load(cls, model_dir: str) -> 'PredictionPipeline':
        """
        디스크에서 저장된 파이프라인을 로드합니다.

        model_dir에 필요한 파일:
            opens_model.txt — LightGBM 앱오픈 모델
            rpo_model.txt   — LightGBM RPO 모델
            metadata.json   — 피처 목록, 보정 계수, 권역 중앙값

        Args:
            model_dir: 저장된 모델이 있는 디렉토리 경로

        Returns:
            PredictionPipeline 인스턴스
        """
        import json
        import os
        import lightgbm as lgb

        # 모델 로드
        opens_model = OpensModel()
        opens_model.model = lgb.Booster(
            model_file=os.path.join(model_dir, 'opens_model.txt')
        )

        rpo_model = RPOModel()
        rpo_model.model = lgb.Booster(
            model_file=os.path.join(model_dir, 'rpo_model.txt')
        )

        # 메타데이터 로드
        meta_path = os.path.join(model_dir, 'metadata.json')
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            opens_model.features = meta.get('opens_features', [])
            rpo_model.features = meta.get('rpo_features', [])
            area_rpo_median = meta.get('area_rpo_median', {})
            calibration_factors = meta.get('calibration_factors', {})
        else:
            area_rpo_median = {}
            calibration_factors = {}

        return cls(
            opens_model=opens_model,
            rpo_model=rpo_model,
            area_rpo_median=area_rpo_median,
            calibration_factors=calibration_factors,
        )

    def save(self, model_dir: str):
        """
        파이프라인을 디스크에 저장합니다.

        Args:
            model_dir: 모델과 메타데이터를 저장할 디렉토리
        """
        import json
        import os

        os.makedirs(model_dir, exist_ok=True)

        # 모델 저장
        if self.opens_model.model:
            self.opens_model.model.save_model(
                os.path.join(model_dir, 'opens_model.txt')
            )
        if self.rpo_model.model:
            self.rpo_model.model.save_model(
                os.path.join(model_dir, 'rpo_model.txt')
            )

        # 메타데이터 저장
        meta = {
            'opens_features': self.opens_model.features,
            'rpo_features': self.rpo_model.features,
            'area_rpo_median': self.area_rpo_median,
            'calibration_factors': self.calibration_factors,
        }
        with open(os.path.join(model_dir, 'metadata.json'), 'w') as f:
            json.dump(meta, f, indent=2)
