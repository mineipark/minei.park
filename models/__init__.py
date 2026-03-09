"""
2단계 수요 예측 앙상블의 모델 정의.

    예측_라이딩 = 예측_앱오픈 x 예측_RPO

모델:
    - OpensModel: 구역별 앱 오픈 수 예측 (LightGBM, MSE)
    - RPOModel: 오픈당 라이딩 비율 예측 (LightGBM, Huber)
    - PostProcessor: RPO 수축, 보정, 공휴일 감쇠
"""
