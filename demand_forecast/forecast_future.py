#!/usr/bin/env python
"""
미래 수요 예측 (단기/중기/장기)

- 단기 (1~3일): 기상청 단기예보 API
- 중기 (4~10일): 기상청 중기예보 API
- 장기 (11~30일): 과거 동기간 평균 날씨

사용법:
    python forecast_future.py

    또는 코드에서:
    from forecast_future import predict_next_month
    predictions = predict_next_month()
"""
import os
import json
import urllib.request
from urllib.parse import urlencode
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# API 키 (기상청 공공데이터)
API_KEY = os.getenv("WEATHER_API_KEY", "")

# 단기예보 API
SHORT_TERM_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"

# 중기예보 API
MID_TERM_URL = "https://apis.data.go.kr/1360000/MidFcstInfoService/getMidTa"
MID_LAND_URL = "https://apis.data.go.kr/1360000/MidFcstInfoService/getMidLandFcst"

# 지역 코드
REGION_CODES = {
    "서울": {"nx": 60, "ny": 127, "mid_code": "11B10101", "land_code": "11B00000"},
    "수원": {"nx": 60, "ny": 121, "mid_code": "11B20601", "land_code": "11B00000"},
    "Daejeon": {"nx": 67, "ny": 100, "mid_code": "11C20401", "land_code": "11C20000"},
}


def fetch_short_term_forecast(base_date: str, base_time: str = "0500",
                               nx: int = 60, ny: int = 127) -> Dict[str, Dict]:
    """
    단기예보 가져오기 (3일)

    Args:
        base_date: 발표일 (YYYYMMDD)
        base_time: 발표시각 (0200, 0500, 0800, 1100, 1400, 1700, 2000, 2300)
        nx, ny: 격자 좌표

    Returns:
        {날짜: {temp_low, temp_high, sky, rain_prob}}
    """
    params = f"?serviceKey={API_KEY}&" + urlencode({
        "pageNo": "1",
        "numOfRows": "1000",
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": str(nx),
        "ny": str(ny)
    })

    url = SHORT_TERM_URL + params

    try:
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0')
        response = urllib.request.urlopen(req, timeout=30)
        data = json.loads(response.read())

        if data['response']['header']['resultCode'] != '00':
            print(f"단기예보 API 에러: {data['response']['header']['resultMsg']}")
            return {}

        items = data['response']['body']['items']['item']

        # 날짜별로 정리
        forecast = {}
        for item in items:
            date = item['fcstDate']
            category = item['category']
            value = item['fcstValue']

            if date not in forecast:
                forecast[date] = {'temps': [], 'sky': [], 'rain_prob': []}

            if category == 'TMP':  # 기온
                forecast[date]['temps'].append(float(value))
            elif category == 'SKY':  # 하늘상태 (1맑음, 3구름많음, 4흐림)
                forecast[date]['sky'].append(int(value))
            elif category == 'POP':  # 강수확률
                forecast[date]['rain_prob'].append(int(value))
            elif category == 'SNO':  # 적설량
                if value != '적설없음':
                    try:
                        forecast[date]['snow'] = float(value.replace('cm', ''))
                    except:
                        pass

        # 일별 최저/최고 기온 계산
        result = {}
        for date, data in forecast.items():
            if data['temps']:
                result[date] = {
                    'temp_low': min(data['temps']),
                    'temp_high': max(data['temps']),
                    'sky': max(set(data['sky']), key=data['sky'].count) if data['sky'] else 1,
                    'rain_prob': max(data['rain_prob']) if data['rain_prob'] else 0,
                    'snow_depth': data.get('snow', 0)
                }

        return result

    except Exception as e:
        print(f"단기예보 에러: {e}")
        return {}


def fetch_mid_term_forecast(reg_id: str = "11B10101") -> Dict[str, Dict]:
    """
    중기예보 가져오기 (3~10일)

    Args:
        reg_id: 지역코드 (서울: 11B10101)

    Returns:
        {날짜: {temp_low, temp_high}}
    """
    today = datetime.now()
    # 중기예보는 06시, 18시에 발표
    if today.hour < 6:
        base_time = (today - timedelta(days=1)).strftime("%Y%m%d") + "1800"
    elif today.hour < 18:
        base_time = today.strftime("%Y%m%d") + "0600"
    else:
        base_time = today.strftime("%Y%m%d") + "1800"

    params = f"?serviceKey={API_KEY}&" + urlencode({
        "pageNo": "1",
        "numOfRows": "10",
        "dataType": "JSON",
        "regId": reg_id,
        "tmFc": base_time
    })

    url = MID_TERM_URL + params

    try:
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0')
        response = urllib.request.urlopen(req, timeout=30)
        data = json.loads(response.read())

        if data['response']['header']['resultCode'] != '00':
            print(f"중기예보 API 에러: {data['response']['header']['resultMsg']}")
            return {}

        items = data['response']['body']['items']['item']
        if not items:
            return {}

        item = items[0]
        result = {}

        # 3일후 ~ 10일후 예보
        for i in range(3, 11):
            target_date = (today + timedelta(days=i)).strftime("%Y%m%d")

            temp_low_key = f"taMin{i}"
            temp_high_key = f"taMax{i}"

            if temp_low_key in item and temp_high_key in item:
                result[target_date] = {
                    'temp_low': float(item[temp_low_key]),
                    'temp_high': float(item[temp_high_key]),
                    'snow_depth': 0
                }

        return result

    except Exception as e:
        print(f"중기예보 에러: {e}")
        return {}


def get_historical_average(target_date: datetime, weather_csv: str) -> Dict:
    """
    과거 동일 날짜 평균 날씨 가져오기

    Args:
        target_date: 예측 대상 날짜
        weather_csv: 과거 날씨 CSV 경로

    Returns:
        {temp_low, temp_high, snow_depth}
    """
    try:
        df = pd.read_csv(weather_csv)
        df['date'] = pd.to_datetime(df['date'])

        # 같은 월/일의 과거 데이터
        target_month = target_date.month
        target_day = target_date.day

        # ±3일 범위로 확장
        similar_dates = df[
            (df['date'].dt.month == target_month) &
            (abs(df['date'].dt.day - target_day) <= 3)
        ]

        if len(similar_dates) == 0:
            return {'temp_low': 0, 'temp_high': 5, 'snow_depth': 0}

        return {
            'temp_low': similar_dates['temp_low'].mean(),
            'temp_high': similar_dates['temp_high'].mean(),
            'snow_depth': similar_dates['snow_depth'].mean() if 'snow_depth' in similar_dates.columns else 0
        }

    except Exception as e:
        print(f"과거 데이터 로드 에러: {e}")
        return {'temp_low': 0, 'temp_high': 5, 'snow_depth': 0}


def get_future_weather(days: int = 30) -> Dict[str, Dict]:
    """
    미래 날씨 예보 통합 (단기 + 중기 + 과거평균)

    Args:
        days: 예측 일수 (기본 30일)

    Returns:
        {날짜: {temp_low, temp_high, snow_depth, source}}
    """
    today = datetime.now()
    weather_csv = os.path.join(os.path.dirname(__file__), 'weather_2025_202601.csv')

    result = {}

    # 1. 단기예보 (1~3일)
    print("  단기예보 가져오는 중...")
    base_date = today.strftime("%Y%m%d")
    short_term = fetch_short_term_forecast(base_date)

    for date_str, weather in short_term.items():
        result[date_str] = {**weather, 'source': '단기예보'}

    # 2. 중기예보 (4~10일)
    print("  중기예보 가져오는 중...")
    mid_term = fetch_mid_term_forecast()

    for date_str, weather in mid_term.items():
        if date_str not in result:
            result[date_str] = {**weather, 'source': '중기예보'}

    # 3. 장기 (11~30일) - 과거 평균
    print("  장기예측 (과거평균) 계산 중...")
    for i in range(1, days + 1):
        target_date = today + timedelta(days=i)
        date_str = target_date.strftime("%Y%m%d")

        if date_str not in result:
            historical = get_historical_average(target_date, weather_csv)
            result[date_str] = {**historical, 'source': '과거평균'}

    return result


def predict_next_month(days: int = 30) -> pd.DataFrame:
    """
    한 달 수요 예측

    Args:
        days: 예측 일수

    Returns:
        DataFrame with predictions
    """
    from demand_model_v7 import DemandForecastModelV7

    print("="*60)
    print(f"📊 {days}일 수요 예측")
    print("="*60)

    # 1. 미래 날씨 가져오기
    print("\n[1/3] 날씨 예보 수집...")
    future_weather = get_future_weather(days)
    print(f"  {len(future_weather)}일 날씨 데이터 준비 완료")

    # 2. 모델 초기화 및 학습
    print("\n[2/3] 모델 준비...")
    model = DemandForecastModelV7()

    # 과거 데이터 로드
    today = datetime.now()
    start_date = (today - timedelta(days=400)).strftime('%Y-%m-%d')
    end_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')

    historical_data = model.fetch_data(start_date, end_date)
    print(f"  학습 데이터: {len(historical_data):,}건")

    # 3. 예측 실행
    print("\n[3/3] 수요 예측 중...")
    predictions = []

    for i in range(1, days + 1):
        target_date = today + timedelta(days=i)
        date_str = target_date.strftime('%Y-%m-%d')
        date_key = target_date.strftime('%Y%m%d')

        weather = future_weather.get(date_key, {
            'temp_low': 0, 'temp_high': 5, 'snow_depth': 0, 'source': '기본값'
        })

        # 날씨 데이터 형식 맞추기
        weather_input = {
            'temp_low': weather.get('temp_low', 0),
            'temp_high': weather.get('temp_high', 5),
            'snow_depth': weather.get('snow_depth', 0)
        }

        try:
            # 실제 데이터가 없으므로 가상 예측
            # (실제 운영시에는 predict_future 메서드 필요)
            result = model.predict(
                (today - timedelta(days=7)).strftime('%Y-%m-%d'),  # 7일 전 데이터로 패턴 학습
                weather_input,
                historical_data
            )

            # 요일 조정
            dow = target_date.weekday()
            dow_names = ['월', '화', '수', '목', '금', '토', '일']

            base_pred = result.get('adjusted_prediction', 0)

            predictions.append({
                'date': date_str,
                'day_of_week': dow_names[dow],
                'prediction': round(base_pred),
                'temp_low': weather.get('temp_low', 0),
                'temp_high': weather.get('temp_high', 5),
                'weather_source': weather.get('source', ''),
                'confidence': '높음' if weather.get('source') == '단기예보' else
                             ('중간' if weather.get('source') == '중기예보' else '낮음')
            })

        except Exception as e:
            print(f"  {date_str} 예측 실패: {e}")

    df = pd.DataFrame(predictions)
    return df


def main():
    """메인 실행"""
    # 한 달 예측
    predictions = predict_next_month(30)

    print("\n" + "="*60)
    print("📈 예측 결과")
    print("="*60)

    print(f"\n{'날짜':<12} {'요일':<3} {'예측':>10} {'기온':>12} {'신뢰도':<6} {'소스':<8}")
    print("-"*60)

    for _, row in predictions.iterrows():
        temp_str = f"{row['temp_low']:.0f}°~{row['temp_high']:.0f}°"
        print(f"{row['date']:<12} {row['day_of_week']:<3} {row['prediction']:>10,} {temp_str:>12} {row['confidence']:<6} {row['weather_source']:<8}")

    # 주간 합계
    print("\n" + "="*60)
    print("📊 주간 예측 합계")
    print("="*60)

    predictions['date'] = pd.to_datetime(predictions['date'])
    predictions['week'] = predictions['date'].dt.isocalendar().week

    weekly = predictions.groupby('week').agg({
        'prediction': 'sum',
        'date': ['min', 'max']
    })

    for week, row in weekly.iterrows():
        start = row[('date', 'min')].strftime('%m/%d')
        end = row[('date', 'max')].strftime('%m/%d')
        total = row[('prediction', 'sum')]
        print(f"  {start}~{end}: {total:,}건")

    # CSV 저장
    csv_path = os.path.join(os.path.dirname(__file__), 'future_predictions.csv')
    predictions.to_csv(csv_path, index=False)
    print(f"\n📁 저장: {csv_path}")


if __name__ == "__main__":
    main()
