#!/usr/bin/env python
"""
날씨 데이터 자동 업데이트 스크립트
Cron에서 비대화형으로 실행

사용법:
    python auto_update_weather.py

Cron 설정 (매일 오전 9시):
    0 9 * * * cd /path/to/project && .venv/bin/python demand_forecast/auto_update_weather.py >> /var/log/weather_update.log 2>&1
"""
import urllib.request
from urllib.parse import urlencode
import json
import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# API 설정
API_KEY = os.getenv("WEATHER_API_KEY", "")
BASE_URL = "https://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList"

# 파일 경로
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WEATHER_CSV = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')


def fetch_weather_data(start_date: str, end_date: str, stn_id: str = "108") -> pd.DataFrame:
    """기상청 API에서 날씨 데이터 가져오기"""
    params = f"?serviceKey={API_KEY}&" + urlencode({
        "pageNo": "1",
        "numOfRows": "100",
        "dataType": "JSON",
        "dataCd": "ASOS",
        "dateCd": "DAY",
        "startDt": start_date,
        "endDt": end_date,
        "stnIds": stn_id
    })

    url = BASE_URL + params

    try:
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0')
        response = urllib.request.urlopen(req, timeout=30)
        data = json.loads(response.read())

        if data['response']['header']['resultCode'] == '00':
            items = data['response']['body']['items']['item']
            df = pd.DataFrame(items)

            col_map = {
                'tm': 'date',
                'minTa': 'temp_low',
                'maxTa': 'temp_high',
                'avgTa': 'temp_avg',
                'ddMes': 'snow_depth',
                'sumRn': 'rain_sum',
                'avgRhm': 'humidity_avg',
                'avgWs': 'windspeed_avg'
            }

            rename_cols = {k: v for k, v in col_map.items() if k in df.columns}
            df = df.rename(columns=rename_cols)

            numeric_cols = ['temp_low', 'temp_high', 'temp_avg', 'snow_depth',
                           'rain_sum', 'humidity_avg', 'windspeed_avg']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            df['date'] = pd.to_datetime(df['date'])
            return df

        else:
            print(f"[ERROR] API 에러: {data['response']['header']['resultMsg']}")
            return pd.DataFrame()

    except Exception as e:
        print(f"[ERROR] {e}")
        return pd.DataFrame()


def update_weather_csv():
    """날씨 CSV 파일 업데이트"""
    now = datetime.now()
    print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 날씨 데이터 업데이트 시작")

    # 기존 CSV 로드
    if os.path.exists(WEATHER_CSV):
        existing_df = pd.read_csv(WEATHER_CSV)
        existing_df['date'] = pd.to_datetime(existing_df['date'])
        last_date = existing_df['date'].max()
        print(f"  기존 데이터: {len(existing_df)}일 (마지막: {last_date.strftime('%Y-%m-%d')})")
    else:
        existing_df = pd.DataFrame()
        last_date = datetime(2025, 1, 1)
        print("  기존 데이터 없음, 새로 생성")

    # 업데이트 필요한 기간 계산
    # 기상청 API는 보통 2일 전까지의 데이터만 제공
    end_date = now - timedelta(days=2)
    start_date = last_date + timedelta(days=1)

    if start_date > end_date:
        print("  업데이트할 데이터 없음 (이미 최신)")
        return False

    print(f"  가져올 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

    # 새 데이터 가져오기
    new_df = fetch_weather_data(
        start_date.strftime('%Y%m%d'),
        end_date.strftime('%Y%m%d'),
        "108"  # 서울
    )

    if len(new_df) == 0:
        print("  새 데이터 없음")
        return False

    print(f"  새 데이터: {len(new_df)}일")

    # 필요한 컬럼만 선택 (모델 피처에 필요한 날씨 컬럼 전부 포함)
    required_cols = ['date', 'temp_low', 'temp_high', 'snow_depth',
                     'rain_sum', 'windspeed_avg', 'humidity_avg']
    new_df = new_df[[c for c in required_cols if c in new_df.columns]]

    # 기존 데이터와 병합
    if len(existing_df) > 0:
        # 중복 제거를 위해 date 기준 병합
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
        combined_df = combined_df.sort_values('date')
    else:
        combined_df = new_df

    # CSV 저장
    combined_df.to_csv(WEATHER_CSV, index=False)
    print(f"  저장 완료: {WEATHER_CSV} ({len(combined_df)}일)")

    # 최근 데이터 출력
    print("\n  [최근 5일 날씨]")
    recent = combined_df.tail(5)
    for _, row in recent.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        temp_low = f"{row['temp_low']:.1f}" if pd.notna(row.get('temp_low')) else "-"
        temp_high = f"{row['temp_high']:.1f}" if pd.notna(row.get('temp_high')) else "-"
        snow = f"{row['snow_depth']:.1f}" if pd.notna(row.get('snow_depth')) else "0"
        rain = f"{row['rain_sum']:.1f}" if pd.notna(row.get('rain_sum')) else "0"
        wind = f"{row['windspeed_avg']:.1f}" if pd.notna(row.get('windspeed_avg')) else "-"
        print(f"    {date_str}: {temp_low}°~{temp_high}°, 비:{rain}mm, 바람:{wind}m/s, 적설:{snow}cm")

    return True


def main():
    """메인 실행"""
    try:
        updated = update_weather_csv()
        if updated:
            print("\n[SUCCESS] 날씨 데이터 업데이트 완료")
        else:
            print("\n[INFO] 업데이트 불필요")
        sys.exit(0)
    except Exception as e:
        print(f"\n[FAILED] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
