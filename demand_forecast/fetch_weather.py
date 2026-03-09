"""
기상청 ASOS 일자료 API로 날씨 데이터 가져오기
실행: python fetch_weather.py

데이터를 BigQuery에 업로드하거나 CSV로 저장합니다.
"""
import urllib.request
from urllib.parse import urlencode
import json
import pandas as pd
from datetime import datetime, timedelta
import os

# API 설정
API_KEY = os.getenv("WEATHER_API_KEY", "")
BASE_URL = "https://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList"

# 관측소 ID (주요 도시)
STATIONS = {
    "108": "서울",
    "119": "수원",
    "133": "Daejeon",
    "143": "대구",
    "156": "광주",
    "159": "부산",
    "184": "제주"
}

def fetch_weather_data(start_date: str, end_date: str, stn_id: str = "108") -> pd.DataFrame:
    """
    기상청 API에서 날씨 데이터 가져오기

    Args:
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)
        stn_id: 관측소 ID (기본: 108 서울)

    Returns:
        DataFrame with weather data
    """
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

            # 컬럼명 정리
            col_map = {
                'tm': 'date',
                'stnId': 'station_id',
                'stnNm': 'station_name',
                'avgTa': 'temp_avg',
                'minTa': 'temp_low',
                'maxTa': 'temp_high',
                'avgRhm': 'humidity_avg',
                'avgWs': 'windspeed_avg',
                'sumRn': 'rain_sum',
                'ddMes': 'snow_depth',
                'sumSsHr': 'sunshine_hours',
                'avgTca': 'cloud_avg'
            }

            # 존재하는 컬럼만 rename
            rename_cols = {k: v for k, v in col_map.items() if k in df.columns}
            df = df.rename(columns=rename_cols)

            # 숫자 컬럼 변환
            numeric_cols = ['temp_avg', 'temp_low', 'temp_high', 'humidity_avg',
                          'windspeed_avg', 'rain_sum', 'snow_depth', 'sunshine_hours', 'cloud_avg']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # 날짜 변환
            df['date'] = pd.to_datetime(df['date'])

            return df
        else:
            print(f"API 에러: {data['response']['header']['resultMsg']}")
            return pd.DataFrame()

    except Exception as e:
        print(f"에러 발생: {e}")
        return pd.DataFrame()


def fetch_all_stations(start_date: str, end_date: str) -> pd.DataFrame:
    """모든 주요 관측소의 날씨 데이터 가져오기"""
    all_data = []

    for stn_id, stn_name in STATIONS.items():
        print(f"  {stn_name} 데이터 가져오는 중...")
        df = fetch_weather_data(start_date, end_date, stn_id)
        if len(df) > 0:
            df['area_name'] = stn_name
            all_data.append(df)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def save_to_bigquery(df: pd.DataFrame, table_id: str = "bikeshare.service.weather"):
    """BigQuery에 데이터 저장"""
    try:
        from google.cloud import bigquery

        # 스크립트 기준 상대 경로로 credentials 찾기
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cred_path = os.path.join(script_dir, '..', 'credentials', 'service-account.json')
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cred_path
        client = bigquery.Client()

        # 스키마에 맞게 컬럼 조정
        bq_cols = ['area_name', 'date', 'temp_avg', 'temp_low', 'temp_high',
                   'humidity_avg', 'windspeed_avg', 'rain_sum', 'snow_depth', 'cloud_avg']

        df_bq = df[[c for c in bq_cols if c in df.columns]].copy()
        df_bq = df_bq.rename(columns={
            'date': 'datetime',
            'rain_sum': 'rain_avg',
            'snow_depth': 'snow_avg',
            'humidity_avg': 'hum_avg'
        })

        # BigQuery에 업로드
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )

        job = client.load_table_from_dataframe(df_bq, table_id, job_config=job_config)
        job.result()

        print(f"✅ BigQuery에 {len(df_bq)}건 업로드 완료")

    except Exception as e:
        print(f"BigQuery 업로드 에러: {e}")


def fetch_year_data(year: int, stn_id: str = "108") -> pd.DataFrame:
    """1년치 데이터를 분기별로 나눠서 가져오기"""
    all_data = []

    quarters = [
        (f"{year}0101", f"{year}0331"),
        (f"{year}0401", f"{year}0630"),
        (f"{year}0701", f"{year}0930"),
        (f"{year}1001", f"{year}1231"),
    ]

    for start, end in quarters:
        print(f"  {start[:4]}/{start[4:6]}~{end[4:6]} 가져오는 중...")
        df = fetch_weather_data(start, end, stn_id)
        if len(df) > 0:
            all_data.append(df)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def main():
    print("="*60)
    print("🌡️ 기상청 날씨 데이터 수집")
    print("="*60)

    # 연도 선택
    print("\n수집할 데이터를 선택하세요:")
    print("  1. 2026년 1월만")
    print("  2. 2025년 전체")
    print("  3. 2025년 + 2026년 1월")

    choice = input("\n선택 (1/2/3): ").strip()

    if choice == "1":
        print("\n2026년 1월 서울 날씨 데이터 가져오는 중...")
        df = fetch_weather_data("20260101", "20260125", "108")
        csv_name = "weather_202601.csv"
    elif choice == "2":
        print("\n2025년 서울 날씨 데이터 가져오는 중...")
        df = fetch_year_data(2025, "108")
        csv_name = "weather_2025.csv"
    elif choice == "3":
        print("\n2025년 + 2026년 1월 서울 날씨 데이터 가져오는 중...")
        df_2025 = fetch_year_data(2025, "108")
        df_2026 = fetch_weather_data("20260101", "20260125", "108")
        df = pd.concat([df_2025, df_2026], ignore_index=True)
        csv_name = "weather_2025_202601.csv"
    else:
        print("잘못된 선택입니다.")
        return

    # 기존: df = fetch_weather_data("20260101", "20260125", "108")

    if len(df) > 0:
        print(f"\n총 {len(df)}일 데이터 수신\n")

        # 결과 출력
        display_cols = ['date', 'temp_avg', 'temp_low', 'temp_high', 'rain_sum', 'snow_depth']
        display_cols = [c for c in display_cols if c in df.columns]

        print(f"{'날짜':<12} {'평균':>8} {'최저':>8} {'최고':>8} {'강수':>8} {'적설':>8}")
        print("-"*54)

        for _, row in df.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            temp_avg = f"{row['temp_avg']:.1f}°" if pd.notna(row.get('temp_avg')) else "-"
            temp_low = f"{row['temp_low']:.1f}°" if pd.notna(row.get('temp_low')) else "-"
            temp_high = f"{row['temp_high']:.1f}°" if pd.notna(row.get('temp_high')) else "-"
            rain = f"{row['rain_sum']:.1f}" if pd.notna(row.get('rain_sum')) else "0.0"
            snow = f"{row['snow_depth']:.1f}" if pd.notna(row.get('snow_depth')) else "0.0"
            print(f"{date_str:<12} {temp_avg:>8} {temp_low:>8} {temp_high:>8} {rain:>8} {snow:>8}")

        # 월별 통계
        print("\n" + "="*60)
        print("📊 월별 평균 기온")
        print("="*60)

        df['month'] = df['date'].dt.month
        monthly = df.groupby('month').agg({
            'temp_avg': 'mean',
            'temp_low': 'min',
            'temp_high': 'max'
        }).round(1)

        print(f"\n{'월':<6} {'평균기온':>10} {'최저':>10} {'최고':>10}")
        print("-"*38)
        for month, row in monthly.iterrows():
            print(f"{month:>2}월    {row['temp_avg']:>9.1f}° {row['temp_low']:>9.1f}° {row['temp_high']:>9.1f}°")

        # CSV 저장 (현재 스크립트 위치에 저장)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(script_dir, csv_name)
        df.to_csv(csv_path, index=False)
        print(f"\n📁 CSV 저장: {csv_path}")

        # BigQuery 업로드 여부 확인
        upload = input("\nBigQuery에 업로드하시겠습니까? (y/n): ")
        if upload.lower() == 'y':
            save_to_bigquery(df)
    else:
        print("데이터를 가져오지 못했습니다.")


if __name__ == "__main__":
    main()
