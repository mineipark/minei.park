"""
기상청 단기예보 + ASOS 관측 통합 날씨 모듈

기능:
  1. 단기예보 API → 미래 날짜 날씨 (최대 3일)
  2. ASOS 관측 API → 과거 날짜 보완 (windspeed, humidity, rain 등)
  3. CSV 날씨 데이터 관리

사용법:
    from fetch_weather_forecast import get_forecast_weather, backfill_asos_weather

    # 내일 예보
    weather = get_forecast_weather('2026-02-28')

    # CSV 누락 필드 보완
    backfill_asos_weather('weather_2025_202601.csv')
"""

import os
import urllib.request
from urllib.parse import urlencode
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, List

# 기상알림 시스템 API 키 (단기예보/중기예보 서비스 포함)
FORECAST_API_KEY = os.getenv("WEATHER_FORECAST_API_KEY", "")

# ASOS 관측 API 키 (기존)
ASOS_API_KEY = os.getenv("WEATHER_API_KEY", "")

# API URLs
FORECAST_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
ASOS_URL = "https://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList"

# 서울 격자 좌표
SEOUL_NX = 60
SEOUL_NY = 127

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# ============================================================
# 단기예보 API
# ============================================================

def _get_base_times(now: datetime = None) -> List[tuple]:
    """현재 시각 기준 시도할 발표 시각 목록 (최신순)"""
    if now is None:
        now = datetime.now()

    base_hours = [23, 20, 17, 14, 11, 8, 5, 2]
    results = []

    today = now.strftime('%Y%m%d')
    for h in base_hours:
        if now.hour > h or (now.hour == h and now.minute >= 10):
            results.append((today, f'{h:02d}00'))

    yesterday = (now - timedelta(days=1)).strftime('%Y%m%d')
    for h in base_hours:
        results.append((yesterday, f'{h:02d}00'))

    return results


def _fetch_forecast_raw(base_date: str, base_time: str,
                        nx: int = SEOUL_NX, ny: int = SEOUL_NY) -> pd.DataFrame:
    """기상청 단기예보 API 호출"""
    params = '?' + urlencode({
        'serviceKey': FORECAST_API_KEY,
        'pageNo': '1',
        'numOfRows': '1000',
        'dataType': 'JSON',
        'base_date': base_date,
        'base_time': base_time,
        'nx': str(nx),
        'ny': str(ny),
    })

    url = FORECAST_URL + params

    try:
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0')
        response = urllib.request.urlopen(req, timeout=30)
        data = json.loads(response.read())

        header = data['response']['header']
        if header['resultCode'] == '00':
            items = data['response']['body']['items']['item']
            return pd.DataFrame(items)
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _parse_precipitation(value: str) -> float:
    """강수량 파싱: '강수없음'→0, '1.0mm'→1.0, '1.0~4.0mm'→2.5"""
    if not value or value == '강수없음':
        return 0.0
    value = value.replace('mm', '').strip()
    if '~' in value:
        parts = value.split('~')
        try:
            return (float(parts[0]) + float(parts[1])) / 2
        except ValueError:
            return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


def _parse_snow(value: str) -> float:
    """적설량 파싱: '적설없음'→0, '1.0cm'→1.0"""
    if not value or value == '적설없음':
        return 0.0
    value = value.replace('cm', '').strip()
    if '~' in value:
        parts = value.split('~')
        try:
            return (float(parts[0]) + float(parts[1])) / 2
        except ValueError:
            return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


def get_forecast_weather(target_date: str, verbose: bool = True) -> Optional[Dict]:
    """특정 날짜의 예보 데이터를 일별 집계하여 반환

    Returns:
        dict: date, temp_low, temp_high, temp_avg, rain_sum,
              windspeed_avg, humidity_avg, snow_sum
        None: 예보 데이터 없음
    """
    target = pd.Timestamp(target_date)
    target_str = target.strftime('%Y%m%d')

    if verbose:
        print(f"  [예보 API] {target_date} 조회 중...")

    base_times = _get_base_times()

    for bd, bt in base_times:
        raw = _fetch_forecast_raw(bd, bt)
        if len(raw) == 0:
            continue

        day_data = raw[raw['fcstDate'] == target_str]
        if len(day_data) == 0:
            continue

        if verbose:
            print(f"  [예보 API] 발표: {bd} {bt}시, {len(day_data)}건")

        result = {'date': target_date}

        # TMN (일 최저기온)
        tmn = day_data[day_data['category'] == 'TMN']
        if len(tmn) > 0:
            result['temp_low'] = float(tmn.iloc[0]['fcstValue'])
        else:
            tmp = day_data[day_data['category'] == 'TMP']
            if len(tmp) > 0:
                result['temp_low'] = tmp['fcstValue'].astype(float).min()

        # TMX (일 최고기온)
        tmx = day_data[day_data['category'] == 'TMX']
        if len(tmx) > 0:
            result['temp_high'] = float(tmx.iloc[0]['fcstValue'])
        else:
            tmp = day_data[day_data['category'] == 'TMP']
            if len(tmp) > 0:
                result['temp_high'] = tmp['fcstValue'].astype(float).max()

        # PCP (강수량 합계)
        pcp = day_data[day_data['category'] == 'PCP']
        result['rain_sum'] = sum(_parse_precipitation(v) for v in pcp['fcstValue']) if len(pcp) > 0 else 0.0

        # WSD (풍속 평균)
        wsd = day_data[day_data['category'] == 'WSD']
        result['windspeed_avg'] = round(wsd['fcstValue'].astype(float).mean(), 1) if len(wsd) > 0 else 0.0

        # REH (습도 평균)
        reh = day_data[day_data['category'] == 'REH']
        result['humidity_avg'] = round(reh['fcstValue'].astype(float).mean(), 1) if len(reh) > 0 else 50.0

        # SNO (적설량)
        sno = day_data[day_data['category'] == 'SNO']
        result['snow_sum'] = sum(_parse_snow(v) for v in sno['fcstValue']) if len(sno) > 0 else 0.0

        if 'temp_low' in result and 'temp_high' in result:
            result['temp_avg'] = round((result['temp_low'] + result['temp_high']) / 2, 1)
            if verbose:
                print(f"  [예보] {result['temp_low']:.1f}~{result['temp_high']:.1f}°C, "
                      f"비 {result['rain_sum']:.1f}mm, 바람 {result['windspeed_avg']:.1f}m/s, "
                      f"습도 {result['humidity_avg']:.0f}%, 눈 {result['snow_sum']:.1f}cm")
            return result

    if verbose:
        print(f"  [예보 API] {target_date} 예보 없음 → ffill 사용")
    return None


def get_multi_day_forecast(dates: List[str], verbose: bool = True) -> pd.DataFrame:
    """여러 날짜의 예보를 한번에 조회"""
    rows = []
    for d in dates:
        result = get_forecast_weather(d, verbose=verbose)
        if result:
            rows.append(result)
    if rows:
        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        return df
    return pd.DataFrame()


# ============================================================
# ASOS 관측 데이터 보완
# ============================================================

def backfill_asos_weather(csv_path: str = None, verbose: bool = True) -> bool:
    """CSV에서 누락된 windspeed_avg, humidity_avg, rain_sum을 ASOS API로 보완"""
    if csv_path is None:
        csv_path = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')

    if not os.path.exists(csv_path):
        if verbose:
            print("  CSV 없음")
        return False

    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['date'])

    # 보완 필요한 행 = windspeed_avg 또는 rain_sum이 NaN이지만 temp_low는 있는 행
    needs_wind = df['windspeed_avg'].isna() if 'windspeed_avg' in df.columns else pd.Series([True] * len(df))
    needs_rain = df['rain_sum'].isna() if 'rain_sum' in df.columns else pd.Series([True] * len(df))
    has_temp = df['temp_low'].notna()

    need_fill = df[(needs_wind | needs_rain) & has_temp]

    if len(need_fill) == 0:
        if verbose:
            print("  날씨 보완 필요 없음")
        return False

    # ASOS는 2일 전까지만 제공
    cutoff = pd.Timestamp(datetime.now() - timedelta(days=2))
    fill_dates = need_fill[need_fill['date'] <= cutoff]

    if len(fill_dates) == 0:
        if verbose:
            print("  ASOS 미도달 기간 (2일 전 이후)")
        return False

    min_date = fill_dates['date'].min()
    max_date = fill_dates['date'].max()

    if verbose:
        print(f"  ASOS 보완: {min_date.strftime('%Y-%m-%d')} ~ {max_date.strftime('%Y-%m-%d')} "
              f"({len(fill_dates)}일)")

    # ASOS API 호출 (월 단위로 분할)
    all_asos = []
    current = min_date
    while current <= max_date:
        month_end = min(current + timedelta(days=30), max_date)
        params = f"?serviceKey={ASOS_API_KEY}&" + urlencode({
            "pageNo": "1", "numOfRows": "100", "dataType": "JSON",
            "dataCd": "ASOS", "dateCd": "DAY",
            "startDt": current.strftime('%Y%m%d'),
            "endDt": month_end.strftime('%Y%m%d'),
            "stnIds": "108"
        })

        try:
            req = urllib.request.Request(ASOS_URL + params)
            req.add_header('User-Agent', 'Mozilla/5.0')
            response = urllib.request.urlopen(req, timeout=30)
            data = json.loads(response.read())

            if data['response']['header']['resultCode'] == '00':
                items = data['response']['body']['items']['item']
                all_asos.extend(items)
        except Exception as e:
            if verbose:
                print(f"  ASOS 호출 실패 ({current.strftime('%Y%m%d')}~): {e}")

        current = month_end + timedelta(days=1)

    if not all_asos:
        if verbose:
            print("  ASOS 데이터 없음")
        return False

    # 보완 적용
    field_map = {
        'avgWs': 'windspeed_avg',
        'avgRhm': 'humidity_avg',
        'sumRn': 'rain_sum',
        'ddMes': 'snow_depth',
    }

    updated = 0
    for item in all_asos:
        item_date = pd.Timestamp(item['tm'])
        mask = df['date'] == item_date
        if mask.sum() == 0:
            continue
        for src, dst in field_map.items():
            if src in item and item[src] is not None and str(item[src]).strip() != '':
                try:
                    val = float(item[src])
                    if dst not in df.columns:
                        df[dst] = np.nan
                    if pd.isna(df.loc[mask, dst].values[0]):
                        df.loc[mask, dst] = val
                        updated += 1
                except (ValueError, TypeError):
                    pass

    df.to_csv(csv_path, index=False)
    if verbose:
        print(f"  보완 완료: {updated}개 필드 업데이트 → {csv_path}")
    return updated > 0


# ============================================================
# CLI
# ============================================================

if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--backfill':
        print("=== ASOS 데이터 보완 ===")
        backfill_asos_weather()
    else:
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"=== 예보 테스트: {tomorrow} ===")
        result = get_forecast_weather(tomorrow)
        if result:
            print(f"\n결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
