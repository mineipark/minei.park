#!/usr/bin/env python
"""
일일 자동화 파이프라인 (Production v2)

매일 GitHub Actions에서 실행:
1.  날씨 데이터 업데이트 (ASOS 백필 + 예보)
2.  오늘+내일 district-level 예측
2b. 내일(D+1) district × hour 시간별 예측 (시계열 비율 분배)
3.  어제 실적 수집 (BigQuery)
4.  예측 vs 실적 비교 → 오차율 계산
5.  production_performance_log.json 갱신
6.  production_predictions.csv 누적 저장
6b. production_hourly_predictions.csv 누적 저장
7.  Google Sheets 동기화 (일별/권역별/센터별)
7b. Google Sheets 시간별예측 시트 동기화

사용법:
    python demand_forecast/daily_pipeline.py
"""
import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

PREDICTIONS_CSV = os.path.join(SCRIPT_DIR, 'production_predictions.csv')
HOURLY_PREDICTIONS_CSV = os.path.join(SCRIPT_DIR, 'production_hourly_predictions.csv')
PERFORMANCE_LOG = os.path.join(SCRIPT_DIR, 'production_performance_log.json')
WEATHER_CSV = os.path.join(SCRIPT_DIR, 'weather_2025_202601.csv')

# Authentication: set GOOGLE_APPLICATION_CREDENTIALS env var
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    _default_cred = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'credentials', 'service-account.json')
    if os.path.exists(_default_cred):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _default_cred


def step1_update_weather():
    """Step 1: 날씨 데이터 업데이트 (ASOS 백필 + 예보)"""
    print("\n[Step 1] 날씨 데이터 업데이트...")

    # ASOS 관측값 백필
    try:
        from fetch_weather_forecast import backfill_asos_weather
        backfill_asos_weather(WEATHER_CSV)
        print("  ASOS 백필 완료")
    except Exception as e:
        print(f"  ASOS 백필 실패 (계속 진행): {e}")

    # CSV 상태 확인
    if os.path.exists(WEATHER_CSV):
        wdf = pd.read_csv(WEATHER_CSV)
        wdf['date'] = pd.to_datetime(wdf['date'])
        max_date = wdf['date'].max()
        print(f"  날씨 CSV 마지막: {max_date.strftime('%Y-%m-%d')}")
    else:
        print("  ⚠ 날씨 CSV 없음")


def step2_predict(target_dates: list) -> dict:
    """Step 2: district-level 예측 실행

    Returns:
        {date_str: {'district_df': df, 'region_df': df, 'total': int}}
    """
    from production_v2_predictor import predict_district_rides, fetch_data_range, LOOKBACK_DAYS

    print(f"\n[Step 2] 예측 실행 ({len(target_dates)}일)...")
    results = {}

    # 데이터 한 번만 fetch (캐시)
    earliest = min(target_dates)
    fetch_start = (pd.Timestamp(earliest) - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    fetch_end = max(target_dates)
    print(f"  데이터 범위: {fetch_start} ~ {fetch_end}")

    try:
        cache_data = fetch_data_range(fetch_start, fetch_end, verbose=True)
    except Exception as e:
        print(f"  ⚠ BQ fetch 실패: {e}")
        cache_data = None

    for date_str in sorted(target_dates):
        try:
            district_df, region_df = predict_district_rides(
                date_str, cache_data=cache_data, verbose=False
            )
            total = int(district_df['adj_pred'].sum()) if len(district_df) > 0 else 0
            results[date_str] = {
                'district_df': district_df,
                'region_df': region_df,
                'total': total
            }
            print(f"  {date_str}: {total:,}건 ({len(district_df)} districts)")
        except Exception as e:
            print(f"  {date_str} 예측 실패: {e}")

    return results


def step2b_hourly_predict(target_dates: list) -> dict:
    """Step 2b: district × hour 시간별 예측 (시계열 비율 분배)

    Returns:
        {date_str: DataFrame[date, district, hour, predicted_rides, lat, lng, center, window]}
    """
    from district_v2_hourly import DistrictV2Hourly

    print(f"\n[Step 2b] 시간별 예측 ({len(target_dates)}일)...")
    predictor = DistrictV2Hourly(verbose=False)
    results = {}

    for date_str in sorted(target_dates):
        try:
            hourly_df = predictor.to_hourly_estimate(date_str)
            if len(hourly_df) > 0:
                results[date_str] = hourly_df
                total = hourly_df['predicted_rides'].sum()
                n_districts = hourly_df['district'].nunique()
                print(f"  {date_str}: {total:,.0f}건 ({n_districts} districts × 24h)")
            else:
                print(f"  {date_str}: 시간별 예측 데이터 없음")
        except Exception as e:
            print(f"  {date_str} 시간별 예측 실패: {e}")

    return results


def step6b_append_hourly_csv(hourly_predictions: dict):
    """Step 6b: production_hourly_predictions.csv에 누적 저장 (중복 제거)"""
    print(f"\n[Step 6b] 시간별 예측 CSV 저장...")

    new_dfs = []
    for date_str, df in hourly_predictions.items():
        new_dfs.append(df)

    if not new_dfs:
        print("  새 시간별 예측 없음")
        return

    new_df = pd.concat(new_dfs, ignore_index=True)

    if os.path.exists(HOURLY_PREDICTIONS_CSV):
        existing = pd.read_csv(HOURLY_PREDICTIONS_CSV)
        # 같은 날짜 중복 제거 (새 데이터 우선)
        existing = existing[~existing['date'].isin(new_df['date'].unique())]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.sort_values(['date', 'district', 'hour'])
    combined.to_csv(HOURLY_PREDICTIONS_CSV, index=False)
    print(f"  {len(new_df)}행 추가 → 총 {len(combined)}행")


def step3_backfill_actual(dates: list) -> dict:
    """Step 3: BigQuery에서 실제 라이딩 데이터 수집

    Returns:
        {date_str: {'total': int, 'by_region': {region: count}, 'by_center': {center: count}}}
    """
    print(f"\n[Step 3] 실적 데이터 수집 ({len(dates)}일)...")

    if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        print("  ⚠ BQ 인증 없음 - 스킵")
        return {}

    try:
        from google.cloud import bigquery
        client = bigquery.Client()
    except Exception as e:
        print(f"  ⚠ BQ 클라이언트 실패: {e}")
        return {}

    query = """
    SELECT
        DATE(r.start_time) as ride_date,
        r.h3_start_area_name as region,
        COALESCE(c.name, 'unknown') as center_name,
        COUNT(*) as ride_count
    FROM `bikeshare.service.rides` r
    LEFT JOIN `bikeshare.service.geo_area` a ON r.h3_start_area_name = a.name
    LEFT JOIN `bikeshare.service.service_center` c ON a.center_id = c.id
    WHERE DATE(r.start_time) IN UNNEST(@dates)
    GROUP BY ride_date, region, center_name
    ORDER BY ride_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("dates", "DATE", dates),
        ]
    )

    try:
        data = client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        print(f"  BQ 조회 실패: {e}")
        return {}

    if len(data) == 0:
        print("  조회 결과 없음")
        return {}

    data['ride_date'] = data['ride_date'].astype(str)
    results = {}

    for date_str in dates:
        day_data = data[data['ride_date'] == date_str]
        if len(day_data) == 0:
            continue

        total = int(day_data['ride_count'].sum())
        by_region = day_data.groupby('region')['ride_count'].sum().to_dict()
        by_center = day_data.groupby('center_name')['ride_count'].sum().to_dict()

        results[date_str] = {
            'total': total,
            'by_region': {k: int(v) for k, v in by_region.items()},
            'by_center': {k: int(v) for k, v in by_center.items()},
        }
        print(f"  {date_str}: {total:,}건 실적")

    return results


def step4_calculate_errors(predictions: dict, actuals: dict) -> list:
    """Step 4: 예측 vs 실적 비교"""
    print(f"\n[Step 4] 오차 계산...")

    errors = []
    for date_str in sorted(set(predictions.keys()) & set(actuals.keys())):
        pred_total = predictions[date_str]['total']
        actual_total = actuals[date_str]['total']

        if actual_total > 0:
            mape = abs(pred_total - actual_total) / actual_total * 100
            bias = (pred_total - actual_total) / actual_total * 100
        else:
            mape = 0
            bias = 0

        entry = {
            'date': date_str,
            'predicted': pred_total,
            'actual': actual_total,
            'mape': round(mape, 1),
            'bias': round(bias, 1),
        }
        errors.append(entry)
        print(f"  {date_str}: pred={pred_total:,} actual={actual_total:,} "
              f"MAPE={mape:.1f}% bias={bias:+.1f}%")

    return errors


def step5_update_performance_log(errors: list):
    """Step 5: production_performance_log.json 갱신"""
    print(f"\n[Step 5] 성과 로그 업데이트...")

    log = {'daily': [], 'weekly_mape': []}
    if os.path.exists(PERFORMANCE_LOG):
        with open(PERFORMANCE_LOG, 'r') as f:
            log = json.load(f)

    existing_dates = {e['date'] for e in log.get('daily', [])}

    added = 0
    for err in errors:
        if err['date'] not in existing_dates:
            log['daily'].append({
                'date': err['date'],
                'mape': err['mape'],
                'bias': err['bias'],
                'predicted': err['predicted'],
                'actual': err['actual'],
            })
            added += 1
        else:
            # 기존 항목 업데이트
            for entry in log['daily']:
                if entry['date'] == err['date']:
                    entry['mape'] = err['mape']
                    entry['bias'] = err['bias']
                    entry['predicted'] = err['predicted']
                    entry['actual'] = err['actual']
                    break

    # 날짜 정렬
    log['daily'] = sorted(log['daily'], key=lambda x: x['date'])

    with open(PERFORMANCE_LOG, 'w') as f:
        json.dump(log, f, indent=2, ensure_ascii=False)

    print(f"  {added}건 추가, 총 {len(log['daily'])}건")


def step6_append_predictions_csv(predictions: dict):
    """Step 6: production_predictions.csv에 누적 저장 (중복 제거)"""
    print(f"\n[Step 6] 예측 CSV 누적 저장...")

    new_rows = []
    for date_str, result in predictions.items():
        ddf = result['district_df']
        if len(ddf) == 0:
            continue
        for _, row in ddf.iterrows():
            new_rows.append({
                'date': date_str,
                'region': row['region'],
                'district': row['district'],
                'adj_pred': row['adj_pred'],
                'pred_opens': row.get('pred_opens', 0),
                'pred_rpo': row.get('pred_rpo', 0),
                'lat': row.get('lat', 0),
                'lng': row.get('lng', 0),
                'center': row.get('center', ''),
                'desc': 'Production v2',
            })

    if not new_rows:
        print("  새 예측 없음")
        return

    new_df = pd.DataFrame(new_rows)

    if os.path.exists(PREDICTIONS_CSV):
        existing = pd.read_csv(PREDICTIONS_CSV)
        # 같은 날짜+district 중복 제거 (새 데이터 우선)
        existing = existing[~existing['date'].isin(new_df['date'].unique())]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.sort_values(['date', 'region', 'district'])
    combined.to_csv(PREDICTIONS_CSV, index=False)
    print(f"  {len(new_rows)}행 추가 → 총 {len(combined)}행")


def step7_sync_to_sheets(predictions: dict, actuals: dict):
    """Step 7: Google Sheets 동기화 (V2 기반)"""
    print(f"\n[Step 7] Google Sheets 동기화...")

    SHEET_ID = os.environ.get('DEMAND_FORECAST_SHEET_ID', '')
    if not SHEET_ID:
        print("  ⚠ DEMAND_FORECAST_SHEET_ID 미설정 - 스킵")
        return

    try:
        from sheets_sync import SheetsSync
        sync = SheetsSync(sheet_id=SHEET_ID)
    except Exception as e:
        print(f"  Sheets 초기화 실패: {e}")
        return

    # 시트 확인/생성
    for name in ['일별예측', '권역별예측', '센터별예측']:
        sync.create_sheet_if_not_exists(name)

    # 기존 데이터 로드
    existing_daily = sync.read_sheet('일별예측')
    existing_region = sync.read_sheet('권역별예측')
    existing_center = sync.read_sheet('센터별예측')

    daily_dict = {}
    for row in existing_daily[1:] if len(existing_daily) > 1 else []:
        if len(row) > 0 and row[0]:
            daily_dict[row[0]] = row

    region_dict = {}
    for row in existing_region[1:] if len(existing_region) > 1 else []:
        if len(row) > 1 and row[0]:
            region_dict[f"{row[0]}_{row[1]}"] = row

    center_dict = {}
    for row in existing_center[1:] if len(existing_center) > 1 else []:
        if len(row) > 1 and row[0]:
            center_dict[f"{row[0]}_{row[1]}"] = row

    dow_names = ['월', '화', '수', '목', '금', '토', '일']
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')

    # 예측 데이터 기록
    for date_str, result in predictions.items():
        d = datetime.strptime(date_str, '%Y-%m-%d')
        dow = dow_names[d.weekday()]
        total_pred = result['total']

        actual_data = actuals.get(date_str, {})
        actual_total = actual_data.get('total', '')
        if actual_total:
            error = round((total_pred - actual_total) / actual_total * 100, 1)
        else:
            error = ''

        existing_row = daily_dict.get(date_str, [])
        if not actual_total and len(existing_row) > 3 and existing_row[3]:
            actual_total = existing_row[3]
            error = existing_row[4] if len(existing_row) > 4 else ''

        daily_dict[date_str] = [
            date_str, dow, round(total_pred),
            actual_total, error,
            '', '',  # 기온 (날씨 CSV에서 추후 채움)
            'Production v2', now_str
        ]

        # 권역별
        rdf = result.get('region_df')
        if rdf is not None and len(rdf) > 0:
            for _, rrow in rdf.iterrows():
                region = rrow['region']
                key = f"{date_str}_{region}"
                pred = int(rrow['adj_pred'])
                region_actual = actual_data.get('by_region', {}).get(region, '')
                region_error = ''
                if region_actual:
                    region_error = round((pred - region_actual) / region_actual * 100, 1)

                existing_rrow = region_dict.get(key, [])
                if not region_actual and len(existing_rrow) > 4 and existing_rrow[4]:
                    region_actual = existing_rrow[4]
                    region_error = existing_rrow[5] if len(existing_rrow) > 5 else ''

                region_dict[key] = [
                    date_str, region, rrow.get('center', ''),
                    pred, region_actual, region_error, ''
                ]

        # 센터별 집계
        ddf = result.get('district_df')
        if ddf is not None and len(ddf) > 0:
            center_preds = ddf.groupby('center')['adj_pred'].sum().to_dict()
            for center, pred in center_preds.items():
                if not center:
                    continue
                key = f"{date_str}_{center}"
                center_actual = actual_data.get('by_center', {}).get(center, '')
                center_error = ''
                if center_actual:
                    center_error = round((pred - center_actual) / center_actual * 100, 1)

                existing_crow = center_dict.get(key, [])
                if not center_actual and len(existing_crow) > 3 and existing_crow[3]:
                    center_actual = existing_crow[3]
                    center_error = existing_crow[4] if len(existing_crow) > 4 else ''

                center_dict[key] = [
                    date_str, center, round(pred),
                    center_actual, center_error,
                    'Production v2', now_str
                ]

    # 실적만 있는 날짜도 반영
    for date_str, actual_data in actuals.items():
        if date_str not in predictions:
            d = datetime.strptime(date_str, '%Y-%m-%d')
            dow = dow_names[d.weekday()]
            existing_row = daily_dict.get(date_str, [])
            pred = existing_row[2] if len(existing_row) > 2 else ''
            actual_total = actual_data['total']
            error = ''
            if pred and actual_total > 0:
                error = round((float(pred) - actual_total) / actual_total * 100, 1)

            daily_dict[date_str] = [
                date_str, dow, pred,
                actual_total, error,
                '', '', '', now_str
            ]

    # 시트 쓰기
    daily_header = ['날짜', '요일', '예측', '실제', '오차(%)', '최저기온', '최고기온', '모델', '업데이트']
    sorted_daily = sorted(daily_dict.values(), key=lambda x: x[0])
    sync.clear_and_write('일별예측', [daily_header] + sorted_daily)

    region_header = ['날짜', '권역', '센터', '예측', '실제', '오차(%)', '보정계수']
    sorted_region = sorted(region_dict.values(), key=lambda x: (x[0], x[1]))
    sync.clear_and_write('권역별예측', [region_header] + sorted_region)

    center_header = ['날짜', '센터', '예측', '실제', '오차(%)', '모델', '업데이트']
    sorted_center = sorted(center_dict.values(), key=lambda x: (x[0], x[1]))
    sync.clear_and_write('센터별예측', [center_header] + sorted_center)

    print(f"  일별 {len(daily_dict)}건, 권역 {len(region_dict)}건, 센터 {len(center_dict)}건 동기화 완료")
    print(f"  시트: https://docs.google.com/spreadsheets/d/{SHEET_ID}")


def step7b_sync_hourly_to_sheets(hourly_predictions: dict):
    """Step 7b: 시간별 예측 → Google Sheets '시간별예측' 시트 동기화"""
    print(f"\n[Step 7b] 시간별 예측 Sheets 동기화...")

    SHEET_ID = os.environ.get('DEMAND_FORECAST_SHEET_ID', '')
    if not SHEET_ID:
        print("  ⚠ DEMAND_FORECAST_SHEET_ID 미설정 - 스킵")
        return

    if not hourly_predictions:
        print("  시간별 예측 데이터 없음 - 스킵")
        return

    try:
        from sheets_sync import SheetsSync
        sync = SheetsSync(sheet_id=SHEET_ID)
    except Exception as e:
        print(f"  Sheets 초기화 실패: {e}")
        return

    sync.create_sheet_if_not_exists('시간별예측')

    # 기존 데이터 로드
    existing = sync.read_sheet('시간별예측')
    existing_dict = {}
    for row in existing[1:] if len(existing) > 1 else []:
        if len(row) >= 4 and row[0]:
            key = f"{row[0]}_{row[1]}_{row[2]}"  # 날짜_district_hour
            existing_dict[key] = row

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')

    # 새 데이터 추가/갱신
    for date_str, df in hourly_predictions.items():
        for _, row in df.iterrows():
            key = f"{date_str}_{row['district']}_{int(row['hour'])}"
            existing_dict[key] = [
                date_str,
                row.get('district', ''),
                int(row['hour']),
                round(row['predicted_rides'], 1),
                row.get('region', ''),
                row.get('center', ''),
                row.get('window', ''),
                now_str
            ]

    # 시트 쓰기 (날짜 → district → hour 정렬)
    header = ['날짜', 'District', '시간', '예측건수', 'Area', '센터', 'Window', '업데이트']
    sorted_rows = sorted(existing_dict.values(),
                         key=lambda x: (x[0], x[1], int(x[2]) if x[2] != '' else 0))
    sync.clear_and_write('시간별예측', [header] + sorted_rows)
    print(f"  시간별예측 {len(existing_dict)}건 동기화 완료")


def main():
    """일일 파이프라인 메인"""
    print("=" * 60)
    print(f"🚀 일일 예측 파이프라인 (Production v2)")
    print(f"   실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    tomorrow_str = (today + timedelta(days=1)).strftime('%Y-%m-%d')
    yesterday_str = (today - timedelta(days=1)).strftime('%Y-%m-%d')

    # Step 1: 날씨 업데이트
    step1_update_weather()

    # Step 2: 오늘+내일 예측
    target_dates = [today_str, tomorrow_str]
    predictions = step2_predict(target_dates)

    # Step 2b: 내일 시간별 예측 (D+1)
    hourly_predictions = step2b_hourly_predict([tomorrow_str])

    # Step 3: 어제 실적 수집
    actuals = step3_backfill_actual([yesterday_str])

    # Step 4: 오차 계산 (어제 예측 vs 어제 실적)
    # 어제 예측이 CSV에 있으면 로드
    yesterday_predictions = {}
    if os.path.exists(PREDICTIONS_CSV):
        csv_df = pd.read_csv(PREDICTIONS_CSV)
        yest_data = csv_df[csv_df['date'] == yesterday_str]
        if len(yest_data) > 0:
            yesterday_predictions[yesterday_str] = {
                'total': int(yest_data['adj_pred'].sum()),
                'district_df': yest_data,
                'region_df': pd.DataFrame(),
            }

    errors = step4_calculate_errors(yesterday_predictions, actuals)

    # Step 5: 성과 로그
    if errors:
        step5_update_performance_log(errors)

    # Step 6: 예측 CSV 누적
    step6_append_predictions_csv(predictions)

    # Step 6b: 시간별 예측 CSV 누적
    step6b_append_hourly_csv(hourly_predictions)

    # Step 7: Sheets 동기화
    all_preds = {**predictions, **yesterday_predictions}
    step7_sync_to_sheets(all_preds, actuals)

    # Step 7b: 시간별 예측 Sheets 동기화
    step7b_sync_hourly_to_sheets(hourly_predictions)

    print("\n" + "=" * 60)
    print("✅ 파이프라인 완료!")
    print("=" * 60)


if __name__ == '__main__':
    main()
