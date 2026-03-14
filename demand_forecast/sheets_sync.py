#!/usr/bin/env python
"""
Google Sheets 연동 - 수요 예측 결과 저장

사용법:
    python sheets_sync.py

필요한 설정:
    1. Google Cloud Console에서 Sheets API 활성화
    2. 서비스 계정에 시트 편집 권한 부여
    3. SHEET_ID 환경변수 또는 직접 설정
"""
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict

# Google API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# 설정
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(SCRIPT_DIR, '..', 'credentials', 'service-account.json')

# 시트 ID (URL에서 확인: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit)
SHEET_ID = os.environ.get('DEMAND_FORECAST_SHEET_ID', '')

# 스코프
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# 타겟 권역: region_params.json에서 동적 로드
def _load_target_regions():
    """region_params.json에서 전체 권역 목록 로드"""
    params_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'region_params.json')
    if os.path.exists(params_path):
        with open(params_path, 'r') as f:
            return list(json.load(f).keys())
    return []

TARGET_REGIONS = _load_target_regions()


class SheetsSync:
    """Google Sheets 동기화 클래스"""

    def __init__(self, sheet_id: Optional[str] = None, credentials_path: Optional[str] = None):
        self.sheet_id = sheet_id or SHEET_ID
        self.credentials_path = credentials_path or CREDENTIALS_PATH

        if not self.sheet_id:
            raise ValueError("SHEET_ID가 설정되지 않았습니다. 환경변수 DEMAND_FORECAST_SHEET_ID를 설정하세요.")

        self._init_service()

    def _init_service(self):
        """Google Sheets API 서비스 초기화"""
        credentials = service_account.Credentials.from_service_account_file(
            self.credentials_path, scopes=SCOPES
        )
        self.service = build('sheets', 'v4', credentials=credentials)
        self.sheets = self.service.spreadsheets()

    def create_sheet_if_not_exists(self, sheet_name: str) -> bool:
        """시트가 없으면 생성"""
        try:
            # 기존 시트 목록 확인
            spreadsheet = self.sheets.get(spreadsheetId=self.sheet_id).execute()
            existing_sheets = [s['properties']['title'] for s in spreadsheet['sheets']]

            if sheet_name in existing_sheets:
                return True

            # 새 시트 생성
            request = {
                'addSheet': {
                    'properties': {'title': sheet_name}
                }
            }
            self.sheets.batchUpdate(
                spreadsheetId=self.sheet_id,
                body={'requests': [request]}
            ).execute()

            print(f"  시트 '{sheet_name}' 생성됨")
            return True

        except HttpError as e:
            print(f"  시트 생성 에러: {e}")
            return False

    def clear_and_write(self, sheet_name: str, data: List[List], start_cell: str = 'A1'):
        """시트 내용 지우고 새로 쓰기"""
        try:
            # 기존 내용 삭제
            range_name = f"{sheet_name}!A:Z"
            self.sheets.values().clear(
                spreadsheetId=self.sheet_id,
                range=range_name
            ).execute()

            # 새 데이터 쓰기
            range_name = f"{sheet_name}!{start_cell}"
            self.sheets.values().update(
                spreadsheetId=self.sheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body={'values': data}
            ).execute()

            print(f"  '{sheet_name}' 시트에 {len(data)}행 저장")
            return True

        except HttpError as e:
            print(f"  쓰기 에러: {e}")
            return False

    def append_rows(self, sheet_name: str, data: List[List]):
        """시트에 행 추가"""
        try:
            range_name = f"{sheet_name}!A:Z"
            self.sheets.values().append(
                spreadsheetId=self.sheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body={'values': data}
            ).execute()

            print(f"  '{sheet_name}'에 {len(data)}행 추가")
            return True

        except HttpError as e:
            print(f"  추가 에러: {e}")
            return False

    def read_sheet(self, sheet_name: str, range_spec: str = 'A:Z') -> List[List]:
        """시트 읽기"""
        try:
            range_name = f"{sheet_name}!{range_spec}"
            result = self.sheets.values().get(
                spreadsheetId=self.sheet_id,
                range=range_name
            ).execute()

            return result.get('values', [])

        except HttpError as e:
            print(f"  읽기 에러: {e}")
            return []


def sync_daily_forecast():
    """일별 예측 결과를 시트에 동기화 (누적형) — Production v2 사용"""
    from production_v2_predictor import predict_district_rides, fetch_data_range, LOOKBACK_DAYS

    print("="*60)
    print("📊 수요 예측 → Google Sheets 동기화 (누적형)")
    print("="*60)

    # 시트 초기화
    try:
        sync = SheetsSync()
    except ValueError as e:
        print(f"\n❌ {e}")
        print("\n설정 방법:")
        print("  1. Google Sheets 새로 만들기")
        print("  2. 서비스 계정 이메일에 편집 권한 부여")
        print("  3. 환경변수 설정: export DEMAND_FORECAST_SHEET_ID='시트ID'")
        return

    sync.create_sheet_if_not_exists('일별예측')
    sync.create_sheet_if_not_exists('센터별예측')
    sync.create_sheet_if_not_exists('권역별예측')

    # 기존 데이터 읽기
    existing_daily = sync.read_sheet('일별예측')
    existing_region = sync.read_sheet('권역별예측')

    # 기존 데이터를 딕셔너리로 변환 (날짜 기준)
    daily_dict = {}
    for row in existing_daily[1:]:  # 헤더 제외
        if len(row) > 3 and row[0]:
            daily_dict[row[0]] = row

    region_dict = {}
    for row in existing_region[1:]:
        if len(row) > 1 and row[0]:
            key = f"{row[0]}_{row[1]}"  # 날짜_권역
            region_dict[key] = row

    # 센터별 기존 데이터
    existing_center = sync.read_sheet('센터별예측')
    center_dict = {}
    for row in existing_center[1:]:
        if len(row) > 1 and row[0]:
            key = f"{row[0]}_{row[1]}"  # 날짜_센터
            center_dict[key] = row

    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')

    # 1. 데이터 준비 (Production v2)
    print("\n[1/4] Production v2 데이터 준비...")
    fetch_start = (today - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    fetch_end = (today + timedelta(days=2)).strftime('%Y-%m-%d')
    try:
        cache_data = fetch_data_range(fetch_start, fetch_end, verbose=True)
    except Exception as e:
        print(f"  ⚠ 데이터 fetch 실패: {e}")
        cache_data = None

    # 2-3. 미래 예측 실행 (Production v2)
    print("\n[2/4] Production v2 예측 실행...")

    dow_names = ['월', '화', '수', '목', '금', '토', '일']
    new_predictions = 0

    # 오늘+내일 예측
    for i in range(0, 2):
        target_date = today + timedelta(days=i)
        date_str = target_date.strftime('%Y-%m-%d')

        try:
            district_df, region_df = predict_district_rides(
                date_str, cache_data=cache_data, verbose=False)

            if len(district_df) == 0:
                continue

            total_pred = int(district_df['adj_pred'].sum())
            dow = dow_names[target_date.weekday()]

            # 기존 데이터에 실제값이 있으면 보존
            existing_row = daily_dict.get(date_str, [])
            has_actual = len(existing_row) > 3 and existing_row[3] != '' and existing_row[3] is not None

            if has_actual:
                daily_dict[date_str] = [
                    date_str, dow, round(total_pred),
                    existing_row[3], existing_row[4],
                    '', '', 'Production v2',
                    datetime.now().strftime('%Y-%m-%d %H:%M')
                ]
            else:
                daily_dict[date_str] = [
                    date_str, dow, round(total_pred),
                    '', '', '', '', 'Production v2',
                    datetime.now().strftime('%Y-%m-%d %H:%M')
                ]

            # 권역별 데이터 업데이트
            for _, rrow in region_df.iterrows():
                key = f"{date_str}_{rrow['region']}"
                existing_region_row = region_dict.get(key, [])
                has_region_actual = len(existing_region_row) > 4 and existing_region_row[4] != '' and existing_region_row[4] is not None

                if has_region_actual:
                    region_dict[key] = [
                        date_str, rrow['region'], rrow.get('center', ''),
                        round(rrow['adj_pred']),
                        existing_region_row[4], existing_region_row[5], ''
                    ]
                else:
                    region_dict[key] = [
                        date_str, rrow['region'], rrow.get('center', ''),
                        round(rrow['adj_pred']), '', '', ''
                    ]

            # 센터별 집계
            center_preds = district_df.groupby('center')['adj_pred'].sum().to_dict()
            for center, pred_sum in center_preds.items():
                if not center:
                    continue
                key = f"{date_str}_{center}"
                existing_center_row = center_dict.get(key, [])
                has_center_actual = len(existing_center_row) > 3 and existing_center_row[3] != '' and existing_center_row[3] is not None

                if has_center_actual:
                    center_dict[key] = [
                        date_str, center, round(pred_sum),
                        existing_center_row[3], existing_center_row[4],
                        'Production v2', datetime.now().strftime('%Y-%m-%d %H:%M')
                    ]
                else:
                    center_dict[key] = [
                        date_str, center, round(pred_sum),
                        '', '', 'Production v2',
                        datetime.now().strftime('%Y-%m-%d %H:%M')
                    ]

            new_predictions += 1

        except Exception as e:
            print(f"  {date_str} 예측 실패: {e}")

    print(f"  {new_predictions}일 예측 완료")

    # 4. 시트에 저장 (날짜순 정렬)
    print("\n[4/4] Google Sheets 저장...")

    # 일별 데이터 정렬 후 저장
    daily_header = ['날짜', '요일', '예측', '실제', '오차(%)', '최저기온', '최고기온', '신뢰도', '업데이트']
    sorted_daily = sorted(daily_dict.values(), key=lambda x: x[0])
    daily_data = [daily_header] + sorted_daily
    sync.clear_and_write('일별예측', daily_data)

    # 권역별 데이터 정렬 후 저장
    region_header = ['날짜', '권역', '센터', '예측', '실제', '오차(%)', '보정계수']
    sorted_region = sorted(region_dict.values(), key=lambda x: (x[0], x[1]))
    region_data = [region_header] + sorted_region
    sync.clear_and_write('권역별예측', region_data)

    # 센터별 데이터 정렬 후 저장
    center_header = ['날짜', '센터', '예측', '실제', '오차(%)', '신뢰도', '업데이트']
    sorted_center = sorted(center_dict.values(), key=lambda x: (x[0], x[1]))
    center_data = [center_header] + sorted_center
    sync.clear_and_write('센터별예측', center_data)

    # 과거 데이터 개수 계산
    past_count = len([d for d in daily_dict.keys() if d < today_str])

    print(f"\n✅ 동기화 완료!")
    print(f"  과거 데이터: {past_count}일 (실제값 보존)")
    print(f"  미래 예측: {new_predictions}일 (업데이트)")
    print(f"  시트 URL: https://docs.google.com/spreadsheets/d/{sync.sheet_id}")


def update_actual_data():
    """실제 데이터로 시트 업데이트 (빈칸 백필 포함)

    어제 하루만이 아니라, 시트에서 실제값이 비어있는 과거 날짜를
    모두 찾아서 BigQuery에서 조회하고 채웁니다.
    """
    from google.cloud import bigquery

    print("="*60)
    print("📊 실제 데이터 업데이트 (백필)")
    print("="*60)

    try:
        sync = SheetsSync()
    except ValueError as e:
        print(f"❌ {e}")
        return

    # BigQuery 클라이언트
    bq_client = bigquery.Client.from_service_account_json(CREDENTIALS_PATH)

    today = datetime.now().strftime('%Y-%m-%d')
    dow_names = ['월', '화', '수', '목', '금', '토', '일']

    # 1. 모든 시트에서 실제값이 비어있는 과거 날짜 찾기
    daily_data = sync.read_sheet('일별예측')
    center_data = sync.read_sheet('센터별예측')
    region_data = sync.read_sheet('권역별예측')

    missing_dates = []

    # 일별예측에서 누락 날짜
    for row in daily_data[1:]:
        if len(row) > 0 and row[0] < today:
            has_actual = len(row) > 3 and row[3] != '' and row[3] is not None
            if not has_actual:
                missing_dates.append(row[0])

    # 센터별예측에서 누락 날짜
    for row in center_data[1:]:
        if len(row) > 0 and row[0] and row[0] < today:
            has_actual = len(row) > 3 and row[3] != '' and row[3] is not None
            if not has_actual:
                missing_dates.append(row[0])

    # 권역별예측에서 누락 날짜
    for row in region_data[1:]:
        if len(row) >= 2 and row[0] and row[0] < today:
            has_actual = len(row) > 4 and row[4] != '' and row[4] is not None
            if not has_actual:
                missing_dates.append(row[0])

    # 어제 날짜도 항상 포함 (당일 첫 실행 대비)
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    if yesterday not in missing_dates and yesterday < today:
        missing_dates.append(yesterday)

    missing_dates = sorted(set(missing_dates))

    if not missing_dates:
        print("  빈칸 없음 - 업데이트 불필요")
        return

    print(f"  실제값 누락 날짜: {len(missing_dates)}일 ({missing_dates[0]} ~ {missing_dates[-1]})")

    # 2. BigQuery에서 누락 날짜의 실제 데이터 한번에 조회
    dates_str = "', '".join(missing_dates)
    regions_str = ','.join([f"'{r}'" for r in TARGET_REGIONS])
    query = f"""
    SELECT
        DATE(r.start_time) as ride_date,
        r.h3_start_area_name as region,
        COALESCE(c.name, 'unknown') as center_name,
        COUNT(*) as ride_count
    FROM `service.rides` r
    LEFT JOIN `service.geo_area` a ON r.h3_start_area_name = a.name
    LEFT JOIN `service.service_center` c ON a.center_id = c.id
    WHERE DATE(r.start_time) IN ('{dates_str}')
        AND r.h3_start_area_name IN ({regions_str})
    GROUP BY ride_date, region, center_name
    ORDER BY ride_date
    """

    try:
        data = bq_client.query(query).to_dataframe()
    except Exception as e:
        print(f"  BigQuery 조회 실패: {e}")
        return

    if len(data) == 0:
        print(f"  조회된 실제 데이터 없음")
        return

    # 날짜별로 집계
    data['ride_date'] = data['ride_date'].astype(str)
    daily_totals = data.groupby('ride_date')['ride_count'].sum().to_dict()
    region_by_date = {}
    center_by_date = {}
    for _, row in data.iterrows():
        d = row['ride_date']
        if d not in region_by_date:
            region_by_date[d] = {}
        region_by_date[d][row['region']] = int(row['ride_count'])

        center = row['center_name']
        if d not in center_by_date:
            center_by_date[d] = {}
        if center not in center_by_date[d]:
            center_by_date[d][center] = 0
        center_by_date[d][center] += int(row['ride_count'])

    print(f"  BigQuery 결과: {len(daily_totals)}일분 데이터 조회됨")

    # 3. 일별예측 시트 업데이트
    daily_updated = 0
    daily_added = 0
    existing_dates = {row[0] for row in daily_data[1:] if len(row) > 0}

    for i, row in enumerate(daily_data):
        if i == 0:
            continue  # 헤더
        if len(row) > 0 and row[0] in daily_totals:
            actual_total = daily_totals[row[0]]
            pred = float(row[2]) if len(row) > 2 and row[2] else 0
            error = round((pred - actual_total) / actual_total * 100, 1) if actual_total > 0 and pred > 0 else ''

            # 컬럼 수 맞추기
            while len(daily_data[i]) < 9:
                daily_data[i].append('')

            daily_data[i][3] = actual_total
            daily_data[i][4] = error
            daily_updated += 1

    # 시트에 행 자체가 없는 날짜는 새로 추가
    for date_str, total in daily_totals.items():
        if date_str not in existing_dates:
            d = datetime.strptime(date_str, '%Y-%m-%d')
            new_row = [
                date_str,
                dow_names[d.weekday()],
                '',  # 예측 없음
                total,
                '',  # 오차 계산 불가
                '', '', '',
                datetime.now().strftime('%Y-%m-%d %H:%M')
            ]
            daily_data.append(new_row)
            daily_added += 1

    # 날짜순 정렬 후 저장
    header = daily_data[0]
    sorted_rows = sorted(daily_data[1:], key=lambda x: x[0] if x[0] else '')
    daily_data = [header] + sorted_rows
    sync.clear_and_write('일별예측', daily_data)
    print(f"  일별: {daily_updated}건 업데이트, {daily_added}건 추가")

    # 4. 권역별예측 시트 업데이트
    region_updated = 0
    region_added = 0

    # 기존 행에서 해당 날짜+권역 업데이트
    existing_keys = set()
    for i, row in enumerate(region_data):
        if i == 0:
            continue
        if len(row) >= 4 and row[0] in region_by_date:
            region_name = row[1]
            existing_keys.add(f"{row[0]}_{region_name}")
            if region_name in region_by_date[row[0]]:
                actual = region_by_date[row[0]][region_name]
                pred = float(row[3]) if row[3] else 0
                error = round((pred - actual) / actual * 100, 1) if actual > 0 and pred > 0 else ''

                while len(region_data[i]) < 7:
                    region_data[i].append('')

                region_data[i][4] = actual
                region_data[i][5] = error
                region_updated += 1

    # 시트에 없는 날짜+권역 추가
    for date_str, regions in region_by_date.items():
        for region_name, actual in regions.items():
            key = f"{date_str}_{region_name}"
            if key not in existing_keys:
                new_row = [date_str, region_name, '', '', actual, '', '']
                region_data.append(new_row)
                region_added += 1

    if region_updated > 0 or region_added > 0:
        header = region_data[0]
        sorted_data = sorted(region_data[1:], key=lambda x: (x[0] if x[0] else '', x[1] if len(x) > 1 else ''))
        region_data = [header] + sorted_data
        sync.clear_and_write('권역별예측', region_data)
        print(f"  권역별: {region_updated}건 업데이트, {region_added}건 추가")

    # 5. 센터별예측 시트 업데이트
    if not center_data:
        center_data = [['날짜', '센터', '예측', '실제', '오차(%)', '신뢰도', '업데이트']]

    center_updated = 0
    center_added = 0

    existing_center_keys = set()
    for i, row in enumerate(center_data):
        if i == 0:
            continue
        if len(row) >= 3 and row[0] in center_by_date:
            center_name = row[1]
            existing_center_keys.add(f"{row[0]}_{center_name}")
            if center_name in center_by_date[row[0]]:
                actual = center_by_date[row[0]][center_name]
                pred = float(row[2]) if row[2] else 0
                error = round((pred - actual) / actual * 100, 1) if actual > 0 and pred > 0 else ''

                while len(center_data[i]) < 7:
                    center_data[i].append('')

                center_data[i][3] = actual
                center_data[i][4] = error
                center_updated += 1

    # 시트에 없는 날짜+센터 추가
    for date_str, centers in center_by_date.items():
        for center_name, actual in centers.items():
            key = f"{date_str}_{center_name}"
            if key not in existing_center_keys:
                new_row = [date_str, center_name, '', actual, '', '',
                           datetime.now().strftime('%Y-%m-%d %H:%M')]
                center_data.append(new_row)
                center_added += 1

    if center_updated > 0 or center_added > 0:
        header = center_data[0]
        sorted_data = sorted(center_data[1:], key=lambda x: (x[0] if x[0] else '', x[1] if len(x) > 1 else ''))
        center_data = [header] + sorted_data
        sync.clear_and_write('센터별예측', center_data)
        print(f"  센터별: {center_updated}건 업데이트, {center_added}건 추가")


def main():
    """메인 실행"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'update':
        update_actual_data()
    else:
        sync_daily_forecast()


if __name__ == "__main__":
    main()
