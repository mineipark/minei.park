"""
Google Sheets 읽기/쓰기 유틸리티
- 실종위기기기 수색 상태 관리용
"""

import gspread
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime
import os

# 서비스 계정 JSON 경로
KEY_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "key.json")

# Sheets 권한 scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Google Sheets ID
SEARCH_STATUS_SHEET_ID = "YOUR_GOOGLE_SHEET_ID"


def get_sheets_client():
    """Google Sheets 클라이언트 생성"""
    credentials = service_account.Credentials.from_service_account_file(
        KEY_PATH, scopes=SCOPES
    )
    return gspread.authorize(credentials)


def get_search_status_sheet():
    """수색 상태 시트 가져오기"""
    client = get_sheets_client()
    spreadsheet = client.open_by_key(SEARCH_STATUS_SHEET_ID)
    return spreadsheet.sheet1  # 첫 번째 시트 사용


def read_search_status() -> pd.DataFrame:
    """수색 상태 전체 읽기"""
    expected_cols = ['sn', 'center_name', 'area_name', 'search_1', 'search_2', 'status', 'updated_at', 'updated_by']
    try:
        sheet = get_search_status_sheet()
        all_values = sheet.get_all_values()

        # 데이터가 없거나 헤더만 있는 경우
        if not all_values or len(all_values) < 2:
            return pd.DataFrame(columns=expected_cols)

        # 첫 행을 헤더로, 나머지를 데이터로 사용
        headers = all_values[0]
        data_rows = all_values[1:]

        # DataFrame 생성
        df = pd.DataFrame(data_rows, columns=headers)
        return df
    except Exception as e:
        print(f"시트 읽기 오류: {e}")
        return pd.DataFrame(columns=expected_cols)


def init_sheet_headers():
    """시트 헤더 초기화 (빈 시트인 경우)"""
    try:
        sheet = get_search_status_sheet()
        existing = sheet.get_all_values()
        if not existing:
            headers = ['sn', 'center_name', 'area_name', 'search_1', 'search_2', 'status', 'updated_at', 'updated_by']
            sheet.append_row(headers)
            return True
        return False
    except Exception as e:
        print(f"헤더 초기화 오류: {e}")
        return False


def upsert_search_status(sn: str, center_name: str, area_name: str,
                         search_1: bool, search_2: bool, status: str, updated_by: str) -> bool:
    """수색 상태 추가 또는 업데이트"""
    try:
        sheet = get_search_status_sheet()
        expected_headers = ['sn', 'center_name', 'area_name', 'search_1', 'search_2', 'status', 'updated_at', 'updated_by']

        # 헤더 확인 및 초기화
        all_values = sheet.get_all_values()
        if not all_values or all_values[0] != expected_headers:
            # 헤더가 없거나 잘못된 경우 초기화
            sheet.clear()
            sheet.append_row(expected_headers)
            all_values = [expected_headers]

        # sn으로 기존 행 찾기
        row_idx = None
        for idx, row in enumerate(all_values):
            if idx == 0:  # 헤더 스킵
                continue
            if row and row[0] == sn:
                row_idx = idx + 1  # gspread는 1-based index
                break

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row_data = [sn, center_name, area_name, str(search_1), str(search_2), status, now, updated_by]

        if row_idx:
            # 기존 행 업데이트
            sheet.update(f'A{row_idx}:H{row_idx}', [row_data])
        else:
            # 새 행 추가
            sheet.append_row(row_data)

        return True
    except Exception as e:
        print(f"수색 상태 업데이트 오류: {e}")
        return False


def update_single_field(sn: str, field: str, value: any, updated_by: str) -> bool:
    """단일 필드 업데이트"""
    try:
        sheet = get_search_status_sheet()
        all_values = sheet.get_all_values()

        if not all_values:
            return False

        # 헤더에서 컬럼 인덱스 찾기
        headers = all_values[0]
        if field not in headers:
            return False
        col_idx = headers.index(field) + 1  # 1-based
        updated_at_idx = headers.index('updated_at') + 1
        updated_by_idx = headers.index('updated_by') + 1

        # sn으로 행 찾기
        row_idx = None
        for idx, row in enumerate(all_values):
            if idx == 0:
                continue
            if row and row[0] == sn:
                row_idx = idx + 1
                break

        if not row_idx:
            return False

        # 값 업데이트
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sheet.update_cell(row_idx, col_idx, str(value))
        sheet.update_cell(row_idx, updated_at_idx, now)
        sheet.update_cell(row_idx, updated_by_idx, updated_by)

        return True
    except Exception as e:
        print(f"필드 업데이트 오류: {e}")
        return False


def batch_update_search_status(updates: list) -> bool:
    """여러 행 일괄 업데이트

    Args:
        updates: [{'sn': '...', 'field': '...', 'value': ...}, ...]
    """
    try:
        for update in updates:
            update_single_field(
                sn=update['sn'],
                field=update['field'],
                value=update['value'],
                updated_by=update.get('updated_by', 'system')
            )
        return True
    except Exception as e:
        print(f"일괄 업데이트 오류: {e}")
        return False
