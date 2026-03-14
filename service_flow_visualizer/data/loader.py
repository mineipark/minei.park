"""
데이터 로더
BigQuery에서 이벤트 데이터를 로드하고 전처리
"""
import os
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st

from data.queries import (
    get_riding_events_query,
    get_app_events_query,
    get_maintenance_events_query,
    get_riding_paths_query,
    get_staff_movements_query,
)

# 인증 설정
CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
]

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
client = bigquery.Client(credentials=credentials, project=os.environ.get("GCP_PROJECT_ID"))


def _create_client():
    """스레드별 BigQuery 클라이언트 생성"""
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


def run_query(query: str) -> pd.DataFrame:
    """BigQuery 쿼리 실행"""
    query_job = client.query(query)
    return query_job.result().to_dataframe()


def _run_query_in_thread(query: str, name: str) -> tuple:
    """스레드에서 쿼리 실행 (스레드별 클라이언트 사용)"""
    try:
        thread_client = _create_client()
        query_job = thread_client.query(query)
        df = query_job.result().to_dataframe()
        return name, df, None
    except Exception as e:
        return name, pd.DataFrame(), e


@st.cache_data(ttl=600, show_spinner=False)
def load_all_events(
    target_date: str,
    region: str = None,
    center: str = None,
    include_riding: bool = True,
    include_app: bool = True,
    include_ops: bool = True,
    hour_start: int = 0,
    hour_end: int = 24,
) -> pd.DataFrame:
    """
    모든 이벤트 로드 (병렬 쿼리 실행 + 캐싱)

    Args:
        target_date: 조회 날짜 (YYYY-MM-DD)
        region: 권역 필터 (None이면 전체)
        center: 센터 필터 (None이면 전체)
        include_riding: 라이딩 이벤트 포함 여부
        include_app: 앱 이벤트 포함 여부
        include_ops: 운영 이벤트 포함 여부
        hour_start: 시작 시간 (0-23)
        hour_end: 종료 시간 (1-24)

    Returns:
        시간순 정렬된 이벤트 DataFrame
    """
    if isinstance(target_date, datetime):
        target_date = target_date.strftime('%Y-%m-%d')

    # 쿼리 목록 준비
    queries = []
    if include_riding:
        queries.append(('riding', get_riding_events_query(target_date, region)))
    if include_app:
        queries.append(('app', get_app_events_query(target_date, region)))
    if include_ops:
        queries.append(('ops', get_maintenance_events_query(target_date, region, center)))

    if not queries:
        return pd.DataFrame()

    dfs = []

    # 병렬 쿼리 실행
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_run_query_in_thread, query, name): name
            for name, query in queries
        }

        for future in as_completed(futures):
            name, df, error = future.result()
            if error:
                print(f"{name} 이벤트 로드 실패: {error}")
            elif not df.empty:
                dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    # 합치기
    df = pd.concat(dfs, ignore_index=True)

    # 시간 컬럼 변환 (UTC → KST)
    df['event_time'] = pd.to_datetime(df['event_time'])

    # UTC를 KST로 변환 (시간대 필터용)
    if df['event_time'].dt.tz is not None:
        df['event_time_kst'] = df['event_time'].dt.tz_convert('Asia/Seoul')
    else:
        # tz-naive인 경우 UTC로 간주하고 KST로 변환
        df['event_time_kst'] = df['event_time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Seoul')

    # 시간대 필터 (KST 기준)
    df = df[
        (df['event_time_kst'].dt.hour >= hour_start) &
        (df['event_time_kst'].dt.hour < hour_end)
    ]

    # event_time을 KST로 대체 (표시용)
    df['event_time'] = df['event_time_kst']
    df = df.drop(columns=['event_time_kst'])

    # NULL 좌표 제거
    df = df.dropna(subset=['lat', 'lng'])

    # NULL event_type 제거
    df = df[df['event_type'].notna()]

    # 시간순 정렬
    df = df.sort_values('event_time').reset_index(drop=True)

    # 카테고리 추가
    df['category'] = df['event_type'].apply(categorize_event)

    return df


def categorize_event(event_type: str) -> str:
    """이벤트 카테고리 분류"""
    if event_type in ['riding_start', 'riding_end', 'app_converted', 'app_accessible', 'app_no_bike']:
        return 'user'
    return 'ops'


@st.cache_data(ttl=600, show_spinner=False)
def load_riding_paths(target_date: str, region: str = None) -> pd.DataFrame:
    """
    라이딩 경로 데이터 로드 (시작-종료 연결용) - 캐싱 적용

    Returns:
        라이딩별 시작/종료 좌표가 포함된 DataFrame
    """
    if isinstance(target_date, datetime):
        target_date = target_date.strftime('%Y-%m-%d')

    query = get_riding_paths_query(target_date, region)
    df = run_query(query)

    if df.empty:
        return df

    # 시간 변환
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])

    return df


@st.cache_data(ttl=600, show_spinner=False)
def load_staff_movements(target_date: str, center: str = None) -> pd.DataFrame:
    """
    관리자 이동 경로 로드 - 캐싱 적용

    Returns:
        관리자별 작업 순서가 포함된 DataFrame
    """
    if isinstance(target_date, datetime):
        target_date = target_date.strftime('%Y-%m-%d')

    query = get_staff_movements_query(target_date, center)

    try:
        df = run_query(query)
    except Exception as e:
        print(f"관리자 이동 경로 로드 실패: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    df['work_time'] = pd.to_datetime(df['work_time'])

    return df


def get_events_in_time_range(
    events_df: pd.DataFrame,
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    """특정 시간 범위의 이벤트 필터링"""
    mask = (events_df['event_time'] >= start_time) & (events_df['event_time'] <= end_time)
    return events_df[mask].copy()


def get_active_ridings_at_time(
    paths_df: pd.DataFrame,
    current_time: datetime,
) -> pd.DataFrame:
    """특정 시점에 진행 중인 라이딩"""
    mask = (paths_df['start_time'] <= current_time) & (paths_df['end_time'] >= current_time)
    return paths_df[mask].copy()


def interpolate_riding_position(row: pd.Series, current_time: datetime) -> tuple:
    """
    라이딩 경로 상의 현재 위치 보간

    Args:
        row: 라이딩 데이터 행
        current_time: 현재 시각

    Returns:
        (lat, lng) 보간된 위치
    """
    if current_time <= row['start_time']:
        return row['start_lat'], row['start_lng']
    if current_time >= row['end_time']:
        return row['end_lat'], row['end_lng']

    # 선형 보간
    total_duration = (row['end_time'] - row['start_time']).total_seconds()
    elapsed = (current_time - row['start_time']).total_seconds()
    progress = elapsed / total_duration if total_duration > 0 else 0

    lat = row['start_lat'] + (row['end_lat'] - row['start_lat']) * progress
    lng = row['start_lng'] + (row['end_lng'] - row['start_lng']) * progress

    return lat, lng


def get_summary_stats(events_df: pd.DataFrame) -> dict:
    """이벤트 요약 통계"""
    if events_df.empty:
        return {
            'total_events': 0,
            'riding_starts': 0,
            'riding_ends': 0,
            'app_converted': 0,
            'app_no_bike': 0,
            'battery_swaps': 0,
            'rebalance_deploys': 0,
            'broken_collects': 0,
        }

    return {
        'total_events': len(events_df),
        'riding_starts': len(events_df[events_df['event_type'] == 'riding_start']),
        'riding_ends': len(events_df[events_df['event_type'] == 'riding_end']),
        'app_converted': len(events_df[events_df['event_type'] == 'app_converted']),
        'app_no_bike': len(events_df[events_df['event_type'] == 'app_no_bike']),
        'battery_swaps': len(events_df[events_df['event_type'] == 'battery_swap']),
        'rebalance_deploys': len(events_df[events_df['event_type'] == 'rebalance_deploy']),
        'broken_collects': len(events_df[events_df['event_type'] == 'broken_collect']),
    }
