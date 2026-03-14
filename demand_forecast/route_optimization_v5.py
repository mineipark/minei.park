"""
동선 최적화 v5 - 종합 개선

개선사항:
1. 시간대별 동선 분리
   - 14~18시: 오후 수요 대응
   - 19~21시: 다음날 오전 수요 사전 준비
2. 과거 라이딩 데이터 기반 핫스팟
3. app_open 미충족 지역 우선
4. 거리 제약 제거 (순수 수요 기반)
5. 클러스터링 개선 (수요 밀집 구역 순회)
6. 작업 유형별 묶음
"""

import os
# Authentication: set GOOGLE_APPLICATION_CREDENTIALS env var
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    _default_cred = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'credentials', 'service-account.json')
    if os.path.exists(_default_cred):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _default_cred

import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime, timedelta
import folium
from math import radians, cos, sin, asin, sqrt
import warnings
warnings.filterwarnings('ignore')

TASK_VALUE = {
    'battery_swap': 1,
    'rebalance_collect': 3,
    'rebalance_deploy': 3,
    'broken_collect': 2,
    'field_fix': 2,
    'repair_deploy': 1,
}

IOS_AOS_MULTIPLIER = 2.0

# 시간대별 설정
TIME_SLOTS = {
    'afternoon': {
        'hours': (14, 18),
        'target': 'current_demand',  # 오후 수요 즉시 대응
        'description': '14~18시: 오후 수요 대응'
    },
    'evening': {
        'hours': (19, 21),
        'target': 'next_morning',  # 다음날 오전 수요 준비
        'description': '19~21시: 다음날 오전 준비'
    }
}


def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon, dlat = lon2 - lon1, lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * asin(sqrt(a)) * 6371


def calculate_route_distance(coords):
    """동선 총 거리 계산"""
    total = 0
    for i in range(1, len(coords)):
        total += haversine(coords[i-1][1], coords[i-1][0], coords[i][1], coords[i][0])
    return total


def load_staff_route(client, target_date: str, staff_name: str):
    query = f"""
    SELECT
        CASE
            WHEN m.type = 1 AND ms.type = 75 THEN 'battery_swap'
            WHEN m.type = 2 AND ms.type = 20 THEN 'field_fix'
            WHEN m.type = 2 AND ms.type = 30 THEN 'broken_collect'
            WHEN m.type = 2 AND ms.type = 80 THEN 'repair_deploy'
            WHEN m.type = 0 AND ms.type = 30 THEN 'rebalance_collect'
            WHEN m.type = 0 AND ms.type = 80 THEN 'rebalance_deploy'
        END as task_type,
        DATETIME(ms.created_at, 'Asia/Seoul') as task_time,
        EXTRACT(HOUR FROM DATETIME(ms.created_at, 'Asia/Seoul')) as task_hour,
        ST_Y(COALESCE(m.location_complete, m.location_call)) as lat,
        ST_X(COALESCE(m.location_complete, m.location_call)) as lng,
        m.vehicle_id as bike_id,
        s.name as staff_name,
        mc.name as center_name,
        COALESCE(m.h3_complete_area, m.h3_call_area) as region
    FROM `service.maintenance_log` ms
    JOIN `service.maintenance` m ON m.id = ms.maintenance_id
    LEFT JOIN `service.staff` s_lookup ON s_lookup.user_id = ms.manager_id
    JOIN `service.staff` s ON s.id = COALESCE(ms.staff_id, s_lookup.id)
    JOIN `service.service_center` mc ON mc.id = s.center_id
    WHERE DATE(DATETIME(ms.created_at, 'Asia/Seoul')) = '{target_date}'
        AND s.name = '{staff_name}'
        AND m.status != 1
        AND COALESCE(m.location_complete, m.location_call) IS NOT NULL
    ORDER BY ms.created_at
    """
    return client.query(query).to_dataframe()


def load_bike_48h_revenue(client, bike_ids: list, task_date: str):
    if not bike_ids:
        return pd.DataFrame()
    bike_ids_str = ','.join([str(int(b)) for b in bike_ids if pd.notna(b)])
    if not bike_ids_str:
        return pd.DataFrame()

    query = f"""
    SELECT
        r.bike_id,
        COUNT(*) as ride_count,
        SUM(r.fee) as total_revenue
    FROM `service.rides` r
    WHERE r.bike_id IN ({bike_ids_str})
        AND r.start_time >= DATETIME('{task_date}')
        AND r.start_time < DATETIME_ADD(DATETIME('{task_date}'), INTERVAL 48 HOUR)
        AND r.fee > 0
    GROUP BY r.bike_id
    """
    return client.query(query).to_dataframe()


def load_app_open_demand(client, target_date: str, region: str = None):
    """앱오픈 수요 - 시간대별로 구분"""
    region_filter = f"AND h3_area_name LIKE '%{region}%'" if region else ""
    query = f"""
    SELECT
        event_time,
        EXTRACT(HOUR FROM event_time) as event_hour,
        ST_Y(location) as lat,
        ST_X(location) as lng,
        h3_area_name as region,
        is_converted,
        is_accessible
    FROM `service.app_accessibility`
    WHERE DATE(event_time) = '{target_date}'
        AND location IS NOT NULL
        {region_filter}
    """
    return client.query(query).to_dataframe()


def load_historical_riding_hotspots(client, region: str, days_back: int = 30, center_lat: float = None, center_lng: float = None):
    """과거 30일 라이딩 핫스팟 - 중심점 기준 반경 검색"""
    # region에서 좌표 범위 추출 (권역 중심 ±0.1도 = 약 10km)
    if center_lat is None or center_lng is None:
        # 기본값: default region
        center_lat, center_lng = 37.72, 126.76

    lat_min, lat_max = center_lat - 0.1, center_lat + 0.1
    lng_min, lng_max = center_lng - 0.1, center_lng + 0.1

    query = f"""
    WITH riding_locations AS (
        SELECT
            ST_Y(r.start_location) as lat,
            ST_X(r.start_location) as lng,
            EXTRACT(HOUR FROM r.start_time) as ride_hour,
            r.fee
        FROM `service.rides` r
        WHERE r.start_time >= DATETIME_SUB(CURRENT_DATETIME('Asia/Seoul'), INTERVAL {days_back} DAY)
            AND r.fee > 0
            AND r.start_location IS NOT NULL
            AND ST_Y(r.start_location) BETWEEN {lat_min} AND {lat_max}
            AND ST_X(r.start_location) BETWEEN {lng_min} AND {lng_max}
    )
    SELECT
        ROUND(lat, 3) as lat_grid,
        ROUND(lng, 3) as lng_grid,
        ride_hour,
        COUNT(*) as ride_count,
        SUM(fee) as total_revenue,
        AVG(fee) as avg_revenue
    FROM riding_locations
    GROUP BY lat_grid, lng_grid, ride_hour
    HAVING COUNT(*) >= 5
    ORDER BY ride_count DESC
    LIMIT 500
    """
    return client.query(query).to_dataframe()


def load_unmet_demand_locations(client, target_date: str, region: str):
    """미충족 수요 지역 - 앱열었지만 기기없음/미전환"""
    region_filter = f"AND h3_area_name LIKE '%{region}%'" if region else ""
    query = f"""
    WITH demand AS (
        SELECT
            ROUND(ST_Y(location), 3) as lat_grid,
            ROUND(ST_X(location), 3) as lng_grid,
            EXTRACT(HOUR FROM event_time) as event_hour,
            is_accessible,
            is_converted
        FROM `service.app_accessibility`
        WHERE DATE(event_time) = '{target_date}'
            AND location IS NOT NULL
            {region_filter}
    )
    SELECT
        lat_grid,
        lng_grid,
        event_hour,
        COUNT(*) as total_opens,
        SUM(CASE WHEN NOT is_accessible THEN 1 ELSE 0 END) as no_bike_count,
        SUM(CASE WHEN is_accessible AND NOT is_converted THEN 1 ELSE 0 END) as no_convert_count,
        SUM(CASE WHEN is_converted THEN 1 ELSE 0 END) as converted_count
    FROM demand
    GROUP BY lat_grid, lng_grid, event_hour
    HAVING no_bike_count > 0 OR no_convert_count > 0
    ORDER BY no_bike_count DESC, no_convert_count DESC
    """
    return client.query(query).to_dataframe()


def load_broken_bikes(client, target_date: str, region: str):
    query = f"""
    SELECT DISTINCT
        m.vehicle_id as bike_id,
        ST_Y(COALESCE(m.location_complete, m.location_call)) as lat,
        ST_X(COALESCE(m.location_complete, m.location_call)) as lng
    FROM `service.maintenance_log` ms
    JOIN `service.maintenance` m ON m.id = ms.maintenance_id
    WHERE DATE(DATETIME(ms.created_at, 'Asia/Seoul')) = '{target_date}'
        AND m.type = 2 AND ms.type = 30
        AND COALESCE(m.h3_complete_area, m.h3_call_area) LIKE '%{region}%'
        AND COALESCE(m.location_complete, m.location_call) IS NOT NULL
    """
    return client.query(query).to_dataframe()


def load_bike_snapshot_by_status(client, target_date: str, region: str, hours: list):
    """
    시간대별 기기 상태 스냅샷 조회

    bike_status 분류:
    - BAV: 배터리 충분 + 이용 가능 (가용)
    - BNB: 배터리 충분 + 재배치 필요 (가용)
    - LAV: 배터리 부족 (배터리 교체 필요)
    - LNB: 배터리 부족 + 재배치 필요 (배터리 교체 필요)
    - BNP/LNP: 수리 필요 (고장)
    - BP/LP: 수리중
    """
    hours_str = ','.join([str(h) for h in hours])
    query = f"""
    SELECT
        bike_id,
        bike_status,
        leftover as battery_level,
        threshold as battery_threshold,
        is_usable,
        ROUND(CAST(JSON_VALUE(location, '$.coordinates[1]') AS FLOAT64), 3) as lat,
        ROUND(CAST(JSON_VALUE(location, '$.coordinates[0]') AS FLOAT64), 3) as lng,
        hour,
        -- 상태 분류
        CASE
            WHEN bike_status IN ('LAV', 'LNB') THEN 'low_battery'  -- 배터리 교체 필요
            WHEN bike_status IN ('BNP', 'LNP') THEN 'broken'       -- 수리 필요
            WHEN bike_status IN ('BNB', 'LNB') THEN 'rebalance'    -- 재배치 필요
            WHEN bike_status IN ('BAV') THEN 'available'           -- 가용
            ELSE 'other'
        END as status_category
    FROM `service.bike_snapshot`
    WHERE DATE(time) = '{target_date}'
        AND h3_area_name LIKE '%{region}%'
        AND hour IN ({hours_str})
    """
    return client.query(query).to_dataframe()


def analyze_unmet_demand_with_snapshot(app_df, snapshot_df, time_slot='afternoon'):
    """
    기기없음 지역 분석 - 스냅샷 기반

    기기없음(is_accessible=False) 원인 분류:
    1. 기기 자체가 없음 → 재배치 필요
    2. 기기는 있지만 배터리 부족(LAV/LNB) → 배터리 교체 필요
    3. 기기는 있지만 고장(BNP/LNP) → 수리 필요
    """
    # 시간대 설정
    if time_slot == 'afternoon':
        target_hours = range(14, 19)
    else:
        target_hours = range(8, 11)

    # 기기없음 이벤트 집계
    no_bike_df = app_df[~app_df['is_accessible']].copy()
    if len(no_bike_df) == 0:
        return pd.DataFrame()

    no_bike_df = no_bike_df[no_bike_df['event_hour'].isin(target_hours)]
    no_bike_df['lat_grid'] = (no_bike_df['lat'] / 0.003).round() * 0.003
    no_bike_df['lng_grid'] = (no_bike_df['lng'] / 0.003).round() * 0.003

    # 그리드별 기기없음 이벤트 수
    no_bike_agg = no_bike_df.groupby(['lat_grid', 'lng_grid']).size().reset_index(name='no_bike_events')

    if len(snapshot_df) == 0:
        no_bike_agg['low_battery_bikes'] = 0
        no_bike_agg['broken_bikes'] = 0
        no_bike_agg['no_bikes'] = no_bike_agg['no_bike_events']
        no_bike_agg['need_action'] = 'rebalance'
        return no_bike_agg

    # 스냅샷에서 각 그리드의 기기 상태 확인
    snapshot_df['lat_grid'] = (snapshot_df['lat'] / 0.003).round() * 0.003
    snapshot_df['lng_grid'] = (snapshot_df['lng'] / 0.003).round() * 0.003

    # 그리드별 상태 집계
    status_agg = snapshot_df.groupby(['lat_grid', 'lng_grid', 'status_category']).size().unstack(fill_value=0).reset_index()

    # 병합
    result = no_bike_agg.merge(status_agg, on=['lat_grid', 'lng_grid'], how='left')

    # 컬럼 정리
    for col in ['low_battery', 'broken', 'rebalance', 'available', 'other']:
        if col not in result.columns:
            result[col] = 0

    result['low_battery_bikes'] = result['low_battery'].fillna(0).astype(int)
    result['broken_bikes'] = result['broken'].fillna(0).astype(int)
    result['available_bikes'] = result['available'].fillna(0).astype(int)

    # 필요 작업 판단
    def determine_action(row):
        if row['low_battery_bikes'] > 0:
            return 'battery_swap'  # 배터리 교체 우선
        elif row['broken_bikes'] > 0:
            return 'broken_collect'  # 고장 수거
        else:
            return 'rebalance_deploy'  # 재배치

    result['need_action'] = result.apply(determine_action, axis=1)

    return result[['lat_grid', 'lng_grid', 'no_bike_events', 'low_battery_bikes',
                   'broken_bikes', 'available_bikes', 'need_action']]


def analyze_actual_route(route_df, revenue_df):
    results = []
    for _, task in route_df.iterrows():
        bike_id = task['bike_id']
        bike_rev = revenue_df[revenue_df['bike_id'] == bike_id] if len(revenue_df) > 0 and pd.notna(bike_id) else pd.DataFrame()

        revenue_48h = bike_rev['total_revenue'].sum() if len(bike_rev) > 0 else 0
        rides_48h = bike_rev['ride_count'].sum() if len(bike_rev) > 0 else 0
        is_inefficient = (revenue_48h == 0)

        results.append({
            **task.to_dict(),
            'revenue_48h': revenue_48h,
            'rides_48h': rides_48h,
            'is_inefficient': is_inefficient,
            'task_value': TASK_VALUE.get(task['task_type'], 1),
        })
    return pd.DataFrame(results)


def build_comprehensive_demand_index(app_df, historical_df, unmet_df, snapshot_analysis_df=None, time_slot='afternoon', grid_size=0.003):
    """
    종합 수요 인덱스 구축
    - 앱오픈 수요
    - 과거 라이딩 핫스팟
    - 미충족 수요 지역
    - 스냅샷 기반 기기 상태 (배터리 부족/고장/없음)
    """
    demand_index = {}

    # 시간대 설정
    if time_slot == 'afternoon':
        target_hours = range(14, 19)  # 14~18시 수요
    else:  # evening -> 다음날 오전 준비
        target_hours = range(8, 11)  # 08~10시 수요 (다음날)

    # 1. 앱오픈 수요 (현재일)
    if len(app_df) > 0:
        df = app_df.copy()
        df['is_accessible'] = df['is_accessible'].fillna(False)
        df['is_converted'] = df['is_converted'].fillna(False)

        # 시간대 필터링
        if time_slot == 'afternoon':
            df = df[df['event_hour'].isin(target_hours)]

        df['weight'] = df.apply(
            lambda x: 3 if not x['is_accessible'] else (2 if not x['is_converted'] else 0.5),
            axis=1
        ) * IOS_AOS_MULTIPLIER

        df['grid_lat'] = (df['lat'] / grid_size).round() * grid_size
        df['grid_lng'] = (df['lng'] / grid_size).round() * grid_size

        for (glat, glng), group in df.groupby(['grid_lat', 'grid_lng']):
            key = (round(glat, 4), round(glng, 4))
            if key not in demand_index:
                demand_index[key] = {
                    'app_open_score': 0, 'historical_score': 0, 'unmet_score': 0,
                    'lat': group['lat'].mean(), 'lng': group['lng'].mean(),
                    'no_bike': 0, 'no_convert': 0,
                    'low_battery_bikes': 0, 'broken_bikes': 0, 'need_action': None
                }
            demand_index[key]['app_open_score'] = group['weight'].sum()
            demand_index[key]['no_bike'] = (~group['is_accessible']).sum()
            demand_index[key]['no_convert'] = ((group['is_accessible']) & (~group['is_converted'])).sum()

    # 2. 과거 라이딩 핫스팟
    if len(historical_df) > 0:
        hist = historical_df.copy()
        hist = hist[hist['ride_hour'].isin(target_hours)]

        for _, row in hist.iterrows():
            key = (round(row['lat_grid'], 4), round(row['lng_grid'], 4))
            if key not in demand_index:
                demand_index[key] = {
                    'app_open_score': 0, 'historical_score': 0, 'unmet_score': 0,
                    'lat': row['lat_grid'], 'lng': row['lng_grid'],
                    'no_bike': 0, 'no_convert': 0,
                    'low_battery_bikes': 0, 'broken_bikes': 0, 'need_action': None
                }
            # 과거 라이딩 수 기반 점수
            demand_index[key]['historical_score'] = row['ride_count'] * IOS_AOS_MULTIPLIER

    # 3. 미충족 수요
    if len(unmet_df) > 0:
        unmet = unmet_df.copy()
        unmet = unmet[unmet['event_hour'].isin(target_hours)]

        for _, row in unmet.iterrows():
            key = (round(row['lat_grid'], 4), round(row['lng_grid'], 4))
            if key not in demand_index:
                demand_index[key] = {
                    'app_open_score': 0, 'historical_score': 0, 'unmet_score': 0,
                    'lat': row['lat_grid'], 'lng': row['lng_grid'],
                    'no_bike': 0, 'no_convert': 0,
                    'low_battery_bikes': 0, 'broken_bikes': 0, 'need_action': None
                }
            # 미충족 수요 (기기없음 x3, 미전환 x2)
            demand_index[key]['unmet_score'] = (row['no_bike_count'] * 3 + row['no_convert_count'] * 2) * IOS_AOS_MULTIPLIER
            demand_index[key]['no_bike'] = max(demand_index[key]['no_bike'], row['no_bike_count'])
            demand_index[key]['no_convert'] = max(demand_index[key]['no_convert'], row['no_convert_count'])

    # 4. 스냅샷 기반 기기 상태 분석 (배터리 부족/고장 위치)
    if snapshot_analysis_df is not None and len(snapshot_analysis_df) > 0:
        for _, row in snapshot_analysis_df.iterrows():
            key = (round(row['lat_grid'], 4), round(row['lng_grid'], 4))
            if key not in demand_index:
                demand_index[key] = {
                    'app_open_score': 0, 'historical_score': 0, 'unmet_score': 0,
                    'lat': row['lat_grid'], 'lng': row['lng_grid'],
                    'no_bike': 0, 'no_convert': 0,
                    'low_battery_bikes': 0, 'broken_bikes': 0, 'need_action': None
                }
            demand_index[key]['low_battery_bikes'] = row.get('low_battery_bikes', 0)
            demand_index[key]['broken_bikes'] = row.get('broken_bikes', 0)
            demand_index[key]['need_action'] = row.get('need_action', None)

    # 5. 예측 수요 (district_hour_model 기반)
    try:
        from district_hour_model import DistrictHourPredictor, TIME_SLOTS as DH_TIME_SLOTS
        from datetime import date as _date

        target_date_str = _date.today().strftime('%Y-%m-%d')
        predictor = DistrictHourPredictor(verbose=False)

        # 시간대에 맞는 예측 가져오기
        if time_slot == 'afternoon':
            dh_slot = 'afternoon'
        else:
            dh_slot = 'morning'  # evening → 다음날 morning 준비

        slot_pred = predictor.predict_time_slot(target_date_str, dh_slot)

        if len(slot_pred) > 0:
            for _, row in slot_pred.iterrows():
                lat, lng = row.get('lat'), row.get('lng')
                if pd.isna(lat) or pd.isna(lng):
                    continue
                key = (round(lat / grid_size, 0) * grid_size,
                       round(lng / grid_size, 0) * grid_size)
                key = (round(key[0], 4), round(key[1], 4))

                if key not in demand_index:
                    demand_index[key] = {
                        'app_open_score': 0, 'historical_score': 0, 'unmet_score': 0,
                        'lat': lat, 'lng': lng,
                        'no_bike': 0, 'no_convert': 0,
                        'low_battery_bikes': 0, 'broken_bikes': 0, 'need_action': None,
                        'predicted_score': 0,
                    }
                demand_index[key]['predicted_score'] = row['predicted_rides'] * IOS_AOS_MULTIPLIER
    except Exception:
        # district_hour_model 미설치/오류 시 무시 (기존 로직 유지)
        pass

    # 종합 점수 계산
    for key in demand_index:
        d = demand_index[key]
        # 가중치: 앱오픈 1.0, 과거라이딩 1.5 (검증된 수요), 미충족 2.0 (긴급), 예측 2.0
        base_score = d['app_open_score'] * 1.0 + d['historical_score'] * 1.5 + d['unmet_score'] * 2.0

        # 예측 수요 점수 (district_hour_model)
        predicted_bonus = d.get('predicted_score', 0) * 2.0

        # 배터리 부족 기기가 있으면 추가 점수 (배터리 교체 우선)
        battery_bonus = d.get('low_battery_bikes', 0) * 5 * IOS_AOS_MULTIPLIER

        d['total_score'] = base_score + predicted_bonus + battery_bonus

    return demand_index


def cluster_high_demand_areas(demand_index, n_clusters=8):
    """수요 밀집 구역 클러스터링 (그리드 기반)"""
    if len(demand_index) < n_clusters:
        return None

    # 상위 수요 지역만
    sorted_demand = sorted(demand_index.items(), key=lambda x: x[1]['total_score'], reverse=True)
    top_n = min(100, len(sorted_demand))
    top_locations = sorted_demand[:top_n]

    # 간단한 그리드 기반 클러스터링 (sklearn 없이)
    # 상위 지역을 n_clusters개의 그룹으로 나눔
    cluster_info = {}
    locations_per_cluster = max(1, top_n // n_clusters)

    for i in range(n_clusters):
        start_idx = i * locations_per_cluster
        end_idx = min(start_idx + locations_per_cluster, top_n)

        if start_idx >= top_n:
            break

        cluster_locs = top_locations[start_idx:end_idx]

        if len(cluster_locs) == 0:
            continue

        # 가중 중심점 계산
        total_score = sum(loc[1]['total_score'] for loc in cluster_locs)
        if total_score > 0:
            center_lat = sum(loc[1]['lat'] * loc[1]['total_score'] for loc in cluster_locs) / total_score
            center_lng = sum(loc[1]['lng'] * loc[1]['total_score'] for loc in cluster_locs) / total_score
        else:
            center_lat = np.mean([loc[1]['lat'] for loc in cluster_locs])
            center_lng = np.mean([loc[1]['lng'] for loc in cluster_locs])

        cluster_info[i] = {
            'center_lat': center_lat,
            'center_lng': center_lng,
            'total_score': total_score,
            'count': len(cluster_locs),
            'avg_score': total_score / len(cluster_locs) if len(cluster_locs) > 0 else 0
        }

    return cluster_info


def optimize_cluster_order(clusters, start_lat, start_lng):
    """클러스터 방문 순서 최적화 (Nearest Neighbor)"""
    if not clusters:
        return []

    remaining = list(clusters.keys())
    order = []
    current_lat, current_lng = start_lat, start_lng

    while remaining:
        # 가장 가까운 클러스터 (수요 점수 가중)
        best_cluster = None
        best_score = -1

        for c in remaining:
            dist = haversine(current_lng, current_lat,
                           clusters[c]['center_lng'], clusters[c]['center_lat'])
            # 거리가 가까울수록 + 수요가 높을수록 점수 높음
            # score = 수요점수 / (거리 + 0.5)
            score = clusters[c]['total_score'] / (dist + 0.5)

            if score > best_score:
                best_score = score
                best_cluster = c

        if best_cluster is not None:
            order.append(best_cluster)
            current_lat = clusters[best_cluster]['center_lat']
            current_lng = clusters[best_cluster]['center_lng']
            remaining.remove(best_cluster)

    return order


def group_tasks_by_type(actual_df):
    """작업 유형별 그룹화"""
    task_groups = {
        'battery': [],      # 배터리 교체
        'deploy': [],       # 재배치 완료
        'collect': [],      # 재배치 수거
        'broken': [],       # 고장 수거
        'fixed': [],        # 현장조치, 수리배치 (위치 고정)
    }

    for _, task in actual_df.iterrows():
        tt = task['task_type']
        if tt == 'battery_swap':
            task_groups['battery'].append(task)
        elif tt == 'rebalance_deploy':
            task_groups['deploy'].append(task)
        elif tt == 'rebalance_collect':
            task_groups['collect'].append(task)
        elif tt == 'broken_collect':
            task_groups['broken'].append(task)
        else:  # field_fix, repair_deploy
            task_groups['fixed'].append(task)

    return task_groups


def find_best_location_in_cluster(cluster_center_lat, cluster_center_lng, demand_index,
                                   task_type, search_radius_km=0.5, grid_size=0.003):
    """
    클러스터 내에서 최적 위치 찾기

    작업 유형별 우선순위:
    - battery_swap: 배터리 부족 기기(LAV/LNB)가 있는 곳 우선
    - rebalance_deploy: 기기 자체가 없는 곳 우선
    """
    best_score = 0
    best_loc = (cluster_center_lat, cluster_center_lng)
    best_info = {'no_bike': 0, 'no_convert': 0, 'low_battery_bikes': 0, 'need_action': None}

    # 검색 반경 (그리드 단위)
    search_grids = int(search_radius_km / (grid_size * 111))

    for di in range(-search_grids, search_grids + 1):
        for dj in range(-search_grids, search_grids + 1):
            key = (
                round(cluster_center_lat + di * grid_size, 4),
                round(cluster_center_lng + dj * grid_size, 4)
            )

            if key not in demand_index:
                continue

            candidate = demand_index[key]
            score = candidate['total_score']

            # 작업 유형에 따른 가중치 조정 (스냅샷 기반)
            if task_type == 'battery_swap':
                # 배터리 부족 기기가 있는 곳 우선 (스냅샷 기반)
                low_battery = candidate.get('low_battery_bikes', 0)
                if low_battery > 0:
                    score += low_battery * 20  # 배터리 부족 기기에 높은 가중치
                # 미전환도 배터리 부족일 가능성
                score += candidate.get('no_convert', 0) * 3

            elif task_type == 'rebalance_deploy':
                # 기기 자체가 없는 곳 우선 (배터리 부족 제외)
                low_battery = candidate.get('low_battery_bikes', 0)
                no_bike = candidate.get('no_bike', 0)
                if low_battery == 0 and no_bike > 0:
                    # 진짜 기기가 없는 곳에만 재배치
                    score += no_bike * 10
                elif low_battery > 0:
                    # 배터리 부족 기기가 있으면 재배치 불필요
                    score -= 10

            if score > best_score:
                best_score = score
                best_loc = (candidate['lat'], candidate['lng'])
                best_info = {
                    'no_bike': candidate.get('no_bike', 0),
                    'no_convert': candidate.get('no_convert', 0),
                    'low_battery_bikes': candidate.get('low_battery_bikes', 0),
                    'need_action': candidate.get('need_action', None)
                }

    return best_loc[0], best_loc[1], best_score, best_info


def calculate_optimal_route_v5(actual_df, demand_index, clusters, cluster_order, broken_df, time_slot):
    """
    v5 최적 동선 계산
    - 클러스터 순서대로 방문
    - 각 클러스터 내에서 작업 유형별 최적 위치
    - 거리 제약 없음
    """
    task_groups = group_tasks_by_type(actual_df)

    optimal_tasks = []
    current_lat = actual_df.iloc[0]['lat'] if len(actual_df) > 0 else 37.5
    current_lng = actual_df.iloc[0]['lng'] if len(actual_df) > 0 else 127.0

    # 위치 고정 작업 먼저 처리
    for task in task_groups['fixed']:
        optimal_tasks.append({
            'task_type': task['task_type'],
            'task_time': task['task_time'],
            'task_hour': task.get('task_hour', 12),
            'actual_lat': task['lat'],
            'actual_lng': task['lng'],
            'optimal_lat': task['lat'],
            'optimal_lng': task['lng'],
            'demand_score': 0,
            'no_bike': 0,
            'no_convert': 0,
            'reason': '위치고정',
            'cluster': -1,
            'time_slot': time_slot,
        })

    # 클러스터 순서대로 작업 배치
    battery_idx = 0
    deploy_idx = 0
    collect_idx = 0
    broken_idx = 0

    for cluster_id in cluster_order:
        if cluster_id not in clusters:
            continue

        cluster = clusters[cluster_id]

        # 배터리 교체 (배터리 부족 기기 우선, 스냅샷 기반)
        while battery_idx < len(task_groups['battery']):
            task = task_groups['battery'][battery_idx]
            opt_lat, opt_lng, score, info = find_best_location_in_cluster(
                cluster['center_lat'], cluster['center_lng'],
                demand_index, 'battery_swap'
            )

            # 이유 결정 (배터리 부족 기기 > 미전환 > 수요 핫스팟)
            low_battery = info.get('low_battery_bikes', 0)
            if low_battery > 0:
                reason = f"클러스터{cluster_id}: 배터리부족 {low_battery}대 (LAV/LNB)"
            elif info['no_convert'] > 0:
                reason = f"클러스터{cluster_id}: 미전환 {info['no_convert']}건"
            else:
                reason = f"클러스터{cluster_id}: 수요 핫스팟"

            optimal_tasks.append({
                'task_type': 'battery_swap',
                'task_time': task['task_time'],
                'task_hour': task.get('task_hour', 12),
                'actual_lat': task['lat'],
                'actual_lng': task['lng'],
                'optimal_lat': opt_lat,
                'optimal_lng': opt_lng,
                'demand_score': score,
                'no_bike': info['no_bike'],
                'no_convert': info['no_convert'],
                'low_battery_bikes': low_battery,
                'reason': reason,
                'cluster': cluster_id,
                'time_slot': time_slot,
            })
            battery_idx += 1

            # 클러스터당 최대 3개 배터리
            if battery_idx % 3 == 0:
                break

        # 재배치 완료 (기기 자체가 없는 곳 - 배터리 부족 제외)
        while deploy_idx < len(task_groups['deploy']):
            task = task_groups['deploy'][deploy_idx]
            opt_lat, opt_lng, score, info = find_best_location_in_cluster(
                cluster['center_lat'], cluster['center_lng'],
                demand_index, 'rebalance_deploy'
            )

            # 이유 결정 (배터리 부족 기기가 있으면 재배치 불필요)
            low_battery = info.get('low_battery_bikes', 0)
            if low_battery > 0:
                reason = f"클러스터{cluster_id}: 배터리교체 필요 (재배치X)"
            elif info['no_bike'] > 0:
                reason = f"클러스터{cluster_id}: 기기없음 {info['no_bike']}건 (재배치O)"
            else:
                reason = f"클러스터{cluster_id}: 수요 핫스팟"

            optimal_tasks.append({
                'task_type': 'rebalance_deploy',
                'task_time': task['task_time'],
                'task_hour': task.get('task_hour', 12),
                'actual_lat': task['lat'],
                'actual_lng': task['lng'],
                'optimal_lat': opt_lat,
                'optimal_lng': opt_lng,
                'demand_score': score,
                'no_bike': info['no_bike'],
                'no_convert': info['no_convert'],
                'low_battery_bikes': low_battery,
                'reason': reason,
                'cluster': cluster_id,
                'time_slot': time_slot,
            })
            deploy_idx += 1

            # 클러스터당 최대 2개 재배치
            if deploy_idx % 2 == 0:
                break

        # 재배치 수거 (클러스터 근처)
        while collect_idx < len(task_groups['collect']):
            task = task_groups['collect'][collect_idx]

            optimal_tasks.append({
                'task_type': 'rebalance_collect',
                'task_time': task['task_time'],
                'task_hour': task.get('task_hour', 12),
                'actual_lat': task['lat'],
                'actual_lng': task['lng'],
                'optimal_lat': cluster['center_lat'],
                'optimal_lng': cluster['center_lng'],
                'demand_score': 0,
                'no_bike': 0,
                'no_convert': 0,
                'reason': f"클러스터{cluster_id}: 수거",
                'cluster': cluster_id,
                'time_slot': time_slot,
            })
            collect_idx += 1

            if collect_idx % 2 == 0:
                break

        # 고장 수거 (클러스터 근처 고장기기)
        while broken_idx < len(task_groups['broken']) and len(broken_df) > 0:
            task = task_groups['broken'][broken_idx]

            # 클러스터 근처 가장 가까운 고장기기
            broken_df_copy = broken_df.copy()
            broken_df_copy['dist'] = broken_df_copy.apply(
                lambda b: haversine(b['lng'], b['lat'], cluster['center_lng'], cluster['center_lat']), axis=1
            )
            nearest = broken_df_copy.nsmallest(1, 'dist').iloc[0]

            optimal_tasks.append({
                'task_type': 'broken_collect',
                'task_time': task['task_time'],
                'task_hour': task.get('task_hour', 12),
                'actual_lat': task['lat'],
                'actual_lng': task['lng'],
                'optimal_lat': nearest['lat'],
                'optimal_lng': nearest['lng'],
                'demand_score': 0,
                'no_bike': 0,
                'no_convert': 0,
                'reason': f"클러스터{cluster_id}: 고장수거",
                'cluster': cluster_id,
                'time_slot': time_slot,
            })
            broken_idx += 1
            break

    # 남은 작업 처리
    remaining_tasks = []
    remaining_tasks.extend(task_groups['battery'][battery_idx:])
    remaining_tasks.extend(task_groups['deploy'][deploy_idx:])
    remaining_tasks.extend(task_groups['collect'][collect_idx:])
    remaining_tasks.extend(task_groups['broken'][broken_idx:])

    for task in remaining_tasks:
        task_type = task['task_type']

        # 수요 인덱스에서 가장 높은 점수 위치
        if demand_index:
            best_key = max(demand_index.keys(), key=lambda k: demand_index[k]['total_score'])
            best = demand_index[best_key]
            opt_lat, opt_lng = best['lat'], best['lng']
            score = best['total_score']
            no_bike, no_convert = best['no_bike'], best['no_convert']
        else:
            opt_lat, opt_lng = task['lat'], task['lng']
            score, no_bike, no_convert = 0, 0, 0

        optimal_tasks.append({
            'task_type': task_type,
            'task_time': task['task_time'],
            'task_hour': task.get('task_hour', 12),
            'actual_lat': task['lat'],
            'actual_lng': task['lng'],
            'optimal_lat': opt_lat,
            'optimal_lng': opt_lng,
            'demand_score': score,
            'no_bike': no_bike,
            'no_convert': no_convert,
            'reason': '잔여 작업',
            'cluster': -1,
            'time_slot': time_slot,
        })

    return pd.DataFrame(optimal_tasks)


def estimate_optimal_revenue_v5(actual_df, optimal_df, demand_index):
    """v5 매출 추정 - 모든 작업을 최적 위치로 이동"""
    actual_revenue = actual_df['revenue_48h'].sum()
    actual_rides = actual_df['rides_48h'].sum()

    avg_revenue_per_ride = actual_revenue / actual_rides if actual_rides > 0 else 2000

    # 비효율 작업 손실 (48h 매출 0원인 작업)
    inefficient_df = actual_df[actual_df['revenue_48h'] == 0]
    lost_count = len(inefficient_df)

    # 저매출 작업도 대체 대상
    nonzero_revenues = actual_df[actual_df['revenue_48h'] > 0]['revenue_48h']
    low_threshold = nonzero_revenues.mean() * 0.5 if len(nonzero_revenues) > 0 else 2000
    low_revenue_df = actual_df[(actual_df['revenue_48h'] > 0) & (actual_df['revenue_48h'] < low_threshold)]

    lost_revenue = low_revenue_df['revenue_48h'].sum()
    replaced_count = lost_count + len(low_revenue_df)

    # 최적 동선 수요 커버리지
    total_no_bike = optimal_df['no_bike'].sum()
    total_no_convert = optimal_df['no_convert'].sum()
    total_demand_score = optimal_df['demand_score'].sum()

    # 추가 라이딩 추정
    # 기기없음 → 30% 전환
    # 미전환 → 20% 추가 전환
    # 수요점수 → 0.1회/점
    additional_from_no_bike = total_no_bike * 0.3 * IOS_AOS_MULTIPLIER
    additional_from_no_convert = total_no_convert * 0.2 * IOS_AOS_MULTIPLIER
    additional_from_demand = total_demand_score * 0.05  # 수요점수 기반

    additional_rides = additional_from_no_bike + additional_from_no_convert + additional_from_demand
    additional_revenue = additional_rides * avg_revenue_per_ride

    estimated_revenue = actual_revenue - lost_revenue + additional_revenue

    return {
        'actual_revenue': actual_revenue,
        'actual_rides': actual_rides,
        'estimated_revenue': estimated_revenue,
        'additional_rides': additional_rides,
        'additional_revenue': additional_revenue,
        'lost_revenue': lost_revenue,
        'replaced_count': replaced_count,
        'low_revenue_threshold': low_threshold,
        'avg_revenue_per_ride': avg_revenue_per_ride,
        'total_no_bike': total_no_bike,
        'total_no_convert': total_no_convert,
        'total_demand_score': total_demand_score,
    }


def create_comparison_map_v5(actual_df, optimal_df, app_df, clusters, output_path, revenue_estimate, time_slot_info):
    """v5 시각화 - 클러스터 표시 포함"""
    center_lat = actual_df['lat'].mean()
    center_lng = actual_df['lng'].mean()

    m = folium.Map(location=[center_lat, center_lng], zoom_start=13, tiles='cartodbpositron')

    # 클러스터 영역 표시
    if clusters:
        cluster_colors = ['#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4', '#ffeaa7', '#dfe6e9', '#a29bfe', '#fd79a8']
        for cluster_id, cluster in clusters.items():
            color = cluster_colors[cluster_id % len(cluster_colors)]
            folium.CircleMarker(
                [cluster['center_lat'], cluster['center_lng']],
                radius=20,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.2,
                opacity=0.5,
                popup=f"클러스터 {cluster_id}<br>수요점수: {cluster['total_score']:.0f}<br>지점수: {cluster['count']}"
            ).add_to(m)

    # 앱오픈 수요 (배경)
    sample_app = app_df.sample(min(500, len(app_df))) if len(app_df) > 0 else app_df
    for _, app in sample_app.iterrows():
        is_acc = bool(app['is_accessible']) if pd.notna(app['is_accessible']) else False
        is_conv = bool(app['is_converted']) if pd.notna(app['is_converted']) else False
        color = '#ff6666' if not is_acc else ('#66ff66' if is_conv else '#6666ff')
        folium.CircleMarker([app['lat'], app['lng']], radius=2, color=color, fill=True, fill_opacity=0.15, opacity=0.2).add_to(m)

    # 실제 동선 (빨간색)
    actual_coords = [[t['lat'], t['lng']] for _, t in actual_df.iterrows()]
    for idx, (_, task) in enumerate(actual_df.iterrows()):
        folium.CircleMarker(
            [task['lat'], task['lng']],
            radius=7,
            color='#ff4444',
            fill=True,
            fill_color='#ff4444',
            fill_opacity=0.8,
            popup=f"<b style='color:#ff4444'>실제 #{idx+1}</b><br>{task['task_type']}<br>48h: {task['revenue_48h']:,.0f}원"
        ).add_to(m)

    if len(actual_coords) > 1:
        folium.PolyLine(actual_coords, color='#ff4444', weight=3, opacity=0.8).add_to(m)

    # 최적 동선 (초록색)
    optimal_coords = [[t['optimal_lat'], t['optimal_lng']] for _, t in optimal_df.iterrows()]
    for idx, (_, task) in enumerate(optimal_df.iterrows()):
        folium.CircleMarker(
            [task['optimal_lat'], task['optimal_lng']],
            radius=6,
            color='#00ff88',
            fill=True,
            fill_color='#00ff88',
            fill_opacity=0.8,
            popup=f"<b style='color:#00ff88'>최적 #{idx+1}</b><br>{task['task_type']}<br>{task['reason']}"
        ).add_to(m)

    if len(optimal_coords) > 1:
        folium.PolyLine(optimal_coords, color='#00ff88', weight=3, opacity=0.8).add_to(m)

    # 거리 계산
    actual_distance = calculate_route_distance(actual_coords)
    optimal_distance = calculate_route_distance(optimal_coords)

    # 매출 정보
    actual_revenue = revenue_estimate['actual_revenue']
    estimated_revenue = revenue_estimate['estimated_revenue']
    lost_revenue = revenue_estimate.get('lost_revenue', 0)
    additional_revenue = revenue_estimate.get('additional_revenue', 0)
    revenue_diff = estimated_revenue - actual_revenue
    revenue_pct = (revenue_diff / actual_revenue * 100) if actual_revenue > 0 else 0

    # 패널 HTML
    compare_html = f"""
    <div style="position: fixed; top: 20px; right: 20px; z-index: 1000;
                background: rgba(0,0,0,0.95); padding: 20px; border-radius: 12px;
                color: white; font-family: Arial; min-width: 380px; box-shadow: 0 4px 20px rgba(0,0,0,0.5);">

        <h3 style="margin: 0 0 10px 0; text-align: center; color: #00ff88;">
            v5 동선 최적화
        </h3>
        <p style="margin: 0 0 15px 0; text-align: center; color: #888; font-size: 12px;">
            {time_slot_info}
        </p>

        <table style="width: 100%; font-size: 13px; border-collapse: collapse;">
            <tr style="border-bottom: 1px solid #333;">
                <th style="text-align: left; padding: 8px 5px; color: #888;"></th>
                <th style="text-align: right; padding: 8px 5px; color: #ff4444;">AS-IS</th>
                <th style="text-align: right; padding: 8px 5px; color: #00ff88;">TO-BE</th>
                <th style="text-align: right; padding: 8px 5px; color: #ffdd00;">차이</th>
            </tr>
            <tr>
                <td style="padding: 8px 5px;"><b>이동 거리</b></td>
                <td style="text-align: right; padding: 8px 5px; color: #ff4444;"><b>{actual_distance:.1f}km</b></td>
                <td style="text-align: right; padding: 8px 5px; color: #00ff88;"><b>{optimal_distance:.1f}km</b></td>
                <td style="text-align: right; padding: 8px 5px; color: {'#00ff88' if optimal_distance <= actual_distance else '#ff8800'};">
                    <b>{optimal_distance - actual_distance:+.1f}km</b>
                </td>
            </tr>
            <tr style="background: rgba(255,255,255,0.05);">
                <td style="padding: 8px 5px;"><b>48h 매출</b></td>
                <td style="text-align: right; padding: 8px 5px; color: #ff4444;"><b>{actual_revenue:,.0f}원</b></td>
                <td style="text-align: right; padding: 8px 5px; color: #00ff88;"><b>{estimated_revenue:,.0f}원</b></td>
                <td style="text-align: right; padding: 8px 5px; color: {'#00ff88' if revenue_diff >= 0 else '#ff4444'};">
                    <b>{revenue_diff:+,.0f}원</b>
                </td>
            </tr>
        </table>

        <div style="margin-top: 15px; padding: 10px; background: rgba(255,255,255,0.05); border-radius: 8px;">
            <p style="margin: 5px 0; font-size: 12px;">
                <span style="color: #ff8800;">손실:</span> -{lost_revenue:,.0f}원 (저매출 대체)
            </p>
            <p style="margin: 5px 0; font-size: 12px;">
                <span style="color: #00ff88;">추가:</span> +{additional_revenue:,.0f}원 (수요 기반)
            </p>
            <p style="margin: 5px 0; font-size: 11px; color: #888;">
                기기없음 {revenue_estimate.get('total_no_bike', 0):.0f}건 |
                미전환 {revenue_estimate.get('total_no_convert', 0):.0f}건
            </p>
        </div>

        <div style="margin-top: 15px; padding-top: 15px; border-top: 1px solid #444; text-align: center;">
            <span style="font-size: 20px; color: #00ff88; font-weight: bold;">
                예상 매출 증가: +{revenue_pct:.1f}%
            </span>
        </div>

        <div style="margin-top: 10px; font-size: 10px; color: #666; text-align: center;">
            * 과거 라이딩 + 앱오픈 + 미충족 수요 종합<br>
            * 거리 제약 없음, 순수 수요 기반
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(compare_html))

    # 범례
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                background: rgba(0,0,0,0.9); padding: 15px; border-radius: 10px;
                color: white; font-family: Arial; min-width: 200px;">
        <h4 style="margin: 0 0 10px 0;">범례</h4>

        <p style="margin: 5px 0; font-size: 12px;"><span style="color: #ff4444; font-size: 14px;">●━</span> 실제 동선</p>
        <p style="margin: 5px 0; font-size: 12px;"><span style="color: #00ff88; font-size: 14px;">●━</span> 최적 동선</p>

        <hr style="border-color: #444; margin: 10px 0;">

        <p style="margin: 3px 0; font-size: 10px;">
            <span style="color: #ff6666;">●</span> 기기없음<br>
            <span style="color: #6666ff;">●</span> 미전환<br>
            <span style="color: #66ff66;">●</span> 전환
        </p>

        <hr style="border-color: #444; margin: 10px 0;">
        <p style="margin: 3px 0; font-size: 10px; color: #888;">
            큰 원 = 수요 클러스터
        </p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    m.save(output_path)
    return m, actual_distance, optimal_distance


def run_analysis_v5(target_date: str, staff_name: str, time_slot: str = 'afternoon'):
    """
    v5 분석 실행
    time_slot: 'afternoon' (14~18시) or 'evening' (19~21시 -> 다음날 오전 준비)
    """
    print("="*70)
    print(f"동선 최적화 v5 (종합 개선)")
    print(f"날짜: {target_date} | 매니저: {staff_name}")
    print(f"시간대: {TIME_SLOTS[time_slot]['description']}")
    print("="*70)

    client = bigquery.Client()

    # 1. 실제 동선
    print("\n1. 실제 동선...")
    route_df = load_staff_route(client, target_date, staff_name)
    print(f"   총 {len(route_df)}건")

    if len(route_df) == 0:
        print("   데이터 없음")
        return None

    for t, c in route_df['task_type'].value_counts().items():
        print(f"   - {t}: {c}건")

    region = route_df['region'].mode().iloc[0] if len(route_df) > 0 else None
    print(f"   권역: {region}")

    # 시간대별 필터링
    slot_config = TIME_SLOTS[time_slot]
    slot_hours = range(slot_config['hours'][0], slot_config['hours'][1] + 1)
    route_filtered = route_df[route_df['task_hour'].isin(slot_hours)]
    print(f"   {time_slot} 시간대 작업: {len(route_filtered)}건")

    if len(route_filtered) == 0:
        print(f"   {time_slot} 시간대 작업 없음, 전체 사용")
        route_filtered = route_df

    # 2. 48시간 매출
    print("\n2. 48시간 매출...")
    bike_ids = route_filtered['bike_id'].dropna().unique().tolist()
    revenue_df = load_bike_48h_revenue(client, bike_ids, target_date)
    total_revenue = revenue_df['total_revenue'].sum() if len(revenue_df) > 0 else 0
    print(f"   총 매출: {total_revenue:,.0f}원")

    # 3. 비효율 분석
    print("\n3. 비효율 분석...")
    actual_df = analyze_actual_route(route_filtered, revenue_df)
    inefficient = actual_df[actual_df['is_inefficient']]
    print(f"   비효율(매출0): {len(inefficient)}건 / {len(actual_df)}건")

    # 4. 앱오픈 수요
    print("\n4. 앱오픈 수요...")
    app_df = load_app_open_demand(client, target_date, region)
    app_df['is_accessible'] = app_df['is_accessible'].fillna(False)
    app_df['is_converted'] = app_df['is_converted'].fillna(False)
    print(f"   앱오픈: {len(app_df)}건 (x2: {len(app_df)*2}건)")

    # 5. 과거 라이딩 핫스팟
    print("\n5. 과거 라이딩 핫스팟 (30일)...")
    center_lat = route_filtered['lat'].mean()
    center_lng = route_filtered['lng'].mean()
    historical_df = load_historical_riding_hotspots(client, region, center_lat=center_lat, center_lng=center_lng)
    print(f"   핫스팟 그리드: {len(historical_df)}개")

    # 6. 미충족 수요
    print("\n6. 미충족 수요 지역...")
    unmet_df = load_unmet_demand_locations(client, target_date, region)
    print(f"   미충족 지역: {len(unmet_df)}개")

    # 7. 고장기기
    print("\n7. 고장기기...")
    broken_df = load_broken_bikes(client, target_date, region)
    print(f"   고장기기: {len(broken_df)}대")

    # 7.5 기기 스냅샷 (배터리 부족/고장 상태)
    print("\n7.5 기기 스냅샷 분석...")
    slot_config = TIME_SLOTS[time_slot]
    snapshot_hours = list(range(slot_config['hours'][0], slot_config['hours'][1] + 1))
    snapshot_df = load_bike_snapshot_by_status(client, target_date, region, snapshot_hours)
    print(f"   스냅샷 기기: {len(snapshot_df)}대")

    if len(snapshot_df) > 0:
        status_counts = snapshot_df['status_category'].value_counts()
        for status, cnt in status_counts.items():
            print(f"   - {status}: {cnt}대")

    # 스냅샷 기반 미충족 수요 분석
    snapshot_analysis_df = analyze_unmet_demand_with_snapshot(app_df, snapshot_df, time_slot)
    if len(snapshot_analysis_df) > 0:
        print(f"   기기없음 지역 분석: {len(snapshot_analysis_df)}개")
        action_counts = snapshot_analysis_df['need_action'].value_counts()
        for action, cnt in action_counts.items():
            print(f"   - {action} 필요: {cnt}개 지역")

    # 8. 종합 수요 인덱스
    print("\n8. 종합 수요 인덱스 구축...")
    demand_index = build_comprehensive_demand_index(app_df, historical_df, unmet_df, snapshot_analysis_df, time_slot)
    print(f"   수요 그리드: {len(demand_index)}개")

    if len(demand_index) == 0:
        print("   수요 데이터 없음")
        return None

    top5 = sorted(demand_index.items(), key=lambda x: x[1]['total_score'], reverse=True)[:5]
    print("   상위 5개 핫스팟:")
    for loc, info in top5:
        print(f"     - ({loc[0]:.3f}, {loc[1]:.3f}): 점수 {info['total_score']:.0f}")

    # 9. 클러스터링
    print("\n9. 수요 클러스터링...")
    n_clusters = min(8, len(actual_df) // 3 + 1)
    clusters = cluster_high_demand_areas(demand_index, n_clusters=n_clusters)
    if clusters:
        print(f"   클러스터 수: {len(clusters)}개")
        for cid, cinfo in sorted(clusters.items(), key=lambda x: x[1]['total_score'], reverse=True)[:3]:
            print(f"     - 클러스터 {cid}: 점수 {cinfo['total_score']:.0f}, {cinfo['count']}개 지점")

    # 10. 클러스터 방문 순서 최적화
    print("\n10. 클러스터 방문 순서 최적화...")
    start_lat = actual_df.iloc[0]['lat']
    start_lng = actual_df.iloc[0]['lng']
    cluster_order = optimize_cluster_order(clusters, start_lat, start_lng) if clusters else []
    print(f"   방문 순서: {cluster_order}")

    # 11. 최적 동선 계산
    print("\n11. 최적 동선 계산...")
    optimal_df = calculate_optimal_route_v5(actual_df, demand_index, clusters, cluster_order, broken_df, time_slot)
    print(f"   최적 작업 수: {len(optimal_df)}건")

    # 12. 거리 비교
    actual_coords = [[t['lat'], t['lng']] for _, t in actual_df.iterrows()]
    optimal_coords = [[t['optimal_lat'], t['optimal_lng']] for _, t in optimal_df.iterrows()]

    actual_dist = calculate_route_distance(actual_coords)
    optimal_dist = calculate_route_distance(optimal_coords)

    print(f"\n   실제 거리: {actual_dist:.2f}km")
    print(f"   최적 거리: {optimal_dist:.2f}km")
    print(f"   거리 변화: {optimal_dist - actual_dist:+.2f}km ({(optimal_dist/actual_dist - 1)*100:+.1f}%)")

    # 13. 매출 추정
    print("\n13. 매출 추정...")
    revenue_estimate = estimate_optimal_revenue_v5(actual_df, optimal_df, demand_index)
    print(f"   실제 48h 매출: {revenue_estimate['actual_revenue']:,.0f}원")
    print(f"   손실 (저매출 대체): -{revenue_estimate['lost_revenue']:,.0f}원")
    print(f"   추가 예상: +{revenue_estimate['additional_revenue']:,.0f}원")
    print(f"   예상 48h 매출: {revenue_estimate['estimated_revenue']:,.0f}원")

    revenue_diff = revenue_estimate['estimated_revenue'] - revenue_estimate['actual_revenue']
    revenue_pct = (revenue_diff / revenue_estimate['actual_revenue'] * 100) if revenue_estimate['actual_revenue'] > 0 else 0
    print(f"   매출 증가: {revenue_diff:+,.0f}원 ({revenue_pct:+.1f}%)")

    # 14. 시각화
    output_path = f"/Users/admin/pmo_ops/demand_forecast/visualizations/route_v5_{target_date}_{staff_name}_{time_slot}.html"
    print(f"\n14. 시각화: {output_path}")
    time_slot_info = TIME_SLOTS[time_slot]['description']
    create_comparison_map_v5(actual_df, optimal_df, app_df, clusters, output_path, revenue_estimate, time_slot_info)

    print("\n" + "="*70)
    print("완료")
    print("="*70)

    return {
        'actual': actual_df,
        'optimal': optimal_df,
        'actual_dist': actual_dist,
        'optimal_dist': optimal_dist,
        'revenue_estimate': revenue_estimate,
        'clusters': clusters
    }


def run_full_day_analysis(target_date: str, staff_name: str):
    """하루 전체 분석 (오후 + 저녁)"""
    print("\n" + "="*70)
    print("전체 일일 분석")
    print("="*70)

    results = {}

    # 오후 분석 (14~18시)
    print("\n>>> 오후 시간대 분석 <<<")
    results['afternoon'] = run_analysis_v5(target_date, staff_name, 'afternoon')

    # 저녁 분석 (19~21시 -> 다음날 오전 준비)
    print("\n>>> 저녁 시간대 분석 (다음날 오전 준비) <<<")
    results['evening'] = run_analysis_v5(target_date, staff_name, 'evening')

    # 종합 결과
    print("\n" + "="*70)
    print("종합 결과")
    print("="*70)

    total_actual_revenue = 0
    total_estimated_revenue = 0

    for slot, result in results.items():
        if result and 'revenue_estimate' in result:
            re = result['revenue_estimate']
            total_actual_revenue += re['actual_revenue']
            total_estimated_revenue += re['estimated_revenue']

            slot_name = TIME_SLOTS[slot]['description']
            diff = re['estimated_revenue'] - re['actual_revenue']
            pct = (diff / re['actual_revenue'] * 100) if re['actual_revenue'] > 0 else 0
            print(f"\n{slot_name}:")
            print(f"  AS-IS: {re['actual_revenue']:,.0f}원")
            print(f"  TO-BE: {re['estimated_revenue']:,.0f}원 ({diff:+,.0f}원, {pct:+.1f}%)")

    if total_actual_revenue > 0:
        total_diff = total_estimated_revenue - total_actual_revenue
        total_pct = (total_diff / total_actual_revenue * 100)
        print(f"\n종합:")
        print(f"  AS-IS: {total_actual_revenue:,.0f}원")
        print(f"  TO-BE: {total_estimated_revenue:,.0f}원 ({total_diff:+,.0f}원, {total_pct:+.1f}%)")

    return results


if __name__ == "__main__":
    # 전체 일일 분석
    run_full_day_analysis("2025-11-26", "Staff_X2")
