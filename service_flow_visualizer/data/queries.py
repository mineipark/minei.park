"""
BigQuery 쿼리 모음
서비스 흐름 시각화를 위한 이벤트 데이터 조회

최적화:
- UDF 호출 제거 (bike_sn 등은 필요시 별도 조회)
- LIMIT 적용으로 데이터량 제한
"""

# 기본 LIMIT 값
DEFAULT_LIMIT = 5000


def get_riding_events_query(target_date: str, region: str = None, limit: int = DEFAULT_LIMIT) -> str:
    """라이딩 이벤트 쿼리 (시작/종료) - UDF 제거, LIMIT 적용"""
    region_filter = f"AND h3_start_area_name LIKE '%{region}%'" if region else ""
    region_filter_end = f"AND h3_end_area_name LIKE '%{region}%'" if region else ""

    return f"""
    -- 라이딩 시작 이벤트
    SELECT
        'riding_start' as event_type,
        r.id as riding_id,
        TIMESTAMP(r.start_time) as event_time,
        ST_Y(r.start_location) as lat,
        ST_X(r.start_location) as lng,
        r.bike_id,
        CAST(NULL AS STRING) as bike_sn,
        CAST(NULL AS STRING) as staff_name,
        CAST(NULL AS STRING) as center_name,
        r.user_id,
        r.distance,
        TIMESTAMP(r.end_time) as end_time,
        ST_Y(r.end_location) as end_lat,
        ST_X(r.end_location) as end_lng,
        h3_start_area_name as region
    FROM `bikeshare.service.rides` r
    WHERE DATE(r.start_time) = '{target_date}'
        AND r.start_location IS NOT NULL
        AND r.end_location IS NOT NULL
        {region_filter}

    UNION ALL

    -- 라이딩 종료 이벤트
    SELECT
        'riding_end' as event_type,
        r.id as riding_id,
        TIMESTAMP(r.end_time) as event_time,
        ST_Y(r.end_location) as lat,
        ST_X(r.end_location) as lng,
        r.bike_id,
        CAST(NULL AS STRING) as bike_sn,
        CAST(NULL AS STRING) as staff_name,
        CAST(NULL AS STRING) as center_name,
        r.user_id,
        r.distance,
        TIMESTAMP(r.start_time) as end_time,
        ST_Y(r.start_location) as end_lat,
        ST_X(r.start_location) as end_lng,
        h3_end_area_name as region
    FROM `bikeshare.service.rides` r
    WHERE DATE(r.end_time) = '{target_date}'
        AND r.end_location IS NOT NULL
        {region_filter_end}

    ORDER BY event_time
    LIMIT {limit}
    """


def get_app_events_query(target_date: str, region: str = None, limit: int = DEFAULT_LIMIT) -> str:
    """앱 오픈 이벤트 쿼리 - LIMIT 적용"""
    region_filter = f"AND h3_area_name LIKE '%{region}%'" if region else ""

    return f"""
    SELECT
        CASE
            WHEN is_converted THEN 'app_converted'
            WHEN is_accessible THEN 'app_accessible'
            ELSE 'app_no_bike'
        END as event_type,
        CAST(NULL AS INT64) as riding_id,
        event_time,
        ST_Y(location) as lat,
        ST_X(location) as lng,
        CAST(NULL AS INT64) as bike_id,
        CAST(NULL AS STRING) as bike_sn,
        CAST(NULL AS STRING) as staff_name,
        CAST(NULL AS STRING) as center_name,
        CAST(NULL AS INT64) as user_id,
        CAST(NULL AS FLOAT64) as distance,
        CAST(NULL AS TIMESTAMP) as end_time,
        CAST(NULL AS FLOAT64) as end_lat,
        CAST(NULL AS FLOAT64) as end_lng,
        h3_area_name as region
    FROM `bikeshare.service.app_accessibility`
    WHERE DATE(event_time) = '{target_date}'
        AND location IS NOT NULL
        {region_filter}
    ORDER BY event_time
    LIMIT {limit}
    """


def get_maintenance_events_query(target_date: str, region: str = None, center: str = None, limit: int = DEFAULT_LIMIT) -> str:
    """관리자 작업 이벤트 쿼리 - UDF 최소화, LIMIT 적용"""
    center_filter = f"AND mc.name LIKE '%{center}%'" if center else ""
    region_filter = f"AND COALESCE(m.h3_complete_area, m.h3_call_area) LIKE '%{region}%'" if region else ""

    return f"""
    SELECT
        CASE
            WHEN m.type = 1 AND ms_base.stack_type = 75 THEN 'battery_swap'
            WHEN m.type = 2 AND ms_base.stack_type = 20 THEN 'field_fix'
            WHEN m.type = 2 AND ms_base.stack_type = 30 THEN 'broken_collect'
            WHEN m.type = 2 AND ms_base.stack_type = 80 THEN 'repair_deploy'
            WHEN m.type = 0 AND ms_base.stack_type = 30 THEN 'rebalance_collect'
            WHEN m.type = 0 AND ms_base.stack_type = 80 THEN 'rebalance_deploy'
        END as event_type,
        CAST(NULL AS INT64) as riding_id,
        TIMESTAMP(ms_base.event_time_kst) as event_time,
        ST_Y(COALESCE(m.location_complete, m.location_call)) as lat,
        ST_X(COALESCE(m.location_complete, m.location_call)) as lng,
        m.vehicle_id as bike_id,
        CAST(NULL AS STRING) as bike_sn,
        s.name as staff_name,
        mc.name as center_name,
        CAST(NULL AS INT64) as user_id,
        CAST(NULL AS FLOAT64) as distance,
        CAST(NULL AS TIMESTAMP) as end_time,
        CAST(NULL AS FLOAT64) as end_lat,
        CAST(NULL AS FLOAT64) as end_lng,
        COALESCE(m.h3_complete_area, m.h3_call_area) as region
    FROM (
        SELECT
            ms.maintenance_id,
            COALESCE(ms.staff_id, s_lookup.id) AS resolved_staff_id,
            ms.type as stack_type,
            DATETIME(ms.created_at, 'Asia/Seoul') as event_time_kst
        FROM `bikeshare.service.maintenance_log` ms
        LEFT JOIN `bikeshare.service.staff` s_lookup
            ON s_lookup.user_id = ms.manager_id
        WHERE DATE(DATETIME(ms.created_at, 'Asia/Seoul')) = '{target_date}'
    ) ms_base
    JOIN `bikeshare.service.maintenance` m ON m.id = ms_base.maintenance_id
    JOIN `bikeshare.service.staff` s ON s.id = ms_base.resolved_staff_id
    JOIN `bikeshare.service.service_center` mc ON mc.id = s.center_id
    WHERE m.status != 1
        AND COALESCE(m.location_complete, m.location_call) IS NOT NULL
        {center_filter}
        {region_filter}
    ORDER BY event_time
    LIMIT {limit}
    """


def get_riding_paths_query(target_date: str, region: str = None, limit: int = 2000) -> str:
    """라이딩 경로 데이터 (시작-종료 연결용) - UDF 제거, LIMIT 적용"""
    region_filter = f"AND h3_start_area_name LIKE '%{region}%'" if region else ""

    return f"""
    SELECT
        r.id as riding_id,
        r.bike_id,
        CAST(NULL AS STRING) as bike_sn,
        r.start_time,
        r.end_time,
        ST_Y(r.start_location) as start_lat,
        ST_X(r.start_location) as start_lng,
        ST_Y(r.end_location) as end_lat,
        ST_X(r.end_location) as end_lng,
        r.distance,
        TIMESTAMP_DIFF(r.end_time, r.start_time, SECOND) as duration_sec,
        h3_start_area_name as start_region,
        h3_end_area_name as end_region
    FROM `bikeshare.service.rides` r
    WHERE DATE(r.start_time) = '{target_date}'
        AND r.start_location IS NOT NULL
        AND r.end_location IS NOT NULL
        AND r.end_time IS NOT NULL
        {region_filter}
    ORDER BY r.start_time
    LIMIT {limit}
    """


def get_staff_movements_query(target_date: str, center: str = None, limit: int = 3000) -> str:
    """관리자 이동 경로 (시간순 작업 위치 연결) - UDF 최소화, LIMIT 적용"""
    center_filter = f"AND mc.name LIKE '%{center}%'" if center else ""

    return f"""
    WITH staff_works AS (
        SELECT
            s.id as staff_id,
            s.name as staff_name,
            mc.name as center_name,
            DATETIME(ms.created_at, 'Asia/Seoul') as work_time,
            ST_Y(COALESCE(m.location_complete, m.location_call)) as lat,
            ST_X(COALESCE(m.location_complete, m.location_call)) as lng,
            CASE
                WHEN m.type = 1 AND ms.type = 75 THEN 'battery_swap'
                WHEN m.type = 2 AND ms.type = 20 THEN 'field_fix'
                WHEN m.type = 2 AND ms.type = 30 THEN 'broken_collect'
                WHEN m.type = 2 AND ms.type = 80 THEN 'repair_deploy'
                WHEN m.type = 0 AND ms.type = 30 THEN 'rebalance_collect'
                WHEN m.type = 0 AND ms.type = 80 THEN 'rebalance_deploy'
            END as work_type,
            CAST(NULL AS STRING) as bike_sn
        FROM `bikeshare.service.maintenance_log` ms
        JOIN `bikeshare.service.maintenance` m ON m.id = ms.maintenance_id
        LEFT JOIN `bikeshare.service.staff` s_lookup ON s_lookup.user_id = ms.manager_id
        JOIN `bikeshare.service.staff` s ON s.id = COALESCE(ms.staff_id, s_lookup.id)
        JOIN `bikeshare.service.service_center` mc ON mc.id = s.center_id
        WHERE DATE(DATETIME(ms.created_at, 'Asia/Seoul')) = '{target_date}'
            AND m.status != 1
            AND COALESCE(m.location_complete, m.location_call) IS NOT NULL
            AND s.maintenance_role IN (15, 20)
            {center_filter}
    )
    SELECT
        staff_id,
        staff_name,
        center_name,
        work_time,
        lat,
        lng,
        work_type,
        bike_sn,
        ROW_NUMBER() OVER (PARTITION BY staff_id ORDER BY work_time) as work_order
    FROM staff_works
    WHERE work_type IS NOT NULL
    ORDER BY staff_name, work_time
    LIMIT {limit}
    """


def get_all_events_query(target_date: str, region: str = None, center: str = None,
                         include_riding: bool = True, include_app: bool = True,
                         include_ops: bool = True, hour_start: int = 0, hour_end: int = 24) -> str:
    """모든 이벤트 통합 쿼리 (사용하지 않음 - 개별 쿼리 사용)"""

    queries = []

    if include_riding:
        queries.append(get_riding_events_query(target_date, region))

    if include_app:
        queries.append(get_app_events_query(target_date, region))

    if include_ops:
        queries.append(get_maintenance_events_query(target_date, region, center))

    if not queries:
        return "SELECT 1 WHERE FALSE"

    combined = "\nUNION ALL\n".join(queries)

    return f"""
    WITH all_events AS (
        {combined}
    )
    SELECT *
    FROM all_events
    WHERE event_type IS NOT NULL
        AND lat IS NOT NULL
        AND lng IS NOT NULL
        AND EXTRACT(HOUR FROM event_time) >= {hour_start}
        AND EXTRACT(HOUR FROM event_time) < {hour_end}
    ORDER BY event_time
    """
