"""
지도 렌더러 - 비행기 관제탑 스타일
실시간 이동 경로 및 이벤트 시각화

최적화:
- MarkerCluster로 마커 클러스터링 (줌 레벨에 따라 그룹화)
- 라이딩 경로 수 제한 및 PolyLine 사용 (AntPath 대신)
"""
import folium
from folium import plugins
from folium.plugins import AntPath, TimestampedGeoJson, MarkerCluster, FastMarkerCluster
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import EVENT_CONFIG, TRAIL_CONFIG, STAFF_TRAIL_CONFIG, MAP_CONFIG, CENTER_COLORS

# 최적화 설정
MAX_RIDING_PATHS = 500  # 최대 라이딩 경로 수
MAX_STAFF_TRAILS = 30   # 최대 관리자 동선 수
USE_CLUSTERING = False  # 타임라인 모드 사용 (True면 클러스터링)


def create_base_map(center: tuple = None, zoom: int = None) -> folium.Map:
    """다크 테마 기본 지도 생성"""
    if center is None:
        center = MAP_CONFIG['default_center']
    if zoom is None:
        zoom = MAP_CONFIG['default_zoom']

    m = folium.Map(
        location=center,
        zoom_start=zoom,
        tiles=MAP_CONFIG['tiles'],
        attr=MAP_CONFIG['attribution'],
    )

    # 풀스크린 버튼 추가
    plugins.Fullscreen(
        position='topleft',
        title='전체화면',
        title_cancel='전체화면 종료',
    ).add_to(m)

    return m


def create_animated_map(
    events_df: pd.DataFrame,
    paths_df: pd.DataFrame = None,
    staff_df: pd.DataFrame = None,
    center: tuple = None,
) -> folium.Map:
    """
    애니메이션 지도 생성 (TimestampedGeoJson 기반)
    - 앱오픈, 라이딩, 관리자 작업이 모두 같은 타임라인에서 표시

    Args:
        events_df: 이벤트 데이터 (앱오픈 + 라이딩 시작/종료 + 관리자 작업)
        paths_df: 라이딩 경로 데이터 (옵션)
        staff_df: 관리자 이동 데이터 (옵션)
        center: 지도 중심 좌표
    """
    if center is None and not events_df.empty:
        center = (events_df['lat'].mean(), events_df['lng'].mean())

    m = create_base_map(center)

    # 통합 타임라인 (앱오픈 + 라이딩 + 관리자 작업 모두 같은 시계열)
    _add_unified_timeline(m, events_df, paths_df, staff_df)

    # 범례 추가
    _add_legend(m)

    # 레이어 컨트롤
    folium.LayerControl(collapsed=False).add_to(m)

    return m


def _add_unified_timeline(m: folium.Map, events_df: pd.DataFrame, paths_df: pd.DataFrame = None, staff_df: pd.DataFrame = None):
    """통합 타임라인 - 앱오픈, 라이딩, 관리자 작업을 모두 같은 시계열에서 표시"""

    features = []

    # 1. 이벤트 포인트 추가 (앱오픈 + 라이딩 시작/종료 + 관리자 작업)
    if not events_df.empty:
        for _, event in events_df.iterrows():
            config = EVENT_CONFIG.get(event['event_type'], {})

            # 시간 문자열 (timezone 제거)
            event_time = event['event_time']
            if hasattr(event_time, 'tz') and event_time.tz is not None:
                time_str = event_time.strftime('%Y-%m-%dT%H:%M:%S')
            else:
                time_str = event_time.isoformat()

            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [event['lng'], event['lat']],
                },
                'properties': {
                    'time': time_str,
                    'popup': _create_popup_html(event, config),
                    'icon': 'circle',
                    'iconstyle': {
                        'fillColor': config.get('color', '#ffffff'),
                        'fillOpacity': 0.9,
                        'stroke': True,
                        'color': config.get('glow', config.get('color', '#ffffff')),
                        'weight': 2,
                        'radius': config.get('radius', 5),
                    },
                },
            }
            features.append(feature)

    # 2. 라이딩 경로 추가 (LineString으로)
    if paths_df is not None and not paths_df.empty:
        # 경로 수 제한
        limited_paths = paths_df.tail(MAX_RIDING_PATHS) if len(paths_df) > MAX_RIDING_PATHS else paths_df

        for _, path in limited_paths.iterrows():
            # 시작 시간 기준으로 경로 표시
            start_time = path['start_time']
            if hasattr(start_time, 'tz') and start_time.tz is not None:
                time_str = start_time.strftime('%Y-%m-%dT%H:%M:%S')
            else:
                time_str = start_time.isoformat()

            # 라이딩 경로 (LineString)
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'LineString',
                    'coordinates': [
                        [path['start_lng'], path['start_lat']],
                        [path['end_lng'], path['end_lat']],
                    ],
                },
                'properties': {
                    'time': time_str,
                    'style': {
                        'color': TRAIL_CONFIG['active']['color'],
                        'weight': 2,
                        'opacity': 0.7,
                    },
                    'popup': f"🛴 라이딩 {start_time.strftime('%H:%M')}~{path['end_time'].strftime('%H:%M')}",
                },
            }
            features.append(feature)

    # 3. 관리자 동선 추가 (작업 포인트 + 이동 경로)
    if staff_df is not None and not staff_df.empty:
        staff_names = staff_df['staff_name'].unique()

        # 관리자 수 제한
        if len(staff_names) > MAX_STAFF_TRAILS:
            staff_work_counts = staff_df.groupby('staff_name').size().sort_values(ascending=False)
            staff_names = staff_work_counts.head(MAX_STAFF_TRAILS).index.tolist()

        colors = STAFF_TRAIL_CONFIG['colors']

        for i, staff_name in enumerate(staff_names):
            staff_works = staff_df[staff_df['staff_name'] == staff_name].sort_values('work_time')
            color = colors[i % len(colors)]

            # 센터별 색상
            center_name = staff_works.iloc[0].get('center_name', '')
            if center_name in CENTER_COLORS:
                color = CENTER_COLORS[center_name]

            # 각 작업 포인트
            prev_work = None
            for _, work in staff_works.iterrows():
                work_time = work['work_time']
                if hasattr(work_time, 'tz') and work_time.tz is not None:
                    time_str = work_time.strftime('%Y-%m-%dT%H:%M:%S')
                else:
                    time_str = work_time.isoformat()

                config = EVENT_CONFIG.get(work['work_type'], {})

                # 작업 포인트 (Point)
                feature = {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [work['lng'], work['lat']],
                    },
                    'properties': {
                        'time': time_str,
                        'popup': f"""
                            <b>{config.get('icon', '🔧')} {config.get('label', work['work_type'])}</b><br>
                            담당: {work['staff_name']}<br>
                            센터: {work.get('center_name', 'N/A')}<br>
                            시간: {work_time.strftime('%H:%M')}<br>
                            순서: {work.get('work_order', 'N/A')}
                        """,
                        'icon': 'circle',
                        'iconstyle': {
                            'fillColor': config.get('color', color),
                            'fillOpacity': 0.9,
                            'stroke': True,
                            'color': color,
                            'weight': 3,
                            'radius': 8,
                        },
                    },
                }
                features.append(feature)

                # 이전 작업과 연결하는 이동 경로 (LineString)
                if prev_work is not None:
                    feature_line = {
                        'type': 'Feature',
                        'geometry': {
                            'type': 'LineString',
                            'coordinates': [
                                [prev_work['lng'], prev_work['lat']],
                                [work['lng'], work['lat']],
                            ],
                        },
                        'properties': {
                            'time': time_str,
                            'style': {
                                'color': color,
                                'weight': 2,
                                'opacity': 0.6,
                                'dashArray': '5, 10',
                            },
                        },
                    }
                    features.append(feature_line)

                prev_work = work

    # TimestampedGeoJson 추가
    if features:
        TimestampedGeoJson(
            {'type': 'FeatureCollection', 'features': features},
            period='PT1M',           # 1분 단위 재생
            duration='PT5M',         # 5분간 표시 유지
            auto_play=False,         # 자동 재생 off
            loop=False,
            max_speed=10,
            loop_button=True,
            date_options='YYYY-MM-DD HH:mm',
            time_slider_drag_update=True,
        ).add_to(m)


def _add_event_layer(m: folium.Map, events_df: pd.DataFrame):
    """이벤트 포인트 레이어 추가 - 클러스터링 적용"""

    if USE_CLUSTERING:
        # 이벤트 타입별로 클러스터 생성
        _add_clustered_events(m, events_df)
    else:
        # 기존 TimestampedGeoJson 방식
        _add_timestamped_events(m, events_df)


def _add_clustered_events(m: folium.Map, events_df: pd.DataFrame):
    """클러스터링된 이벤트 레이어"""

    # 카테고리별 클러스터 생성
    user_cluster = MarkerCluster(
        name='🛴 유저 이벤트',
        show=True,
        options={
            'maxClusterRadius': 50,
            'disableClusteringAtZoom': 16,
            'spiderfyOnMaxZoom': True,
        }
    )

    ops_cluster = MarkerCluster(
        name='🔧 운영 이벤트',
        show=True,
        options={
            'maxClusterRadius': 40,
            'disableClusteringAtZoom': 15,
        }
    )

    for _, event in events_df.iterrows():
        config = EVENT_CONFIG.get(event['event_type'], {})
        category = event.get('category', 'user')

        # CircleMarker 대신 간단한 Marker 사용 (클러스터링 호환)
        marker = folium.CircleMarker(
            location=[event['lat'], event['lng']],
            radius=config.get('radius', 5),
            color=config.get('color', '#ffffff'),
            fill=True,
            fill_color=config.get('color', '#ffffff'),
            fill_opacity=0.8,
            popup=_create_popup_html(event, config),
            tooltip=f"{config.get('icon', '●')} {event['event_time'].strftime('%H:%M')}",
        )

        if category == 'ops':
            marker.add_to(ops_cluster)
        else:
            marker.add_to(user_cluster)

    user_cluster.add_to(m)
    ops_cluster.add_to(m)


def _add_timestamped_events(m: folium.Map, events_df: pd.DataFrame):
    """TimestampedGeoJson 방식 (기존)"""

    features = []
    for _, event in events_df.iterrows():
        config = EVENT_CONFIG.get(event['event_type'], {})

        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [event['lng'], event['lat']],
            },
            'properties': {
                'time': event['event_time'].isoformat(),
                'popup': _create_popup_html(event, config),
                'icon': 'circle',
                'iconstyle': {
                    'fillColor': config.get('color', '#ffffff'),
                    'fillOpacity': 0.9,
                    'stroke': True,
                    'color': config.get('glow', config.get('color', '#ffffff')),
                    'weight': 2,
                    'radius': config.get('radius', 5),
                },
            },
        }
        features.append(feature)

    if features:
        TimestampedGeoJson(
            {'type': 'FeatureCollection', 'features': features},
            period='PT1M',
            duration='PT10M',
            auto_play=True,
            loop=False,
            max_speed=10,
            loop_button=True,
            date_options='YYYY-MM-DD HH:mm',
            time_slider_drag_update=True,
        ).add_to(m)


def _add_riding_trails(m: folium.Map, paths_df: pd.DataFrame):
    """라이딩 경로 추가 - 최적화 (경로 수 제한 + PolyLine)"""

    riding_group = folium.FeatureGroup(name='🚴 라이딩 경로', show=True)

    # 경로 수 제한 (최근 것 우선)
    if len(paths_df) > MAX_RIDING_PATHS:
        paths_df = paths_df.tail(MAX_RIDING_PATHS)

    # 모든 경로를 하나의 PolyLine 그룹으로
    all_lines = []

    for _, path in paths_df.iterrows():
        coords = [
            [path['start_lat'], path['start_lng']],
            [path['end_lat'], path['end_lng']],
        ]
        all_lines.append(coords)

    # 일반 PolyLine 사용 (AntPath보다 훨씬 빠름)
    for coords in all_lines:
        folium.PolyLine(
            locations=coords,
            color=TRAIL_CONFIG['active']['color'],
            weight=2,
            opacity=0.6,
            dash_array='5, 10',
        ).add_to(riding_group)

    # 시작/종료점은 클러스터링
    start_cluster = MarkerCluster(
        name='▶ 라이딩 시작점',
        show=False,  # 기본 숨김
        options={'maxClusterRadius': 60}
    )
    end_cluster = MarkerCluster(
        name='■ 라이딩 종료점',
        show=False,  # 기본 숨김
        options={'maxClusterRadius': 60}
    )

    for _, path in paths_df.iterrows():
        # 시작점
        folium.CircleMarker(
            location=[path['start_lat'], path['start_lng']],
            radius=4,
            color='#00ff88',
            fill=True,
            fill_color='#00ff88',
            fill_opacity=0.7,
            tooltip=f"▶ {path['start_time'].strftime('%H:%M')}",
        ).add_to(start_cluster)

        # 종료점
        folium.CircleMarker(
            location=[path['end_lat'], path['end_lng']],
            radius=4,
            color='#ff4444',
            fill=True,
            fill_color='#ff4444',
            fill_opacity=0.7,
            tooltip=f"■ {path['end_time'].strftime('%H:%M')}",
        ).add_to(end_cluster)

    riding_group.add_to(m)
    start_cluster.add_to(m)
    end_cluster.add_to(m)


def _add_staff_trails(m: folium.Map, staff_df: pd.DataFrame):
    """관리자 이동 경로 추가 - 최적화"""

    staff_group = folium.FeatureGroup(name='👷 관리자 동선', show=True)

    # 관리자별 그룹화
    staff_names = staff_df['staff_name'].unique()

    # 관리자 수 제한
    if len(staff_names) > MAX_STAFF_TRAILS:
        # 작업량이 많은 관리자 우선
        staff_work_counts = staff_df.groupby('staff_name').size().sort_values(ascending=False)
        staff_names = staff_work_counts.head(MAX_STAFF_TRAILS).index.tolist()

    colors = STAFF_TRAIL_CONFIG['colors']

    for i, staff_name in enumerate(staff_names):
        staff_works = staff_df[staff_df['staff_name'] == staff_name].sort_values('work_time')

        if len(staff_works) < 2:
            continue

        color = colors[i % len(colors)]
        center_name = staff_works.iloc[0].get('center_name', '')

        # 센터별 색상 오버라이드
        if center_name in CENTER_COLORS:
            color = CENTER_COLORS[center_name]

        # 경로 좌표
        coords = list(zip(staff_works['lat'], staff_works['lng']))

        # PolyLine 사용 (AntPath 대신 - 더 빠름)
        folium.PolyLine(
            locations=coords,
            color=color,
            weight=STAFF_TRAIL_CONFIG['weight'],
            opacity=STAFF_TRAIL_CONFIG['opacity'],
            dash_array='5, 10',
            tooltip=f"{staff_name} ({len(staff_works)}건)",
        ).add_to(staff_group)

        # 작업 포인트 마커 (첫/마지막만 표시, 나머지는 클릭시)
        for idx, (_, work) in enumerate(staff_works.iterrows()):
            config = EVENT_CONFIG.get(work['work_type'], {})

            # 첫/마지막 작업만 마커 표시, 중간은 작은 점으로
            is_endpoint = (idx == 0 or idx == len(staff_works) - 1)
            radius = 6 if is_endpoint else 3
            opacity = 0.9 if is_endpoint else 0.5

            folium.CircleMarker(
                location=[work['lat'], work['lng']],
                radius=radius,
                color=config.get('color', color),
                fill=True,
                fill_color=config.get('color', color),
                fill_opacity=opacity,
                popup=f"""
                    <b>{config.get('icon', '🔧')} {config.get('label', work['work_type'])}</b><br>
                    담당: {work['staff_name']}<br>
                    센터: {work.get('center_name', 'N/A')}<br>
                    시간: {work['work_time'].strftime('%H:%M')}<br>
                    순서: {work.get('work_order', 'N/A')}
                """,
                tooltip=f"{work['staff_name']} #{work.get('work_order', '')}",
            ).add_to(staff_group)

    staff_group.add_to(m)


def _create_popup_html(event: pd.Series, config: dict) -> str:
    """이벤트 팝업 HTML 생성"""
    icon = config.get('icon', '●')
    label = config.get('label', event['event_type'])
    time_str = event['event_time'].strftime('%H:%M:%S')

    html = f"""
    <div style="font-family: 'Consolas', monospace; min-width: 180px;">
        <div style="font-size: 16px; font-weight: bold; color: {config.get('color', '#fff')};">
            {icon} {label}
        </div>
        <hr style="margin: 5px 0; border-color: #333;">
        <table style="font-size: 12px;">
            <tr><td style="color: #888;">시간</td><td>{time_str}</td></tr>
    """

    if pd.notna(event.get('bike_sn')):
        html += f'<tr><td style="color: #888;">SN</td><td>{event["bike_sn"]}</td></tr>'

    if pd.notna(event.get('staff_name')):
        html += f'<tr><td style="color: #888;">담당</td><td>{event["staff_name"]}</td></tr>'

    if pd.notna(event.get('center_name')):
        html += f'<tr><td style="color: #888;">센터</td><td>{event["center_name"]}</td></tr>'

    if pd.notna(event.get('distance')) and event['distance'] > 0:
        html += f'<tr><td style="color: #888;">거리</td><td>{event["distance"]/1000:.1f}km</td></tr>'

    if pd.notna(event.get('region')):
        html += f'<tr><td style="color: #888;">권역</td><td>{event["region"]}</td></tr>'

    html += '</table></div>'

    return html


def _add_legend(m: folium.Map):
    """범례 추가"""
    legend_html = """
    <div style="
        position: fixed;
        bottom: 30px;
        left: 30px;
        z-index: 1000;
        background: rgba(0, 0, 0, 0.85);
        padding: 15px;
        border-radius: 8px;
        font-family: 'Consolas', monospace;
        font-size: 12px;
        color: #fff;
        border: 1px solid #333;
        max-height: 400px;
        overflow-y: auto;
    ">
        <div style="font-weight: bold; margin-bottom: 10px; font-size: 14px;">📡 이벤트 범례</div>

        <div style="margin-bottom: 8px; color: #888;">─ 유저 이벤트 ─</div>
    """

    # 유저 이벤트
    user_events = ['riding_start', 'riding_end', 'app_converted', 'app_accessible', 'app_no_bike']
    for evt_type in user_events:
        cfg = EVENT_CONFIG.get(evt_type, {})
        legend_html += f"""
        <div style="display: flex; align-items: center; margin: 4px 0;">
            <div style="
                width: 12px; height: 12px;
                background: {cfg.get('color', '#fff')};
                border-radius: 50%;
                margin-right: 8px;
                box-shadow: 0 0 6px {cfg.get('glow', cfg.get('color', '#fff'))};
            "></div>
            <span>{cfg.get('icon', '')} {cfg.get('label', evt_type)}</span>
        </div>
        """

    legend_html += '<div style="margin: 8px 0; color: #888;">─ 운영 이벤트 ─</div>'

    # 운영 이벤트
    ops_events = ['battery_swap', 'rebalance_deploy', 'rebalance_collect', 'broken_collect', 'field_fix', 'repair_deploy']
    for evt_type in ops_events:
        cfg = EVENT_CONFIG.get(evt_type, {})
        legend_html += f"""
        <div style="display: flex; align-items: center; margin: 4px 0;">
            <div style="
                width: 12px; height: 12px;
                background: {cfg.get('color', '#fff')};
                border-radius: 50%;
                margin-right: 8px;
                box-shadow: 0 0 6px {cfg.get('glow', cfg.get('color', '#fff'))};
            "></div>
            <span>{cfg.get('icon', '')} {cfg.get('label', evt_type)}</span>
        </div>
        """

    legend_html += """
        <div style="margin-top: 10px; padding-top: 8px; border-top: 1px solid #333; color: #888;">
            <div>── 흐르는 선: 이동 중</div>
            <div>-- 점선: 관리자 동선</div>
        </div>
    </div>
    """

    m.get_root().html.add_child(folium.Element(legend_html))


def create_static_snapshot(
    events_df: pd.DataFrame,
    current_time: datetime,
    time_window_minutes: int = 30,
    paths_df: pd.DataFrame = None,
    staff_df: pd.DataFrame = None,
    center: tuple = None,
) -> folium.Map:
    """
    특정 시점 스냅샷 지도 생성

    Args:
        events_df: 전체 이벤트 데이터
        current_time: 표시할 시점
        time_window_minutes: 표시할 시간 범위 (분)
        paths_df: 라이딩 경로 데이터
        staff_df: 관리자 이동 데이터
        center: 지도 중심 좌표 (옵션)
    """
    # 시간 범위 필터
    window_start = current_time - timedelta(minutes=time_window_minutes)
    window_end = current_time

    filtered_events = events_df[
        (events_df['event_time'] >= window_start) &
        (events_df['event_time'] <= window_end)
    ].copy()

    # 지도 중심 결정 (파라미터 > 데이터 > 기본값)
    if center is None:
        if not filtered_events.empty:
            center = (filtered_events['lat'].mean(), filtered_events['lng'].mean())
        else:
            center = MAP_CONFIG['default_center']

    m = create_base_map(center)

    # 이벤트 마커
    for _, event in filtered_events.iterrows():
        config = EVENT_CONFIG.get(event['event_type'], {})

        # 시간에 따른 투명도 (최근일수록 진하게)
        age_minutes = (current_time - event['event_time']).total_seconds() / 60
        opacity = max(0.3, 1 - (age_minutes / time_window_minutes) * 0.7)

        folium.CircleMarker(
            location=[event['lat'], event['lng']],
            radius=config.get('radius', 5),
            color=config.get('color', '#ffffff'),
            fill=True,
            fill_color=config.get('color', '#ffffff'),
            fill_opacity=opacity,
            popup=_create_popup_html(event, config),
            tooltip=f"{config.get('icon', '')} {event['event_time'].strftime('%H:%M')}",
        ).add_to(m)

    # 현재 진행 중인 라이딩 표시
    if paths_df is not None and not paths_df.empty:
        active_ridings = paths_df[
            (paths_df['start_time'] <= current_time) &
            (paths_df['end_time'] >= current_time)
        ]

        for _, riding in active_ridings.iterrows():
            _add_active_riding_marker(m, riding, current_time)

    # 타임스탬프 표시
    _add_timestamp_display(m, current_time)

    _add_legend(m)

    return m


def _add_active_riding_marker(m: folium.Map, riding: pd.Series, current_time: datetime):
    """진행 중인 라이딩의 현재 위치 마커"""
    # 위치 보간
    total_duration = (riding['end_time'] - riding['start_time']).total_seconds()
    elapsed = (current_time - riding['start_time']).total_seconds()
    progress = min(1.0, elapsed / total_duration) if total_duration > 0 else 0

    current_lat = riding['start_lat'] + (riding['end_lat'] - riding['start_lat']) * progress
    current_lng = riding['start_lng'] + (riding['end_lng'] - riding['start_lng']) * progress

    # 이동 중인 자전거 마커 (펄스 효과)
    folium.CircleMarker(
        location=[current_lat, current_lng],
        radius=8,
        color='#00ffff',
        fill=True,
        fill_color='#00ffff',
        fill_opacity=0.9,
        tooltip=f"🚴 이동중 {riding.get('bike_sn', '')}",
        popup=f"""
            <b>🚴 라이딩 중</b><br>
            SN: {riding.get('bike_sn', 'N/A')}<br>
            진행률: {progress*100:.0f}%<br>
            시작: {riding['start_time'].strftime('%H:%M')}
        """,
    ).add_to(m)

    # 경로 (시작점 → 현재 → 종료점)
    AntPath(
        locations=[
            [riding['start_lat'], riding['start_lng']],
            [current_lat, current_lng],
        ],
        color='#00ffff',
        weight=3,
        opacity=0.8,
        pulse_color='#ffffff',
        delay=800,
    ).add_to(m)

    # 예상 경로 (점선)
    folium.PolyLine(
        locations=[
            [current_lat, current_lng],
            [riding['end_lat'], riding['end_lng']],
        ],
        color='#00ffff',
        weight=2,
        opacity=0.3,
        dash_array='5, 10',
    ).add_to(m)


def _add_timestamp_display(m: folium.Map, current_time: datetime):
    """현재 시간 표시"""
    timestamp_html = f"""
    <div style="
        position: fixed;
        top: 20px;
        left: 50%;
        transform: translateX(-50%);
        z-index: 1000;
        background: rgba(0, 0, 0, 0.9);
        padding: 10px 25px;
        border-radius: 8px;
        font-family: 'Consolas', monospace;
        font-size: 24px;
        color: #00ffff;
        border: 2px solid #00ffff;
        box-shadow: 0 0 20px rgba(0, 255, 255, 0.3);
    ">
        📡 {current_time.strftime('%Y-%m-%d %H:%M:%S')}
    </div>
    """
    m.get_root().html.add_child(folium.Element(timestamp_html))
