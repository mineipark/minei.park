"""
서비스 흐름 시각화 - 메인 앱
비행기 관제탑 스타일의 실시간 서비스 모니터링
"""
import streamlit as st
from streamlit_folium import st_folium
from datetime import datetime, timedelta
import pandas as pd
import time

# 페이지 설정 (반드시 첫 번째)
st.set_page_config(
    page_title="서비스 흐름 관제",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 로컬 모듈 import
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import EVENT_CONFIG, CENTER_INFO, CENTERS, CENTER_COLORS, ANIMATION_CONFIG
from data.loader import (
    load_all_events,
    load_riding_paths,
    load_staff_movements,
    get_summary_stats,
)
from components.map_renderer import (
    create_animated_map,
    create_static_snapshot,
)

# 다크 테마 CSS
st.markdown("""
<style>
    /* 다크 테마 기본 */
    .stApp {
        background-color: #0a0a1a;
        color: #e0e0e0;
    }

    /* 모든 텍스트 기본 색상 */
    .stApp p, .stApp span, .stApp div, .stApp label {
        color: #e0e0e0 !important;
    }

    /* 사이드바 */
    [data-testid="stSidebar"] {
        background-color: #0f0f23;
        border-right: 1px solid #1a1a3e;
    }

    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] div {
        color: #e0e0e0 !important;
    }

    /* 메트릭 카드 */
    [data-testid="stMetricValue"] {
        font-family: 'Consolas', monospace;
        color: #00ffff !important;
    }

    [data-testid="stMetricLabel"] {
        color: #aaa !important;
    }

    [data-testid="stMetricDelta"] {
        color: #ff6b6b !important;
    }

    /* 헤더 */
    h1, h2, h3, h4 {
        color: #00ffff !important;
        font-family: 'Consolas', monospace;
    }

    /* 일반 텍스트 */
    p, span, label {
        color: #e0e0e0;
    }

    /* 버튼 */
    .stButton > button {
        background-color: #1a1a3e;
        color: #00ffff !important;
        border: 1px solid #00ffff;
        font-family: 'Consolas', monospace;
    }

    .stButton > button:hover {
        background-color: #00ffff;
        color: #0a0a1a !important;
    }

    /* 슬라이더 */
    .stSlider > div > div > div {
        background-color: #00ffff !important;
    }

    .stSlider label, .stSlider span {
        color: #e0e0e0 !important;
    }

    /* 셀렉트박스 */
    .stSelectbox > div > div {
        background-color: #1a1a3e;
        color: #e0e0e0 !important;
    }

    .stSelectbox label {
        color: #e0e0e0 !important;
    }

    /* 체크박스 */
    .stCheckbox label span {
        color: #e0e0e0 !important;
    }

    /* 라디오 버튼 */
    .stRadio label span {
        color: #e0e0e0 !important;
    }

    /* 날짜 입력 */
    .stDateInput label {
        color: #e0e0e0 !important;
    }

    /* 경고/정보 박스 */
    .stAlert {
        background-color: #1a1a3e !important;
        color: #e0e0e0 !important;
    }

    [data-testid="stNotification"] {
        background-color: #1a1a3e !important;
    }

    [data-testid="stNotification"] p {
        color: #e0e0e0 !important;
    }

    /* Expander */
    .streamlit-expanderHeader {
        color: #00ffff !important;
        background-color: #1a1a3e !important;
    }

    .streamlit-expanderContent {
        background-color: #0f0f23 !important;
        color: #e0e0e0 !important;
    }

    /* 데이터프레임 */
    .stDataFrame {
        color: #e0e0e0 !important;
    }

    /* 스피너 */
    .stSpinner > div {
        color: #00ffff !important;
    }

    /* 정보 박스 */
    .info-box {
        background: rgba(0, 255, 255, 0.1);
        border: 1px solid #00ffff;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
        font-family: 'Consolas', monospace;
        color: #e0e0e0;
    }

    /* 범례 아이템 */
    .legend-item {
        display: flex;
        align-items: center;
        margin: 5px 0;
        font-size: 12px;
        color: #e0e0e0 !important;
    }

    .legend-dot {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        margin-right: 8px;
        box-shadow: 0 0 8px currentColor;
    }

    /* 구분선 */
    hr {
        border-color: #333 !important;
    }

    /* 마크다운 텍스트 */
    .stMarkdown {
        color: #e0e0e0 !important;
    }

    /* 서브헤더 */
    .stSubheader {
        color: #00ffff !important;
    }
</style>
""", unsafe_allow_html=True)


def main():
    # 헤더
    st.markdown("""
    <div style="text-align: center; padding: 20px 0;">
        <h1 style="margin: 0; font-size: 2.5em; color: #00ffff;">📡 서비스 흐름 관제</h1>
        <p style="color: #888; font-family: 'Consolas', monospace;">
            Service Flow Control Tower
        </p>
    </div>
    """, unsafe_allow_html=True)

    # ===== 사이드바 =====
    with st.sidebar:
        st.markdown("## ⚙️ 조회 설정")

        # 날짜 선택
        target_date = st.date_input(
            "📅 날짜",
            value=datetime.now().date() - timedelta(days=1),
            max_value=datetime.now().date(),
        )

        st.markdown("---")

        # 센터/권역 필터 (센터 > 권역 순서)
        st.markdown("### 📍 위치 필터")

        # 센터 선택
        center = st.selectbox("센터", CENTERS, index=0)
        center_filter = None if center == "전체" else center

        # 센터 선택에 따른 권역 목록
        if center == "전체":
            available_regions = ["전체"]
            for info in CENTER_INFO.values():
                available_regions.extend(info['regions'])
            available_regions = ["전체"] + sorted(list(set(available_regions) - {"전체"}))
        else:
            available_regions = ["전체"] + CENTER_INFO[center]['regions']

        region = st.selectbox("권역", available_regions, index=0)
        region_filter = None if region == "전체" else region

        st.markdown("---")

        # 이벤트 필터
        st.markdown("### 🔘 이벤트 필터")
        show_riding = st.checkbox("🛴 라이딩", value=True)
        show_app = st.checkbox("📍 앱 오픈", value=False)  # 기본 off (데이터 많음)
        show_ops = st.checkbox("🔧 운영 작업", value=True)

        st.markdown("---")

        # 시간대 필터
        st.markdown("### ⏰ 시간대")
        time_range = st.slider(
            "시간 범위",
            min_value=0,
            max_value=24,
            value=(6, 22),
            format="%d시",
        )

        st.markdown("---")

        # 뷰 모드
        st.markdown("### 🖥️ 뷰 모드")
        view_mode = st.radio(
            "표시 방식",
            ["🎬 애니메이션", "📸 스냅샷"],
            index=0,
            horizontal=True,
        )

        if view_mode == "📸 스냅샷":
            snapshot_hour = st.slider(
                "시점 (시)",
                min_value=time_range[0],
                max_value=time_range[1],
                value=time_range[0],
            )
            snapshot_minute = st.slider(
                "시점 (분)",
                min_value=0,
                max_value=59,
                value=0,
                step=5,
            )
            time_window = st.slider(
                "표시 범위 (분)",
                min_value=5,
                max_value=60,
                value=30,
                step=5,
            )

    # ===== 데이터 로드 =====
    with st.spinner("📡 데이터 수신 중..."):
        events_df = load_all_events(
            target_date=str(target_date),
            region=region_filter,
            center=center_filter,
            include_riding=show_riding,
            include_app=show_app,
            include_ops=show_ops,
            hour_start=time_range[0],
            hour_end=time_range[1],
        )

        paths_df = None
        staff_df = None

        if show_riding:
            paths_df = load_riding_paths(str(target_date), region_filter)

        if show_ops:
            staff_df = load_staff_movements(str(target_date), center_filter)

    # ===== 통계 대시보드 =====
    stats = get_summary_stats(events_df)

    st.markdown("### 📊 실시간 통계")

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric(
            "총 이벤트",
            f"{stats['total_events']:,}",
            help="조회된 전체 이벤트 수",
        )

    with col2:
        st.metric(
            "🚴 라이딩",
            f"{stats['riding_starts']:,}",
            help="라이딩 시작 횟수",
        )

    with col3:
        st.metric(
            "✅ 앱→라이딩",
            f"{stats['app_converted']:,}",
            help="앱 오픈 후 라이딩 전환",
        )

    with col4:
        st.metric(
            "❌ 기기 부족",
            f"{stats['app_no_bike']:,}",
            delta=f"-{stats['app_no_bike']}" if stats['app_no_bike'] > 0 else None,
            delta_color="inverse",
            help="기기 없어서 실패한 앱 오픈",
        )

    with col5:
        st.metric(
            "🔋 배터리 교체",
            f"{stats['battery_swaps']:,}",
            help="배터리 교체 작업",
        )

    with col6:
        st.metric(
            "🚚 재배치",
            f"{stats['rebalance_deploys']:,}",
            help="재배치 완료 작업",
        )

    st.markdown("---")

    # ===== 지도 렌더링 =====
    if events_df.empty and (paths_df is None or paths_df.empty):
        st.warning("⚠️ 해당 조건에 맞는 데이터가 없습니다.")
        st.info("필터 조건을 변경해보세요. (시간대, 권역, 이벤트 타입 등)")
        return

    # 지도 중심 계산 (필터 기준으로 설정)
    if center_filter and center_filter in CENTER_INFO:
        # 센터가 선택된 경우 해당 센터 중심으로
        map_center = tuple(CENTER_INFO[center_filter]['center'])
    elif not events_df.empty:
        # 데이터 기반 중심
        map_center = (events_df['lat'].mean(), events_df['lng'].mean())
    elif paths_df is not None and not paths_df.empty:
        map_center = (paths_df['start_lat'].mean(), paths_df['start_lng'].mean())
    else:
        map_center = (37.5665, 126.9780)  # 서울 기본

    st.markdown(f"### 🗺️ {target_date} 서비스 흐름")

    if view_mode == "🎬 애니메이션":
        # 애니메이션 모드
        st.info("⏯️ 지도 하단의 타임라인 슬라이더로 시간을 조절하세요. 재생 버튼으로 자동 재생됩니다.")

        m = create_animated_map(
            events_df=events_df,
            paths_df=paths_df,
            staff_df=staff_df,
            center=map_center,
        )

        st_folium(m, width=1400, height=700, returned_objects=[])

    else:
        # 스냅샷 모드
        snapshot_time = datetime.combine(
            target_date,
            datetime.min.time().replace(hour=snapshot_hour, minute=snapshot_minute)
        )

        st.info(f"📸 **{snapshot_time.strftime('%H:%M')}** 시점 기준 (최근 {time_window}분간 이벤트 표시)")

        m = create_static_snapshot(
            events_df=events_df,
            current_time=snapshot_time,
            time_window_minutes=time_window,
            paths_df=paths_df,
            staff_df=staff_df,
            center=map_center,
        )

        st_folium(m, width=1400, height=700, returned_objects=[])

    # ===== 범례 (사이드바 외부) =====
    with st.expander("📋 이벤트 범례", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**유저 이벤트**")
            for evt_type in ['riding_start', 'riding_end', 'app_converted', 'app_accessible', 'app_no_bike']:
                cfg = EVENT_CONFIG.get(evt_type, {})
                st.markdown(f"""
                <div class="legend-item">
                    <div class="legend-dot" style="background: {cfg.get('color', '#fff')};"></div>
                    {cfg.get('icon', '')} {cfg.get('label', evt_type)}
                </div>
                """, unsafe_allow_html=True)

        with col2:
            st.markdown("**운영 이벤트**")
            for evt_type in ['battery_swap', 'rebalance_deploy', 'broken_collect', 'field_fix', 'repair_deploy']:
                cfg = EVENT_CONFIG.get(evt_type, {})
                st.markdown(f"""
                <div class="legend-item">
                    <div class="legend-dot" style="background: {cfg.get('color', '#fff')};"></div>
                    {cfg.get('icon', '')} {cfg.get('label', evt_type)}
                </div>
                """, unsafe_allow_html=True)

    # ===== 데이터 테이블 =====
    with st.expander("📋 이벤트 상세 데이터", expanded=False):
        if not events_df.empty:
            display_df = events_df[[
                'event_time', 'event_type', 'bike_sn', 'staff_name',
                'center_name', 'region', 'lat', 'lng'
            ]].copy()
            display_df['event_time'] = display_df['event_time'].dt.strftime('%H:%M:%S')
            st.dataframe(display_df, use_container_width=True, height=400)
        else:
            st.info("표시할 데이터가 없습니다.")


if __name__ == "__main__":
    main()
