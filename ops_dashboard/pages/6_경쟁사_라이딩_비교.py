"""
경쟁사 vs 자사 라이딩 비교 분석 페이지

테이블 참조:
- service.tf_riding: 자사 라이딩 데이터
  - start_time (DATETIME), start_location (GEOGRAPHY), distance, h3_start_area_name
- service.tf_competition_riding: 경쟁사 라이딩 데이터
  - date (DATE), hour (INTEGER), location_before (STRING), distance, service_name, h3_start_area_name

분석 관점:
1. 지역별 점유율 비교 (권역 기반)
2. 시간대별 이용 패턴
3. 라이딩 거리 분포
4. 경쟁사별 비교
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import folium
from streamlit_folium import st_folium
from utils.bigquery import run_query
from utils.sidebar_style import apply_sidebar_style

# -------------------------------------------------------
# 설정
# -------------------------------------------------------
st.set_page_config(layout="wide", page_title="경쟁사 라이딩 비교")
apply_sidebar_style()

# -------------------------------------------------------
# 로그인 체크
# -------------------------------------------------------
if "user" not in st.session_state:
    st.warning("로그인이 필요합니다. 메인 페이지에서 로그인하세요.")
    st.stop()

# -------------------------------------------------------
# 관리자 권한 체크
# -------------------------------------------------------
allowed_centers = st.session_state.get("allowed_centers", [])
if "전체" not in allowed_centers:
    st.error("이 페이지는 관리자만 접근 가능합니다.")
    st.stop()

st.title("🔍 경쟁사 vs 자사 라이딩 비교 분석")

# -------------------------------------------------------
# 기간 설정 (최근 1주일 고정)
# -------------------------------------------------------
end_date = datetime.now().date() - timedelta(days=1)
start_date = end_date - timedelta(days=6)

st.info(f"📅 조회 기간: {start_date} ~ {end_date} (최근 1주일)")

# -------------------------------------------------------
# 쿼리 함수들 (캐싱 적용)
# -------------------------------------------------------

@st.cache_data(ttl=600)  # 10분 캐싱
def get_riding_comparison_by_area(start_date: str, end_date: str) -> pd.DataFrame:
    """권역 기반 라이딩 비교 데이터 조회"""
    query = f"""
    WITH our_riding AS (
        SELECT
            h3_start_area_name AS area_name,
            COUNT(*) AS our_count,
            AVG(distance) AS our_avg_distance
        FROM `bikeshare.service.rides`
        WHERE DATE(start_time) BETWEEN '{start_date}' AND '{end_date}'
            AND h3_start_area_name IS NOT NULL
        GROUP BY 1
    ),
    comp_riding AS (
        SELECT
            h3_start_area_name AS area_name,
            COUNT(*) AS comp_count,
            AVG(distance) AS comp_avg_distance
        FROM `bikeshare.service.competitor_rides`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND h3_start_area_name IS NOT NULL
        GROUP BY 1
    )
    SELECT
        COALESCE(o.area_name, c.area_name) AS area_name,
        COALESCE(o.our_count, 0) AS our_count,
        COALESCE(c.comp_count, 0) AS comp_count,
        COALESCE(o.our_avg_distance, 0) AS our_avg_distance,
        COALESCE(c.comp_avg_distance, 0) AS comp_avg_distance,
        SAFE_DIVIDE(COALESCE(o.our_count, 0), COALESCE(o.our_count, 0) + COALESCE(c.comp_count, 0)) AS our_share,
        COALESCE(o.our_count, 0) + COALESCE(c.comp_count, 0) AS total_count
    FROM our_riding o
    FULL OUTER JOIN comp_riding c ON o.area_name = c.area_name
    WHERE COALESCE(o.our_count, 0) + COALESCE(c.comp_count, 0) >= 10
    ORDER BY total_count DESC
    """
    return run_query(query)


@st.cache_data(ttl=600)
def get_riding_with_geo(start_date: str, end_date: str) -> pd.DataFrame:
    """지도 표시용 권역별 좌표 데이터"""
    query = f"""
    WITH our_riding AS (
        SELECT
            h3_start_area_name AS area_name,
            COUNT(*) AS our_count,
            AVG(distance) AS our_avg_distance,
            AVG(ST_Y(start_location)) AS lat,
            AVG(ST_X(start_location)) AS lng
        FROM `bikeshare.service.rides`
        WHERE DATE(start_time) BETWEEN '{start_date}' AND '{end_date}'
            AND h3_start_area_name IS NOT NULL
            AND start_location IS NOT NULL
        GROUP BY 1
    ),
    comp_riding AS (
        SELECT
            h3_start_area_name AS area_name,
            COUNT(*) AS comp_count,
            AVG(distance) AS comp_avg_distance
        FROM `bikeshare.service.competitor_rides`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND h3_start_area_name IS NOT NULL
        GROUP BY 1
    )
    SELECT
        o.area_name,
        o.our_count,
        COALESCE(c.comp_count, 0) AS comp_count,
        o.our_avg_distance,
        COALESCE(c.comp_avg_distance, 0) AS comp_avg_distance,
        SAFE_DIVIDE(o.our_count, o.our_count + COALESCE(c.comp_count, 0)) AS our_share,
        o.our_count + COALESCE(c.comp_count, 0) AS total_count,
        o.lat,
        o.lng
    FROM our_riding o
    LEFT JOIN comp_riding c ON o.area_name = c.area_name
    WHERE o.our_count + COALESCE(c.comp_count, 0) >= 10
    ORDER BY total_count DESC
    """
    return run_query(query)


@st.cache_data(ttl=600)
def get_hourly_pattern_comparison(start_date: str, end_date: str) -> pd.DataFrame:
    """시간대별 이용 패턴 비교"""
    query = f"""
    WITH our_hourly AS (
        SELECT
            EXTRACT(HOUR FROM start_time) AS hour,
            COUNT(*) AS our_count
        FROM `bikeshare.service.rides`
        WHERE DATE(start_time) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1
    ),
    comp_hourly AS (
        SELECT
            hour,
            COUNT(*) AS comp_count
        FROM `bikeshare.service.competitor_rides`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1
    )
    SELECT
        COALESCE(o.hour, c.hour) AS hour,
        COALESCE(o.our_count, 0) AS our_count,
        COALESCE(c.comp_count, 0) AS comp_count
    FROM our_hourly o
    FULL OUTER JOIN comp_hourly c ON o.hour = c.hour
    ORDER BY hour
    """
    return run_query(query)


@st.cache_data(ttl=600)
def get_daily_trend_comparison(start_date: str, end_date: str) -> pd.DataFrame:
    """일별 추이 비교"""
    query = f"""
    WITH our_daily AS (
        SELECT
            DATE(start_time) AS date,
            COUNT(*) AS our_count
        FROM `bikeshare.service.rides`
        WHERE DATE(start_time) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1
    ),
    comp_daily AS (
        SELECT
            date,
            COUNT(*) AS comp_count
        FROM `bikeshare.service.competitor_rides`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1
    )
    SELECT
        COALESCE(o.date, c.date) AS date,
        COALESCE(o.our_count, 0) AS our_count,
        COALESCE(c.comp_count, 0) AS comp_count
    FROM our_daily o
    FULL OUTER JOIN comp_daily c ON o.date = c.date
    ORDER BY date
    """
    return run_query(query)


@st.cache_data(ttl=600)
def get_area_comparison(start_date: str, end_date: str) -> pd.DataFrame:
    """권역별 비교"""
    query = f"""
    WITH our_area AS (
        SELECT
            h3_start_area_name AS area_name,
            COUNT(*) AS our_count,
            AVG(distance) AS our_avg_distance
        FROM `bikeshare.service.rides`
        WHERE DATE(start_time) BETWEEN '{start_date}' AND '{end_date}'
            AND h3_start_area_name IS NOT NULL
        GROUP BY 1
    ),
    comp_area AS (
        SELECT
            h3_start_area_name AS area_name,
            COUNT(*) AS comp_count,
            AVG(distance) AS comp_avg_distance
        FROM `bikeshare.service.competitor_rides`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND h3_start_area_name IS NOT NULL
        GROUP BY 1
    )
    SELECT
        COALESCE(o.area_name, c.area_name) AS area_name,
        COALESCE(o.our_count, 0) AS our_count,
        COALESCE(c.comp_count, 0) AS comp_count,
        COALESCE(o.our_avg_distance, 0) AS our_avg_distance,
        COALESCE(c.comp_avg_distance, 0) AS comp_avg_distance,
        SAFE_DIVIDE(COALESCE(o.our_count, 0), COALESCE(o.our_count, 0) + COALESCE(c.comp_count, 0)) AS our_share
    FROM our_area o
    FULL OUTER JOIN comp_area c ON o.area_name = c.area_name
    ORDER BY COALESCE(o.our_count, 0) + COALESCE(c.comp_count, 0) DESC
    """
    return run_query(query)


@st.cache_data(ttl=600)
def get_competitor_breakdown(start_date: str, end_date: str) -> pd.DataFrame:
    """경쟁사별 라이딩 현황"""
    query = f"""
    SELECT
        service_name AS competitor,
        COUNT(*) AS ride_count,
        AVG(distance) AS avg_distance,
        COUNT(DISTINCT h3_start_area_name) AS area_count
    FROM `bikeshare.service.competitor_rides`
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1
    ORDER BY ride_count DESC
    """
    return run_query(query)


@st.cache_data(ttl=3600)  # 1시간 캐싱
def get_summary_data(start_date: str, end_date: str) -> dict:
    """요약 데이터만 빠르게 조회"""
    query = f"""
    SELECT
        (SELECT COUNT(*) FROM `bikeshare.service.rides` WHERE DATE(start_time) BETWEEN '{start_date}' AND '{end_date}') AS our_total,
        (SELECT COUNT(*) FROM `bikeshare.service.competitor_rides` WHERE date BETWEEN '{start_date}' AND '{end_date}') AS comp_total
    """
    return run_query(query)


def create_comparison_map(df: pd.DataFrame, view_mode: str = "점유율") -> folium.Map:
    """비교 지도 생성 (권역 기반)"""
    # 중심점 계산
    center_lat, center_lng = 37.5, 127.0

    if not df.empty and 'lat' in df.columns and 'lng' in df.columns:
        valid_df = df[(df['lat'].notna()) & (df['lng'].notna())]
        if not valid_df.empty:
            center_lat = valid_df['lat'].mean()
            center_lng = valid_df['lng'].mean()

    m = folium.Map(location=[center_lat, center_lng], zoom_start=10)

    if df.empty:
        return m

    # 권역별 마커
    for _, row in df.iterrows():
        lat = row.get('lat')
        lng = row.get('lng')
        if pd.isna(lat) or pd.isna(lng):
            continue

        our_share = row.get('our_share', 0.5)
        if our_share is None:
            our_share = 0.5

        total_count = row.get('total_count', 0)

        # 색상 결정
        if view_mode == "점유율":
            if our_share > 0.6:
                color = 'blue'
            elif our_share < 0.4:
                color = 'red'
            else:
                color = 'purple'
        else:
            color = 'green'

        # 크기 결정 (라이딩 수 기반)
        radius = min(20, max(5, (total_count / df['total_count'].max()) * 20))

        folium.CircleMarker(
            location=[lat, lng],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.6,
            popup=f"""
                <b>권역:</b> {row['area_name']}<br>
                <b>자사:</b> {row['our_count']:,}건<br>
                <b>경쟁사:</b> {row['comp_count']:,}건<br>
                <b>자사 점유율:</b> {our_share*100:.1f}%
            """
        ).add_to(m)

    return m


# -------------------------------------------------------
# 요약 데이터 먼저 로딩
# -------------------------------------------------------
with st.spinner("요약 데이터 로딩 중..."):
    summary = get_summary_data(str(start_date), str(end_date))
    if not summary.empty:
        our_total_all = summary['our_total'].iloc[0]
        comp_total_all = summary['comp_total'].iloc[0]
        total_all = our_total_all + comp_total_all

        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1:
            st.metric("자사 총 라이딩", f"{our_total_all:,}건")
        with col_s2:
            st.metric("경쟁사 총 라이딩", f"{comp_total_all:,}건")
        with col_s3:
            if total_all > 0:
                st.metric("자사 점유율", f"{our_total_all/total_all*100:.1f}%")

# -------------------------------------------------------
# 탭 구성
# -------------------------------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs(["🗺️ 지도 비교", "📊 시간대별 패턴", "📈 일별 추이", "🏢 권역별 비교", "🏷️ 경쟁사별 현황"])

with tab1:
    st.subheader("권역별 점유율 비교")

    view_mode = st.radio("시각화 모드", ["점유율", "총 라이딩 수"], horizontal=True)

    with st.spinner("지도 데이터 로딩 중..."):
        geo_df = get_riding_with_geo(str(start_date), str(end_date))

    if not geo_df.empty:
        # 점유율 계산
        geo_df['our_share'] = geo_df['our_count'] / (geo_df['our_count'] + geo_df['comp_count'])
        geo_df['total_count'] = geo_df['our_count'] + geo_df['comp_count']

        # 자사 우세 권역 수
        our_dominant = len(geo_df[geo_df['our_share'] > 0.5])
        st.metric("자사 우세 권역", f"{our_dominant}개 / {len(geo_df)}개")

        # 범례
        st.markdown("""
        **범례** (점유율 모드):
        - 🔵 파랑: 자사 우세 (60% 이상)
        - 🔴 빨강: 경쟁사 우세 (40% 미만)
        - 🟣 보라: 경쟁 구간 (40~60%)
        - 원 크기: 총 라이딩 수 비례
        """)

        # 지도
        comparison_map = create_comparison_map(geo_df, view_mode)
        st_folium(comparison_map, width=None, height=600, use_container_width=True)

        # 상세 데이터
        with st.expander("📋 상세 데이터 보기"):
            display_df = geo_df[['area_name', 'our_count', 'comp_count', 'our_share', 'total_count']].copy()
            display_df['our_share'] = display_df['our_share'].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
            display_df.columns = ['권역', '자사', '경쟁사', '자사 점유율', '합계']
            st.dataframe(display_df, use_container_width=True)
    else:
        st.info("해당 기간에 데이터가 없습니다.")

with tab2:
    st.subheader("시간대별 이용 패턴 비교")

    with st.spinner("시간대별 데이터 로딩 중..."):
        hourly_df = get_hourly_pattern_comparison(str(start_date), str(end_date))

    if not hourly_df.empty:
        import altair as alt

        # 데이터 변환
        hourly_melted = hourly_df.melt(
            id_vars=['hour'],
            value_vars=['our_count', 'comp_count'],
            var_name='company',
            value_name='count'
        )
        hourly_melted['company'] = hourly_melted['company'].map({
            'our_count': '자사',
            'comp_count': '경쟁사'
        })

        chart = alt.Chart(hourly_melted).mark_line(point=True).encode(
            x=alt.X('hour:O', title='시간대'),
            y=alt.Y('count:Q', title='라이딩 수'),
            color=alt.Color('company:N', title='구분', scale=alt.Scale(
                domain=['자사', '경쟁사'],
                range=['#0066ff', '#ff3333']
            )),
            tooltip=['hour', 'company', 'count']
        ).properties(height=400)

        st.altair_chart(chart, use_container_width=True)

        # 피크 시간 비교
        col_p1, col_p2 = st.columns(2)
        with col_p1:
            our_peak = hourly_df.loc[hourly_df['our_count'].idxmax(), 'hour']
            st.metric("자사 피크 시간", f"{int(our_peak)}시")
        with col_p2:
            comp_peak = hourly_df.loc[hourly_df['comp_count'].idxmax(), 'hour']
            st.metric("경쟁사 피크 시간", f"{int(comp_peak)}시")
    else:
        st.info("해당 기간에 데이터가 없습니다.")

with tab3:
    st.subheader("일별 라이딩 추이 비교")

    with st.spinner("일별 데이터 로딩 중..."):
        daily_df = get_daily_trend_comparison(str(start_date), str(end_date))

    if not daily_df.empty:
        import altair as alt

        daily_df['date'] = pd.to_datetime(daily_df['date'])

        daily_melted = daily_df.melt(
            id_vars=['date'],
            value_vars=['our_count', 'comp_count'],
            var_name='company',
            value_name='count'
        )
        daily_melted['company'] = daily_melted['company'].map({
            'our_count': '자사',
            'comp_count': '경쟁사'
        })

        chart = alt.Chart(daily_melted).mark_line(point=True).encode(
            x=alt.X('date:T', title='날짜'),
            y=alt.Y('count:Q', title='라이딩 수'),
            color=alt.Color('company:N', title='구분', scale=alt.Scale(
                domain=['자사', '경쟁사'],
                range=['#0066ff', '#ff3333']
            )),
            tooltip=['date', 'company', 'count']
        ).properties(height=400)

        st.altair_chart(chart, use_container_width=True)

        # 일평균 비교
        col_d1, col_d2, col_d3 = st.columns(3)
        with col_d1:
            st.metric("자사 일평균", f"{daily_df['our_count'].mean():,.0f}건")
        with col_d2:
            st.metric("경쟁사 일평균", f"{daily_df['comp_count'].mean():,.0f}건")
        with col_d3:
            daily_df['share'] = daily_df['our_count'] / (daily_df['our_count'] + daily_df['comp_count'])
            st.metric("평균 점유율", f"{daily_df['share'].mean()*100:.1f}%")
    else:
        st.info("해당 기간에 데이터가 없습니다.")

with tab4:
    st.subheader("권역별 라이딩 비교")

    with st.spinner("권역별 데이터 로딩 중..."):
        area_df = get_area_comparison(str(start_date), str(end_date))

    if not area_df.empty:
        import altair as alt

        # 상위 20개 권역
        top_areas = area_df.head(20).copy()

        # 막대 차트
        area_melted = top_areas.melt(
            id_vars=['area_name'],
            value_vars=['our_count', 'comp_count'],
            var_name='company',
            value_name='count'
        )
        area_melted['company'] = area_melted['company'].map({
            'our_count': '자사',
            'comp_count': '경쟁사'
        })

        chart = alt.Chart(area_melted).mark_bar().encode(
            x=alt.X('count:Q', title='라이딩 수'),
            y=alt.Y('area_name:N', title='권역', sort='-x'),
            color=alt.Color('company:N', title='구분', scale=alt.Scale(
                domain=['자사', '경쟁사'],
                range=['#0066ff', '#ff3333']
            )),
            tooltip=['area_name', 'company', 'count']
        ).properties(height=500)

        st.altair_chart(chart, use_container_width=True)

        # 점유율 테이블
        st.markdown("### 권역별 점유율")
        display_area = area_df[['area_name', 'our_count', 'comp_count', 'our_share']].copy()
        display_area.columns = ['권역', '자사', '경쟁사', '자사 점유율']
        display_area['자사 점유율'] = display_area['자사 점유율'].apply(
            lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A"
        )
        st.dataframe(display_area, use_container_width=True, height=400)
    else:
        st.info("해당 기간에 데이터가 없습니다.")

with tab5:
    st.subheader("경쟁사별 라이딩 현황")

    with st.spinner("경쟁사별 데이터 로딩 중..."):
        comp_df = get_competitor_breakdown(str(start_date), str(end_date))

    if not comp_df.empty:
        import altair as alt

        our_total = our_total_all if 'our_total_all' in dir() else 0

        # 경쟁사 데이터 정리
        comp_display = comp_df[['competitor', 'ride_count', 'avg_distance']].copy()

        # 자사 데이터 추가
        all_companies = pd.concat([
            pd.DataFrame([{'competitor': '자사 (엘레클)', 'ride_count': our_total, 'avg_distance': 0}]),
            comp_display
        ], ignore_index=True)

        # 막대 차트
        chart = alt.Chart(all_companies).mark_bar().encode(
            x=alt.X('ride_count:Q', title='라이딩 수'),
            y=alt.Y('competitor:N', title='서비스', sort='-x'),
            color=alt.condition(
                alt.datum.competitor == '자사 (엘레클)',
                alt.value('#0066ff'),
                alt.value('#ff3333')
            ),
            tooltip=['competitor', 'ride_count', 'avg_distance']
        ).properties(height=300)

        st.altair_chart(chart, use_container_width=True)

        # 상세 테이블
        st.markdown("### 경쟁사 상세 정보")
        display_comp = comp_display[comp_display['competitor'] != '자사 (엘레클)'].copy()
        display_comp.columns = ['경쟁사', '라이딩 수', '평균 거리(m)']
        display_comp['평균 거리(m)'] = display_comp['평균 거리(m)'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
        st.dataframe(display_comp, use_container_width=True)

        # 점유율 파이차트
        st.markdown("### 시장 점유율")
        total_market = our_total + comp_display['ride_count'].sum()

        pie_data = pd.DataFrame({
            'company': ['자사'] + comp_display['competitor'].tolist(),
            'count': [our_total] + comp_display['ride_count'].tolist()
        })
        pie_data['share'] = pie_data['count'] / total_market * 100

        pie_chart = alt.Chart(pie_data).mark_arc().encode(
            theta=alt.Theta('count:Q'),
            color=alt.Color('company:N', title='서비스'),
            tooltip=['company', 'count', alt.Tooltip('share:Q', format='.1f', title='점유율(%)')]
        ).properties(height=300)

        st.altair_chart(pie_chart, use_container_width=True)
    else:
        st.info("해당 기간에 경쟁사 데이터가 없습니다.")

# -------------------------------------------------------
# 푸터
# -------------------------------------------------------
st.markdown("---")
st.caption("💡 데이터 기준: tf_riding (자사), tf_competition_riding (경쟁사)")
