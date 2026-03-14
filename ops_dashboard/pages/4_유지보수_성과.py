"""
유지보수 성과 모니터링 대시보드
- 5개 In-house센터 + 4개 CJ센터 비교
- 4가지 KPI: Time Efficiency, Cost Efficiency, Quality Efficiency, Processing Rate
- 원본 테이블 기반 (Repairs, Repair_parts, daily_bike)
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from utils.bigquery import run_query
from utils.sidebar_style import apply_sidebar_style

# =============================================================================
# 페이지 설정 및 스타일
# =============================================================================

st.set_page_config(
    page_title="유지보수 성과 모니터링",
    page_icon="🔧",
    layout="wide"
)
apply_sidebar_style()

# =============================================================================
# 로그인 및 권한 체크
# =============================================================================
if "user" not in st.session_state:
    st.warning("로그인이 필요합니다. 메인 페이지에서 로그인하세요.")
    st.stop()

allowed_centers = st.session_state.get("allowed_centers", [])
if "전체" not in allowed_centers:
    st.error("이 페이지는 관리자만 접근 가능합니다.")
    st.stop()

# 커스텀 CSS - 폰트 및 스타일 통일
st.markdown("""
<style>
    /* 전체 폰트 통일 */
    html, body, [class*="css"] {
        font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    }

    /* 제목 스타일 통일 */
    .main-title {
        font-size: 2rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }

    .section-title {
        font-size: 1.5rem;
        font-weight: 600;
        color: #374151;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e5e7eb;
    }

    .subsection-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #4b5563;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
    }

    /* 메트릭 카드 스타일 */
    [data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
    }

    [data-testid="stMetricLabel"] {
        font-size: 0.9rem;
        font-weight: 500;
    }

    /* 탭 스타일 */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        font-size: 1rem;
        font-weight: 500;
        padding: 0.75rem 1.5rem;
    }

    /* 데이터프레임 스타일 */
    .stDataFrame {
        font-size: 0.9rem;
    }

    /* 사이드바 스타일 */
    [data-testid="stSidebar"] {
        background-color: #f8fafc;
    }

    [data-testid="stSidebar"] .stMarkdown {
        font-size: 0.9rem;
    }

    /* expander 스타일 */
    .streamlit-expanderHeader {
        font-size: 0.95rem;
        font-weight: 500;
        color: #6b7280;
    }

    /* 구분선 */
    hr {
        margin: 1.5rem 0;
        border: none;
        border-top: 1px solid #e5e7eb;
    }
</style>
""", unsafe_allow_html=True)

# Plotly 템플릿 설정 - 차트 폰트 통일
CHART_TEMPLATE = {
    'layout': {
        'font': {'family': 'Pretendard, -apple-system, sans-serif', 'size': 12},
        'title': {'font': {'size': 14, 'color': '#374151'}},
        'xaxis': {'title': {'font': {'size': 12}}, 'tickfont': {'size': 11}},
        'yaxis': {'title': {'font': {'size': 12}}, 'tickfont': {'size': 11}},
        'legend': {'font': {'size': 11}},
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
    }
}


# =============================================================================
# 상수 정의
# =============================================================================

CENTERS = {
    "In-house": ["Center_West", "Center_North", "Center_East", "Center_Central", "Center_South"],
    "Outsourced": ["Partner_Seoul", "Partner_Daejeon", "Partner_Ansan", "Partner_Gwacheon"]
}
ALL_CENTERS = CENTERS["In-house"] + CENTERS["Outsourced"]
CJ_CENTERS = set(CENTERS["Outsourced"])
CACHE_VERSION = "v5"

# daily_bike 테이블의 센터명 매핑 (Repairs 테이블과 다름)
DAILY_BIKE_CENTER_MAPPING = {
    'Center_West': 'Center_West',
    'Center_North': 'Center_North',
    'Center_East': 'Center_East',
    'Center_Central': 'Center_Central',
    'Center_South': 'Center_South',
    'Partner_Seoul': 'Partner_Seoul',
    'Partner_Daejeon': 'Partner_Daejeon',
    'Partner_Ansan': 'Partner_Ansan',
    'Partner_Gwacheon': 'Partner_Gwacheon',
}

# KPI 가중치 설정 (수리율 30% : 시간 30% : 비용 30% : 품질 10%)
KPI_WEIGHTS = {
    'processing': 0.30,  # 수리율 (100% - 적체율)
    'time': 0.30,        # 시간효율
    'cost': 0.30,        # 비용효율
    'quality': 0.10      # 품질효율
}

# 색상 팔레트
COLORS = {
    'In-house': '#3b82f6',      # 파란색
    'Outsourced': '#f97316',    # 주황색
    'average': '#ef4444',   # 빨간색 (평균선)
    'best': '#10b981',      # 초록색 (최고)
    'worst': '#f43f5e',     # 분홍색 (최저)
}


# =============================================================================
# 데이터 로드 함수
# =============================================================================

@st.cache_data(ttl=600)
def load_time_efficiency_data(start_date: str, end_date: str, _version: str = CACHE_VERSION) -> pd.DataFrame:
    """KPI 1: Time Efficiency - 수리 시간 효율성"""
    query = f"""
    SELECT
        center_name,
        COUNT(DISTINCT repair_id) as repair_count,
        ROUND(AVG(total_duration_min), 1) as avg_duration,
        ROUND(APPROX_QUANTILES(total_duration_min, 100)[OFFSET(50)], 1) as median_duration,
        ROUND(STDDEV(total_duration_min), 1) as stddev_duration,
        ROUND(MIN(total_duration_min), 1) as min_duration,
        ROUND(MAX(total_duration_min), 1) as max_duration
    FROM `sheets.repairs`
    WHERE DATE(created_at) BETWEEN '{start_date}' AND '{end_date}'
        AND total_duration_min BETWEEN 1 AND 120
        AND center_name IN ({','.join([f"'{c}'" for c in ALL_CENTERS])})
    GROUP BY center_name
    ORDER BY avg_duration ASC
    """
    return run_query(query)


@st.cache_data(ttl=600)
def load_time_distribution_data(start_date: str, end_date: str, _version: str = CACHE_VERSION) -> pd.DataFrame:
    """시간 효율성 분포 데이터 (박스플롯용)"""
    query = f"""
    SELECT
        center_name,
        total_duration_min as duration
    FROM `sheets.repairs`
    WHERE DATE(created_at) BETWEEN '{start_date}' AND '{end_date}'
        AND total_duration_min BETWEEN 1 AND 120
        AND center_name IN ({','.join([f"'{c}'" for c in ALL_CENTERS])})
    """
    return run_query(query)


@st.cache_data(ttl=600)
def load_cost_efficiency_data(start_date: str, end_date: str, _version: str = CACHE_VERSION) -> pd.DataFrame:
    """KPI 2: Cost Efficiency - 부품비 효율성"""
    query = f"""
    SELECT
        r.center_name,
        COUNT(DISTINCT r.repair_id) as repair_count,
        ROUND(AVG(p.total_parts_cost), 0) as avg_parts_cost,
        ROUND(APPROX_QUANTILES(p.total_parts_cost, 100)[OFFSET(50)], 0) as median_parts_cost,
        ROUND(AVG(p.part_item_cnt), 1) as avg_part_types,
        SUM(p.total_parts_cost) as total_parts_cost
    FROM `sheets.repairs` r
    LEFT JOIN (
        SELECT
            repair_id,
            SUM(parts_cost_sum) as total_parts_cost,
            COUNT(DISTINCT Item) as part_item_cnt
        FROM `sheets.repair_parts`
        GROUP BY repair_id
    ) p ON r.repair_id = p.repair_id
    WHERE DATE(r.created_at) BETWEEN '{start_date}' AND '{end_date}'
        AND r.center_name IN ({','.join([f"'{c}'" for c in ALL_CENTERS])})
    GROUP BY r.center_name
    ORDER BY avg_parts_cost ASC
    """
    return run_query(query)


@st.cache_data(ttl=600)
def load_cost_scatter_data(start_date: str, end_date: str, _version: str = CACHE_VERSION) -> pd.DataFrame:
    """비용 효율성 산점도 데이터"""
    query = f"""
    SELECT
        r.center_name,
        r.repair_id,
        COALESCE(p.total_parts_cost, 0) as parts_cost,
        COALESCE(p.part_item_cnt, 0) as part_count
    FROM `sheets.repairs` r
    LEFT JOIN (
        SELECT
            repair_id,
            SUM(parts_cost_sum) as total_parts_cost,
            COUNT(DISTINCT Item) as part_item_cnt
        FROM `sheets.repair_parts`
        GROUP BY repair_id
    ) p ON r.repair_id = p.repair_id
    WHERE DATE(r.created_at) BETWEEN '{start_date}' AND '{end_date}'
        AND r.center_name IN ({','.join([f"'{c}'" for c in ALL_CENTERS])})
    """
    return run_query(query)


@st.cache_data(ttl=600)
def load_quality_efficiency_data(start_date: str, end_date: str, _version: str = CACHE_VERSION) -> pd.DataFrame:
    """KPI 3: Quality Efficiency - 재수리 간격"""
    query = f"""
    WITH repair_sequence AS (
        SELECT
            center_name,
            bike_sn,
            created_at,
            LEAD(created_at) OVER (PARTITION BY bike_sn ORDER BY created_at) as next_repair_at
        FROM `sheets.repairs`
        WHERE DATE(created_at) BETWEEN '{start_date}' AND '{end_date}'
            AND center_name IN ({','.join([f"'{c}'" for c in ALL_CENTERS])})
    )
    SELECT
        center_name,
        COUNT(*) as sample_count,
        ROUND(AVG(DATE_DIFF(DATE(next_repair_at), DATE(created_at), DAY)), 1) as avg_days_to_next,
        ROUND(APPROX_QUANTILES(DATE_DIFF(DATE(next_repair_at), DATE(created_at), DAY), 100)[OFFSET(50)], 1) as median_days_to_next,
        ROUND(MIN(DATE_DIFF(DATE(next_repair_at), DATE(created_at), DAY)), 1) as min_days,
        ROUND(MAX(DATE_DIFF(DATE(next_repair_at), DATE(created_at), DAY)), 1) as max_days
    FROM repair_sequence
    WHERE next_repair_at IS NOT NULL
        AND DATE_DIFF(DATE(next_repair_at), DATE(created_at), DAY) > 0
    GROUP BY center_name
    ORDER BY avg_days_to_next DESC
    """
    return run_query(query)


@st.cache_data(ttl=600)
def load_quality_distribution_data(start_date: str, end_date: str, _version: str = CACHE_VERSION) -> pd.DataFrame:
    """품질 효율성 분포 데이터"""
    query = f"""
    WITH repair_sequence AS (
        SELECT
            center_name,
            bike_sn,
            created_at,
            LEAD(created_at) OVER (PARTITION BY bike_sn ORDER BY created_at) as next_repair_at
        FROM `sheets.repairs`
        WHERE DATE(created_at) BETWEEN '{start_date}' AND '{end_date}'
            AND center_name IN ({','.join([f"'{c}'" for c in ALL_CENTERS])})
    )
    SELECT
        center_name,
        DATE_DIFF(DATE(next_repair_at), DATE(created_at), DAY) as days_to_next
    FROM repair_sequence
    WHERE next_repair_at IS NOT NULL
        AND DATE_DIFF(DATE(next_repair_at), DATE(created_at), DAY) > 0
        AND DATE_DIFF(DATE(next_repair_at), DATE(created_at), DAY) <= 90
    """
    return run_query(query)


@st.cache_data(ttl=600)
def load_daily_trend_data(start_date: str, end_date: str, _version: str = CACHE_VERSION) -> pd.DataFrame:
    """일별 추이 데이터"""
    query = f"""
    SELECT
        DATE(r.created_at) as work_date,
        r.center_name,
        COUNT(DISTINCT r.repair_id) as repair_count,
        ROUND(AVG(r.total_duration_min), 1) as avg_duration,
        ROUND(SUM(p.total_parts_cost), 0) as total_cost,
        ROUND(AVG(p.total_parts_cost), 0) as avg_cost
    FROM `sheets.repairs` r
    LEFT JOIN (
        SELECT repair_id, SUM(parts_cost_sum) as total_parts_cost
        FROM `sheets.repair_parts`
        GROUP BY repair_id
    ) p ON r.repair_id = p.repair_id
    WHERE r.total_duration_min BETWEEN 1 AND 120
        AND DATE(r.created_at) BETWEEN '{start_date}' AND '{end_date}'
        AND r.center_name IN ({','.join([f"'{c}'" for c in ALL_CENTERS])})
    GROUP BY work_date, r.center_name
    ORDER BY work_date, r.center_name
    """
    return run_query(query)


@st.cache_data(ttl=600)
def load_processing_rate_data(start_date: str, end_date: str, _version: str = CACHE_VERSION) -> pd.DataFrame:
    """KPI 4: Processing Rate - 수리율 (100% - 적체율)

    수리율 = 100% - (관리중 기기 / 전체 기기 * 100%)
    - 높을수록 좋음 (적체 없이 빠르게 처리)
    """
    daily_bike_centers = list(DAILY_BIKE_CENTER_MAPPING.keys())
    centers_str = ", ".join([f"'{c}'" for c in daily_bike_centers])

    query = f"""
    WITH daily_stats AS (
        SELECT
            date,
            center_name,
            SUM(CASE WHEN bike_status_tf = '관리중' THEN bike_cnt ELSE 0 END) as in_repair,
            SUM(bike_cnt) as total
        FROM `management.daily_bike`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
          AND center_name IN ({centers_str})
          AND is_usable = TRUE
        GROUP BY date, center_name
    )
    SELECT
        center_name as raw_center_name,
        ROUND(AVG(in_repair), 1) as avg_in_repair,
        ROUND(AVG(total), 1) as avg_total,
        ROUND(100 - AVG(in_repair / total * 100), 1) as processing_rate
    FROM daily_stats
    WHERE total > 0
    GROUP BY center_name
    ORDER BY processing_rate DESC
    """
    df = run_query(query)

    # 센터명 매핑 적용
    df['center_name'] = df['raw_center_name'].map(DAILY_BIKE_CENTER_MAPPING)
    return df[['center_name', 'avg_in_repair', 'avg_total', 'processing_rate']]


# =============================================================================
# 유틸리티 함수
# =============================================================================

def add_center_type(df: pd.DataFrame) -> pd.DataFrame:
    """센터 유형 (In-house/CJ) 컬럼 추가"""
    df = df.copy()
    df['center_type'] = df['center_name'].apply(lambda x: 'Outsourced' if x in CJ_CENTERS else 'In-house')
    return df


def apply_chart_style(fig: go.Figure) -> go.Figure:
    """차트에 통일된 스타일 적용"""
    fig.update_layout(
        font=dict(family='Pretendard, -apple-system, sans-serif', size=12),
        title_font=dict(size=14, color='#374151'),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(gridcolor='#f3f4f6', title_font=dict(size=12)),
        yaxis=dict(gridcolor='#f3f4f6', title_font=dict(size=12)),
        legend=dict(font=dict(size=11)),
        margin=dict(t=50, b=50, l=50, r=30)
    )
    return fig


def create_kpi_bar_chart(df: pd.DataFrame, value_col: str, title: str,
                         lower_is_better: bool = True, unit: str = "") -> go.Figure:
    """KPI 바 차트 생성"""
    df = add_center_type(df)
    colors = [COLORS['In-house'] if t == 'In-house' else COLORS['Outsourced'] for t in df['center_type']]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df['center_name'],
        y=df[value_col],
        marker_color=colors,
        text=[f"{v:,.1f}{unit}" for v in df[value_col]],
        textposition='outside',
        textfont=dict(size=11)
    ))

    avg_val = df[value_col].mean()
    fig.add_hline(
        y=avg_val,
        line_dash="dash",
        line_color=COLORS['average'],
        annotation_text=f"평균: {avg_val:,.1f}{unit}",
        annotation_position="top right",
        annotation_font=dict(size=11, color=COLORS['average'])
    )

    fig.update_layout(
        title=dict(text=title, font=dict(size=14)),
        xaxis_title="센터",
        yaxis_title=unit if unit else "값",
        showlegend=False,
        height=380
    )
    return apply_chart_style(fig)


def create_group_comparison_chart(df: pd.DataFrame, value_col: str, title: str, unit: str = "") -> go.Figure:
    """In-house vs CJ 그룹 비교 차트"""
    df = add_center_type(df)
    summary = df.groupby('center_type').agg({value_col: 'mean'}).reset_index()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=summary['center_type'],
        y=summary[value_col],
        marker_color=[COLORS['In-house'], COLORS['Outsourced']],
        text=[f"{v:,.1f}{unit}" for v in summary[value_col]],
        textposition='outside',
        textfont=dict(size=11, color='#374151')
    ))

    # y축 범위를 여유있게 설정하여 텍스트가 잘리지 않도록 함
    max_val = summary[value_col].max()
    fig.update_layout(
        title=dict(text=title, font=dict(size=13)),
        xaxis_title="",
        yaxis_title="",
        yaxis=dict(range=[0, max_val * 1.25]),  # 상단 25% 여유 공간
        showlegend=False,
        height=280,
        margin=dict(t=40, b=30, l=40, r=20)
    )
    return apply_chart_style(fig)


def calculate_ranking_scores(time_df: pd.DataFrame, cost_df: pd.DataFrame,
                             quality_df: pd.DataFrame, processing_df: pd.DataFrame = None) -> pd.DataFrame:
    """4개 KPI 종합 점수 계산 (가중치: 수리율 30% + 시간 30% + 비용 30% + 품질 10%)"""
    def normalize_score(series, lower_is_better=True):
        if series.empty:
            return series
        min_val, max_val = series.min(), series.max()
        if max_val == min_val:
            return pd.Series([50] * len(series), index=series.index)
        if lower_is_better:
            return 100 - ((series - min_val) / (max_val - min_val) * 100)
        return (series - min_val) / (max_val - min_val) * 100

    ranking = pd.DataFrame({'center_name': ALL_CENTERS})

    # 시간효율 점수 (낮을수록 좋음)
    if not time_df.empty:
        time_scores = time_df[['center_name', 'avg_duration']].copy()
        time_scores['time_score'] = normalize_score(time_scores['avg_duration'], lower_is_better=True)
        ranking = ranking.merge(time_scores[['center_name', 'time_score', 'avg_duration']], on='center_name', how='left')

    # 비용효율 점수 (낮을수록 좋음)
    if not cost_df.empty:
        cost_scores = cost_df[['center_name', 'avg_parts_cost']].copy()
        cost_scores['cost_score'] = normalize_score(cost_scores['avg_parts_cost'], lower_is_better=True)
        ranking = ranking.merge(cost_scores[['center_name', 'cost_score', 'avg_parts_cost']], on='center_name', how='left')

    # 품질효율 점수 (높을수록 좋음)
    if not quality_df.empty:
        quality_scores = quality_df[['center_name', 'avg_days_to_next']].copy()
        quality_scores['quality_score'] = normalize_score(quality_scores['avg_days_to_next'], lower_is_better=False)
        ranking = ranking.merge(quality_scores[['center_name', 'quality_score', 'avg_days_to_next']], on='center_name', how='left')

    # 수리율 점수 (높을수록 좋음 = 적체가 적음)
    if processing_df is not None and not processing_df.empty:
        processing_scores = processing_df[['center_name', 'processing_rate']].copy()
        processing_scores['processing_score'] = normalize_score(processing_scores['processing_rate'], lower_is_better=False)
        ranking = ranking.merge(processing_scores[['center_name', 'processing_score', 'processing_rate']], on='center_name', how='left')

    # 가중치 적용 종합점수 계산
    ranking['total_score'] = (
        ranking.get('processing_score', pd.Series([0] * len(ranking))).fillna(0) * KPI_WEIGHTS['processing'] +
        ranking.get('time_score', pd.Series([0] * len(ranking))).fillna(0) * KPI_WEIGHTS['time'] +
        ranking.get('cost_score', pd.Series([0] * len(ranking))).fillna(0) * KPI_WEIGHTS['cost'] +
        ranking.get('quality_score', pd.Series([0] * len(ranking))).fillna(0) * KPI_WEIGHTS['quality']
    )

    ranking = ranking.sort_values('total_score', ascending=False)
    ranking['rank'] = range(1, len(ranking) + 1)

    ranking = add_center_type(ranking)
    return ranking


def create_radar_chart(ranking_df: pd.DataFrame) -> go.Figure:
    """레이더 차트 생성 (4축: 수리율, 시간, 비용, 품질)"""
    fig = go.Figure()
    categories = ['수리율(30%)', '시간효율(30%)', '비용효율(30%)', '품질효율(10%)']

    for _, row in ranking_df.iterrows():
        values = [
            row.get('processing_score', 0) or 0,
            row.get('time_score', 0) or 0,
            row.get('cost_score', 0) or 0,
            row.get('quality_score', 0) or 0
        ]
        values.append(values[0])

        color = COLORS['In-house'] if row['center_type'] == 'In-house' else COLORS['Outsourced']

        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=categories + [categories[0]],
            name=row['center_name'],
            line_color=color,
            fill='toself',
            opacity=0.25
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], tickfont=dict(size=10)),
            angularaxis=dict(tickfont=dict(size=11))
        ),
        showlegend=True,
        legend=dict(font=dict(size=10)),
        title=dict(text="센터별 KPI 레이더 차트 (4개 KPI)", font=dict(size=14)),
        height=450
    )
    return apply_chart_style(fig)


# =============================================================================
# 메인 앱
# =============================================================================

def main():
    # 타이틀
    st.markdown('<p class="main-title">🔧 유지보수 성과 모니터링 대시보드</p>', unsafe_allow_html=True)
    st.caption("In-house 5개 센터 + CJ 외주 4개 센터 비교 | 데이터 기반 성과 분석")

    # =========================================================================
    # Sidebar
    # =========================================================================
    st.sidebar.markdown("### 📅 분석 기간")

    default_end = datetime.now().date()
    default_start = default_end - timedelta(days=28)

    date_range = st.sidebar.date_input(
        "기간 선택",
        value=(default_start, default_end),
        max_value=default_end
    )

    if len(date_range) != 2:
        st.warning("시작일과 종료일을 모두 선택해주세요.")
        return

    start_date = date_range[0].strftime('%Y-%m-%d')
    end_date = date_range[1].strftime('%Y-%m-%d')

    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    **센터 구분**
    - 🔵 **In-house**: Center_West, Center_North, Center_East, Center_Central, Center_South
    - 🟠 **Outsourced**: Partner_Seoul, Partner_Daejeon, Partner_Ansan, Partner_Gwacheon
    """)

    if st.sidebar.button("🔄 데이터 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # =========================================================================
    # 데이터 로드
    # =========================================================================
    with st.spinner("데이터 로딩 중..."):
        try:
            time_df = load_time_efficiency_data(start_date, end_date)
            time_dist_df = load_time_distribution_data(start_date, end_date)
            cost_df = load_cost_efficiency_data(start_date, end_date)
            cost_scatter_df = load_cost_scatter_data(start_date, end_date)
            quality_df = load_quality_efficiency_data(start_date, end_date)
            quality_dist_df = load_quality_distribution_data(start_date, end_date)
            trend_df = load_daily_trend_data(start_date, end_date)
            processing_df = load_processing_rate_data(start_date, end_date)
        except Exception as e:
            st.error(f"데이터 로드 실패: {str(e)}")
            return

    # =========================================================================
    # Executive Summary
    # =========================================================================
    st.markdown('<p class="section-title">📊 Executive Summary</p>', unsafe_allow_html=True)

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        total_repairs = time_df['repair_count'].sum() if not time_df.empty else 0
        st.metric("총 수리 건수", f"{total_repairs:,}건")

    with col2:
        avg_processing = processing_df['processing_rate'].mean() if not processing_df.empty else 0
        st.metric("평균 수리율", f"{avg_processing:.1f}%")

    with col3:
        avg_duration = time_df['avg_duration'].mean() if not time_df.empty else 0
        st.metric("평균 수리시간", f"{avg_duration:.1f}분")

    with col4:
        avg_cost = cost_df['avg_parts_cost'].mean() if not cost_df.empty else 0
        st.metric("평균 부품비", f"₩{avg_cost:,.0f}")

    with col5:
        avg_quality = quality_df['avg_days_to_next'].mean() if not quality_df.empty else 0
        st.metric("평균 재수리간격", f"{avg_quality:.1f}일")

    # In-house vs CJ 비교
    st.markdown('<p class="subsection-title">In-house vs Outsourced 비교</p>', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if not processing_df.empty:
            fig = create_group_comparison_chart(processing_df, 'processing_rate', '수리율', '%')
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if not time_df.empty:
            fig = create_group_comparison_chart(time_df, 'avg_duration', '수리시간', '분')
            st.plotly_chart(fig, use_container_width=True)

    with col3:
        if not cost_df.empty:
            fig = create_group_comparison_chart(cost_df, 'avg_parts_cost', '부품비', '원')
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        if not quality_df.empty:
            fig = create_group_comparison_chart(quality_df, 'avg_days_to_next', '재수리간격', '일')
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # =========================================================================
    # KPI 탭
    # =========================================================================
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "⏱️ 시간효율",
        "💰 비용효율",
        "🎯 품질효율",
        "📈 추이분석",
        "🏆 센터랭킹"
    ])

    # -------------------------------------------------------------------------
    # Tab 1: 시간 효율성
    # -------------------------------------------------------------------------
    with tab1:
        st.markdown('<p class="section-title">⏱️ 시간 효율성</p>', unsafe_allow_html=True)

        with st.expander("ℹ️ KPI 정의 및 조건", expanded=False):
            st.markdown("""
            - **정의**: 평균 수리시간 (낮을수록 좋음)
            - **샘플 조건**: 1~120분 범위 내 유효 데이터
            - **측정 단위**: 분(minute)
            """)

        if time_df.empty:
            st.warning("해당 기간에 데이터가 없습니다.")
        else:
            col1, col2, col3 = st.columns(3)
            best, worst = time_df.iloc[0], time_df.iloc[-1]

            with col1:
                st.metric("🥇 최고 효율", best['center_name'], f"{best['avg_duration']:.1f}분")
            with col2:
                st.metric("🥉 최저 효율", worst['center_name'], f"{worst['avg_duration']:.1f}분")
            with col3:
                st.metric("최대-최소 차이", f"{worst['avg_duration'] - best['avg_duration']:.1f}분")

            col1, col2 = st.columns(2)

            with col1:
                fig = create_kpi_bar_chart(time_df, 'avg_duration', '센터별 평균 수리시간', True, '분')
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                if not time_dist_df.empty:
                    df = add_center_type(time_dist_df)
                    fig = px.box(df, x='center_name', y='duration', color='center_type',
                                 color_discrete_map={'In-house': COLORS['In-house'], 'Outsourced': COLORS['Outsourced']},
                                 title='센터별 수리시간 분포')
                    fig = apply_chart_style(fig)
                    fig.update_layout(height=380)
                    st.plotly_chart(fig, use_container_width=True)

            st.markdown('<p class="subsection-title">상세 데이터</p>', unsafe_allow_html=True)
            display_df = add_center_type(time_df)[['center_type', 'center_name', 'repair_count', 'avg_duration', 'median_duration', 'stddev_duration']]
            display_df.columns = ['유형', '센터', '수리건수', '평균(분)', '중앙값(분)', '표준편차']
            st.dataframe(display_df, hide_index=True, use_container_width=True)

    # -------------------------------------------------------------------------
    # Tab 2: 비용 효율성
    # -------------------------------------------------------------------------
    with tab2:
        st.markdown('<p class="section-title">💰 비용 효율성</p>', unsafe_allow_html=True)

        with st.expander("ℹ️ KPI 정의 및 조건", expanded=False):
            st.markdown("""
            - **정의**: 건당 평균 부품비 (낮을수록 좋음)
            - **보조 지표**: 평균 부품 종류 수, 총 부품비
            - **측정 단위**: 원(KRW)
            """)

        if cost_df.empty:
            st.warning("해당 기간에 데이터가 없습니다.")
        else:
            col1, col2, col3 = st.columns(3)
            best, worst = cost_df.iloc[0], cost_df.iloc[-1]

            with col1:
                st.metric("🥇 최고 효율", best['center_name'], f"₩{best['avg_parts_cost']:,.0f}")
            with col2:
                st.metric("🥉 최저 효율", worst['center_name'], f"₩{worst['avg_parts_cost']:,.0f}")
            with col3:
                st.metric("총 부품비", f"₩{cost_df['total_parts_cost'].sum():,.0f}")

            col1, col2 = st.columns(2)

            with col1:
                fig = create_kpi_bar_chart(cost_df, 'avg_parts_cost', '센터별 건당 평균 부품비', True, '원')
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                if not cost_scatter_df.empty:
                    df = add_center_type(cost_scatter_df)
                    agg_df = df.groupby('center_name').agg({
                        'parts_cost': 'mean', 'part_count': 'mean', 'center_type': 'first'
                    }).reset_index()

                    fig = px.scatter(agg_df, x='part_count', y='parts_cost', color='center_type', text='center_name',
                                     color_discrete_map={'In-house': COLORS['In-house'], 'Outsourced': COLORS['Outsourced']},
                                     title='부품 종류 수 vs 평균 부품비')
                    fig.update_traces(textposition='top center', marker=dict(size=12))
                    fig = apply_chart_style(fig)
                    fig.update_layout(height=380, xaxis_title='평균 부품 종류 수', yaxis_title='평균 부품비 (원)')
                    st.plotly_chart(fig, use_container_width=True)

            st.markdown('<p class="subsection-title">상세 데이터</p>', unsafe_allow_html=True)
            display_df = add_center_type(cost_df)
            display_df['avg_parts_cost'] = display_df['avg_parts_cost'].apply(lambda x: f"₩{x:,.0f}" if pd.notna(x) else "-")
            display_df['median_parts_cost'] = display_df['median_parts_cost'].apply(lambda x: f"₩{x:,.0f}" if pd.notna(x) else "-")
            display_df['total_parts_cost'] = display_df['total_parts_cost'].apply(lambda x: f"₩{x:,.0f}" if pd.notna(x) else "-")
            display_df = display_df[['center_type', 'center_name', 'repair_count', 'avg_parts_cost', 'median_parts_cost', 'avg_part_types', 'total_parts_cost']]
            display_df.columns = ['유형', '센터', '수리건수', '평균 부품비', '중앙값', '평균 부품종류', '총 부품비']
            st.dataframe(display_df, hide_index=True, use_container_width=True)

    # -------------------------------------------------------------------------
    # Tab 3: 품질 효율성
    # -------------------------------------------------------------------------
    with tab3:
        st.markdown('<p class="section-title">🎯 품질 효율성</p>', unsafe_allow_html=True)

        with st.expander("ℹ️ KPI 정의 및 조건", expanded=False):
            st.markdown("""
            - **정의**: 평균 재수리 간격 (길수록 좋음 = 수리 품질 우수)
            - **샘플 조건**: 동일 자전거(bike_sn), 재수리 간격 > 0일
            - **측정 단위**: 일(day)
            """)

        if quality_df.empty:
            st.warning("해당 기간에 데이터가 없습니다.")
        else:
            col1, col2, col3 = st.columns(3)
            best, worst = quality_df.iloc[0], quality_df.iloc[-1]

            with col1:
                st.metric("🥇 최고 품질", best['center_name'], f"{best['avg_days_to_next']:.1f}일")
            with col2:
                st.metric("🥉 최저 품질", worst['center_name'], f"{worst['avg_days_to_next']:.1f}일")
            with col3:
                st.metric("총 샘플 수", f"{quality_df['sample_count'].sum():,}건")

            col1, col2 = st.columns(2)

            with col1:
                fig = create_kpi_bar_chart(quality_df, 'avg_days_to_next', '센터별 평균 재수리 간격', False, '일')
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                if not quality_dist_df.empty:
                    df = add_center_type(quality_dist_df)
                    fig = px.histogram(df, x='days_to_next', color='center_type',
                                       color_discrete_map={'In-house': COLORS['In-house'], 'Outsourced': COLORS['Outsourced']},
                                       title='재수리 간격 분포 (90일 이내)', nbins=30, barmode='overlay', opacity=0.7)
                    fig = apply_chart_style(fig)
                    fig.update_layout(height=380, xaxis_title='재수리 간격 (일)', yaxis_title='빈도')
                    st.plotly_chart(fig, use_container_width=True)

            st.markdown('<p class="subsection-title">상세 데이터</p>', unsafe_allow_html=True)
            display_df = add_center_type(quality_df)[['center_type', 'center_name', 'sample_count', 'avg_days_to_next', 'median_days_to_next']]
            display_df.columns = ['유형', '센터', '샘플수', '평균(일)', '중앙값(일)']
            st.dataframe(display_df, hide_index=True, use_container_width=True)

    # -------------------------------------------------------------------------
    # Tab 4: 추이 분석
    # -------------------------------------------------------------------------
    with tab4:
        st.markdown('<p class="section-title">📈 추이 분석</p>', unsafe_allow_html=True)

        if trend_df.empty:
            st.warning("해당 기간에 데이터가 없습니다.")
        else:
            trend_df = add_center_type(trend_df)

            fig1 = px.line(trend_df, x='work_date', y='repair_count', color='center_name',
                           title='일별 수리 건수 추이',
                           labels={'work_date': '날짜', 'repair_count': '수리 건수', 'center_name': '센터'})
            fig1 = apply_chart_style(fig1)
            fig1.update_layout(height=380)
            st.plotly_chart(fig1, use_container_width=True)

            col1, col2 = st.columns(2)

            with col1:
                fig2 = px.line(trend_df, x='work_date', y='avg_duration', color='center_name',
                               title='일별 평균 수리시간 추이',
                               labels={'work_date': '날짜', 'avg_duration': '평균 수리시간(분)', 'center_name': '센터'})
                fig2 = apply_chart_style(fig2)
                fig2.update_layout(height=320)
                st.plotly_chart(fig2, use_container_width=True)

            with col2:
                fig3 = px.line(trend_df, x='work_date', y='avg_cost', color='center_name',
                               title='일별 평균 부품비 추이',
                               labels={'work_date': '날짜', 'avg_cost': '평균 부품비(원)', 'center_name': '센터'})
                fig3 = apply_chart_style(fig3)
                fig3.update_layout(height=320)
                st.plotly_chart(fig3, use_container_width=True)

            st.markdown('<p class="subsection-title">주간 집계</p>', unsafe_allow_html=True)
            trend_df['week'] = pd.to_datetime(trend_df['work_date']).dt.isocalendar().week
            weekly_df = trend_df.groupby(['week', 'center_type']).agg({
                'repair_count': 'sum', 'avg_duration': 'mean', 'avg_cost': 'mean'
            }).reset_index()

            fig4 = px.bar(weekly_df, x='week', y='repair_count', color='center_type',
                          color_discrete_map={'In-house': COLORS['In-house'], 'Outsourced': COLORS['Outsourced']},
                          title='주간 수리 건수 (In-house vs CJ)', barmode='group')
            fig4 = apply_chart_style(fig4)
            fig4.update_layout(height=320, xaxis_title='주차', yaxis_title='수리 건수')
            st.plotly_chart(fig4, use_container_width=True)

    # -------------------------------------------------------------------------
    # Tab 5: 센터 랭킹
    # -------------------------------------------------------------------------
    with tab5:
        st.markdown('<p class="section-title">🏆 센터 종합 랭킹</p>', unsafe_allow_html=True)

        with st.expander("ℹ️ 랭킹 산출 방식", expanded=False):
            st.markdown(f"""
            **4개 KPI 가중치 평가 시스템**

            | KPI | 가중치 | 설명 |
            |-----|--------|------|
            | 수리율 | **{int(KPI_WEIGHTS['processing']*100)}%** | 100% - 적체율 (높을수록 좋음) |
            | 시간효율 | **{int(KPI_WEIGHTS['time']*100)}%** | 평균 수리시간 (낮을수록 좋음) |
            | 비용효율 | **{int(KPI_WEIGHTS['cost']*100)}%** | 평균 부품비 (낮을수록 좋음) |
            | 품질효율 | **{int(KPI_WEIGHTS['quality']*100)}%** | 평균 재수리간격 (높을수록 좋음) |

            - 각 KPI는 **0~100점**으로 정규화
            - **종합점수** = 수리율×{int(KPI_WEIGHTS['processing']*100)}% + 시간×{int(KPI_WEIGHTS['time']*100)}% + 비용×{int(KPI_WEIGHTS['cost']*100)}% + 품질×{int(KPI_WEIGHTS['quality']*100)}%
            """)

        ranking_df = calculate_ranking_scores(time_df, cost_df, quality_df, processing_df)

        if ranking_df.empty or 'total_score' not in ranking_df.columns:
            st.warning("랭킹 계산에 필요한 데이터가 부족합니다.")
        else:
            col1, col2 = st.columns([2, 1])

            with col1:
                fig = create_radar_chart(ranking_df)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown('<p class="subsection-title">종합 순위</p>', unsafe_allow_html=True)
                for _, row in ranking_df.iterrows():
                    emoji = "🔵" if row['center_type'] == 'In-house' else "🟠"
                    medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(row['rank'], "")
                    st.markdown(f"{medal} **{int(row['rank'])}위** {emoji} {row['center_name']} ({row['total_score']:.1f}점)")

            st.markdown('<p class="subsection-title">상세 점수</p>', unsafe_allow_html=True)
            display_cols = ['rank', 'center_type', 'center_name', 'total_score']
            col_names = ['순위', '유형', '센터', '종합점수']

            # 수리율 점수 추가
            if 'processing_score' in ranking_df.columns:
                display_cols.extend(['processing_score', 'processing_rate'])
                col_names.extend(['수리율점수', '수리율(%)'])
            if 'time_score' in ranking_df.columns:
                display_cols.extend(['time_score', 'avg_duration'])
                col_names.extend(['시간점수', '평균시간(분)'])
            if 'cost_score' in ranking_df.columns:
                display_cols.extend(['cost_score', 'avg_parts_cost'])
                col_names.extend(['비용점수', '평균부품비'])
            if 'quality_score' in ranking_df.columns:
                display_cols.extend(['quality_score', 'avg_days_to_next'])
                col_names.extend(['품질점수', '재수리간격(일)'])

            display_df = ranking_df[display_cols].copy()
            display_df.columns = col_names

            for col in display_df.columns:
                if '점수' in col:
                    display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "-")
                elif '부품비' in col:
                    display_df[col] = display_df[col].apply(lambda x: f"₩{x:,.0f}" if pd.notna(x) else "-")
                elif '수리율(%)' in col:
                    display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "-")
                elif '시간' in col or '간격' in col:
                    display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "-")

            st.dataframe(display_df, hide_index=True, use_container_width=True)


if __name__ == "__main__":
    main()
