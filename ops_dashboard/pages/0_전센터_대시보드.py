import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import altair as alt
from utils.bigquery import run_query
from utils.sidebar_style import apply_sidebar_style

# ------------------------------------------------------------
# MUST BE FIRST
# ------------------------------------------------------------
st.set_page_config(layout="wide")
apply_sidebar_style()

# ------------------------------------------------------------
# 🔐 로그인 체크
# ------------------------------------------------------------
if "user" not in st.session_state:
    st.warning("로그인이 필요합니다. 메인 페이지에서 로그인하세요.")
    st.stop()

user = st.session_state["user"]
allowed_centers = st.session_state["allowed_centers"]

# ------------------------------------------------------------
# 🔒 전센터 접근 권한 체크
# ------------------------------------------------------------
if "전체" not in allowed_centers:
    st.markdown(
        """
        <script>
        const nav = window.parent.document.querySelector('[data-testid="stSidebarNav"]');
        if (nav) {
          const links = Array.from(nav.querySelectorAll('li a'));
          links.forEach((link) => {
            const text = link.innerText.trim();
            if (text === "전센터 대시보드") {
              const li = link.closest('li');
              if (li) li.style.display = 'none';
            }
          });
        }
        </script>
        """,
        unsafe_allow_html=True,
    )
    st.error("전센터 대시보드는 전체 센터 권한이 있는 사용자만 접근 가능합니다.")
    st.stop()

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
st.title("🏗️ 센터 비교용 주/야간 업무량 대시보드")

# ------------------------------------------------------------
# 1) 날짜 선택
# ------------------------------------------------------------
date = st.date_input(
    "조회 기준 날짜 선택",
    value=datetime.now().date() - timedelta(days=1)
)

# ------------------------------------------------------------
# 2) 센터 선택 (다중 선택)
# ------------------------------------------------------------
VALID_CENTERS = [
    "Center_North",
    "Center_West",
    "Center_South",
    "Center_East",
    "Center_Central",
    "Partner_Gwacheon",
    "Partner_Daejeon",
    "Partner_Seoul",
    "Partner_Ansan",
]

centers_available = VALID_CENTERS if "전체" in allowed_centers else allowed_centers

selected_centers = st.multiselect(
    "비교할 센터 선택 (기본: 권한 내 모든 센터)",
    centers_available,
    default=centers_available
)

st.divider()

# ------------------------------------------------------------
# 🔒 권한 체크
# ------------------------------------------------------------
if not selected_centers:
    st.error("최소 1개 센터를 선택하세요.")
    st.stop()

# ------------------------------------------------------------
# 🧠 BigQuery 조회 (캐시)
# ------------------------------------------------------------
@st.cache_data(show_spinner=False, ttl=600)
def load_work_data(date: str, centers: list[str]) -> pd.DataFrame:
    center_list = [c for c in centers if c in VALID_CENTERS]
    if not center_list:
        return pd.DataFrame()
    center_filter = ", ".join([f"'{c}'" for c in center_list])
    query = f"""
    SELECT
        work_date AS date_kst,
        staff_name AS name,
        duty,
        work_type AS type,
        manager_center,
        task_count,
        first_task_time AS ms_completed_time,
        last_task_time,
        avg_battery_pct AS ex_leftover
    FROM `service.daily_maintenance`
    WHERE work_date = DATE('{date}')
      AND manager_center IN ({center_filter})
    ORDER BY first_task_time
    """

    df = run_query(query)

    # 🔒 Streamlit 안정화
    df["ms_completed_time"] = pd.to_datetime(
        df["ms_completed_time"], errors="coerce"
    )
    df["last_task_time"] = pd.to_datetime(
        df["last_task_time"], errors="coerce"
    )

    return df

# ------------------------------------------------------------
# 데이터 로드
# ------------------------------------------------------------
with st.spinner("데이터 불러오는 중..."):
    raw_df = load_work_data(str(date), selected_centers)

if raw_df.empty:
    st.info("해당 날짜/센터에 작업 데이터가 없습니다.")
    st.stop()

# ------------------------------------------------------------
# 4) 주/야간 분할 뷰 (좌: 주간, 우: 야간)
# ------------------------------------------------------------
st.subheader("📊 근무구분별 비교")

FIXED_TYPES = [
    "배터리교체",
    "고장수거",
    "현장조치완료",
    "재배치수거",
    "재배치완료",
    "수리후배치",
]

def render_duty_section(container, duty_label: str):
    duty_df = raw_df[raw_df["duty"] == duty_label]
    with container:
        st.markdown(f"### {duty_label}")

        if duty_df.empty:
            st.info(f"{duty_label} 데이터가 없습니다.")
            return

        # 센터별 근무자 수 (이미 집계된 데이터에서 unique name 카운트)
        staff_counts = (
            duty_df.groupby("manager_center")["name"]
            .nunique()
            .reset_index()
            .rename(columns={"name": "근무자 수"})
        )

        # 센터별 작업유형 집계 (task_count 합산)
        task_counts = (
            duty_df.groupby(["manager_center", "type"])["task_count"]
            .sum()
            .reset_index()
        )

        task_pivot = task_counts.pivot_table(
            index=["manager_center"],
            columns="type",
            values="task_count",
            aggfunc="sum",
            fill_value=0,
        )

        for col in FIXED_TYPES:
            if col not in task_pivot.columns:
                task_pivot[col] = 0

        # 모든 선택 센터 포함
        base_centers = pd.DataFrame({"manager_center": selected_centers})
        task_pivot = (
            base_centers.merge(
                task_pivot.reset_index().rename(columns={"index": "manager_center"}),
                on="manager_center",
                how="left",
            )
            .fillna(0)
        )
        task_pivot = task_pivot.merge(staff_counts, on="manager_center", how="left").fillna({"근무자 수": 0})
        task_pivot["근무자 수"] = task_pivot["근무자 수"].astype(int)
        task_pivot["총 작업량"] = task_pivot[FIXED_TYPES].sum(axis=1).astype(int)
        for col in FIXED_TYPES:
            task_pivot[col] = task_pivot[col].astype(int)
        task_pivot = task_pivot[["manager_center", "근무자 수", *FIXED_TYPES, "총 작업량"]].sort_values("manager_center")

        st.caption("센터별 작업량")
        st.dataframe(task_pivot, use_container_width=True, hide_index=True)

        melted = task_pivot.melt(
            id_vars=["manager_center"],
            value_vars=FIXED_TYPES,
            var_name="작업유형",
            value_name="작업량",
        )

        task_chart = (
            alt.Chart(melted)
            .mark_bar()
            .encode(
                x=alt.X("manager_center:N", title="센터", sort=selected_centers),
                y=alt.Y("작업량:Q", stack="zero", axis=alt.Axis(labels=False, ticks=False, title=None)),
                color=alt.Color("작업유형:N"),
                tooltip=["manager_center", "작업유형", "작업량"],
            )
            .properties(height=260)
        )
        st.altair_chart(task_chart, use_container_width=True)
        st.markdown("<div style='border-top:1px solid #e5e7eb;margin:12px 0'></div>", unsafe_allow_html=True)

        # 근무시간 평균 (first_task_time ~ last_task_time 사용)
        shift_df = (
            duty_df.groupby(["manager_center", "name"])
            .agg(
                근무시작=("ms_completed_time", "min"),
                근무종료=("last_task_time", "max"),
            )
            .reset_index()
        )

        shift_df["총근무시간"] = (
            (shift_df["근무종료"] - shift_df["근무시작"]).dt.total_seconds() / 3600
        ).round(1)

        avg_shift = (
            shift_df.groupby(["manager_center"])["총근무시간"]
            .mean()
            .round(1)
            .reset_index()
            .rename(columns={"총근무시간": "총근무시간 평균"})
        )
        # 선택된 센터 모두 포함해 빈 값은 0으로
        avg_shift = (
            pd.DataFrame({"manager_center": selected_centers})
            .merge(avg_shift, on="manager_center", how="left")
            .fillna({"총근무시간 평균": 0})
        )

        st.caption("센터별 총 근무시간 평균")
        st.dataframe(avg_shift.sort_values("manager_center"), use_container_width=True, hide_index=True)

        shift_chart = (
            alt.Chart(avg_shift)
            .mark_bar()
            .encode(
                x=alt.X("manager_center:N", title="센터", sort=selected_centers),
                y=alt.Y("총근무시간 평균:Q", axis=alt.Axis(labels=False, ticks=False, title=None)),
                tooltip=["manager_center", "총근무시간 평균"],
            )
            .properties(height=260)
        )
        st.altair_chart(shift_chart, use_container_width=True)

left, mid, right = st.columns([1, 0.02, 1], gap="small")
render_duty_section(left, "주간")
with mid:
    st.markdown("<div style='border-left:1px solid #e5e7eb;height:100%'></div>", unsafe_allow_html=True)
render_duty_section(right, "야간")

# ------------------------------------------------------------
# 5) 전체 Raw 확인용 (주석 처리 요청)
# ------------------------------------------------------------
# with st.expander("원천 데이터 보기"):
#     st.dataframe(raw_df, use_container_width=True)
