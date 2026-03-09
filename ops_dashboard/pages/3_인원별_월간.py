import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from utils.bigquery import run_query
from utils.sidebar_style import apply_sidebar_style

st.set_page_config(layout="wide")
apply_sidebar_style()

# ------------------------------------------------------------
# 로그인 체크
# ------------------------------------------------------------
if "user" not in st.session_state:
    st.warning("로그인이 필요합니다. 메인 페이지에서 로그인하세요.")
    st.stop()

# 직전 페이지 정보
default_name = st.session_state.get("map_name", None)
default_center = st.session_state.get("map_center", None)

st.title("📅 직원 월간 요약")

# ------------------------------------------------------------
# 센터 목록
# ------------------------------------------------------------
VALID_CENTERS = [
    "Center_North", "Center_West", "Center_South", "Center_Gimpo",
    "Center_East", "Center_Central", "Partner_Gwacheon",
    "Partner_Ansan", "Partner_Seoul", "Partner_Daejeon"
]

allowed_centers = st.session_state["allowed_centers"]
centers_available = VALID_CENTERS if "전체" in allowed_centers else allowed_centers

# ============================================================
# 📅 1) 연 / 월 선택 (🔥 위로 이동)
# ============================================================
col_y, col_m = st.columns(2)

with col_y:
    years = list(range(datetime.now().year - 2, datetime.now().year + 1))
    selected_year = st.selectbox(
        "연도",
        years,
        index=years.index(datetime.now().year)
    )

with col_m:
    months = list(range(1, 13))
    selected_month = st.selectbox(
        "월",
        months,
        index=datetime.now().month - 1
    )

month_start = datetime(selected_year, selected_month, 1)
month_end = (
    datetime(selected_year + 1, 1, 1)
    if selected_month == 12
    else datetime(selected_year, selected_month + 1, 1)
)

month_start_str = month_start.strftime("%Y-%m-%d")
month_end_str = month_end.strftime("%Y-%m-%d")

# st.markdown(
#     f"📌 선택된 기간: **{selected_year}-{str(selected_month).zfill(2)}**"
# )

st.divider()

# ============================================================
# 📌 2) 센터 / 직원 선택
# ============================================================
colA, colB = st.columns(2)

with colA:
    selected_center = st.selectbox(
        "센터 선택",
        centers_available,
        index=centers_available.index(default_center)
        if default_center in centers_available else 0
    )

# ------------------------------------------------------------
# 🔍 직원 목록 (연·월 + 센터 기준 근무자만)
# ------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_staff_list(center, start_dt, end_dt):
    q = f"""
    SELECT DISTINCT staff_name AS name
    FROM `service.daily_maintenance`
    WHERE manager_center = '{center}'
      AND work_date >= '{start_dt}'
      AND work_date < '{end_dt}'
    ORDER BY name
    """
    df = run_query(q)
    return df["name"].tolist()

staff_list = load_staff_list(
    selected_center,
    month_start_str,
    month_end_str
)

with colB:
    if not staff_list:
        st.warning("선택한 기간에 근무 이력이 있는 직원이 없습니다.")
        st.stop()

    selected_name = st.selectbox(
        "직원 선택",
        staff_list,
        index=staff_list.index(default_name)
        if default_name in staff_list else 0
    )

# session_state 저장
st.session_state["map_center"] = selected_center
st.session_state["map_name"] = selected_name

# ============================================================
# 🔥 월간 데이터 조회
# ============================================================
@st.cache_data(show_spinner=True)
def load_monthly_data(name, center, start_dt, end_dt):
    query = f"""
    SELECT
        work_date AS date_kst,
        staff_name AS name,
        work_type AS type,
        task_count,
        first_task_time AS ms_completed_time,
        last_task_time,
        avg_battery_pct AS ex_leftover
    FROM `service.daily_maintenance`
    WHERE staff_name = "{name}"
      AND manager_center = "{center}"
      AND work_date >= "{start_dt}"
      AND work_date < "{end_dt}"
    ORDER BY work_date, first_task_time
    """
    return run_query(query)

with st.spinner("월간 데이터를 불러오는 중..."):
    df = load_monthly_data(
        selected_name,
        selected_center,
        month_start_str,
        month_end_str
    )

if df.empty:
    st.info("해당 월에 작업 데이터가 없습니다.")
    st.stop()

# ============================================================
# ⏱ 날짜별 근무시간
# ============================================================
st.subheader("⏱ 날짜별 근무시간")

# datetime 변환
df["ms_completed_time"] = pd.to_datetime(df["ms_completed_time"], errors="coerce")
df["last_task_time"] = pd.to_datetime(df["last_task_time"], errors="coerce")

shift_df = (
    df.groupby("date_kst")
    .agg(
        근무시작=("ms_completed_time", "min"),
        근무종료=("last_task_time", "max")
    )
    .reset_index()
)

shift_df["총근무시간"] = (
    (shift_df["근무종료"] - shift_df["근무시작"])
    .dt.total_seconds() / 3600
).round(1)

# 평균 교체% 추가 (배터리교체 작업의 avg_battery_pct 값 직접 사용)
battery_avg_shift = (
    df[df["type"] == "배터리교체"][["date_kst", "ex_leftover"]]
    .rename(columns={"ex_leftover": "평균 교체%"})
)
shift_df = shift_df.merge(battery_avg_shift, on="date_kst", how="left")
shift_df["평균 교체%"] = shift_df["평균 교체%"].apply(
    lambda x: "-" if pd.isna(x) else f"{x:.1f}"
)

st.dataframe(shift_df, use_container_width=True)

# ============================================================
# 🛠 날짜별 작업 유형 + 평균 교체%
# ============================================================
st.subheader("🛠 날짜별 작업 유형별 수")

daily_task = (
    df.groupby(["date_kst", "type"])["task_count"]
    .sum()
    .reset_index()
    .pivot_table(
        index="date_kst",
        columns="type",
        values="task_count",
        fill_value=0
    )
    .reset_index()
)

# 평균 교체% (이미 집계된 값 사용)
daily_battery_avg = (
    df[
        (df["type"] == "배터리교체") &
        (df["ex_leftover"].notna())
    ]
    .groupby("date_kst")["ex_leftover"]
    .mean()
    .round(1)
    .rename("평균 교체%")
    .reset_index()
)

daily_task = daily_task.merge(daily_battery_avg, on="date_kst", how="left")

ORDERED_COLS = [
    "date_kst",
    "평균 교체%",
    "배터리교체",
    "현장조치완료",
    "고장수거",
    "수리후배치",
    "재배치수거",
    "재배치완료",
]

for col in ORDERED_COLS:
    if col not in daily_task.columns:
        daily_task[col] = 0

daily_task = daily_task[ORDERED_COLS]

# 정수형 변환
for col in ["배터리교체", "현장조치완료", "고장수거", "수리후배치", "재배치수거", "재배치완료"]:
    daily_task[col] = daily_task[col].astype(int)

daily_task["평균 교체%"] = daily_task["평균 교체%"].apply(
    lambda x: "-" if pd.isna(x) else f"{x:.1f}"
)

st.dataframe(daily_task, use_container_width=True)
