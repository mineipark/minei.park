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

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
st.title("📍 센터별 현장관리 대시보드")

# ------------------------------------------------------------
# 1) 날짜 선택
# ------------------------------------------------------------
date = st.date_input(
    "조회 기준 날짜 선택",
    value=datetime.now().date() - timedelta(days=1)
)

# ------------------------------------------------------------
# 2) 센터 선택
# ------------------------------------------------------------
VALID_CENTERS = [
    "Center_North", "Center_West", "Center_South", "Center_Gimpo",
    "Center_East", "Center_Central",
    "Partner_Gwacheon", "Partner_Ansan",
    "Partner_Seoul", "Partner_Daejeon"
]

centers_available = VALID_CENTERS if "전체" in allowed_centers else allowed_centers

query_center = st.query_params.get("center")
default_center = (
    query_center if query_center in centers_available else centers_available[0]
)

center = st.selectbox(
    "센터 선택",
    centers_available,
    index=centers_available.index(default_center)
)

st.divider()

# ------------------------------------------------------------
# 🔒 권한 체크
# ------------------------------------------------------------
if "전체" not in allowed_centers and center not in allowed_centers:
    st.error("해당 센터 접근 권한이 없습니다.")
    st.stop()

# ------------------------------------------------------------
# 🧠 BigQuery 조회 (캐시)
# ------------------------------------------------------------
@st.cache_data(show_spinner=False, ttl=600)
def load_work_data(date: str, center: str) -> pd.DataFrame:
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
      AND manager_center = '{center}'
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
    raw_df = load_work_data(str(date), center)

if raw_df.empty:
    st.info("해당 날짜에 작업 데이터가 없습니다.")
    st.stop()

# ------------------------------------------------------------
# 4) KPI
# ------------------------------------------------------------
st.subheader("📊 현장관리 총 업무량")

FIXED_TYPES = [
    "배터리교체", "고장수거", "현장조치완료",
    "재배치수거", "재배치완료", "수리후배치"
]

def build_row(label, df):
    row = {"근무구분": label}
    for t in FIXED_TYPES:
        row[t] = int(df[df["type"] == t]["task_count"].sum())
    row["총 작업량"] = int(df["task_count"].sum())
    row["근무자 수"] = df["name"].nunique()
    return row

kpi_df = pd.DataFrame([
    build_row("전체", raw_df),
    build_row("주간", raw_df[raw_df["duty"] == "주간"]),
    build_row("야간", raw_df[raw_df["duty"] == "야간"]),
])

st.dataframe(kpi_df, use_container_width=True)

# ------------------------------------------------------------
# 5) 근무 시작 / 종료
# ------------------------------------------------------------
st.subheader("⏱ 근무 시작 / 종료")

shift_df = (
    raw_df.groupby(["date_kst", "name"])
    .agg(
        근무시작=("ms_completed_time", "min"),
        근무종료=("last_task_time", "max"),
    )
    .reset_index()
)

shift_df["총근무시간"] = (
    (shift_df["근무종료"] - shift_df["근무시작"])
    .dt.total_seconds() / 3600
).round(1)

# 평균 교체% 추가 (배터리교체 작업의 avg_battery_pct 값 직접 사용)
battery_avg_shift = (
    raw_df[raw_df["type"] == "배터리교체"][["name", "ex_leftover"]]
    .rename(columns={"ex_leftover": "평균 교체%"})
)
shift_df = shift_df.merge(battery_avg_shift, on="name", how="left")
shift_df["평균 교체%"] = shift_df["평균 교체%"].apply(
    lambda x: "-" if pd.isna(x) else f"{x:.1f}"
)

st.dataframe(shift_df, use_container_width=True)

# ------------------------------------------------------------
# 6) 근무자별 작업 요약 + 평균 교체%
# ------------------------------------------------------------
st.subheader("👥 근무자별 작업 요약")

summary = (
    raw_df.pivot_table(
        index="name",
        columns="type",
        values="task_count",
        aggfunc="sum",
        fill_value=0
    )
    .reset_index()
)

duty_map = raw_df[["name", "duty"]].drop_duplicates()
summary = summary.merge(duty_map, on="name", how="left")

# 평균 교체% (이미 집계 테이블에 avg_battery_pct로 있음)
battery_avg = (
    raw_df[
        (raw_df["type"] == "배터리교체") &
        (raw_df["ex_leftover"].notna())
    ]
    .groupby("name")["ex_leftover"]
    .mean()
    .round(1)
    .reset_index()
    .rename(columns={"ex_leftover": "평균 교체%"})
)

# 평균 교체% 병합
summary = summary.merge(battery_avg, on="name", how="left")
summary["평균 교체%"] = summary["평균 교체%"].astype("Float64")

# ------------------------------------------------------------
# 🔧 작업요약 컬럼 순서 고정
# ------------------------------------------------------------
ORDERED_COLUMNS = [
    "name",
    "duty",
    "평균 교체%",
    "배터리교체",
    "현장조치완료",
    "고장수거",
    "수리후배치",
    "재배치수거",
    "재배치완료",
]

# 없는 작업유형 컬럼은 0으로 생성
for col in ORDERED_COLUMNS:
    if col not in summary.columns:
        summary[col] = 0

# 순서 강제 적용
display_df = summary[ORDERED_COLUMNS].copy()

# (선택) 총 작업량 컬럼 유지하고 싶으면
display_df["총 작업량"] = display_df[
    [
        "배터리교체",
        "현장조치완료",
        "고장수거",
        "수리후배치",
        "재배치수거",
        "재배치완료",
    ]
].sum(axis=1)

display_df["평균 교체%"] = display_df["평균 교체%"].apply(
    lambda x: "-" if pd.isna(x) else f"{x:.1f}"
)


# 출력
st.dataframe(display_df, use_container_width=True)


# ------------------------------------------------------------
# 🔗 직원별 이동
# ------------------------------------------------------------
st.markdown("### 🔗 직원별 이동")

for i, row in summary.iterrows():
    name = str(row["name"]).strip()

    with st.container():
        st.markdown(f"**{name}**")
        col1, col2 = st.columns(2)

        if col1.button("🗺️ 동선 보기", key=f"map_{i}"):
            st.session_state.update({
                "map_name": name,
                "map_date": str(date),
                "map_center": center
            })
            st.switch_page("pages/2_직원별_동선.py")

        if col2.button("📅 월간 보기", key=f"month_{i}"):
            st.session_state.update({
                "map_name": name,
                "map_center": center
            })
            st.switch_page("pages/3_인원별_월간.py")

        st.divider()
