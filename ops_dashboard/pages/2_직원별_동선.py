import json
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import folium
from streamlit_folium import st_folium
from folium.plugins import Fullscreen
from shapely.geometry import shape
from utils.bigquery import run_query
from utils.sidebar_style import apply_sidebar_style

# -------------------------------------------------------
# MUST BE FIRST
# -------------------------------------------------------
st.set_page_config(layout="wide")
apply_sidebar_style()

# -------------------------------------------------------
# 🔐 로그인 체크
# -------------------------------------------------------
if "user" not in st.session_state:
    st.warning("로그인이 필요합니다. 메인 페이지에서 로그인하세요.")
    st.stop()

allowed_centers = st.session_state["allowed_centers"]

st.title("📍 직원별 작업 동선")

VALID_CENTERS = [
    "Center_North", "Center_West", "Center_South", "Center_Gimpo",
    "Center_East", "Center_Central",
    "Partner_Gwacheon", "Partner_Ansan",
    "Partner_Seoul", "Partner_Daejeon"
]

# -------------------------------------------------------
# 📌 session_state 기본값
# -------------------------------------------------------
if "map_date" not in st.session_state:
    st.session_state["map_date"] = str(datetime.now().date() - timedelta(days=1))

if "map_center" not in st.session_state:
    st.session_state["map_center"] = (
        VALID_CENTERS[0] if "전체" in allowed_centers else allowed_centers[0]
    )

if "map_name" not in st.session_state:
    st.session_state["map_name"] = "전체"

map_date = st.session_state["map_date"]
map_center = st.session_state["map_center"]
map_name = st.session_state["map_name"]

if "전체" not in allowed_centers and map_center not in allowed_centers:
    st.error("해당 센터 접근 권한이 없습니다.")
    st.stop()

# -------------------------------------------------------
# UI — 날짜 / 센터 / 직원
# -------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    date = st.date_input("조회 날짜", value=datetime.strptime(map_date, "%Y-%m-%d").date())
    st.session_state["map_date"] = str(date)

with col2:
    centers_available = VALID_CENTERS if "전체" in allowed_centers else allowed_centers
    center = st.selectbox("센터 선택", centers_available, index=centers_available.index(map_center))
    st.session_state["map_center"] = center

# -------------------------------------------------------
# 👤 직원 목록 조회
# -------------------------------------------------------
staff_query = f"""
WITH base AS (
  SELECT
    IFNULL(ms.staff_id, udf.user_id_to_staff_id(ms.manager_id)) AS staff_id,
    DATETIME(ms.created_at, 'Asia/Seoul') AS ms_completed_time
  FROM service.maintenance_stack ms
),
joined AS (
  SELECT
    s.name AS staff_name,
    DATE(DATETIME_SUB(b.ms_completed_time, INTERVAL 6 HOUR)) AS date_kst
  FROM base b
  JOIN service.staff s ON b.staff_id = s.id
  JOIN service.maintenance_center mc ON mc.id = s.center_id
  WHERE maintenance_role in (15,20) AND mc.name = '{center}'
)
SELECT DISTINCT staff_name
FROM joined
WHERE date_kst = '{date}'
ORDER BY staff_name
"""
staff_df = run_query(staff_query)
staff_list = ["전체"] + staff_df["staff_name"].dropna().tolist()

with col3:
    name = st.selectbox(
        "직원 선택",
        staff_list,
        index=staff_list.index(map_name) if map_name in staff_list else 0
    )
    st.session_state["map_name"] = name

# -------------------------------------------------------
# 🔥 지도 전용 작업 데이터 쿼리 (좌표 1차 필터)
# -------------------------------------------------------
name_filter = "" if name == "전체" else f"AND name = '{name}'"

data_query = f"""
WITH ms AS (
  SELECT
    id,
    IFNULL(staff_id, udf.user_id_to_staff_id(manager_id)) AS staff_id,
    type,
    maintenance_id,
    created_at
  FROM service.maintenance_stack
),
work AS (
  SELECT
    m.id,
    s.name,
    udf.id_to_sn(m.vehicle_id) AS bike_sn,
    CASE
      WHEN m.type = 1 AND ms.type = 75 THEN '배터리교체'
      WHEN m.type = 2 AND ms.type = 20 THEN '현장조치완료'
      WHEN m.type = 2 AND ms.type = 30 THEN '고장수거'
      WHEN m.type = 2 AND ms.type = 80 THEN '수리후배치'
      WHEN m.type = 0 AND ms.type = 30 THEN '재배치수거'
      WHEN m.type = 0 AND ms.type = 80 THEN '재배치완료'
    END AS type,
    DATETIME(ms.created_at, 'Asia/Seoul') AS ms_completed_time,
    CASE
      WHEN m.location_complete IS NULL THEN m.location_call
      ELSE m.location_complete
    END AS location,
    CASE WHEN m.type = 1 AND ms.type = 75 THEN tf.ex_leftover END AS battery_pct
  FROM ms
  JOIN service.maintenance m ON m.id = ms.maintenance_id
  LEFT JOIN service.tf_maintenance tf ON tf.id = m.id
  JOIN service.staff s ON ms.staff_id = s.id
  JOIN service.maintenance_center mc ON mc.id = s.center_id
  WHERE m.status != 1 and maintenance_role in (15,20)
    AND mc.name = '{center}'
),
filtered AS (
  SELECT
    *,
    DATE(DATETIME_SUB(ms_completed_time, INTERVAL 6 HOUR)) AS date_kst
  FROM work
  WHERE type IS NOT NULL
    AND location IS NOT NULL
    AND location != 'POINT EMPTY'
)
SELECT
  id, name, bike_sn, type, ms_completed_time, date_kst, location, battery_pct
FROM filtered
WHERE date_kst = '{date}'
  {name_filter}
ORDER BY ms_completed_time
"""
df = run_query(data_query)

if df.empty:
    st.info("해당 조건에 맞는 작업 데이터가 없습니다.")
    st.stop()

# ✅ (중요) 시간 컬럼 강제 datetime 변환 — 시작/끝 꼬임 방지
df["ms_completed_time"] = pd.to_datetime(df["ms_completed_time"], errors="coerce")
df = df.dropna(subset=["ms_completed_time"])

# -------------------------------------------------------
# 🧠 geometry 안전 변환
# -------------------------------------------------------
def safe_point(x):
    try:
        g = shape(json.loads(str(x)))
        if g.geom_type != "Point":
            return None
        if pd.isna(g.x) or pd.isna(g.y):
            return None
        return g
    except Exception:
        return None

map_df = df.copy()
map_df["geom"] = map_df["location"].apply(safe_point)
map_df = map_df[map_df["geom"].notna()].copy()

map_df["lat"] = map_df["geom"].apply(lambda g: float(g.y))
map_df["lon"] = map_df["geom"].apply(lambda g: float(g.x))
map_df = map_df.dropna(subset=["lat", "lon"])

if map_df.empty:
    st.warning("좌표가 있는 작업이 없어 지도를 표시할 수 없습니다.")
    st.stop()

# -------------------------------------------------------
# 🗺 지도 생성
# -------------------------------------------------------
m = folium.Map(
    [map_df["lat"].mean(), map_df["lon"].mean()],
    zoom_start=12,
    tiles="CartoDB positron"
)
Fullscreen().add_to(m)

COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
staff_names = map_df["name"].unique().tolist()
staff_color = {n: COLORS[i % len(COLORS)] for i, n in enumerate(staff_names)}

TYPE_COLOR = {
    "배터리교체": "#1f77b4",
    "현장조치완료": "#2ca02c",
    "고장수거": "#d62728",
    "수리후배치": "#ff7f0e",
    "재배치수거": "#9467bd",
    "재배치완료": "#8c564b",
}


# -------------------------------------------------------
# 👥 직원별 레이어 기준 (None 안전 처리)
# -------------------------------------------------------
if name == "전체":
    staff_names = sorted(
        map_df["name"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )
else:
    staff_names = [name]


for staff in staff_names:
    sdf = (
        map_df[map_df["name"] == staff]
        .sort_values("ms_completed_time")
        .reset_index(drop=True)
    )
    if sdf.empty:
        continue

    fg = folium.FeatureGroup(name=staff, show=True)

    coords = list(zip(sdf["lat"], sdf["lon"]))
    if len(coords) >= 2:
        folium.PolyLine(
            coords,
            color=staff_color[staff],
            weight=4,
            opacity=0.85
        ).add_to(fg)

    # 시작 / 종료
    start_row = sdf.iloc[0]
    end_row = sdf.iloc[-1]

    folium.CircleMarker(
        [start_row["lat"], start_row["lon"]],
        radius=9,
        color="#2ecc71",
        fill=True,
        fill_opacity=1,
        tooltip=f"🟢 시작 · {start_row['ms_completed_time'].strftime('%H:%M')}"
    ).add_to(fg)

    folium.CircleMarker(
        [end_row["lat"], end_row["lon"]],
        radius=9,
        color="#e74c3c",
        fill=True,
        fill_opacity=1,
        tooltip=f"🔴 종료 · {end_row['ms_completed_time'].strftime('%H:%M')}"
    ).add_to(fg)

    # 작업 점
    for _, r in sdf.iterrows():
        popup_html = (
            f"<b>직원</b>: {r['name']}<br>"
            f"<b>업무</b>: {r['type']}<br>"
            f"<b>시간</b>: {r['ms_completed_time'].strftime('%H:%M')}<br>"
            f"<b>SN</b>: {r['bike_sn']}"
        )
        if r["type"] == "배터리교체" and pd.notna(r.get("battery_pct")):
            popup_html += f"<br><b>교체 시 배터리</b>: {r['battery_pct']:.0f}%"
        folium.CircleMarker(
            [r["lat"], r["lon"]],
            radius=5,
            color=TYPE_COLOR.get(r["type"], "#7f7f7f"),
            fill=True,
            fill_opacity=0.9,
            popup=folium.Popup(
                popup_html,
                max_width=280
            )
        ).add_to(fg)

    m.add_child(fg)

folium.LayerControl(collapsed=False).add_to(m)
st_folium(m, width=1200, height=800)

# -------------------------------------------------------
# 🔋 배터리 교체 요약
# -------------------------------------------------------
bat_df = map_df[(map_df["type"] == "배터리교체") & (map_df["battery_pct"].notna())]
if not bat_df.empty:
    st.subheader("🔋 배터리 교체 요약")
    bat_summary = (
        bat_df.groupby("name")["battery_pct"]
        .agg(["count", "mean"])
        .rename(columns={"count": "교체 건수", "mean": "평균 교체%"})
        .reset_index()
        .rename(columns={"name": "직원"})
    )
    bat_summary["평균 교체%"] = bat_summary["평균 교체%"].round(1)
    st.dataframe(bat_summary, use_container_width=True, hide_index=True)
