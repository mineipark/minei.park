"""
관리 페이지
- 탭1: 실종 위기 기기 (GPS 미송신 + 장기 미이용)
- 탭2: 필드 인터렉터 해제 기기
- 탭3: 반복 이슈 분석 (관리자 전용)
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
from utils.bigquery import run_query
from utils.sheets import read_search_status, upsert_search_status, init_sheet_headers
from utils.sidebar_style import apply_sidebar_style

# =============================================================================
# 페이지 설정
# =============================================================================
st.set_page_config(
    page_title="관리",
    page_icon="📋",
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
if not allowed_centers:
    st.error("접근 권한이 없습니다.")
    st.stop()

# 관리자 여부 확인
is_admin = "전체" in allowed_centers

# =============================================================================
# 상수 정의
# =============================================================================
ALL_CENTERS = [
    "Center_North", "Center_West", "Center_South", "Center_East", "Center_Central",
    "Partner_Seoul", "Partner_Gwacheon", "Partner_Ansan", "Partner_Daejeon"
]

STATUS_OPTIONS = ["미확인", "수색중", "실종"]

# =============================================================================
# 데이터 로드 함수
# =============================================================================
@st.cache_data(ttl=300)
def load_missing_bikes() -> pd.DataFrame:
    """실종 위기 기기 조회 (GPS 7일 미송신 + 60일 라이딩/정비 없음)"""
    centers_str = ", ".join([f"'{c}'" for c in ALL_CENTERS])

    query = f"""
    WITH
    gps_active AS (
      SELECT DISTINCT device_id
      FROM `bikeshare-project.service.PRD`
      WHERE created_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 10 DAY))
        AND DATETIME_DIFF(CURRENT_DATETIME('Asia/Seoul'), CAST(created_at AS DATETIME), HOUR) <= 168
    ),
    riding_active AS (
      SELECT DISTINCT bike_id
      FROM `bikeshare.service.rides`
      WHERE DATE(started_at, 'Asia/Seoul') >= DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 60 DAY)
        AND status = 10
    ),
    maintenance_active AS (
      SELECT DISTINCT vehicle_id AS bike_id
      FROM `bikeshare-project.service.maintenance`
      WHERE created_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 60 DAY))
        AND status IN (2, 3)
    ),
    field_bikes AS (
      SELECT b.id, b.sn, b.device_id, b.status, b.vendor, b.leftover, b.area
      FROM `bikeshare.service.bike` b
      WHERE b.is_active = TRUE AND b.is_usable = TRUE AND b.in_testing = FALSE
        AND b.vendor IN (3, 5) AND b.status NOT IN (2, 3)
    )
    SELECT fb.sn, re.area_name, re.center_name
    FROM field_bikes fb
    LEFT JOIN gps_active ga ON fb.device_id = ga.device_id
    LEFT JOIN riding_active ra ON fb.id = ra.bike_id
    LEFT JOIN maintenance_active ma ON fb.id = ma.bike_id
    LEFT JOIN `bikeshare.management.region` AS re ON re.area_cd = fb.area
    WHERE ga.device_id IS NULL AND ra.bike_id IS NULL AND ma.bike_id IS NULL
      AND center_name IN ({centers_str})
    ORDER BY fb.area, fb.sn
    """
    return run_query(query)


@st.cache_data(ttl=300)
def load_interactor_released_bikes() -> pd.DataFrame:
    """필드 인터렉터 해제 기기 조회"""
    centers_str = ", ".join([f"'{c}'" for c in ALL_CENTERS])

    query = f"""
    SELECT b.sn, r.area_name, r.center_name
    FROM `bikeshare.service.bike` AS b
    LEFT JOIN `bikeshare.service.bike` AS bi ON b.id = bi.id
    LEFT JOIN `bikeshare.management.region` AS r ON r.area_cd = b.bike_area
    WHERE bike_status IN ('BAV', 'LAV', 'LNB', 'BNB')
      AND mac_id = '-'
      AND in_testing = false
      AND b.is_active = true
      AND b.is_usable = true
      AND r.center_name IN ({centers_str})
    ORDER BY r.center_name, b.sn
    """
    return run_query(query)


@st.cache_data(ttl=300)
def load_repeat_issues() -> pd.DataFrame:
    """반복 이슈 분석 (최근 30일 완료 건 중 재고장 발생)"""
    centers_str = ", ".join([f"'{c}'" for c in ALL_CENTERS])

    query = f"""
    WITH
    onsite_complete AS (
      SELECT
        m.id AS maintenance_id,
        m.vehicle_id AS bike_id,
        m.completed_time,
        m.comment AS completed_comment,
        ms.staff_id,
        s.name AS staff_name,
        mc.name AS center_name
      FROM `bikeshare.service.maintenance` m
      JOIN `bikeshare.service.maintenance_log` ms
        ON m.id = ms.maintenance_id AND ms.type = 20
      LEFT JOIN `bikeshare.service.staff` s ON ms.staff_id = s.id
      LEFT JOIN `bikeshare.service.service_center` mc ON s.center_id = mc.id
      WHERE m.type = 2 AND m.status = 3
        AND m.completed_time >= DATETIME_SUB(CURRENT_DATETIME('Asia/Seoul'), INTERVAL 30 DAY)
    ),

    -- 다음 고장 정보 (created_time, comment)를 ROW_NUMBER로 가져옴
    next_broken_ranked AS (
      SELECT
        o.maintenance_id,
        o.bike_id,
        o.completed_time,
        m.created_time AS next_broken_time,
        m.comment AS next_broken_comment,
        ROW_NUMBER() OVER (
          PARTITION BY o.maintenance_id
          ORDER BY m.created_time ASC
        ) AS rn
      FROM onsite_complete o
      JOIN `bikeshare.service.maintenance` m
        ON o.bike_id = m.vehicle_id
        AND m.created_time > o.completed_time
        AND m.type = 2
    ),

    with_next_broken AS (
      SELECT
        o.*,
        nb.next_broken_time,
        nb.next_broken_comment
      FROM onsite_complete o
      JOIN next_broken_ranked nb
        ON o.maintenance_id = nb.maintenance_id AND nb.rn = 1
    ),

    with_riding_count AS (
      SELECT
        nb.*,
        DATETIME_DIFF(nb.next_broken_time, nb.completed_time, MINUTE) AS lead_time_minutes,
        (
          SELECT COUNT(*)
          FROM `bikeshare.service.rides` r
          WHERE r.bike_id = nb.bike_id
            AND r.start_time > nb.completed_time
            AND r.start_time < nb.next_broken_time
            AND r.status = 10
        ) AS riding_count_between
      FROM with_next_broken nb
    )

    SELECT
      maintenance_id,
      bike_id,
      `bikeshare-project.udf`.id_to_sn(bike_id) AS bike_sn,
      center_name,
      staff_name,
      completed_time,
      next_broken_time,
      lead_time_minutes,
      ROUND(lead_time_minutes / 60.0, 1) AS lead_time_hours,
      riding_count_between,
      completed_comment,
      next_broken_comment,
      CASE
        WHEN lead_time_minutes < 1440 THEN '1일내_재고장'
        WHEN riding_count_between <= 1 THEN '라이딩1건이하'
      END AS outlier_reason
    FROM with_riding_count
    WHERE (lead_time_minutes < 1440 OR riding_count_between <= 1)
      AND center_name IN ({centers_str})
      -- 1시간(60분) 미만 제외 (수거 공간 부족으로 재신고한 케이스)
      AND lead_time_minutes >= 60
    ORDER BY center_name, completed_time DESC
    """
    return run_query(query)


def merge_with_search_status(bikes_df: pd.DataFrame) -> pd.DataFrame:
    """BigQuery 데이터와 Sheets 수색 상태 병합"""
    if bikes_df.empty:
        return bikes_df

    # Sheets에서 수색 상태 읽기
    status_df = read_search_status()

    # 필요한 컬럼이 없거나 빈 데이터면 기본값 사용
    required_cols = ['sn', 'search_1', 'search_2', 'status', 'updated_at']
    if status_df.empty or not all(col in status_df.columns for col in required_cols):
        # 상태 데이터 없으면 기본값으로 컬럼 추가
        bikes_df['search_1'] = False
        bikes_df['search_2'] = False
        bikes_df['status'] = '미확인'
        bikes_df['updated_at'] = ''
        return bikes_df

    # sn 타입 통일 (문자열로)
    bikes_df['sn'] = bikes_df['sn'].astype(str)
    status_df['sn'] = status_df['sn'].astype(str)

    # sn 기준으로 병합
    merged = bikes_df.merge(
        status_df[['sn', 'search_1', 'search_2', 'status', 'updated_at']],
        on='sn',
        how='left'
    )

    # 기본값 설정
    merged['search_1'] = merged['search_1'].apply(lambda x: x == 'True' if pd.notna(x) else False)
    merged['search_2'] = merged['search_2'].apply(lambda x: x == 'True' if pd.notna(x) else False)
    merged['status'] = merged['status'].fillna('미확인')
    merged['updated_at'] = merged['updated_at'].fillna('')

    return merged


# =============================================================================
# 메인 UI
# =============================================================================

# 테이블 가운데 정렬 스타일
st.markdown("""
<style>
    /* 컬럼 내용 가운데 정렬 */
    [data-testid="stHorizontalBlock"] > div > div {
        display: flex;
        justify-content: center;
        align-items: center;
    }
</style>
""", unsafe_allow_html=True)

st.title("📋 관리")

# 저장 성공 메시지 표시
if 'save_success' in st.session_state:
    st.success(st.session_state.save_success)
    del st.session_state.save_success

# 탭 구성 (모든 센터 권한자가 탭3 접근 가능)
tab1, tab2, tab3 = st.tabs(["🔍 실종 위기 기기", "📡 필드 인터렉터 해제", "🔄 반복 이슈 분석"])

# =============================================================================
# 탭1: 실종 위기 기기
# =============================================================================
with tab1:
    st.caption("GPS 7일 미송신 + 60일 라이딩 없음 + 60일 정비 없음")

    # 사이드바 컨트롤
    with st.sidebar:
        st.markdown("### 필터")

        # 관리자는 전체 센터 선택 가능, 일반 사용자는 본인 센터만
        if is_admin:
            center_options = ["전체"] + ALL_CENTERS
            selected_center = st.selectbox(
                "센터 선택",
                center_options,
                key="missing_center"
            )
        else:
            # 일반 사용자: 본인 권한 센터만 표시
            user_centers = [c for c in allowed_centers if c in ALL_CENTERS]
            if len(user_centers) == 1:
                selected_center = user_centers[0]
                st.info(f"📍 {selected_center}")
            else:
                selected_center = st.selectbox(
                    "센터 선택",
                    user_centers,
                    key="missing_center"
                )

        if st.button("🔄 새로고침", key="refresh_missing"):
            st.cache_data.clear()
            st.rerun()

    # 데이터 로드
    with st.spinner("데이터 로딩 중..."):
        try:
            bikes_df = load_missing_bikes()
            merged_df = merge_with_search_status(bikes_df)
        except Exception as e:
            st.error(f"데이터 로드 실패: {str(e)}")
            st.stop()

    if merged_df.empty:
        st.success("실종 위기 기기가 없습니다!")
    else:
        # 센터 필터 적용
        if selected_center != "전체":
            display_df = merged_df[merged_df['center_name'] == selected_center].copy()
        else:
            display_df = merged_df.copy()

        # 요약 정보
        total_count = len(display_df)
        st.metric("총 실종 위기 기기", f"{total_count}대")

        # 센터별 현황
        center_counts = display_df['center_name'].value_counts()
        center_summary = " | ".join([f"{c}: {cnt}대" for c, cnt in center_counts.items()])
        st.caption(f"센터별: {center_summary}")

        st.markdown("---")

        # session_state로 체크박스 상태 관리
        if 'search_updates' not in st.session_state:
            st.session_state.search_updates = {}

        # 상태 필터 (기본값: 전체 선택)
        status_filter = st.multiselect(
            "상태 필터",
            STATUS_OPTIONS,
            default=STATUS_OPTIONS,
            key="status_filter"
        )

        if status_filter:
            display_df = display_df[display_df['status'].isin(status_filter)]

        st.markdown("---")

        # 기기 목록 타이틀 + 버튼 (같은 행)
        col_title, col_spacer, col_btn1, col_btn2 = st.columns([3, 4, 1, 1])

        with col_title:
            st.markdown("### 기기 목록")

        with col_btn1:
            save_clicked = st.button("💾 저장", type="primary", key="save_btn", use_container_width=True)

        with col_btn2:
            if not display_df.empty:
                csv = display_df.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 CSV",
                    data=csv,
                    file_name=f"실종위기기기_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    key="csv_download",
                    use_container_width=True
                )

        if display_df.empty:
            st.info("해당 조건에 맞는 기기가 없습니다.")
        else:
            # 테이블 헤더 (고정 스타일 적용)
            st.markdown("""
            <div style="background-color: #f0f2f6; padding: 10px 0; border-radius: 5px; margin-bottom: 10px;">
                <div style="display: flex; font-weight: bold; font-size: 14px;">
                    <div style="flex: 1.5; text-align: center;">기기SN</div>
                    <div style="flex: 2; text-align: center;">지역</div>
                    <div style="flex: 2; text-align: center;">센터</div>
                    <div style="flex: 1; text-align: center;">1차수색</div>
                    <div style="flex: 1; text-align: center;">2차수색</div>
                    <div style="flex: 1.5; text-align: center;">상태</div>
                    <div style="flex: 2; text-align: center;">수정일</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            # 각 행 표시 (스크롤 가능 영역)
            for idx, row in display_df.iterrows():
                cols = st.columns([1.5, 2, 2, 1, 1, 1.5, 2])

                sn = row['sn']
                cols[0].write(sn)
                cols[1].write(row['area_name'] if pd.notna(row['area_name']) else '-')
                cols[2].write(row['center_name'])

                # 1차 수색 체크박스
                search_1 = cols[3].checkbox(
                    "1차",
                    value=row['search_1'],
                    key=f"s1_{sn}",
                    label_visibility="collapsed"
                )

                # 2차 수색 체크박스
                search_2 = cols[4].checkbox(
                    "2차",
                    value=row['search_2'],
                    key=f"s2_{sn}",
                    label_visibility="collapsed"
                )

                # 상태 선택
                current_status = row['status'] if row['status'] in STATUS_OPTIONS else '미확인'
                status = cols[5].selectbox(
                    "상태",
                    STATUS_OPTIONS,
                    index=STATUS_OPTIONS.index(current_status),
                    key=f"st_{sn}",
                    label_visibility="collapsed"
                )

                cols[6].write(row['updated_at'][:10] if row['updated_at'] else '-')

                # 변경 감지 및 저장
                if (search_1 != row['search_1'] or
                    search_2 != row['search_2'] or
                    status != row['status']):

                    st.session_state.search_updates[sn] = {
                        'sn': sn,
                        'center_name': row['center_name'],
                        'area_name': row['area_name'] if pd.notna(row['area_name']) else '',
                        'search_1': search_1,
                        'search_2': search_2,
                        'status': status
                    }

            st.markdown("---")

            # 상단 저장 버튼 클릭 처리
            if save_clicked:
                if st.session_state.search_updates:
                    with st.spinner("저장 중..."):
                        success_count = 0
                        for sn, data in st.session_state.search_updates.items():
                            result = upsert_search_status(
                                sn=data['sn'],
                                center_name=data['center_name'],
                                area_name=data['area_name'],
                                search_1=data['search_1'],
                                search_2=data['search_2'],
                                status=data['status'],
                                updated_by=st.session_state.get('user', 'unknown')
                            )
                            if result:
                                success_count += 1

                        st.session_state.search_updates = {}
                        st.session_state.save_success = f"✅ {success_count}건 저장이 완료되었습니다!"
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.info("변경된 내용이 없습니다.")

# =============================================================================
# 탭2: 필드 인터렉터 해제 기기
# =============================================================================
with tab2:
    st.caption("bike_status: BAV, LAV, LNB, BNB + mac_id = '-'")

    # 데이터 로드
    with st.spinner("데이터 로딩 중..."):
        try:
            interactor_df = load_interactor_released_bikes()
        except Exception as e:
            st.error(f"데이터 로드 실패: {str(e)}")
            interactor_df = pd.DataFrame()

    if interactor_df.empty:
        st.success("필드 인터렉터 해제 기기가 없습니다!")
    else:
        # 센터 필터 적용
        if is_admin:
            filter_center = selected_center
        else:
            user_centers = [c for c in allowed_centers if c in ALL_CENTERS]
            filter_center = user_centers[0] if len(user_centers) == 1 else selected_center

        if filter_center != "전체":
            interactor_display = interactor_df[interactor_df['center_name'] == filter_center].copy()
        else:
            interactor_display = interactor_df.copy()

        # 요약 정보
        total_count = len(interactor_display)
        st.metric("총 필드 인터렉터 해제 기기", f"{total_count}대")

        # 센터별 현황
        if not interactor_display.empty:
            center_counts = interactor_display['center_name'].value_counts()
            center_summary = " | ".join([f"{c}: {cnt}대" for c, cnt in center_counts.items()])
            st.caption(f"센터별: {center_summary}")

        st.markdown("---")

        # 기기 목록 타이틀 + CSV 버튼
        col_title2, col_spacer2, col_btn2 = st.columns([3, 5, 1])

        with col_title2:
            st.markdown("### 기기 목록")

        with col_btn2:
            if not interactor_display.empty:
                csv2 = interactor_display.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 CSV",
                    data=csv2,
                    file_name=f"인터렉터해제기기_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    key="csv_download_interactor",
                    use_container_width=True
                )

        if interactor_display.empty:
            st.info("해당 조건에 맞는 기기가 없습니다.")
        else:
            # 테이블 헤더
            st.markdown("""
            <div style="background-color: #f0f2f6; padding: 10px 0; border-radius: 5px; margin-bottom: 10px;">
                <div style="display: flex; font-weight: bold; font-size: 14px;">
                    <div style="flex: 2; text-align: center;">기기SN</div>
                    <div style="flex: 3; text-align: center;">지역</div>
                    <div style="flex: 3; text-align: center;">센터</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            # 각 행 표시
            for idx, row in interactor_display.iterrows():
                cols = st.columns([2, 3, 3])
                cols[0].write(row['sn'])
                cols[1].write(row['area_name'] if pd.notna(row['area_name']) else '-')
                cols[2].write(row['center_name'] if pd.notna(row['center_name']) else '-')

# =============================================================================
# 탭3: 반복 이슈 분석
# =============================================================================
with tab3:
    st.caption("최근 30일 완료 건 중 1일 내 재고장 또는 라이딩 1건 이하 재발생")

    # 데이터 로드
    with st.spinner("데이터 로딩 중..."):
        try:
            repeat_df = load_repeat_issues()
        except Exception as e:
            st.error(f"데이터 로드 실패: {str(e)}")
            repeat_df = pd.DataFrame()

    if repeat_df.empty:
        st.success("반복 이슈가 없습니다!")
    else:
        # 필터 옵션 - 센터 권한에 따라 필터링
        col_f1, col_f2, col_f3 = st.columns(3)

        with col_f1:
            if is_admin:
                center_filter = st.selectbox(
                    "센터",
                    ["전체"] + ALL_CENTERS,
                    key="repeat_center"
                )
            else:
                # 일반 사용자: 본인 권한 센터만 표시
                user_centers = [c for c in allowed_centers if c in ALL_CENTERS]
                if len(user_centers) == 1:
                    center_filter = user_centers[0]
                    st.info(f"📍 {center_filter}")
                else:
                    center_filter = st.selectbox(
                        "센터",
                        user_centers,
                        key="repeat_center"
                    )

        # 완료일 기준 날짜 범위 필터
        min_date = repeat_df['completed_time'].min().date()
        max_date = repeat_df['completed_time'].max().date()

        with col_f2:
            start_date = st.date_input(
                "완료일 시작",
                value=min_date,
                min_value=min_date,
                max_value=max_date,
                key="repeat_start_date"
            )

        with col_f3:
            end_date = st.date_input(
                "완료일 종료",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
                key="repeat_end_date"
            )

        # 필터 적용
        filtered_df = repeat_df.copy()

        # 센터 필터 적용 (관리자가 "전체" 선택 시 제외)
        if is_admin and center_filter != "전체":
            filtered_df = filtered_df[filtered_df['center_name'] == center_filter]
        elif not is_admin:
            # 일반 사용자는 본인 센터만
            user_centers = [c for c in allowed_centers if c in ALL_CENTERS]
            filtered_df = filtered_df[filtered_df['center_name'].isin(user_centers)]
            if center_filter and center_filter != "전체":
                filtered_df = filtered_df[filtered_df['center_name'] == center_filter]

        # 날짜 필터 적용
        filtered_df = filtered_df[
            (filtered_df['completed_time'].dt.date >= start_date) &
            (filtered_df['completed_time'].dt.date <= end_date)
        ]

        # 요약 메트릭
        total_issues = len(filtered_df)
        day_rebreak = len(filtered_df[filtered_df['outlier_reason'] == '1일내_재고장'])
        low_riding = len(filtered_df[filtered_df['outlier_reason'] == '라이딩1건이하'])

        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric("총 반복이슈", f"{total_issues}건")
        col_m2.metric("1일내 재고장", f"{day_rebreak}건")
        col_m3.metric("라이딩 1건이하", f"{low_riding}건")

        st.markdown("---")

        # 매니저별 분석
        st.markdown("### 👤 매니저별 반복 이슈")

        manager_summary = filtered_df.groupby(['staff_name', 'center_name']).agg(
            재발생건수=('maintenance_id', 'count')
        ).reset_index()
        manager_summary = manager_summary.sort_values('재발생건수', ascending=False)
        manager_summary.columns = ['매니저', '센터', '재발생건수']

        # 테이블 헤더
        st.markdown("""
        <div style="background-color: #f0f2f6; padding: 10px 0; border-radius: 5px; margin-bottom: 10px;">
            <div style="display: flex; font-weight: bold; font-size: 14px;">
                <div style="flex: 2; text-align: center;">매니저</div>
                <div style="flex: 2; text-align: center;">센터</div>
                <div style="flex: 1; text-align: center;">재발생건수</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        for idx, row in manager_summary.iterrows():
            cols = st.columns([2, 2, 1])
            cols[0].write(row['매니저'] if pd.notna(row['매니저']) else '-')
            cols[1].write(row['센터'] if pd.notna(row['센터']) else '-')
            cols[2].write(str(row['재발생건수']))

        st.markdown("---")

        # 상세 내역
        st.markdown("### 📋 상세 내역")

        # Excel 다운로드
        if not filtered_df.empty:
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                filtered_df.to_excel(writer, index=False, sheet_name='반복이슈')
            excel_data = output.getvalue()
            st.download_button(
                label="📥 Excel 다운로드",
                data=excel_data,
                file_name=f"반복이슈_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="excel_download_repeat"
            )

        # 상세 테이블 헤더
        st.markdown("""
        <div style="background-color: #f0f2f6; padding: 10px 0; border-radius: 5px; margin-bottom: 10px;">
            <div style="display: flex; font-weight: bold; font-size: 13px;">
                <div style="flex: 1.5; text-align: center;">SN</div>
                <div style="flex: 1.5; text-align: center;">센터</div>
                <div style="flex: 1.5; text-align: center;">매니저</div>
                <div style="flex: 2; text-align: center;">완료일</div>
                <div style="flex: 2; text-align: center;">재고장일</div>
                <div style="flex: 1; text-align: center;">시간(h)</div>
                <div style="flex: 1; text-align: center;">라이딩</div>
                <div style="flex: 1.5; text-align: center;">사유</div>
                <div style="flex: 2.5; text-align: center;">완료코멘트</div>
                <div style="flex: 2.5; text-align: center;">재고장코멘트</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        for idx, row in filtered_df.iterrows():
            cols = st.columns([1.5, 1.5, 1.5, 2, 2, 1, 1, 1.5, 2.5, 2.5])
            cols[0].write(row['bike_sn'] if pd.notna(row['bike_sn']) else '-')
            cols[1].write(row['center_name'] if pd.notna(row['center_name']) else '-')
            cols[2].write(row['staff_name'] if pd.notna(row['staff_name']) else '-')
            cols[3].write(str(row['completed_time'])[:16] if pd.notna(row['completed_time']) else '-')
            cols[4].write(str(row['next_broken_time'])[:16] if pd.notna(row['next_broken_time']) else '-')
            cols[5].write(str(row['lead_time_hours']) if pd.notna(row['lead_time_hours']) else '-')
            cols[6].write(str(row['riding_count_between']) if pd.notna(row['riding_count_between']) else '-')
            cols[7].write(row['outlier_reason'] if pd.notna(row['outlier_reason']) else '-')
            cols[8].write(row['completed_comment'] if pd.notna(row['completed_comment']) else '-')
            cols[9].write(row['next_broken_comment'] if pd.notna(row['next_broken_comment']) else '-')
