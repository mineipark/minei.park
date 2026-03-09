"""
사이드바 스타일링 공통 함수
- 홈 페이지 강조
- 하위 페이지 계층 구조 표시
- 권한별 그룹핑 표시
"""

import streamlit as st


def apply_sidebar_style():
    """사이드바 네비게이션 스타일 적용"""
    is_admin = "전체" in st.session_state.get("allowed_centers", [])

    st.markdown("""
    <style>
        /* 사이드바 네비게이션 스타일 */
        [data-testid="stSidebarNav"] {
            padding-top: 1rem;
        }

        /* 첫 번째 항목(홈) 강조 */
        [data-testid="stSidebarNav"] > ul > li:first-child {
            background: linear-gradient(90deg, #ff4b4b22, transparent);
            border-left: 3px solid #ff4b4b;
            margin-bottom: 1rem;
        }

        [data-testid="stSidebarNav"] > ul > li:first-child a {
            font-weight: bold;
            font-size: 1.1rem;
        }

        /* 나머지 페이지들 들여쓰기 */
        [data-testid="stSidebarNav"] > ul > li:not(:first-child) {
            padding-left: 0.5rem;
            border-left: 1px solid #ddd;
            margin-left: 0.5rem;
        }
    </style>
    """, unsafe_allow_html=True)

    # 관리자일 경우 관리자 전용 페이지 표시
    if is_admin:
        st.markdown("""
        <style>
            /* 관리자 전용 페이지 스타일: 전센터 대시보드(2번), 유지보수 성과(6번) */
            [data-testid="stSidebarNav"] > ul > li:nth-child(2),
            [data-testid="stSidebarNav"] > ul > li:nth-child(6) {
                background-color: #fffbf0;
                border-left: 2px solid #ffc107 !important;
                margin-left: 0.5rem;
                padding-left: 0.5rem;
            }

            /* 관리자 전용 라벨 추가 */
            [data-testid="stSidebarNav"] > ul > li:nth-child(2) a::after,
            [data-testid="stSidebarNav"] > ul > li:nth-child(6) a::after {
                content: " 🔒";
                font-size: 10px;
            }
        </style>
        """, unsafe_allow_html=True)
