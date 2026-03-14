import os
import streamlit as st
from utils.sidebar_style import apply_sidebar_style

st.set_page_config(page_title="BikeShare 운영 대시보드", layout="wide")

def hide_sidebar_pages(labels: list[str]):
    # Hide specific sidebar nav links by text match
    joined = ",".join([f'"{lbl}"' for lbl in labels])
    st.markdown(
        f"""
        <script>
        const targets = [{joined}];
        const nav = window.parent.document.querySelector('[data-testid="stSidebarNav"]');
        if (nav) {{
          const links = Array.from(nav.querySelectorAll('li a'));
          links.forEach((link) => {{
            const text = link.innerText.trim();
            if (targets.includes(text)) {{
              const li = link.closest('li');
              if (li) li.style.display = 'none';
            }}
          }});
        }}
        </script>
        """,
        unsafe_allow_html=True,
    )

# -----------------------------------------------------------
# User DB - loaded from environment or config file
# In production, this uses a database. Sample structure shown below.
# -----------------------------------------------------------

USER_DB = {
    # Passwords loaded from environment variables (required)
    "demo_user": {
        "password": os.environ["DEMO_PASSWORD"],
        "centers": ["Center_North"]
    },
    "admin": {
        "password": os.environ["ADMIN_PASSWORD"],
        "centers": ["all"]
    },
}


VALID_CENTERS = [
    "Center_North", "Center_West", "Center_South",
    "Center_East", "Center_Central", "Partner_Gwacheon",
    "Partner_Ansan", "Partner_Seoul", "Partner_Daejeon"
]

# -----------------------------------------------------------
# 로그인 화면
# -----------------------------------------------------------
def login_screen():
    st.title("🔐 로그인")
    st.write("BikeShare 운영 대시보드 접근을 위해 이메일/비밀번호를 입력하세요.")

    email = st.text_input("이름", placeholder="이름을 입력하세요.")
    pw = st.text_input("비밀번호", type="password")

    if st.button("로그인"):
        if email in USER_DB and pw == USER_DB[email]["password"]:

            # 유저 정보 세션에 저장
            st.session_state["user"] = email
            allowed_centers = USER_DB[email]["centers"]

            # all 권한일 경우 모든 센터 허용
            if "all" in allowed_centers:
                first_center = VALID_CENTERS[0]
            else:
                first_center = allowed_centers[0]

            st.session_state["center"] = first_center
            st.session_state["allowed_centers"] = allowed_centers

            st.success("로그인 성공! 대시보드로 이동합니다…")

            # 👉 자동으로 센터별 대시보드로 이동
            st.switch_page("pages/1_센터별_대시보드.py")
        else:
            st.error("이메일 또는 비밀번호가 일치하지 않습니다.")

# -----------------------------------------------------------
# 로그인 체크
# -----------------------------------------------------------
if "user" not in st.session_state:
    login_screen()
    st.stop()

# -----------------------------------------------------------
# 로그인 이후 메인 페이지
# -----------------------------------------------------------

# 사이드바 스타일 적용
apply_sidebar_style()

# all 권한이 없으면 관리자 전용 페이지 숨김
if "all" not in st.session_state.get("allowed_centers", []):
    hide_sidebar_pages(["전센터 대시보드", "유지보수 성과"])

# 메인 콘텐츠
st.title("🏠 BikeShare 운영 대시보드")
st.write(f"환영합니다 **{st.session_state['user']}** 님 🙌")

# 로그아웃
with st.sidebar:
    st.markdown("---")
    if st.button("🚪 로그아웃", use_container_width=True):
        st.session_state.clear()
        st.rerun()
