# utils/auth.py
import os
import streamlit as st
from typing import Optional

def _render_password_prompt(expected_password: str, state_key: str) -> bool:
    """
    실제 비밀번호 프롬프트 렌더링/검증.
    state_key는 세션 스테이트에서 결과를 구분하기 위함 (prod/dev 구분).
    """
    def password_entered():
        if st.session_state.get("password", "") == expected_password:
            st.session_state[state_key] = True
            try:
                del st.session_state["password"]
            except KeyError:
                pass
        else:
            st.session_state[state_key] = False

    # 이미 검증되어 있으면 True 반환
    if st.session_state.get(state_key):
        return True

    st.text_input("Password", type="password", key="password", on_change=password_entered)
    if state_key in st.session_state and not st.session_state[state_key]:
        st.error("😕 Password incorrect")
    return False

def _get_secret_password() -> Optional[str]:
    """
    st.secrets가 있고 'password' 키가 있으면 반환.
    안전하게 접근하도록 예외를 방지합니다.
    """
    try:
        # st.secrets may be a mapping; use .get when available
        sec = getattr(st, "secrets", None)
        if sec is None:
            return None
        # sec might be Mapping or Secrets object; use get if present, else index with try/except
        if hasattr(sec, "get"):
            return sec.get("password")
        return sec["password"] if "password" in sec else None
    except Exception:
        return None

def check_password() -> bool:
    """
    동작 규칙 (목표)
      - 로컬(기본): 프리패스(True)
      - 배포(또는 st.secrets에 password가 있는 환경): st.secrets["password"]로 인증
    추가 옵션:
      - ENV=prod 이나 AUTH_REQUIRE=1 이면 로컬에서도 비밀번호 요구(테스트용)
    """
    # 사용자가 강제로 인증을 요구하면 그 설정을 따름 (테스트/CI 용도)
    force_auth = os.getenv("AUTH_REQUIRE") == "1" or os.getenv("ENV", "").lower() == "prod"

    secret_pw = _get_secret_password()

    # 배포(또는 secrets에 password가 있으면) -> 반드시 비밀번호 검사
    if secret_pw:
        return _render_password_prompt(secret_pw, state_key="password_correct_prod")

    # secrets에 비밀번호가 없더라도 사용자가 강제로 인증 요구하면 로컬 DEV_PASSWORD 사용
    if force_auth:
        dev_pw = os.getenv("DEV_PASSWORD")
        if not dev_pw:
            # 강제 설정했는데도 DEV_PASSWORD가 없으면 경고 후 프리패스(혹은 예외로 할 수도 있음)
            st.warning("AUTH_REQUIRE=1로 설정되어 있으나 DEV_PASSWORD가 없습니다. 인증을 우회합니다.")
            return True
        return _render_password_prompt(dev_pw, state_key="password_correct_dev")

    # 기본(로컬) 동작: 프리패스
    return True