"""
BikeShare admin web 자동화
- Playwright를 사용하여 admin web 자동 조작
- 반납구역 제외/축소/확대/추가 작업 수행

⚠️ 주의: 이 코드는 BikeShare admin web의 실제 UI에 맞게 수정이 필요합니다.
         아래는 일반적인 구조로 작성된 템플릿입니다.
"""
import os
import logging
from datetime import datetime
from typing import Optional
from pathlib import Path
from dataclasses import dataclass

from playwright.sync_api import sync_playwright, Page, Browser, TimeoutError as PlaywrightTimeout

import sys
sys.path.append('..')
from config import config

logger = logging.getLogger(__name__)


@dataclass
class AutomationResult:
    """자동화 실행 결과"""
    success: bool
    message: str
    screenshot_path: Optional[str] = None
    error: Optional[str] = None


class AdminWebAutomation:
    """BikeShare admin web 자동화"""

    def __init__(self):
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self._ensure_screenshot_dir()

    def _ensure_screenshot_dir(self):
        """스크린샷 디렉토리 생성"""
        Path(config.admin_web.screenshot_dir).mkdir(parents=True, exist_ok=True)

    def _take_screenshot(self, name: str) -> str:
        """스크린샷 저장"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.png"
        filepath = os.path.join(config.admin_web.screenshot_dir, filename)

        if self.page:
            self.page.screenshot(path=filepath)
            logger.info(f"스크린샷 저장: {filepath}")

        return filepath

    def start_browser(self):
        """브라우저 시작"""
        playwright = sync_playwright().start()

        self.browser = playwright.chromium.launch(
            headless=config.admin_web.headless,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )

        self.page = self.browser.new_page()
        self.page.set_default_timeout(30000)  # 30초 타임아웃

        logger.info("브라우저 시작됨")

    def close_browser(self):
        """브라우저 종료"""
        if self.browser:
            self.browser.close()
            self.browser = None
            self.page = None
            logger.info("브라우저 종료됨")

    def login(self) -> bool:
        """admin web 로그인"""
        if not self.page:
            logger.error("브라우저가 시작되지 않았습니다.")
            return False

        try:
            logger.info(f"admin web 접속: {config.admin_web.base_url}")
            self.page.goto(config.admin_web.base_url)

            # 로그인 페이지 대기
            # ⚠️ 실제 admin web의 선택자에 맞게 수정 필요
            self.page.wait_for_selector("input[type='text'], input[name='username'], input[id='username']")

            # 로그인 정보 입력
            # ⚠️ 실제 admin web의 선택자에 맞게 수정 필요
            username_selector = "input[type='text'], input[name='username'], input[id='username']"
            password_selector = "input[type='password']"
            login_button_selector = "button[type='submit'], input[type='submit'], button:has-text('로그인')"

            self.page.fill(username_selector, config.admin_web.username)
            self.page.fill(password_selector, config.admin_web.password)
            self.page.click(login_button_selector)

            # 로그인 성공 확인 (대시보드 로드 대기)
            # ⚠️ 실제 admin web의 로그인 성공 지표에 맞게 수정 필요
            self.page.wait_for_load_state("networkidle")

            logger.info("로그인 성공")
            self._take_screenshot("login_success")
            return True

        except PlaywrightTimeout:
            logger.error("로그인 타임아웃")
            self._take_screenshot("login_timeout")
            return False
        except Exception as e:
            logger.error(f"로그인 실패: {e}")
            self._take_screenshot("login_error")
            return False

    def navigate_to_zone_management(self) -> bool:
        """반납구역 관리 페이지로 이동"""
        if not self.page:
            return False

        try:
            # ⚠️ 실제 admin web의 메뉴 구조에 맞게 수정 필요
            # 예시: 사이드바 메뉴 클릭
            menu_selectors = [
                "text=반납구역",
                "text=구역 관리",
                "text=Zone Management",
                "a[href*='zone']",
                "[data-menu='zone']",
            ]

            for selector in menu_selectors:
                try:
                    self.page.click(selector, timeout=5000)
                    self.page.wait_for_load_state("networkidle")
                    logger.info("반납구역 관리 페이지 이동 완료")
                    self._take_screenshot("zone_management")
                    return True
                except:
                    continue

            logger.error("반납구역 관리 메뉴를 찾을 수 없습니다.")
            return False

        except Exception as e:
            logger.error(f"페이지 이동 실패: {e}")
            return False

    def search_zone(self, zone_name: str) -> bool:
        """구역 검색"""
        if not self.page:
            return False

        try:
            # ⚠️ 실제 admin web의 검색 UI에 맞게 수정 필요
            search_selectors = [
                "input[placeholder*='검색']",
                "input[type='search']",
                "input[name='search']",
                ".search-input",
            ]

            for selector in search_selectors:
                try:
                    self.page.fill(selector, zone_name, timeout=5000)
                    self.page.press(selector, "Enter")
                    self.page.wait_for_load_state("networkidle")
                    logger.info(f"구역 검색 완료: {zone_name}")
                    self._take_screenshot("zone_search")
                    return True
                except:
                    continue

            logger.warning("검색 입력란을 찾을 수 없습니다.")
            return False

        except Exception as e:
            logger.error(f"구역 검색 실패: {e}")
            return False

    def exclude_zone(self, zone_name: str, reason: str) -> AutomationResult:
        """
        반납구역 제외 실행

        ⚠️ 이 메서드는 BikeShare admin web의 실제 UI에 맞게 수정이 필요합니다.
        """
        try:
            self.start_browser()

            if not self.login():
                return AutomationResult(
                    success=False,
                    message="로그인 실패",
                    error="로그인에 실패했습니다. 계정 정보를 확인하세요.",
                )

            if not self.navigate_to_zone_management():
                return AutomationResult(
                    success=False,
                    message="페이지 이동 실패",
                    error="반납구역 관리 페이지로 이동할 수 없습니다.",
                )

            # 구역 검색
            self.search_zone(zone_name)

            # ⚠️ 아래 코드는 실제 UI에 맞게 수정 필요
            # 예시: 구역 선택 후 제외 버튼 클릭
            try:
                # 검색 결과에서 구역 선택
                self.page.click(f"text={zone_name}", timeout=10000)

                # 제외 버튼 클릭
                exclude_button_selectors = [
                    "button:has-text('제외')",
                    "button:has-text('비활성화')",
                    "button:has-text('Exclude')",
                    "[data-action='exclude']",
                ]

                for selector in exclude_button_selectors:
                    try:
                        self.page.click(selector, timeout=5000)
                        break
                    except:
                        continue

                # 확인 다이얼로그 처리
                try:
                    self.page.click("button:has-text('확인')", timeout=5000)
                except:
                    pass

                self.page.wait_for_load_state("networkidle")
                screenshot = self._take_screenshot("exclude_complete")

                return AutomationResult(
                    success=True,
                    message=f"구역 '{zone_name}' 제외 완료",
                    screenshot_path=screenshot,
                )

            except PlaywrightTimeout:
                screenshot = self._take_screenshot("exclude_timeout")
                return AutomationResult(
                    success=False,
                    message="작업 타임아웃",
                    screenshot_path=screenshot,
                    error=f"구역 '{zone_name}'을(를) 찾을 수 없거나 제외 버튼이 없습니다.",
                )

        except Exception as e:
            logger.error(f"구역 제외 실패: {e}")
            screenshot = self._take_screenshot("exclude_error")
            return AutomationResult(
                success=False,
                message="예외 발생",
                screenshot_path=screenshot,
                error=str(e),
            )
        finally:
            self.close_browser()

    def reduce_zone(self, zone_name: str, reason: str) -> AutomationResult:
        """
        반납구역 축소 실행

        ⚠️ 이 메서드는 BikeShare admin web의 실제 UI에 맞게 수정이 필요합니다.
        축소는 보통 지도에서 영역을 재설정해야 하므로 자동화가 복잡할 수 있습니다.
        """
        try:
            self.start_browser()

            if not self.login():
                return AutomationResult(
                    success=False,
                    message="로그인 실패",
                    error="로그인에 실패했습니다.",
                )

            if not self.navigate_to_zone_management():
                return AutomationResult(
                    success=False,
                    message="페이지 이동 실패",
                )

            self.search_zone(zone_name)

            # 축소는 지도 조작이 필요할 수 있어 완전 자동화가 어려울 수 있음
            # 현재는 해당 페이지까지 이동 후 스크린샷만 제공
            screenshot = self._take_screenshot("reduce_ready")

            return AutomationResult(
                success=True,
                message=f"구역 '{zone_name}' 수정 준비 완료. 수동 조정이 필요합니다.",
                screenshot_path=screenshot,
            )

        except Exception as e:
            logger.error(f"구역 축소 준비 실패: {e}")
            return AutomationResult(
                success=False,
                message="예외 발생",
                error=str(e),
            )
        finally:
            self.close_browser()

    def execute_action(
        self,
        action_type: str,
        zone_name: str,
        reason: str,
        **kwargs
    ) -> AutomationResult:
        """
        액션 타입에 따라 적절한 메서드 실행

        Args:
            action_type: "exclude", "reduce", "expand", "add"
            zone_name: 대상 구역명
            reason: 변경 사유
        """
        action_map = {
            "exclude": self.exclude_zone,
            "reduce": self.reduce_zone,
            # expand, add는 reduce와 유사하게 구현
            "expand": self.reduce_zone,  # 임시로 동일한 플로우
            "add": self.reduce_zone,
        }

        if action_type not in action_map:
            return AutomationResult(
                success=False,
                message="알 수 없는 액션 타입",
                error=f"지원하지 않는 액션: {action_type}",
            )

        return action_map[action_type](zone_name, reason)


# 테스트용
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # 설정 확인
    print(f"Base URL: {config.admin_web.base_url}")
    print(f"Headless: {config.admin_web.headless}")

    # 실제 테스트는 .env 설정 후 실행
    # automation = AdminWebAutomation()
    # result = automation.exclude_zone("테스트 구역", "테스트 사유")
    # print(result)
