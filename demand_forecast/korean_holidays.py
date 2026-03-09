"""
한국 공휴일 데이터

BigQuery korean_holiday 테이블 보충 + 최신 모델에서 단독 사용.
음력 기반 공휴일(설, 추석, 부처님오신날)은 매년 날짜가 달라지므로 수동 관리.

사용법:
    from korean_holidays import ADDITIONAL_HOLIDAYS
    from korean_holidays import get_holidays
"""
from datetime import date

# --- 2025년 공휴일 ---
HOLIDAYS_2025 = {
    date(2025, 1, 1),    # 신정
    date(2025, 1, 28),   # 설 연휴
    date(2025, 1, 29),   # 설날
    date(2025, 1, 30),   # 설 연휴
    date(2025, 3, 1),    # 삼일절 (토)
    date(2025, 3, 3),    # 삼일절 대체공휴일 (월)
    date(2025, 5, 5),    # 어린이날 / 부처님오신날
    date(2025, 5, 6),    # 대체공휴일 (부처님오신날)
    date(2025, 6, 6),    # 현충일
    date(2025, 8, 15),   # 광복절
    date(2025, 10, 3),   # 개천절
    date(2025, 10, 5),   # 추석 연휴 (일)
    date(2025, 10, 6),   # 추석
    date(2025, 10, 7),   # 추석 연휴
    date(2025, 10, 8),   # 추석 대체공휴일 (수)
    date(2025, 10, 9),   # 한글날
    date(2025, 12, 25),  # 크리스마스
}

# --- 2026년 공휴일 ---
HOLIDAYS_2026 = {
    date(2026, 1, 1),    # 신정
    date(2026, 2, 16),   # 설 연휴
    date(2026, 2, 17),   # 설날
    date(2026, 2, 18),   # 설 연휴
    date(2026, 3, 1),    # 삼일절 (일)
    date(2026, 3, 2),    # 삼일절 대체공휴일 (월)
    date(2026, 5, 5),    # 어린이날
    date(2026, 5, 24),   # 부처님오신날 (일)
    date(2026, 5, 25),   # 부처님오신날 대체공휴일 (월)
    date(2026, 6, 3),    # 제9회 지방선거
    date(2026, 6, 6),    # 현충일 (토)
    date(2026, 7, 17),   # 제헌절
    date(2026, 8, 15),   # 광복절 (토)
    date(2026, 8, 17),   # 광복절 대체공휴일 (월)
    date(2026, 9, 24),   # 추석 연휴
    date(2026, 9, 25),   # 추석
    date(2026, 9, 26),   # 추석 연휴 (토)
    date(2026, 9, 28),   # 추석 대체공휴일 (월)
    date(2026, 10, 3),   # 개천절 (토)
    date(2026, 10, 5),   # 개천절 대체공휴일 (월)
    date(2026, 10, 9),   # 한글날
    date(2026, 12, 25),  # 크리스마스
}

# 전체 합산 (기존 호환)
ADDITIONAL_HOLIDAYS = HOLIDAYS_2025 | HOLIDAYS_2026


def get_holidays(year: int = None) -> set:
    """연도별 공휴일 반환. year=None이면 전체."""
    _all = {
        2025: HOLIDAYS_2025,
        2026: HOLIDAYS_2026,
    }
    if year is None:
        return ADDITIONAL_HOLIDAYS
    return _all.get(year, set())
