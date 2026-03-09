"""
AI 메시지 파서
- Claude API를 사용하여 비정형 메시지를 정형 데이터로 변환
"""
import json
import logging
from typing import Optional
from dataclasses import dataclass, asdict
from anthropic import Anthropic

import sys
sys.path.append('..')
from config import config

logger = logging.getLogger(__name__)


@dataclass
class ParsedRequest:
    """파싱된 반납구역 요청"""
    action_type: str  # "exclude" (제외), "reduce" (축소), "expand" (확대), "add" (추가)
    zone_name: str  # 구역명
    region: Optional[str]  # 지역명 (시/구/동)
    reason: str  # 요청 사유
    time_range: Optional[str]  # 적용 시간대 (예: "09:00-18:00", "종일")
    duration: Optional[str]  # 적용 기간 (예: "오늘만", "1주일", "상시")
    coordinates: Optional[str]  # 좌표 또는 위치 정보
    urgency: str  # "high", "medium", "low"
    confidence: float  # 파싱 신뢰도 (0.0 ~ 1.0)
    raw_text: str  # 원본 텍스트
    notes: Optional[str]  # 추가 메모

    def to_dict(self) -> dict:
        return asdict(self)


PARSING_PROMPT = """당신은 공유자전거 관제 시스템의 요청 파서입니다.
사용자 메시지를 분석하여 반납구역 관련 요청을 정형화된 JSON으로 변환하세요.

## 요청 유형
- exclude: 반납구역 제외 (특정 구역에서 반납 불가능하게)
- reduce: 반납구역 축소 (구역 범위를 줄임)
- expand: 반납구역 확대 (구역 범위를 늘림)
- add: 반납구역 추가 (새로운 구역 추가)

## 긴급도 기준
- high: 즉시 처리 필요 (안전 문제, 민원, 긴급 요청)
- medium: 오늘 중 처리 필요
- low: 시간 여유 있음

## 출력 형식 (JSON)
{
    "action_type": "exclude|reduce|expand|add",
    "zone_name": "구역명 (정확히 파악 안되면 추정)",
    "region": "지역명 (시/구/동)",
    "reason": "요청 사유",
    "time_range": "적용 시간대 (없으면 null)",
    "duration": "적용 기간 (없으면 null)",
    "coordinates": "좌표/위치 정보 (없으면 null)",
    "urgency": "high|medium|low",
    "confidence": 0.0-1.0,
    "notes": "추가 메모 또는 불확실한 부분"
}

## 예시

입력: "동탄역 앞 반납존 좀 빼주세요. 민원 들어왔어요"
출력:
{
    "action_type": "exclude",
    "zone_name": "동탄역 앞",
    "region": "화성시 동탄",
    "reason": "민원 발생",
    "time_range": null,
    "duration": null,
    "coordinates": null,
    "urgency": "high",
    "confidence": 0.85,
    "notes": "정확한 구역 범위 확인 필요"
}

입력: "City Hall area Please reduce the return zone boundary nearby. The parking lot area is too wide"
출력:
{
    "action_type": "reduce",
    "zone_name": "정부청사 반납구역",
    "region": "City Hall area",
    "reason": "구역 범위 과대 (주차장 포함)",
    "time_range": null,
    "duration": null,
    "coordinates": null,
    "urgency": "medium",
    "confidence": 0.80,
    "notes": "주차장 쪽 경계 조정 필요"
}

---
JSON만 출력하세요. 다른 설명은 하지 마세요.
"""


def parse_return_zone_request(message: str) -> Optional[ParsedRequest]:
    """
    비정형 메시지를 파싱하여 정형화된 요청 객체로 변환

    Args:
        message: 원본 메시지 텍스트

    Returns:
        ParsedRequest 객체 또는 None (파싱 실패 시)
    """
    if not config.anthropic.api_key:
        logger.error("ANTHROPIC_API_KEY가 설정되지 않았습니다.")
        return None

    client = Anthropic(api_key=config.anthropic.api_key)

    try:
        response = client.messages.create(
            model=config.anthropic.model,
            max_tokens=1024,
            messages=[
                {
                    "role": "user",
                    "content": f"{PARSING_PROMPT}\n\n---\n입력: {message}",
                }
            ],
        )

        # 응답에서 JSON 추출
        response_text = response.content[0].text.strip()

        # JSON 파싱
        # 코드 블록으로 감싸져 있을 수 있음
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            response_text = "\n".join(lines[1:-1])

        parsed_data = json.loads(response_text)

        # ParsedRequest 객체 생성
        return ParsedRequest(
            action_type=parsed_data.get("action_type", "exclude"),
            zone_name=parsed_data.get("zone_name", "알 수 없음"),
            region=parsed_data.get("region"),
            reason=parsed_data.get("reason", "사유 미상"),
            time_range=parsed_data.get("time_range"),
            duration=parsed_data.get("duration"),
            coordinates=parsed_data.get("coordinates"),
            urgency=parsed_data.get("urgency", "medium"),
            confidence=float(parsed_data.get("confidence", 0.5)),
            raw_text=message,
            notes=parsed_data.get("notes"),
        )

    except json.JSONDecodeError as e:
        logger.error(f"JSON 파싱 실패: {e}")
        logger.error(f"응답: {response_text}")
        return None
    except Exception as e:
        logger.error(f"AI 파싱 중 오류: {e}")
        return None


def format_parsed_request_for_display(parsed: ParsedRequest) -> str:
    """파싱 결과를 사람이 읽기 좋은 형태로 포맷"""
    action_type_kr = {
        "exclude": "제외",
        "reduce": "축소",
        "expand": "확대",
        "add": "추가",
    }

    urgency_kr = {
        "high": "높음 🔴",
        "medium": "보통 🟡",
        "low": "낮음 🟢",
    }

    lines = [
        f"📍 구역명: {parsed.zone_name}",
        f"📋 요청 유형: {action_type_kr.get(parsed.action_type, parsed.action_type)}",
        f"📝 사유: {parsed.reason}",
        f"⚡ 긴급도: {urgency_kr.get(parsed.urgency, parsed.urgency)}",
    ]

    if parsed.region:
        lines.append(f"🗺️ 지역: {parsed.region}")

    if parsed.time_range:
        lines.append(f"🕐 적용 시간: {parsed.time_range}")

    if parsed.duration:
        lines.append(f"📅 적용 기간: {parsed.duration}")

    if parsed.coordinates:
        lines.append(f"📌 좌표/위치: {parsed.coordinates}")

    lines.append(f"🎯 신뢰도: {parsed.confidence * 100:.0f}%")

    if parsed.notes:
        lines.append(f"💬 비고: {parsed.notes}")

    return "\n".join(lines)


# 테스트용
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    test_messages = [
        "Please remove the return zone in front of Station A. We received complaints.",
        "The zone boundary near City Hall is too wide, please reduce it.",
        "Please add a return zone on the west exit side of Station B.",
    ]

    for msg in test_messages:
        print(f"\n{'='*50}")
        print(f"입력: {msg}")
        print("-" * 50)

        result = parse_return_zone_request(msg)
        if result:
            print(format_parsed_request_for_display(result))
        else:
            print("파싱 실패")
