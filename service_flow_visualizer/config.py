"""
서비스 흐름 시각화 - 설정
비행기 관제탑 스타일의 실시간 추적 시각화
"""

# 이벤트 타입별 설정
EVENT_CONFIG = {
    # ===== 유저 이벤트 =====
    'riding_start': {
        'icon': '▶',
        'color': '#00ff88',  # 네온 그린
        'glow': '#00ff8855',
        'label': '라이딩 시작',
        'category': 'user',
        'radius': 6,
        'priority': 10,
    },
    'riding_end': {
        'icon': '■',
        'color': '#ff4444',  # 빨강
        'glow': '#ff444455',
        'label': '라이딩 종료',
        'category': 'user',
        'radius': 6,
        'priority': 10,
    },
    'app_converted': {
        'icon': '✓',
        'color': '#00cc66',  # 초록
        'glow': '#00cc6633',
        'label': '앱→라이딩 전환',
        'category': 'user',
        'radius': 4,
        'priority': 5,
    },
    'app_accessible': {
        'icon': '○',
        'color': '#3399ff',  # 파랑
        'glow': '#3399ff33',
        'label': '앱 오픈 (기기 있음)',
        'category': 'user',
        'radius': 3,
        'priority': 3,
    },
    'app_no_bike': {
        'icon': '×',
        'color': '#ff6b6b',  # 연빨강
        'glow': '#ff6b6b33',
        'label': '앱 오픈 (기기 없음)',
        'category': 'user',
        'radius': 4,
        'priority': 7,  # 문제 상황이라 우선순위 높음
    },

    # ===== 운영 이벤트 =====
    'battery_swap': {
        'icon': '⚡',
        'color': '#ffdd00',  # 노랑
        'glow': '#ffdd0055',
        'label': '배터리 교체',
        'category': 'ops',
        'radius': 8,
        'priority': 8,
    },
    'rebalance_deploy': {
        'icon': '📍',
        'color': '#aa55ff',  # 보라
        'glow': '#aa55ff55',
        'label': '재배치 완료',
        'category': 'ops',
        'radius': 8,
        'priority': 8,
    },
    'rebalance_collect': {
        'icon': '↑',
        'color': '#9966cc',  # 연보라
        'glow': '#9966cc55',
        'label': '재배치 수거',
        'category': 'ops',
        'radius': 7,
        'priority': 6,
    },
    'broken_collect': {
        'icon': '🔧',
        'color': '#ff5555',  # 빨강
        'glow': '#ff555555',
        'label': '고장 수거',
        'category': 'ops',
        'radius': 8,
        'priority': 9,
    },
    'field_fix': {
        'icon': '🛠',
        'color': '#ff8800',  # 주황
        'glow': '#ff880055',
        'label': '현장조치',
        'category': 'ops',
        'radius': 8,
        'priority': 8,
    },
    'repair_deploy': {
        'icon': '✔',
        'color': '#44aa44',  # 녹색
        'glow': '#44aa4455',
        'label': '수리 후 배치',
        'category': 'ops',
        'radius': 8,
        'priority': 7,
    },
}

# 라이딩 경로(트레일) 설정 - 비행기 관제 스타일
TRAIL_CONFIG = {
    'active': {
        'color': '#00ffff',      # 시안 (활성 라이딩)
        'weight': 3,
        'opacity': 0.9,
        'dash_array': None,      # 실선
    },
    'completed': {
        'color': '#666688',      # 회색 (완료된 라이딩)
        'weight': 2,
        'opacity': 0.4,
        'dash_array': '5, 5',    # 점선
    },
    'fade_steps': 10,            # 페이드 아웃 단계
}

# 관리자 동선 설정
STAFF_TRAIL_CONFIG = {
    'weight': 2,
    'opacity': 0.7,
    'dash_array': '3, 6',        # 점선
    'colors': [
        '#ff7700', '#00aaff', '#ff00aa', '#aaff00',
        '#7700ff', '#00ffaa', '#ffaa00', '#aa00ff',
    ],
}

# 애니메이션 설정
ANIMATION_CONFIG = {
    'default_speed': 60,         # 1초당 60분 (1시간)
    'min_speed': 10,             # 최소 속도
    'max_speed': 300,            # 최대 속도 (5시간/초)
    'marker_fade_time': 300,     # 마커 페이드 시간 (초, 실제 시간 기준)
    'trail_duration': 600,       # 경로 표시 유지 시간 (초)
    'frame_interval': 100,       # 프레임 간격 (ms)
}

# 지도 스타일 - 다크 테마 (관제탑 느낌)
MAP_CONFIG = {
    'tiles': 'cartodbdark_matter',  # 다크 테마
    'default_zoom': 13,
    'default_center': [37.5665, 126.9780],  # 서울 기본
    'attribution': '© OpenStreetMap contributors © CARTO',
}

# 시간대별 배경색 (옵션)
TIME_COLORS = {
    'night': '#0a0a1a',      # 00-06시
    'morning': '#1a1a2e',    # 06-12시
    'afternoon': '#16213e',  # 12-18시
    'evening': '#0f0f23',    # 18-24시
}

# 센터 정보 (센터명, 색상, 중심좌표, 소속 권역들)
CENTER_INFO = {
    'Center_North': {
        'color': '#ff7700',
        'center': [37.6584, 126.8320],
        'regions': ['Center_North', 'Region_NW', 'Center_Gimpo', 'Region_N'],
    },
    'Center_West': {
        'color': '#00aaff',
        'center': [37.4954, 126.8874],
        'regions': ['Center_West', 'Region_W1', 'Region_W2'],
    },
    'Center_South': {
        'color': '#ff00aa',
        'center': [37.3617, 126.9352],
        'regions': ['Center_South', 'Region_S1', 'Region_S2'],
    },
    'Center_Central': {
        'color': '#aaff00',
        'center': [36.4800, 127.2890],
        'regions': ['Center_Central'],
    },
    'Center_East': {
        'color': '#7700ff',
        'center': [37.6360, 127.2165],
        'regions': ['Center_East', 'Region_E1'],
    },
    'Partner_Seoul': {
        'color': '#00ffaa',
        'center': [37.5665, 126.9780],
        'regions': ['서울', '강남', '송파'],
    },
    'Partner_Daejeon': {
        'color': '#ffaa00',
        'center': [36.3504, 127.3845],
        'regions': ['Region_G1', 'Region_G2'],
    },
    'Partner_Gwacheon': {
        'color': '#aa00ff',
        'center': [37.4292, 126.9876],
        'regions': ['Region_H1', 'Region_H2'],
    },
    'Partner_Ansan': {
        'color': '#ff5577',
        'center': [37.3219, 126.8309],
        'regions': ['Region_I1', 'Region_I2'],
    },
}

# 센터 목록 (UI용)
CENTERS = ['전체'] + list(CENTER_INFO.keys())

# 센터별 색상 (하위호환)
CENTER_COLORS = {f"{k}센터": v['color'] for k, v in CENTER_INFO.items()}
