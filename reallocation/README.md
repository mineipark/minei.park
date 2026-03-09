# 재배치 알고리즘 (Reallocation)

자전거 최적 재배치를 위한 알고리즘 시스템

## 개요

H3 헥사곤 인덱스 기반으로 자전거 수요와 공급을 분석하여 최적의 재배치 위치를 계산합니다.

## 주요 파일

- `reallocation_algorithm.py` - 재배치 알고리즘 핵심 로직

## 알고리즘 개요

### 데이터 처리 (raw_to_hex)
자전거 위치와 라이딩 데이터를 H3 헥사곤 단위로 집계

**입력 데이터**:
- `bike_raw`: 자전거 현재 위치
- `ride_raw`: 과거 라이딩 이력
- `bike_snapshot_raw`: 자전거 스냅샷
- `zone`: 권역 정보
- `geo_district`: 행정구역 지오메트리

**처리 과정**:
1. 자전거 위치를 H3 인덱스로 변환 (resolution 9)
2. 행정구역 필터링 (제외 구역 처리)
3. 라이딩 데이터 가중치 적용 (주말/평일 차등)
4. 헥사곤별 수요/공급 집계

### 재배치 대상 선정 (get_algorithm_hex)
수거(from_hex) 대상 헥사곤 선정

**선정 기준**:
- 자전거 대수 >= 3대
- 수요 대비 공급 과잉 지역
- 라이딩 실적 저조 지역

**특수 로직**:
- **동탄 피크타임**: 21시 이후 특정 헥사곤 지정 수거
- **천안 권역**: 자전거 대수 조건 완화 (>= 1대)
- **Central PoC**: 잔여 배터리 35% 미만 필터링

## 핵심 지표

| 지표 | 설명 |
|------|------|
| `ride_cnt` | 헥사곤별 과거 라이딩 수 |
| `bike_cnt` | 헥사곤별 현재 자전거 수 |
| `fee` | 48시간 내 발생 요금 |
| `fpb` | Fee Per Bike (자전거당 수익) |
| `rpb` | Ride Per Bike (자전거당 라이딩) |
| `ride_distance` | 표준화된 수요 거리 (낮을수록 수거 우선) |

## 사용법

```python
from reallocation_algorithm import raw_to_hex, get_algorithm_hex

# 데이터 전처리
all_hex = raw_to_hex(
    bike_raw, ride_raw, bike_snapshot_raw, ride_new,
    zone, geo_district, area_dict, district_exception,
    hour, isweekend
)

# 수거 대상 선정
from_hex = get_algorithm_hex(
    arg=100,  # 재배치 건수
    all_hex=all_hex,
    ride_raw=ride_raw,
    to_hex_neighbor_list=to_hex_list,
    debug=False,
    hour=14
)
```

## 의존성

```bash
pip install pandas numpy shapely h3 pytz
```

## 참고

- H3: Uber의 헥사곤 지리 인덱스 시스템
- Resolution 9: 약 105m 반경 헥사곤
