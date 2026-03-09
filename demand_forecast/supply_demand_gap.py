"""
수요-공급 갭 분석 + 현장팀 시간대별 작업지시서

district_hour_model의 예측 → 현재 공급량 비교 → 부족/과잉 분류
→ 센터별 × 시간대별 작업지시서 Excel + Folium 갭 시각화 맵

사용법:
    python supply_demand_gap.py --date 2026-02-25
    python supply_demand_gap.py --date 2026-02-25 --center Center_North
    python supply_demand_gap.py --date 2026-02-25 --snapshot-date 2026-02-24 --snapshot-hour 21
"""
import os
import sys
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import folium
from folium import FeatureGroup, LayerControl

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

# Authentication: set GOOGLE_APPLICATION_CREDENTIALS env var
SCRIPT_DIR_CREDS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'credentials', 'service-account.json')
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    if os.path.exists(SCRIPT_DIR_CREDS):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SCRIPT_DIR_CREDS

OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'visualizations')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 시간대 슬롯 (district_hour_model과 동일)
TIME_SLOTS = {
    'night_prep': {'hours': list(range(22, 24)) + list(range(0, 7)), 'desc': '야간준비 (22~06시)'},
    'morning':    {'hours': list(range(7, 13)),  'desc': '오전 피크 (07~12시)'},
    'afternoon':  {'hours': list(range(13, 19)), 'desc': '오후 수요 (13~18시)'},
    'evening':    {'hours': list(range(19, 22)), 'desc': '저녁 (19~21시)'},
}

# 갭 분류 기준
DEFICIT_THRESHOLD = 0.8   # 가용 < 수요 × 0.8 → 부족
SURPLUS_THRESHOLD = 1.2   # 가용 > 수요 × 1.2 → 과잉

# 색상
COLOR_DEFICIT = '#D32F2F'  # 빨강 (부족)
COLOR_SURPLUS = '#1976D2'  # 파랑 (과잉)
COLOR_BALANCED = '#4CAF50' # 초록 (균형)
COLOR_BATTERY = '#FBC02D'  # 노랑 (배터리)


class SupplyDemandAnalyzer:
    """수요-공급 갭 분석기"""

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self._client = None
        self._center_mapping = None

    @property
    def client(self):
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client()
        return self._client

    # ================================================================
    # 수요 예측 (district_hour_model 연결)
    # ================================================================

    def get_demand_prediction(self, target_date: str) -> pd.DataFrame:
        """
        district×hour 수요 예측

        Returns:
            DataFrame[region, district, hour, predicted_rides, lat, lng, center]
        """
        from district_hour_model import DistrictHourPredictor

        predictor = DistrictHourPredictor(verbose=self.verbose)
        pred_df = predictor.predict(target_date)

        return pred_df

    # ================================================================
    # 공급 현황 (tf_bike_snapshot)
    # ================================================================

    def get_supply_snapshot(
        self,
        snapshot_date: str = None,
        snapshot_hour: int = 21
    ) -> pd.DataFrame:
        """
        district별 현재 바이크 공급량 (단일 시간대)

        Args:
            snapshot_date: 기준 날짜 (None이면 오늘)
            snapshot_hour: 기준 시간 (기본 21시)

        Returns:
            DataFrame[region, district, total_bikes, usable_count,
                      available_count, low_battery_count, avg_battery, lat, lng]
        """
        if snapshot_date is None:
            date_filter = "DATE(time) = CURRENT_DATE('Asia/Seoul')"
        else:
            date_filter = f"DATE(time) = '{snapshot_date}'"

        query = f"""
        SELECT
            h3_area_name as region,
            h3_district_name as district,
            COUNT(*) as total_bikes,
            SUM(CASE WHEN is_usable = TRUE THEN 1 ELSE 0 END) as usable_count,
            SUM(CASE WHEN bike_status IN ('BAV', 'BNB') THEN 1 ELSE 0 END) as available_count,
            SUM(CASE WHEN leftover < 30 THEN 1 ELSE 0 END) as low_battery_count,
            AVG(leftover) as avg_battery,
            AVG(CAST(JSON_VALUE(location, '$.coordinates[1]') AS FLOAT64)) as lat,
            AVG(CAST(JSON_VALUE(location, '$.coordinates[0]') AS FLOAT64)) as lng
        FROM `bikeshare.service.bike_snapshot`
        WHERE {date_filter}
            AND EXTRACT(HOUR FROM time) = {snapshot_hour}
            AND h3_area_name IS NOT NULL
            AND h3_district_name IS NOT NULL
        GROUP BY 1, 2
        """

        if self.verbose:
            print(f"\n📡 공급 현황 조회 중... "
                  f"({'오늘' if not snapshot_date else snapshot_date} {snapshot_hour}시)")

        df = self.client.query(query).to_dataframe()

        if self.verbose:
            total_bikes = df['total_bikes'].sum()
            usable = df['usable_count'].sum()
            available = df['available_count'].sum()
            low_bat = df['low_battery_count'].sum()
            print(f"  총 {len(df)}개 district, {total_bikes:,}대 "
                  f"(가용 {available:,}대, 배터리부족 {low_bat:,}대)")

        return df

    # 시간대별 대표 시간 (각 TIME_SLOT의 시작 시간)
    SLOT_REPRESENTATIVE_HOURS = {
        'night_prep': 22,
        'morning': 7,
        'afternoon': 13,
        'evening': 19,
    }

    def get_supply_by_timeslot(
        self,
        snapshot_date: str = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        시간대별 공급 스냅샷 (각 TIME_SLOT별 대표 시간의 공급 현황)

        단일 쿼리로 4개 시간대 동시 조회 → 시간대별 gap 분석 정확도 향상
        - night_prep (22시): 야간 작업 시작 시점 공급
        - morning (7시): 오전 피크 시작 시점 공급
        - afternoon (13시): 오후 시작 시점 공급
        - evening (19시): 저녁 시작 시점 공급

        Args:
            snapshot_date: 기준 날짜 (None이면 오늘)

        Returns:
            {slot_name: DataFrame[region, district, ..., available_count, ...]}
        """
        if snapshot_date is None:
            date_filter = "DATE(time) = CURRENT_DATE('Asia/Seoul')"
        else:
            date_filter = f"DATE(time) = '{snapshot_date}'"

        rep_hours = list(self.SLOT_REPRESENTATIVE_HOURS.values())
        hours_str = ','.join(str(h) for h in rep_hours)

        query = f"""
        SELECT
            EXTRACT(HOUR FROM time) as snapshot_hour,
            h3_area_name as region,
            h3_district_name as district,
            COUNT(*) as total_bikes,
            SUM(CASE WHEN is_usable = TRUE THEN 1 ELSE 0 END) as usable_count,
            SUM(CASE WHEN bike_status IN ('BAV', 'BNB') THEN 1 ELSE 0 END) as available_count,
            SUM(CASE WHEN leftover < 30 THEN 1 ELSE 0 END) as low_battery_count,
            AVG(leftover) as avg_battery,
            AVG(CAST(JSON_VALUE(location, '$.coordinates[1]') AS FLOAT64)) as lat,
            AVG(CAST(JSON_VALUE(location, '$.coordinates[0]') AS FLOAT64)) as lng
        FROM `bikeshare.service.bike_snapshot`
        WHERE {date_filter}
            AND EXTRACT(HOUR FROM time) IN ({hours_str})
            AND h3_area_name IS NOT NULL
            AND h3_district_name IS NOT NULL
        GROUP BY 1, 2, 3
        """

        if self.verbose:
            print(f"\n📡 시간대별 공급 현황 조회 중... "
                  f"({'오늘' if not snapshot_date else snapshot_date}, "
                  f"시간: {rep_hours})")

        df = self.client.query(query).to_dataframe()

        # 시간대별 분리
        result = {}
        for slot_name, rep_hour in self.SLOT_REPRESENTATIVE_HOURS.items():
            slot_df = df[df['snapshot_hour'] == rep_hour].drop(
                columns=['snapshot_hour']).copy()
            result[slot_name] = slot_df

            if self.verbose and len(slot_df) > 0:
                avail = slot_df['available_count'].sum()
                total = slot_df['total_bikes'].sum()
                print(f"  {TIME_SLOTS[slot_name]['desc']}: "
                      f"{len(slot_df)}개 district, 가용 {avail:,}/{total:,}대")

        # fallback: 데이터 없는 시간대는 21시 스냅샷 사용
        missing_slots = [s for s, d in result.items() if len(d) == 0]
        if missing_slots:
            fallback = self.get_supply_snapshot(snapshot_date, 21)
            for slot_name in missing_slots:
                result[slot_name] = fallback
                if self.verbose:
                    print(f"  ⚠️ {slot_name}: 스냅샷 없음 → 21시 데이터 fallback")

        return result

    # ================================================================
    # 센터 매핑
    # ================================================================

    def get_center_mapping(self) -> pd.DataFrame:
        """
        region → 센터 매핑 (geo_area JOIN maintenance_center)

        Returns:
            DataFrame[region, center_name, center_id]
        """
        if self._center_mapping is not None:
            return self._center_mapping

        query = """
        SELECT DISTINCT
            a.name as region,
            c.name as center_name,
            c.id as center_id
        FROM `bikeshare.service.geo_area` a
        JOIN `bikeshare.service.service_center` c ON a.center_id = c.id
        WHERE a.name IS NOT NULL
        """
        df = self.client.query(query).to_dataframe()
        self._center_mapping = df

        if self.verbose:
            centers = df['center_name'].nunique()
            regions = df['region'].nunique()
            print(f"  센터 매핑: {centers}개 센터, {regions}개 region")

        return df

    # ================================================================
    # 갭 분석
    # ================================================================

    def calculate_gap(
        self,
        demand_df: pd.DataFrame,
        supply_df: pd.DataFrame,
        time_slot: str = None
    ) -> pd.DataFrame:
        """
        수요-공급 갭 계산

        Args:
            demand_df: district×hour 수요 예측
            supply_df: district별 공급 현황
            time_slot: 특정 시간대 필터 (None이면 일 전체)

        Returns:
            DataFrame with gap analysis per district
        """
        if self.verbose:
            slot_desc = TIME_SLOTS[time_slot]['desc'] if time_slot else '일 전체'
            print(f"\n📊 갭 분석 중... ({slot_desc})")

        # 시간대 필터
        if time_slot and time_slot in TIME_SLOTS:
            hours = TIME_SLOTS[time_slot]['hours']
            demand_filtered = demand_df[demand_df['hour'].isin(hours)].copy()
        else:
            demand_filtered = demand_df.copy()

        # district별 수요 합계
        demand_agg = demand_filtered.groupby(['region', 'district']).agg({
            'predicted_rides': 'sum',
            'lat': 'first',
            'lng': 'first',
            'center': 'first',
        }).reset_index()

        # 공급과 매칭
        gap_df = demand_agg.merge(
            supply_df[['region', 'district', 'total_bikes', 'usable_count',
                       'available_count', 'low_battery_count', 'avg_battery']],
            on=['region', 'district'],
            how='outer',
            suffixes=('', '_supply')
        )

        # NaN 처리
        gap_df['predicted_rides'] = gap_df['predicted_rides'].fillna(0)
        gap_df['available_count'] = gap_df['available_count'].fillna(0)
        gap_df['usable_count'] = gap_df['usable_count'].fillna(0)
        gap_df['total_bikes'] = gap_df['total_bikes'].fillna(0)
        gap_df['low_battery_count'] = gap_df['low_battery_count'].fillna(0)
        gap_df['avg_battery'] = gap_df['avg_battery'].fillna(0)

        # supply에만 있고 demand에 없는 district의 좌표 채우기
        if 'lat_supply' in gap_df.columns:
            gap_df['lat'] = gap_df['lat'].fillna(gap_df.get('lat_supply', 0))
            gap_df['lng'] = gap_df['lng'].fillna(gap_df.get('lng_supply', 0))

        # 갭 계산
        gap_df['gap'] = gap_df['predicted_rides'] - gap_df['available_count']

        # 잠재수요 기반 갭 (unconstrained_demand가 있을 때)
        has_unconstrained = 'unconstrained_demand' in demand_agg.columns
        if has_unconstrained:
            # demand_agg에서 잠재수요 관련 컬럼 가져오기
            unc_cols = ['region', 'district']
            for col in ['unconstrained_demand', 'suppressed_demand',
                        'avg_bike_count', 'conversion_rate']:
                if col in demand_filtered.columns:
                    unc_cols.append(col)

            unc_agg = demand_filtered.groupby(['region', 'district']).agg({
                c: 'sum' if c in ('unconstrained_demand', 'suppressed_demand')
                else 'mean'
                for c in unc_cols if c not in ('region', 'district')
            }).reset_index()

            gap_df = gap_df.merge(
                unc_agg, on=['region', 'district'], how='left',
                suffixes=('', '_unc'))

            for col in ['unconstrained_demand', 'suppressed_demand',
                        'avg_bike_count', 'conversion_rate']:
                if col not in gap_df.columns:
                    gap_df[col] = 0.0
                gap_df[col] = gap_df[col].fillna(0)

            gap_df['gap_potential'] = (
                gap_df['unconstrained_demand'] - gap_df['available_count'])
        else:
            gap_df['unconstrained_demand'] = gap_df['predicted_rides']
            gap_df['suppressed_demand'] = 0.0
            gap_df['gap_potential'] = gap_df['gap']

        # 분류 (기존 기준 유지 — 하위호환)
        gap_df['status'] = 'balanced'
        gap_df.loc[
            gap_df['available_count'] < gap_df['predicted_rides'] * DEFICIT_THRESHOLD,
            'status'
        ] = 'deficit'
        gap_df.loc[
            gap_df['available_count'] > gap_df['predicted_rides'] * SURPLUS_THRESHOLD,
            'status'
        ] = 'surplus'

        # 우선순위 점수: 잠재수요 가중 (gap × √잠재수요)
        gap_df['priority_score'] = (
            gap_df['gap'].clip(lower=0) *
            np.sqrt(gap_df['unconstrained_demand'].clip(lower=1))
        ).round(1)

        gap_df = gap_df.sort_values('priority_score', ascending=False)

        if self.verbose:
            n_deficit = len(gap_df[gap_df['status'] == 'deficit'])
            n_surplus = len(gap_df[gap_df['status'] == 'surplus'])
            n_balanced = len(gap_df[gap_df['status'] == 'balanced'])
            total_gap = gap_df[gap_df['gap'] > 0]['gap'].sum()

            print(f"  부족: {n_deficit}개 district (총 {total_gap:,.0f}대 필요)")
            print(f"  과잉: {n_surplus}개 district")
            print(f"  균형: {n_balanced}개 district")

        return gap_df

    # ================================================================
    # 작업지시서 생성
    # ================================================================

    def generate_work_orders(
        self,
        target_date: str,
        demand_df: pd.DataFrame,
        supply_df: pd.DataFrame,
        center_filter: str = None,
        supply_by_slot: Dict[str, pd.DataFrame] = None
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        센터별 × 시간대별 작업지시서 생성

        Args:
            supply_df: 기본 공급 스냅샷 (supply_by_slot 없을 때 fallback)
            supply_by_slot: 시간대별 공급 스냅샷 (P4 개선)
                {slot_name: supply_df} 형태

        Returns:
            {center_name: {time_slot: work_order_df}}
        """
        if self.verbose:
            print(f"\n📋 작업지시서 생성 중...")
            if supply_by_slot:
                print(f"  → 시간대별 공급 데이터 사용 (P4)")

        center_mapping = self.get_center_mapping()
        work_orders = {}

        # 센터 목록
        if center_filter:
            centers = [center_filter]
        else:
            centers = demand_df['center'].dropna().unique().tolist()
            # center_mapping에 없는 것도 포함
            mapped_centers = center_mapping['center_name'].unique().tolist()
            centers = list(set(centers) | set(mapped_centers))

        for center_name in sorted(centers):
            if not center_name:
                continue

            # 해당 센터의 region들
            center_regions = center_mapping[
                center_mapping['center_name'] == center_name
            ]['region'].tolist()

            # demand에서 center 매칭
            center_demand = demand_df[
                (demand_df['center'] == center_name) |
                (demand_df['region'].isin(center_regions))
            ]

            if len(center_demand) == 0:
                continue

            center_districts = center_demand[['region', 'district']].drop_duplicates()

            center_orders = {}

            for slot_name, slot_info in TIME_SLOTS.items():
                # 시간대별 공급 선택 (P4): 해당 시간대 스냅샷 → fallback: 기본 스냅샷
                if supply_by_slot and slot_name in supply_by_slot:
                    slot_supply = supply_by_slot[slot_name]
                else:
                    slot_supply = supply_df

                center_supply = slot_supply[
                    slot_supply['district'].isin(
                        center_districts['district'].tolist())
                ]

                # 해당 시간대 갭 분석
                gap_df = self.calculate_gap(center_demand, center_supply, slot_name)

                if len(gap_df) == 0:
                    continue

                # 부족 지점 → 재배치 대상
                deficit = gap_df[gap_df['status'] == 'deficit'].copy()
                surplus = gap_df[gap_df['status'] == 'surplus'].copy()

                if len(deficit) == 0:
                    continue

                # 작업지시서 구성
                orders = []
                priority = 0

                for _, row in deficit.iterrows():
                    priority += 1

                    # 가장 가까운 과잉 지점 찾기
                    source_info = ''
                    movable = 0
                    if len(surplus) > 0 and not pd.isna(row.get('lat', np.nan)):
                        # 거리 계산 (간단한 유클리드)
                        surplus_with_coords = surplus.dropna(subset=['lat', 'lng'])
                        if len(surplus_with_coords) > 0:
                            dists = np.sqrt(
                                (surplus_with_coords['lat'] - row['lat'])**2 +
                                (surplus_with_coords['lng'] - row['lng'])**2
                            )
                            nearest_idx = dists.idxmin()
                            nearest = surplus_with_coords.loc[nearest_idx]
                            source_info = f"{nearest.get('district', nearest.get('region', ''))}"
                            movable = int(min(-nearest['gap'], row['gap']))

                    order_row = {
                        '우선순위': priority,
                        '권역': row.get('region', ''),
                        '구역': row.get('district', ''),
                        '시간대': slot_info['desc'],
                        '예측수요': round(row['predicted_rides']),
                        '잠재수요': round(row.get('unconstrained_demand',
                                              row['predicted_rides'])),
                        '억제수요': round(row.get('suppressed_demand', 0)),
                        '현재가용': int(row['available_count']),
                        '부족대수': max(0, int(row['gap'])),
                        '출발지(과잉구역)': source_info,
                        '이동가능대수': movable,
                        '우선순위점수': row['priority_score'],
                    }
                    orders.append(order_row)

                if orders:
                    center_orders[slot_name] = pd.DataFrame(orders)

            if center_orders:
                work_orders[center_name] = center_orders

        if self.verbose:
            for center, slots in work_orders.items():
                total_tasks = sum(len(df) for df in slots.values())
                print(f"  {center}: {len(slots)}개 시간대, {total_tasks}건 작업")

        return work_orders

    # ================================================================
    # Excel 출력
    # ================================================================

    def export_excel(
        self,
        work_orders: Dict[str, Dict[str, pd.DataFrame]],
        supply_df: pd.DataFrame,
        target_date: str
    ) -> List[str]:
        """
        센터별 작업지시서 Excel 파일 생성

        센터별 1파일, 시간대별 시트 + 배터리교체 시트
        """
        output_files = []

        for center_name, slots in work_orders.items():
            safe_name = center_name.replace('/', '_')
            output_path = os.path.join(
                OUTPUT_DIR,
                f'work_order_{safe_name}_{target_date}.xlsx'
            )

            try:
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    # 시간대별 시트
                    for slot_name, order_df in slots.items():
                        slot_desc = TIME_SLOTS[slot_name]['desc']
                        sheet_name = slot_desc[:31]  # Excel 시트명 31자 제한

                        order_df.to_excel(writer, sheet_name=sheet_name, index=False)

                    # 배터리교체 시트 (해당 센터의 배터리 부족 지점)
                    center_supply = supply_df[
                        supply_df['low_battery_count'] > 0
                    ].copy()

                    if len(center_supply) > 0:
                        battery_df = center_supply[[
                            'region', 'district', 'low_battery_count',
                            'avg_battery', 'lat', 'lng'
                        ]].rename(columns={
                            'region': '권역',
                            'district': '구역',
                            'low_battery_count': '배터리부족대수',
                            'avg_battery': '평균배터리(%)',
                            'lat': '위도',
                            'lng': '경도',
                        })
                        battery_df = battery_df.sort_values(
                            '배터리부족대수', ascending=False)
                        battery_df.to_excel(
                            writer, sheet_name='배터리교체', index=False)

                    # 요약 시트
                    summary_rows = []
                    for slot_name, order_df in slots.items():
                        slot_desc = TIME_SLOTS[slot_name]['desc']
                        summary_rows.append({
                            '시간대': slot_desc,
                            '작업건수': len(order_df),
                            '총부족대수': order_df['부족대수'].sum(),
                            '이동가능합계': order_df['이동가능대수'].sum(),
                            '최대부족구역': (order_df.iloc[0]['구역']
                                       if len(order_df) > 0 else ''),
                        })
                    summary_df = pd.DataFrame(summary_rows)
                    summary_df.to_excel(writer, sheet_name='요약', index=False)

                output_files.append(output_path)

                if self.verbose:
                    print(f"  📁 {output_path}")

            except Exception as e:
                print(f"  ❌ Excel 생성 실패 ({center_name}): {e}")

        return output_files

    # ================================================================
    # 갭 시각화 맵
    # ================================================================

    def create_gap_map(
        self,
        gap_df: pd.DataFrame,
        supply_df: pd.DataFrame,
        target_date: str,
        time_slot: str = None,
        center_filter: str = None
    ) -> folium.Map:
        """
        수요-공급 갭 시각화 Folium 맵

        빨강: 부족 (deficit) → 재배치 필요
        파랑: 과잉 (surplus) → 여기서 가져갈 수 있음
        초록: 균형 (balanced)
        노랑: 배터리 부족
        """
        slot_desc = TIME_SLOTS[time_slot]['desc'] if time_slot else '일 전체'

        # 센터 필터
        if center_filter and 'center' in gap_df.columns:
            gap_df = gap_df[gap_df['center'] == center_filter]

        # 유효한 좌표만
        plot_df = gap_df.dropna(subset=['lat', 'lng'])
        plot_df = plot_df[(plot_df['lat'] != 0) & (plot_df['lng'] != 0)]

        if len(plot_df) == 0:
            print("  ⚠️ 표시할 데이터 없음")
            return None

        # 지도 중심
        center_lat = plot_df['lat'].mean()
        center_lng = plot_df['lng'].mean()

        m = folium.Map(
            location=[center_lat, center_lng],
            zoom_start=11,
            tiles='cartodbpositron'
        )

        # 레이어 그룹
        deficit_group = FeatureGroup(name='🔴 부족 (재배치 필요)', show=True)
        surplus_group = FeatureGroup(name='🔵 과잉 (출발지)', show=True)
        balanced_group = FeatureGroup(name='🟢 균형', show=False)
        battery_group = FeatureGroup(name='🟡 배터리 교체', show=True)

        max_gap = max(plot_df['gap'].abs().max(), 1)

        for _, row in plot_df.iterrows():
            status = row.get('status', 'balanced')
            gap = row.get('gap', 0)
            predicted = row.get('predicted_rides', 0)
            available = row.get('available_count', 0)
            district = row.get('district', row.get('region', ''))
            region = row.get('region', '')

            radius = max(4, min(np.sqrt(abs(gap) / max_gap) * 18, 22))

            if status == 'deficit':
                color = COLOR_DEFICIT
                group = deficit_group
                label = f"부족 {int(gap)}대"
            elif status == 'surplus':
                color = COLOR_SURPLUS
                group = surplus_group
                label = f"여유 {int(-gap)}대"
            else:
                color = COLOR_BALANCED
                group = balanced_group
                label = "균형"

            unconstrained = row.get('unconstrained_demand', predicted)
            suppressed = row.get('suppressed_demand', 0)
            conv_rate = row.get('conversion_rate', 0)

            # 잠재수요 정보 (있을 때만 표시)
            unc_html = ''
            if suppressed > 0:
                unc_html = f"""
                    <tr style="color:#FF6F00;">
                        <td>잠재수요</td><td style="text-align:right;">{unconstrained:,.0f}건</td>
                    </tr>
                    <tr style="color:#FF6F00;font-size:11px;">
                        <td>전환율</td><td style="text-align:right;">{conv_rate:.0%}</td>
                    </tr>
                    <tr style="color:#FF6F00;font-size:11px;">
                        <td>억제수요</td><td style="text-align:right;">+{suppressed:,.0f}건</td>
                    </tr>
                """

            popup_html = f"""
            <div style="font-family:Arial;font-size:12px;min-width:200px;">
                <b style="color:{color};">{district}</b>
                <span style="color:#999;font-size:11px;"> ({region})</span>
                <hr style="margin:4px 0;">
                <table style="font-size:12px;width:100%;">
                    <tr><td>예측 수요</td><td style="text-align:right;font-weight:bold;">{predicted:,.0f}건</td></tr>
                    {unc_html}
                    <tr><td>가용 바이크</td><td style="text-align:right;">{available:,.0f}대</td></tr>
                    <tr style="color:{color};font-weight:bold;">
                        <td>갭</td><td style="text-align:right;">{'+' if gap > 0 else ''}{gap:,.0f}대</td>
                    </tr>
                </table>
            </div>
            """

            folium.CircleMarker(
                location=[row['lat'], row['lng']],
                radius=radius,
                color=color,
                weight=2,
                fill=True,
                fill_color=color,
                fill_opacity=0.6,
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=f"{label} | {district}"
            ).add_to(group)

        # 배터리 부족 지점
        battery_spots = supply_df[supply_df['low_battery_count'] > 0].copy()
        if center_filter:
            # center 필터가 있으면 해당 센터의 region만
            center_regions = gap_df['region'].unique().tolist()
            battery_spots = battery_spots[
                battery_spots['region'].isin(center_regions)]

        for _, row in battery_spots.iterrows():
            if pd.isna(row.get('lat')) or pd.isna(row.get('lng')):
                continue
            if row['lat'] == 0 or row['lng'] == 0:
                continue

            folium.CircleMarker(
                location=[row['lat'], row['lng']],
                radius=4 + row['low_battery_count'],
                color=COLOR_BATTERY,
                weight=1,
                fill=True,
                fill_color=COLOR_BATTERY,
                fill_opacity=0.5,
                tooltip=f"배터리 {int(row['low_battery_count'])}대 | {row.get('district', '')}"
            ).add_to(battery_group)

        # 레이어 추가
        deficit_group.add_to(m)
        surplus_group.add_to(m)
        balanced_group.add_to(m)
        battery_group.add_to(m)
        LayerControl(collapsed=False).add_to(m)

        # 통계 범례
        n_deficit = len(plot_df[plot_df['status'] == 'deficit'])
        n_surplus = len(plot_df[plot_df['status'] == 'surplus'])
        total_deficit = plot_df[plot_df['gap'] > 0]['gap'].sum()
        total_surplus = -plot_df[plot_df['gap'] < 0]['gap'].sum()
        n_battery = len(battery_spots)

        # 잠재수요 통계
        total_suppressed = (plot_df['suppressed_demand'].sum()
                           if 'suppressed_demand' in plot_df.columns else 0)
        suppressed_html = ''
        if total_suppressed > 0:
            suppressed_html = f"""
                <tr>
                    <td style="padding:3px 0;">
                        <span style="color:#FF6F00;font-weight:bold;">●</span> 억제수요
                    </td>
                    <td style="text-align:right;">{total_suppressed:,.0f}건</td>
                </tr>
            """

        title = center_filter or '전체'
        stats_html = f"""
        <div style="position:fixed; bottom:30px; left:30px; z-index:1000;
                    background:white; padding:15px 18px; border-radius:10px;
                    box-shadow:0 2px 8px rgba(0,0,0,0.25); font-family:Arial; font-size:13px;
                    max-width:280px;">
            <div style="font-size:15px;font-weight:bold;margin-bottom:8px;">
                🚲 {title} 수요-공급 갭
            </div>
            <div style="font-size:12px;color:#666;margin-bottom:6px;">
                📅 {target_date} | {slot_desc}
            </div>
            <table style="width:100%;font-size:13px;border-collapse:collapse;">
                <tr>
                    <td style="padding:3px 0;">
                        <span style="color:{COLOR_DEFICIT};font-weight:bold;">●</span> 부족
                    </td>
                    <td style="text-align:right;">{n_deficit}개소, {total_deficit:,.0f}대</td>
                </tr>
                <tr>
                    <td style="padding:3px 0;">
                        <span style="color:{COLOR_SURPLUS};font-weight:bold;">●</span> 과잉
                    </td>
                    <td style="text-align:right;">{n_surplus}개소, {total_surplus:,.0f}대</td>
                </tr>
                {suppressed_html}
                <tr>
                    <td style="padding:3px 0;">
                        <span style="color:{COLOR_BATTERY};font-weight:bold;">●</span> 배터리
                    </td>
                    <td style="text-align:right;">{n_battery}개소</td>
                </tr>
            </table>
        </div>
        """
        m.get_root().html.add_child(folium.Element(stats_html))

        return m

    # ================================================================
    # 전체 파이프라인
    # ================================================================

    def run(
        self,
        target_date: str,
        snapshot_date: str = None,
        snapshot_hour: int = 21,
        center_filter: str = None,
        export: bool = True
    ) -> Dict:
        """
        전체 갭 분석 파이프라인

        Args:
            target_date: 예측 대상 날짜
            snapshot_date: 공급 스냅샷 날짜 (None=오늘)
            snapshot_hour: 공급 스냅샷 시간
            center_filter: 특정 센터만 분석
            export: Excel/Map 파일 생성 여부

        Returns:
            {'demand': df, 'supply': df, 'gap': df, 'work_orders': dict,
             'files': list}
        """
        print(f"\n{'='*60}")
        print(f"🚲 수요-공급 갭 분석: {target_date}")
        if center_filter:
            print(f"   센터 필터: {center_filter}")
        print(f"{'='*60}")

        # 1. 수요 예측
        demand_df = self.get_demand_prediction(target_date)
        if len(demand_df) == 0:
            print("❌ 수요 예측 실패")
            return {'error': 'no demand predictions'}

        # 2. 공급 현황 (기본 스냅샷 + 시간대별 스냅샷)
        supply_df = self.get_supply_snapshot(snapshot_date, snapshot_hour)
        if len(supply_df) == 0:
            print("⚠️ 공급 데이터 없음 (스냅샷 미존재)")

        # P4: 시간대별 공급 스냅샷 (과거 날짜만 - 미래는 기본 스냅샷 fallback)
        supply_by_slot = None
        if snapshot_date:
            from datetime import date as date_type
            try:
                snap_date = pd.Timestamp(snapshot_date).date()
                today = datetime.now().date()
                if snap_date <= today:
                    supply_by_slot = self.get_supply_by_timeslot(snapshot_date)
            except Exception as e:
                if self.verbose:
                    print(f"  ⚠️ 시간대별 공급 조회 실패: {e} → 기본 스냅샷 사용")

        # 3. 갭 분석 (일 전체 - 기본 스냅샷 사용)
        gap_df = self.calculate_gap(demand_df, supply_df)

        # 4. 작업지시서 (시간대별 공급 사용)
        work_orders = self.generate_work_orders(
            target_date, demand_df, supply_df, center_filter,
            supply_by_slot=supply_by_slot)

        result = {
            'demand': demand_df,
            'supply': supply_df,
            'supply_by_slot': supply_by_slot,
            'gap': gap_df,
            'work_orders': work_orders,
            'files': [],
        }

        # 5. Excel 출력
        if export and work_orders:
            files = self.export_excel(work_orders, supply_df, target_date)
            result['files'].extend(files)

        # 6. 갭 맵 (시간대별 - 시간대별 공급 사용)
        if export:
            for slot_name in TIME_SLOTS:
                # P4: 시간대에 맞는 공급 데이터 사용
                slot_supply = (supply_by_slot.get(slot_name, supply_df)
                              if supply_by_slot else supply_df)
                slot_gap = self.calculate_gap(
                    demand_df, slot_supply, slot_name)
                m = self.create_gap_map(
                    slot_gap, slot_supply, target_date,
                    time_slot=slot_name, center_filter=center_filter
                )
                if m:
                    center_str = center_filter.replace('/', '_') if center_filter else 'all'
                    map_path = os.path.join(
                        OUTPUT_DIR,
                        f'gap_map_{center_str}_{slot_name}_{target_date}.html'
                    )
                    m.save(map_path)
                    result['files'].append(map_path)
                    if self.verbose:
                        print(f"  🗺️ {map_path}")

        # 7. 요약
        if self.verbose:
            self._print_summary(gap_df, work_orders, target_date)

        return result

    def _print_summary(self, gap_df, work_orders, target_date):
        """최종 요약 출력"""
        print(f"\n{'='*60}")
        print(f"📊 분석 요약: {target_date}")
        print(f"{'='*60}")

        # 전체 통계
        n_deficit = len(gap_df[gap_df['status'] == 'deficit'])
        n_surplus = len(gap_df[gap_df['status'] == 'surplus'])
        n_balanced = len(gap_df[gap_df['status'] == 'balanced'])
        total_demand = gap_df['predicted_rides'].sum()
        total_supply = gap_df['available_count'].sum()
        total_gap = gap_df[gap_df['gap'] > 0]['gap'].sum()

        total_unconstrained = (gap_df['unconstrained_demand'].sum()
                              if 'unconstrained_demand' in gap_df.columns
                              else total_demand)
        total_suppressed = (gap_df['suppressed_demand'].sum()
                           if 'suppressed_demand' in gap_df.columns else 0)

        print(f"\n  전체 수요: {total_demand:,.0f}건 (실현 예측)")
        if total_suppressed > 0:
            print(f"  잠재수요: {total_unconstrained:,.0f}건 "
                  f"(억제 {total_suppressed:,.0f}건, "
                  f"+{total_suppressed/total_demand*100:.1f}%)")
        print(f"  전체 공급: {total_supply:,.0f}대")
        print(f"  부족: {n_deficit}개 district ({total_gap:,.0f}대 필요)")
        print(f"  과잉: {n_surplus}개 district")
        print(f"  균형: {n_balanced}개 district")

        # 센터별 요약
        if work_orders:
            print(f"\n  📋 센터별 작업 현황:")
            print(f"  {'센터':<14} {'시간대':>4} {'작업건수':>8} {'총부족':>8}")
            print(f"  {'-'*38}")

            for center, slots in sorted(work_orders.items()):
                total_tasks = sum(len(df) for df in slots.values())
                total_deficit = sum(
                    df['부족대수'].sum() for df in slots.values())
                print(f"  {center:<14} {len(slots):>4} {total_tasks:>8} "
                      f"{total_deficit:>8,.0f}")

        # 우선순위 TOP 5
        top_deficit = gap_df[gap_df['status'] == 'deficit'].head(5)
        if len(top_deficit) > 0:
            print(f"\n  ⚠️ 부족 우선순위 TOP 5:")
            for rank, (_, row) in enumerate(top_deficit.iterrows(), 1):
                print(f"  {rank}. {row.get('district', row.get('region', ''))} "
                      f"(수요 {row['predicted_rides']:,.0f}, "
                      f"가용 {row['available_count']:,.0f}, "
                      f"부족 {row['gap']:+,.0f})")


# ================================================================
# CLI
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description='수요-공급 갭 분석 + 작업지시서',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
    python supply_demand_gap.py --date 2026-02-25
    python supply_demand_gap.py --date 2026-02-25 --center Center_North
    python supply_demand_gap.py --date 2026-02-25 --snapshot-date 2026-02-24 --snapshot-hour 21
    python supply_demand_gap.py --date 2026-02-25 --no-export
        """
    )
    parser.add_argument('--date', type=str, required=True,
                       help='예측 대상 날짜 (YYYY-MM-DD)')
    parser.add_argument('--center', type=str, default=None,
                       help='특정 센터만 분석')
    parser.add_argument('--snapshot-date', type=str, default=None,
                       help='공급 스냅샷 날짜 (기본: 오늘)')
    parser.add_argument('--snapshot-hour', type=int, default=21,
                       help='공급 스냅샷 시간 (기본: 21)')
    parser.add_argument('--no-export', action='store_true',
                       help='파일 출력 비활성화')

    args = parser.parse_args()

    analyzer = SupplyDemandAnalyzer(verbose=True)
    result = analyzer.run(
        target_date=args.date,
        snapshot_date=args.snapshot_date,
        snapshot_hour=args.snapshot_hour,
        center_filter=args.center,
        export=not args.no_export
    )

    if 'files' in result and result['files']:
        print(f"\n📁 생성된 파일:")
        for f in result['files']:
            print(f"  {f}")


if __name__ == '__main__':
    main()
