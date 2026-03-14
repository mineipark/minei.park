"""
저녁 재배치 업무 지원 시스템
- 20-22시에 실행
- 다음날 수요 예측 vs 현재 기기수 Gap 분석
- 재배치 필수 지점 + 동선 사이 배터리 교체 지점 표시
"""
import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime, timedelta
import folium
from folium.plugins import AntPath

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Authentication: set GOOGLE_APPLICATION_CREDENTIALS env var
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    _default_cred = os.path.join(SCRIPT_DIR, '..', 'credentials', 'service-account.json')
    if os.path.exists(_default_cred):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _default_cred

OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'visualizations')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 분석 대상 센터: region_params.json에서 동적 로드
def _load_target_centers():
    """region_params.json에서 센터→권역 매핑 동적 생성"""
    import json
    params_path = os.path.join(SCRIPT_DIR, 'region_params.json')
    if os.path.exists(params_path):
        with open(params_path, 'r') as f:
            params = json.load(f)
        centers = {}
        for region, data in params.items():
            center = data.get('center', 'unknown')
            if center not in centers:
                centers[center] = []
            centers[center].append(region)
        return centers
    return {}

TARGET_CENTERS = _load_target_centers()


def get_demand_forecast(target_date, target_regions):
    """
    다음날 시간대별 수요 예측

    district_hour_model 기반 예측을 우선 시도.
    실패 시 기존 방식 (과거 4주 동일 요일 평균)으로 fallback.
    """
    # --- district_hour_model 기반 예측 (Phase 3 연결) ---
    try:
        from district_hour_model import DistrictHourPredictor

        target_date_str = target_date if isinstance(target_date, str) else target_date.strftime('%Y-%m-%d')
        predictor = DistrictHourPredictor(verbose=False)
        pred_df = predictor.predict(target_date_str)

        if len(pred_df) > 0:
            # target_regions 필터
            result = pred_df[pred_df['region'].isin(target_regions)].copy()

            if len(result) > 0:
                # 기존 gap_df 포맷에 맞게 변환
                result = result.rename(columns={
                    'predicted_rides': 'predicted_demand',
                    'district': 'h3_index',  # district를 h3_index 대용으로
                })
                result = result[['region', 'h3_index', 'hour', 'lat', 'lng', 'predicted_demand']]

                # 하루 1회 이상만
                result = result[result['predicted_demand'] >= 1]
                result = result.sort_values('predicted_demand', ascending=False)

                print(f"다음날({target_date_str}) 수요 예측 중... (district_hour_model)")
                print(f"  {len(result)}개 위치-시간대 조합 (예측 모델 기반)")
                return result

    except Exception as e:
        print(f"  ⚠️ district_hour_model fallback: {e}")

    # --- Fallback: 과거 4주 동일 요일 평균 ---
    client = bigquery.Client()
    regions_str = "', '".join(target_regions)

    if isinstance(target_date, str):
        target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')
    else:
        target_date_obj = target_date
    day_of_week = (target_date_obj.isoweekday() % 7) + 1

    query = f"""
    WITH historical_demand AS (
        SELECT
            h3_start_area_name as region,
            udf.geo_to_h3(ST_Y(start_location), ST_X(start_location), 9) as h3_index,
            EXTRACT(HOUR FROM start_time) as hour,
            AVG(ST_Y(start_location)) as lat,
            AVG(ST_X(start_location)) as lng,
            COUNT(*) as rides
        FROM `service.rides`
        WHERE DATE(start_time) BETWEEN DATE_SUB('{target_date}', INTERVAL 28 DAY) AND DATE_SUB('{target_date}', INTERVAL 1 DAY)
            AND EXTRACT(DAYOFWEEK FROM start_time) = {day_of_week}
            AND h3_start_area_name IN ('{regions_str}')
        GROUP BY 1, 2, 3
    )
    SELECT
        region,
        h3_index,
        hour,
        ANY_VALUE(lat) as lat,
        ANY_VALUE(lng) as lng,
        ROUND(AVG(rides), 0) as predicted_demand
    FROM historical_demand
    GROUP BY 1, 2, 3
    HAVING AVG(rides) >= 1
    ORDER BY predicted_demand DESC
    """

    print(f"다음날({target_date}) 수요 예측 중... (fallback: 4주 평균, 요일: {day_of_week})")
    df = client.query(query).to_dataframe()
    print(f"  {len(df)}개 위치-시간대 조합 로드")

    return df


def get_current_bike_positions(target_regions, snapshot_hour=21):
    """
    현재 바이크 위치 및 배터리 상태 (20-22시 기준)
    """
    client = bigquery.Client()
    regions_str = "', '".join(target_regions)

    query = f"""
    SELECT
        h3_area_name as region,
        udf.geo_to_h3(
            CAST(JSON_VALUE(location, '$.coordinates[1]') AS FLOAT64),
            CAST(JSON_VALUE(location, '$.coordinates[0]') AS FLOAT64),
            9
        ) as h3_index,
        CAST(JSON_VALUE(location, '$.coordinates[1]') AS FLOAT64) as lat,
        CAST(JSON_VALUE(location, '$.coordinates[0]') AS FLOAT64) as lng,
        COUNT(*) as bike_count,
        SUM(CASE WHEN is_usable = TRUE THEN 1 ELSE 0 END) as usable_count,
        SUM(CASE WHEN leftover < 30 THEN 1 ELSE 0 END) as low_battery_count,
        AVG(leftover) as avg_battery
    FROM `service.bike_snapshot`
    WHERE DATE(time) = CURRENT_DATE('Asia/Seoul')
        AND EXTRACT(HOUR FROM time) = {snapshot_hour}
        AND h3_area_name IN ('{regions_str}')
    GROUP BY 1, 2, 3, 4
    """

    print(f"현재 바이크 위치 조회 중... ({snapshot_hour}시 기준)")
    df = client.query(query).to_dataframe()
    print(f"  {len(df)}개 위치 로드, 총 {df['bike_count'].sum():,}대")

    return df


def get_current_bike_positions_historical(target_regions, reference_date, snapshot_hour=21):
    """
    과거 데이터 기반 현재 바이크 위치 (테스트용)
    """
    client = bigquery.Client()
    regions_str = "', '".join(target_regions)

    query = f"""
    SELECT
        h3_area_name as region,
        udf.geo_to_h3(
            CAST(JSON_VALUE(location, '$.coordinates[1]') AS FLOAT64),
            CAST(JSON_VALUE(location, '$.coordinates[0]') AS FLOAT64),
            9
        ) as h3_index,
        AVG(CAST(JSON_VALUE(location, '$.coordinates[1]') AS FLOAT64)) as lat,
        AVG(CAST(JSON_VALUE(location, '$.coordinates[0]') AS FLOAT64)) as lng,
        COUNT(*) as bike_count,
        SUM(CASE WHEN is_usable = TRUE THEN 1 ELSE 0 END) as usable_count,
        SUM(CASE WHEN leftover < 30 THEN 1 ELSE 0 END) as low_battery_count,
        AVG(leftover) as avg_battery
    FROM `service.bike_snapshot`
    WHERE DATE(time) = '{reference_date}'
        AND EXTRACT(HOUR FROM time) = {snapshot_hour}
        AND h3_area_name IN ('{regions_str}')
    GROUP BY 1, 2
    """

    print(f"바이크 위치 조회 중... ({reference_date} {snapshot_hour}시 기준)")
    df = client.query(query).to_dataframe()
    print(f"  {len(df)}개 위치 로드, 총 {df['bike_count'].sum():,}대")

    return df


def calculate_supply_demand_gap(demand_df, supply_df, peak_hours=[7, 8, 9, 12, 18, 19]):
    """
    수요-공급 Gap 계산
    peak_hours: 다음날 피크 시간대
    """
    print(f"\n수요-공급 Gap 계산 중... (피크 시간: {peak_hours})")

    # 피크 시간대 수요 합계
    peak_demand = demand_df[demand_df['hour'].isin(peak_hours)].groupby('h3_index').agg({
        'region': 'first',
        'lat': 'mean',
        'lng': 'mean',
        'predicted_demand': 'sum'
    }).reset_index()

    # 현재 공급
    supply = supply_df.groupby('h3_index').agg({
        'region': 'first',
        'lat': 'mean',
        'lng': 'mean',
        'bike_count': 'sum',
        'usable_count': 'sum',
        'low_battery_count': 'sum',
        'avg_battery': 'mean'
    }).reset_index()

    # Gap 계산
    gap_df = peak_demand.merge(
        supply[['h3_index', 'region', 'lat', 'lng', 'bike_count', 'usable_count', 'low_battery_count', 'avg_battery']],
        on='h3_index',
        how='outer',
        suffixes=('_demand', '_supply')
    ).fillna(0)

    # 수요 예측이 있는데 공급 없는 경우 좌표 보정
    gap_df['lat'] = gap_df['lat_demand'].where(gap_df['lat_demand'] != 0, gap_df['lat_supply'])
    gap_df['lng'] = gap_df['lng_demand'].where(gap_df['lng_demand'] != 0, gap_df['lng_supply'])
    gap_df['region'] = gap_df['region_demand'].where(gap_df['region_demand'] != 0, gap_df['region_supply'])

    # Gap = 예측 수요 - 현재 이용가능 바이크
    gap_df['gap'] = gap_df['predicted_demand'] - gap_df['usable_count']

    # 필요한 컬럼만
    gap_df = gap_df[[
        'h3_index', 'region', 'lat', 'lng',
        'predicted_demand', 'bike_count', 'usable_count', 'low_battery_count', 'avg_battery', 'gap'
    ]]

    gap_df = gap_df.sort_values('gap', ascending=False)

    print(f"  Gap > 0 (공급 부족): {len(gap_df[gap_df['gap'] > 0])}개 위치")
    print(f"  Gap < 0 (공급 초과): {len(gap_df[gap_df['gap'] < 0])}개 위치")

    return gap_df


def get_low_battery_spots(supply_df, min_low_battery=1):
    """
    배터리 부족 지점 추출
    """
    low_battery = supply_df[supply_df['low_battery_count'] >= min_low_battery].copy()
    low_battery = low_battery.sort_values('low_battery_count', ascending=False)

    print(f"\n배터리 부족 지점: {len(low_battery)}개")

    return low_battery


def create_relocation_map(gap_df, battery_df, center_name, top_n=20):
    """
    재배치 업무 지도 생성
    - 빨강: 재배치 필요 (Gap 큰 곳)
    - 노랑: 배터리 교체 필요
    - 파랑: 공급 초과 (여기서 가져갈 수 있음)
    """
    print(f"\n{center_name} 재배치 업무 지도 생성 중...")

    # 재배치 필요 TOP N
    relocation_targets = gap_df[gap_df['gap'] > 0].head(top_n)

    # 공급 초과 (여기서 바이크 가져감)
    supply_excess = gap_df[gap_df['gap'] < -2].head(top_n)

    # 배터리 교체 필요
    battery_spots = battery_df.head(30)

    if len(relocation_targets) == 0:
        print("  재배치 필요 지점 없음")
        return None

    # 지도 중심
    center_lat = gap_df['lat'].mean()
    center_lng = gap_df['lng'].mean()

    m = folium.Map(
        location=[center_lat, center_lng],
        zoom_start=12,
        tiles='cartodbpositron'
    )

    # 레이어 그룹
    relocation_group = folium.FeatureGroup(name='🔴 재배치 필요 (Gap > 0)')
    battery_group = folium.FeatureGroup(name='🟡 배터리 교체')
    excess_group = folium.FeatureGroup(name='🔵 공급 초과 (출발지)')

    # 재배치 필요 지점 (빨강)
    for rank, (_, row) in enumerate(relocation_targets.iterrows(), 1):
        radius = 10 + min(row['gap'] / 2, 15)

        popup_html = f"""
        <div style="font-family: Arial; width: 250px;">
            <h4 style="color: #E53935; margin: 0 0 10px 0;">
                🔴 #{rank} 재배치 필요
            </h4>
            <table style="font-size: 12px; width: 100%;">
                <tr><td>권역</td><td><b>{row['region']}</b></td></tr>
                <tr><td>예측 수요</td><td><b>{row['predicted_demand']:.0f}건</b></td></tr>
                <tr><td>현재 바이크</td><td>{row['usable_count']:.0f}대</td></tr>
                <tr style="color: #E53935;">
                    <td>Gap</td><td><b>+{row['gap']:.0f}대 필요</b></td>
                </tr>
            </table>
            <div style="margin-top: 10px; padding: 8px; background: #FFEBEE; border-radius: 4px; font-size: 11px;">
                → 이 위치로 바이크 {row['gap']:.0f}대 재배치
            </div>
        </div>
        """

        folium.CircleMarker(
            location=[row['lat'], row['lng']],
            radius=radius,
            color='#E53935',
            fill=True,
            fill_color='#E53935',
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=f"#{rank} Gap +{row['gap']:.0f}"
        ).add_to(relocation_group)

        # 순위 라벨
        folium.Marker(
            location=[row['lat'], row['lng']],
            icon=folium.DivIcon(
                html=f'<div style="font-size: 10px; font-weight: bold; color: white; background: #E53935; border-radius: 50%; width: 20px; height: 20px; text-align: center; line-height: 20px;">{rank}</div>',
                icon_size=(20, 20),
                icon_anchor=(10, 10)
            )
        ).add_to(relocation_group)

    # 배터리 교체 지점 (노랑)
    for _, row in battery_spots.iterrows():
        popup_html = f"""
        <div style="font-family: Arial; width: 200px;">
            <h4 style="color: #FBC02D; margin: 0 0 10px 0;">
                🟡 배터리 교체
            </h4>
            <table style="font-size: 12px;">
                <tr><td>권역</td><td>{row['region']}</td></tr>
                <tr><td>배터리 부족</td><td><b>{row['low_battery_count']:.0f}대</b></td></tr>
                <tr><td>평균 배터리</td><td>{row['avg_battery']:.0f}%</td></tr>
            </table>
        </div>
        """

        folium.CircleMarker(
            location=[row['lat'], row['lng']],
            radius=6 + row['low_battery_count'],
            color='#FBC02D',
            fill=True,
            fill_color='#FBC02D',
            fill_opacity=0.6,
            popup=folium.Popup(popup_html, max_width=220),
            tooltip=f"배터리 {row['low_battery_count']:.0f}대"
        ).add_to(battery_group)

    # 공급 초과 지점 (파랑) - 여기서 가져감
    for _, row in supply_excess.iterrows():
        popup_html = f"""
        <div style="font-family: Arial; width: 200px;">
            <h4 style="color: #1976D2; margin: 0 0 10px 0;">
                🔵 공급 초과 (출발지)
            </h4>
            <table style="font-size: 12px;">
                <tr><td>권역</td><td>{row['region']}</td></tr>
                <tr><td>현재 바이크</td><td><b>{row['usable_count']:.0f}대</b></td></tr>
                <tr><td>예측 수요</td><td>{row['predicted_demand']:.0f}건</td></tr>
                <tr style="color: #1976D2;">
                    <td>여유</td><td><b>{-row['gap']:.0f}대 이동 가능</b></td></tr>
            </table>
        </div>
        """

        folium.CircleMarker(
            location=[row['lat'], row['lng']],
            radius=8,
            color='#1976D2',
            fill=True,
            fill_color='#1976D2',
            fill_opacity=0.5,
            popup=folium.Popup(popup_html, max_width=220),
            tooltip=f"여유 {-row['gap']:.0f}대"
        ).add_to(excess_group)

    relocation_group.add_to(m)
    battery_group.add_to(m)
    excess_group.add_to(m)
    folium.LayerControl().add_to(m)

    # 범례
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                background: white; padding: 15px; border-radius: 8px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.3); font-family: Arial;">
        <h4 style="margin: 0 0 12px 0;">저녁 재배치 업무</h4>
        <div style="margin: 8px 0;">
            <span style="color: #E53935;">●</span> 재배치 목적지 (Gap > 0)
        </div>
        <div style="margin: 8px 0;">
            <span style="color: #1976D2;">●</span> 바이크 출발지 (공급 초과)
        </div>
        <div style="margin: 8px 0;">
            <span style="color: #FBC02D;">●</span> 동선 중 배터리 교체
        </div>
        <hr style="margin: 10px 0;">
        <small>숫자 = 재배치 우선순위</small>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # 요약
    total_gap = relocation_targets['gap'].sum()
    total_excess = -supply_excess['gap'].sum()
    total_battery = battery_spots['low_battery_count'].sum()

    stats_html = f"""
    <div style="position: fixed; top: 10px; right: 10px; z-index: 1000;
                background: white; padding: 15px; border-radius: 8px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.3); font-family: Arial;">
        <h4 style="margin: 0 0 12px 0;">{center_name} 업무 요약</h4>
        <table style="font-size: 13px;">
            <tr style="color: #E53935;">
                <td>재배치 필요</td>
                <td style="padding-left: 15px;"><b>{len(relocation_targets)}개소, {total_gap:.0f}대</b></td>
            </tr>
            <tr style="color: #1976D2;">
                <td>출발 가능</td>
                <td style="padding-left: 15px;"><b>{len(supply_excess)}개소, {total_excess:.0f}대</b></td>
            </tr>
            <tr style="color: #FBC02D;">
                <td>배터리 교체</td>
                <td style="padding-left: 15px;"><b>{len(battery_spots)}개소, {total_battery:.0f}대</b></td>
            </tr>
        </table>
    </div>
    """
    m.get_root().html.add_child(folium.Element(stats_html))

    # 타이틀
    title_html = f"""
    <div style="position: fixed; top: 10px; left: 50%; transform: translateX(-50%); z-index: 1000;
                background: white; padding: 12px 24px; border-radius: 8px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.3); font-family: Arial;">
        <h3 style="margin: 0;">🚲 {center_name} 저녁 재배치 업무</h3>
        <small style="color: #666;">다음날 피크시간 수요 예측 기반</small>
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    return m


def create_relocation_excel(gap_df, battery_df, center_name):
    """
    재배치 업무 Excel 생성
    """
    output_path = os.path.join(OUTPUT_DIR, f'relocation_task_{center_name}.xlsx')

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # 1. 재배치 필요 목록
        relocation = gap_df[gap_df['gap'] > 0].copy()
        relocation['우선순위'] = range(1, len(relocation) + 1)
        relocation = relocation[[
            '우선순위', 'region', 'lat', 'lng', 'predicted_demand', 'usable_count', 'gap'
        ]].rename(columns={
            'region': '권역', 'lat': '위도', 'lng': '경도',
            'predicted_demand': '예측수요', 'usable_count': '현재바이크', 'gap': '필요대수'
        })
        relocation.to_excel(writer, sheet_name='재배치목적지', index=False)

        # 2. 공급 초과 (출발지)
        excess = gap_df[gap_df['gap'] < -2].copy()
        excess['이동가능'] = -excess['gap']
        excess = excess[[
            'region', 'lat', 'lng', 'usable_count', 'predicted_demand', '이동가능'
        ]].rename(columns={
            'region': '권역', 'lat': '위도', 'lng': '경도',
            'usable_count': '현재바이크', 'predicted_demand': '예측수요'
        })
        excess.to_excel(writer, sheet_name='바이크출발지', index=False)

        # 3. 배터리 교체
        battery = battery_df[[
            'region', 'lat', 'lng', 'low_battery_count', 'avg_battery'
        ]].rename(columns={
            'region': '권역', 'lat': '위도', 'lng': '경도',
            'low_battery_count': '배터리부족대수', 'avg_battery': '평균배터리'
        })
        battery.to_excel(writer, sheet_name='배터리교체', index=False)

    print(f"✅ Excel 저장: {output_path}")
    return output_path


def main():
    """메인 실행"""
    print("="*60)
    print("저녁 재배치 업무 지원 시스템")
    print("="*60)

    # 테스트용: 과거 날짜 사용 (실제로는 오늘/내일 사용)
    reference_date = '2025-11-15'  # 바이크 위치 기준일
    target_date = '2025-11-16'    # 수요 예측 대상일

    print(f"\n기준일: {reference_date} 21시")
    print(f"예측일: {target_date} (피크 시간: 7-9시, 12시, 18-19시)")

    for center_name, regions in TARGET_CENTERS.items():
        print(f"\n{'='*60}")
        print(f"[{center_name}]")
        print("="*60)

        # 1. 다음날 수요 예측
        demand_df = get_demand_forecast(target_date, regions)

        # 2. 현재 바이크 위치 (테스트용 과거 데이터)
        supply_df = get_current_bike_positions_historical(regions, reference_date, snapshot_hour=21)

        if len(supply_df) == 0:
            print("  바이크 데이터 없음, 스킵")
            continue

        # 3. Gap 계산
        gap_df = calculate_supply_demand_gap(demand_df, supply_df)

        # 4. 배터리 부족 지점
        battery_df = get_low_battery_spots(supply_df)

        # 5. 지도 생성
        m = create_relocation_map(gap_df, battery_df, center_name)
        if m:
            map_path = os.path.join(OUTPUT_DIR, f'relocation_map_{center_name}.html')
            m.save(map_path)
            print(f"✅ 지도 저장: {map_path}")

        # 6. Excel 생성
        create_relocation_excel(gap_df, battery_df, center_name)

        # 요약 출력
        relocation_needed = gap_df[gap_df['gap'] > 0]
        print(f"\n[{center_name} 요약]")
        print(f"  재배치 필요: {len(relocation_needed)}개소, 총 {relocation_needed['gap'].sum():.0f}대")
        print(f"  배터리 교체: {len(battery_df)}개소, 총 {battery_df['low_battery_count'].sum():.0f}대")

    print("\n" + "="*60)
    print("완료!")
    print("="*60)


if __name__ == "__main__":
    main()
