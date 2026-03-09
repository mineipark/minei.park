"""
롤링 예측 시뮬레이션 결과 Excel 내보내기
권역별 1주일 데이터 (2026-01-27 ~ 2026-02-02)
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# credentials 설정
CREDENTIALS_PATH = 'os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'credentials/service-account.json')'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_PATH

from demand_model_v7 import DemandForecastModelV7

def run_and_export():
    print("="*80)
    print("권역별 롤링 예측 → Excel 내보내기")
    print("="*80)

    model = DemandForecastModelV7(credentials_path=CREDENTIALS_PATH)

    # 시뮬레이션 날짜
    test_dates = pd.date_range('2026-01-27', '2026-02-02')

    # 전체 데이터 로드
    print("\n데이터 로딩 중...")
    all_data = model.fetch_data('2025-01-01', '2026-02-02')
    print(f"로드 완료: {len(all_data):,}건")

    # 날씨 데이터 (로컬 CSV에서)
    weather_csv = '/path/to/project/demand_forecast/weather_2025_202601.csv'
    weather_df = pd.read_csv(weather_csv)
    weather_df['date'] = pd.to_datetime(weather_df['date'])

    weather_data = {}
    for _, row in weather_df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        weather_data[date_str] = {
            'temp_low': float(row['temp_low']) if pd.notna(row['temp_low']) else 0,
            'temp_high': float(row['temp_high']) if pd.notna(row['temp_high']) else 10,
            'snow_depth': float(row.get('snow_depth', 0)) if pd.notna(row.get('snow_depth', 0)) else 0
        }

    # 결과 저장용
    daily_results = []
    region_details = []

    dow_names = {1: '일', 2: '월', 3: '화', 4: '수', 5: '목', 6: '금', 7: '토'}

    print("\n롤링 시뮬레이션 실행 중...")

    for target_date in test_dates:
        target_str = target_date.strftime('%Y-%m-%d')

        # 학습/테스트 데이터 분리
        cutoff_date = target_date - timedelta(days=1)
        train_data = all_data[pd.to_datetime(all_data['date']) <= cutoff_date].copy()
        test_data = all_data[pd.to_datetime(all_data['date']) == target_date].copy()

        if len(test_data) == 0:
            continue

        # 모델 학습
        model.train(train_data)

        # 날씨
        weather = weather_data.get(target_str, {'temp_low': 0, 'temp_high': 5, 'snow_depth': 0})

        # 피처 준비
        combined_data = pd.concat([train_data, test_data])
        df_features, _ = model.prepare_features(combined_data)
        test_df = df_features[df_features['date'] == target_date]

        dow = int(test_df['day_of_week'].iloc[0])
        is_saturday = (dow == 7)
        is_sunday = (dow == 1)
        is_monday = (dow == 2)

        total_actual = 0
        total_pred = 0

        # 권역별 예측
        for region in test_df['region'].unique():
            region_df = test_df[test_df['region'] == region]
            center = region_df['center_name'].iloc[0] if 'center_name' in region_df.columns else ''

            base_pred = model.model.predict(region_df[model.feature_cols]).sum()
            actual = region_df['ride_count'].sum()

            factor, desc = model.correction.calculate(
                region,
                weather['temp_low'],
                weather['temp_high'],
                is_saturday, is_sunday, is_monday,
                weather['snow_depth']
            )
            adj_pred = base_pred * factor

            total_actual += actual
            total_pred += adj_pred

            error_pct = ((adj_pred - actual) / actual * 100) if actual > 0 else 0

            region_details.append({
                '날짜': target_str,
                '요일': dow_names[dow],
                '권역': region,
                '센터': center,
                '실제': round(actual),
                '예측': round(adj_pred),
                '오차': round(adj_pred - actual),
                '오차율(%)': round(error_pct, 1),
                '보정계수': round(factor, 2),
                '최저기온': weather['temp_low'],
                '최고기온': weather['temp_high'],
                '적설(cm)': weather['snow_depth']
            })

        error_pct = ((total_pred - total_actual) / total_actual * 100) if total_actual > 0 else 0
        daily_results.append({
            '날짜': target_str,
            '요일': dow_names[dow],
            '실제': round(total_actual),
            '예측': round(total_pred),
            '오차': round(total_pred - total_actual),
            '오차율(%)': round(error_pct, 1),
            '최저기온': weather['temp_low'],
            '최고기온': weather['temp_high'],
            '적설(cm)': weather['snow_depth']
        })

        print(f"  {target_str} ({dow_names[dow]}): 실제 {total_actual:,.0f} / 예측 {total_pred:,.0f} / 오차 {error_pct:+.1f}%")

    # DataFrame 생성
    df_daily = pd.DataFrame(daily_results)
    df_region = pd.DataFrame(region_details)

    # 권역별 요약 (7일 평균)
    region_summary = df_region.groupby('권역').agg({
        '실제': 'sum',
        '예측': 'sum',
        '센터': 'first'
    }).reset_index()
    region_summary['평균_실제'] = round(region_summary['실제'] / 7, 1)
    region_summary['평균_예측'] = round(region_summary['예측'] / 7, 1)
    region_summary['총_오차율(%)'] = round((region_summary['예측'] - region_summary['실제']) / region_summary['실제'] * 100, 1)
    region_summary = region_summary.sort_values('실제', ascending=False)

    # Excel 저장
    output_path = '/path/to/project/demand_forecast/rolling_simulation_weekly.xlsx'

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_daily.to_excel(writer, sheet_name='일별요약', index=False)
        df_region.to_excel(writer, sheet_name='권역별상세', index=False)
        region_summary.to_excel(writer, sheet_name='권역별요약', index=False)

    print(f"\n✅ Excel 저장 완료: {output_path}")
    print(f"  - 일별요약: {len(df_daily)}행")
    print(f"  - 권역별상세: {len(df_region)}행")
    print(f"  - 권역별요약: {len(region_summary)}행")

    return output_path


if __name__ == "__main__":
    run_and_export()
