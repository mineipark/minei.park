"""
Hex-level 공간 분포 안정성 검증
- district 내 hex별 라이딩 비율이 주간 단위로 얼마나 안정적인지
- 시간대 비율 안정성과 비교
"""
import os, sys
import numpy as np
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_cred = os.path.join(SCRIPT_DIR, '..', 'credentials', 'service-account.json')
if os.path.exists(_cred):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _cred

from google.cloud import bigquery
client = bigquery.Client()

# ── 1. Hex-level 라이딩 데이터 (최근 4주, 주간 단위) ──
print("=" * 60)
print("🔍 Hex-level 공간 분포 안정성 분석")
print("=" * 60)

query = """
WITH weekly_hex AS (
    SELECT
        h3_start_area_name as region,
        h3_start_district_name as district,
        udf.geo_to_h3(ST_Y(start_location), ST_X(start_location), 9) as h3_index,
        DATE_TRUNC(DATE(start_time), WEEK(MONDAY)) as week_start,
        COUNT(*) as rides
    FROM `service.rides`
    WHERE DATE(start_time) BETWEEN '2026-01-26' AND '2026-02-22'
        AND bike_type = 1
        AND h3_start_area_name IS NOT NULL
        AND h3_start_district_name IS NOT NULL
    GROUP BY 1, 2, 3, 4
),
district_weekly AS (
    SELECT region, district, week_start, SUM(rides) as district_rides
    FROM weekly_hex
    GROUP BY 1, 2, 3
),
hex_ratio AS (
    SELECT
        h.region, h.district, h.h3_index, h.week_start,
        h.rides as hex_rides,
        d.district_rides,
        SAFE_DIVIDE(h.rides, d.district_rides) as hex_ratio
    FROM weekly_hex h
    JOIN district_weekly d USING (region, district, week_start)
)
SELECT *
FROM hex_ratio
ORDER BY region, district, h3_index, week_start
"""

print("\n📡 BQ 쿼리 실행 중 (4주 hex-level)...")
df = client.query(query).to_dataframe()
print(f"  → {len(df):,} rows, {df['district'].nunique()} districts, {df['h3_index'].nunique()} hexes")

# ── 2. Hex 비율 안정성 분석 ──
# hex별로 4주간 비율의 CV (변동계수) 계산
hex_stats = df.groupby(['region', 'district', 'h3_index']).agg(
    mean_ratio=('hex_ratio', 'mean'),
    std_ratio=('hex_ratio', 'std'),
    n_weeks=('hex_ratio', 'count'),
    mean_rides=('hex_rides', 'mean'),
    total_rides=('hex_rides', 'sum'),
).reset_index()

# 최소 3주 이상 데이터 있는 hex만
hex_stats_3w = hex_stats[hex_stats['n_weeks'] >= 3].copy()
hex_stats_3w['cv'] = hex_stats_3w['std_ratio'] / hex_stats_3w['mean_ratio']

print(f"\n📊 3주 이상 데이터 있는 hex: {len(hex_stats_3w):,} / {len(hex_stats):,}")

# 규모별 안정성
print("\n── Hex 주간 비율 안정성 (CV = 변동계수, 낮을수록 안정) ──")
bins = [0, 5, 10, 20, 50, 100, float('inf')]
labels = ['1-5건', '6-10건', '11-20건', '21-50건', '51-100건', '100건+']
hex_stats_3w['rides_group'] = pd.cut(hex_stats_3w['mean_rides'], bins=bins, labels=labels)

group_summary = hex_stats_3w.groupby('rides_group', observed=True).agg(
    hex_count=('cv', 'count'),
    median_cv=('cv', 'median'),
    mean_cv=('cv', 'mean'),
    mean_ratio=('mean_ratio', 'mean'),
    pct_cv_under_30=('cv', lambda x: (x < 0.3).mean() * 100),
).round(3)

print(group_summary.to_string())

# ── 3. 시간대 비율과 비교 ──
print("\n\n── 비교: 시간대 비율 안정성 ──")
query_time = """
WITH weekly_window AS (
    SELECT
        h3_start_district_name as district,
        DATE_TRUNC(DATE(start_time), WEEK(MONDAY)) as week_start,
        CASE
            WHEN EXTRACT(HOUR FROM start_time) BETWEEN 0 AND 5 THEN 'dawn'
            WHEN EXTRACT(HOUR FROM start_time) BETWEEN 6 AND 8 THEN 'morning'
            WHEN EXTRACT(HOUR FROM start_time) BETWEEN 9 AND 11 THEN 'midday'
            WHEN EXTRACT(HOUR FROM start_time) BETWEEN 12 AND 14 THEN 'lunch'
            WHEN EXTRACT(HOUR FROM start_time) BETWEEN 15 AND 17 THEN 'afternoon'
            WHEN EXTRACT(HOUR FROM start_time) BETWEEN 18 AND 20 THEN 'evening'
            ELSE 'night'
        END as time_window,
        COUNT(*) as rides
    FROM `service.rides`
    WHERE DATE(start_time) BETWEEN '2026-01-26' AND '2026-02-22'
        AND bike_type = 1
        AND h3_start_district_name IS NOT NULL
    GROUP BY 1, 2, 3
),
district_weekly AS (
    SELECT district, week_start, SUM(rides) as total
    FROM weekly_window
    GROUP BY 1, 2
)
SELECT w.district, w.week_start, w.time_window, w.rides,
       d.total, SAFE_DIVIDE(w.rides, d.total) as window_ratio
FROM weekly_window w
JOIN district_weekly d USING (district, week_start)
"""

print("📡 BQ 쿼리 실행 중 (시간대 비율)...")
df_time = client.query(query_time).to_dataframe()

time_stats = df_time.groupby(['district', 'time_window']).agg(
    mean_ratio=('window_ratio', 'mean'),
    std_ratio=('window_ratio', 'std'),
    n_weeks=('window_ratio', 'count'),
    mean_rides=('rides', 'mean'),
).reset_index()
time_stats_3w = time_stats[time_stats['n_weeks'] >= 3].copy()
time_stats_3w['cv'] = time_stats_3w['std_ratio'] / time_stats_3w['mean_ratio']

print(f"\n시간대 비율 CV 중앙값: {time_stats_3w['cv'].median():.3f}")
print(f"Hex 비율 CV 중앙값: {hex_stats_3w['cv'].median():.3f}")
print(f"\n시간대 비율 CV<0.3 비율: {(time_stats_3w['cv'] < 0.3).mean()*100:.1f}%")
print(f"Hex 비율 CV<0.3 비율: {(hex_stats_3w['cv'] < 0.3).mean()*100:.1f}%")

# ── 4. District별 Top hex 집중도 ──
print("\n\n── District 내 Top-N hex 집중도 ──")
district_hex_count = hex_stats.groupby('district').agg(
    total_hexes=('h3_index', 'count'),
    total_rides=('total_rides', 'sum'),
).reset_index()

# Top 5 hex가 차지하는 비율
def top_n_share(grp, n=5):
    top = grp.nlargest(n, 'total_rides')
    return top['total_rides'].sum() / grp['total_rides'].sum()

top5_share = hex_stats.groupby('district').apply(lambda g: top_n_share(g, 5)).reset_index()
top5_share.columns = ['district', 'top5_share']

top10_share = hex_stats.groupby('district').apply(lambda g: top_n_share(g, 10)).reset_index()
top10_share.columns = ['district', 'top10_share']

conc = district_hex_count.merge(top5_share).merge(top10_share)
print(f"District 수: {len(conc)}")
print(f"District당 평균 hex 수: {conc['total_hexes'].mean():.1f}")
print(f"Top 5 hex 점유율 평균: {conc['top5_share'].mean()*100:.1f}%")
print(f"Top 10 hex 점유율 평균: {conc['top10_share'].mean()*100:.1f}%")

# 규모별
print("\n── 규모별 hex 집중도 ──")
conc['rides_per_week'] = conc['total_rides'] / 4  # 4주
bins2 = [0, 50, 100, 200, 500, float('inf')]
labels2 = ['~50/w', '50-100/w', '100-200/w', '200-500/w', '500+/w']
conc['size_group'] = pd.cut(conc['rides_per_week'], bins=bins2, labels=labels2)

conc_group = conc.groupby('size_group', observed=True).agg(
    districts=('district', 'count'),
    avg_hexes=('total_hexes', 'mean'),
    avg_top5=('top5_share', 'mean'),
    avg_top10=('top10_share', 'mean'),
).round(3)
print(conc_group.to_string())

# ── 5. 결론 ──
print("\n" + "=" * 60)
print("📋 결론")
print("=" * 60)
hex_cv_med = hex_stats_3w['cv'].median()
time_cv_med = time_stats_3w['cv'].median()
if hex_cv_med < 0.4:
    print(f"✅ Hex 비율 CV 중앙값 {hex_cv_med:.3f} → 공간 분포 예측 가능")
elif hex_cv_med < 0.6:
    print(f"⚠️ Hex 비율 CV 중앙값 {hex_cv_med:.3f} → 대형 hex 위주로 예측 가능")
else:
    print(f"❌ Hex 비율 CV 중앙값 {hex_cv_med:.3f} → 안정성 부족, 주의 필요")

print(f"  시간대 비율 CV: {time_cv_med:.3f} (비교 기준)")
print(f"  큰 hex (20건+/주) CV<0.3: {(hex_stats_3w[hex_stats_3w['mean_rides']>=20]['cv'] < 0.3).mean()*100:.1f}%")
