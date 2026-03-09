"""
Stage 2: 공간 피처 (이웃 + 허브)

구역 간 공간적 상호작용을 포착한다:
- 이웃 집계: 2km 반경 내 거리 가중 평균
- 허브 기준점: 가장 트래픽이 높은 이웃을 앵커 포인트로 사용

핵심 인사이트: 구역의 라이딩 수요는 인근 지역의 영향을 받는다.
이웃 구역의 전환율이 높으면 해당 구역도 높을 가능성이 크다 (수요 파급 효과).

생성 피처:
    - neighbor_avg_rpo: 이웃의 단순 평균 RPO
    - neighbor_weighted_rpo: 이웃의 거리 가중 RPO
    - neighbor_max_rpo: 이웃 중 최대 RPO
    - neighbor_avg_bikes_400m: 이웃의 평균 기기 공급량
    - neighbor_count: 반경 내 구역 수
    - hub_prev_rpo/opens: 허브 구역의 전일 지표
    - hub_distance: 허브까지 거리
    - is_self_hub: 자기 자신이 허브인지 여부
"""

import warnings
import numpy as np
import pandas as pd
from scipy.spatial.distance import cdist
from config import NEIGHBOR_RADIUS_KM


def create_neighbor_hub_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    벡터화된 거리 행렬을 사용하여 공간 이웃 피처 계산.

    접근 방식:
    1. 모든 구역 중심점 간 쌍별 거리 행렬 생성
    2. NEIGHBOR_RADIUS_KM 내 이웃 식별
    3. RPO에 대한 단순 평균, 거리 가중 평균, 최댓값 계산
    4. 구역별 허브 (가장 트래픽이 높은 이웃) 식별

    Args:
        df: [h3_district_name, date, center_lat, center_lng,
            app_opens, rides_per_open, avg_bikes_400m] 컬럼을 가진 DataFrame

    Returns:
        공간 피처가 추가된 DataFrame
    """
    print("[Stage 2] 공간 피처 (이웃 + 허브)...")

    # ── 거리 행렬 생성 ──
    district_coords = df.groupby('h3_district_name').agg(
        lat=('center_lat', 'mean'),
        lng=('center_lng', 'mean'),
        avg_opens=('app_opens', 'mean')
    ).reset_index()

    districts = district_coords['h3_district_name'].values
    dist_to_idx = {d: i for i, d in enumerate(districts)}

    # 위도 약 37°N 기준 근사 계수를 사용하여 km로 변환
    lat_km, lng_km = 111.0, 111.0 * np.cos(np.radians(37.0))
    coords_km = np.column_stack([
        district_coords['lat'].values * lat_km,
        district_coords['lng'].values * lng_km
    ])

    dist_matrix = cdist(coords_km, coords_km, 'euclidean')
    neighbor_mask = (dist_matrix > 0) & (dist_matrix <= NEIGHBOR_RADIUS_KM)

    # ── 이웃 수 ──
    df['neighbor_count'] = df['h3_district_name'].map(
        {d: int(neighbor_mask[i].sum()) for i, d in enumerate(districts)}
    )

    # ── 허브 식별: 이웃 중 가장 높은 평균 앱 오픈 ──
    avg_opens_arr = district_coords['avg_opens'].values
    hub_map = {}
    hub_dist_map = {}

    for i, d in enumerate(districts):
        nbr_idx = np.where(neighbor_mask[i])[0]
        all_idx = np.append(nbr_idx, i) if len(nbr_idx) > 0 else np.array([i])
        max_idx = all_idx[np.argmax(avg_opens_arr[all_idx])]
        hub_map[d] = districts[max_idx]
        hub_dist_map[d] = dist_matrix[i, max_idx]

    n_self_hub = sum(1 for d in districts if hub_map[d] == d)
    print(f"  허브: {n_self_hub}/{len(districts)} 자체 허브, "
          f"{len(districts) - n_self_hub} 외부 허브 참조")

    # ── 정적 허브 피처 ──
    df['hub_district'] = df['h3_district_name'].map(hub_map)
    df['hub_distance'] = df['h3_district_name'].map(hub_dist_map)
    df['is_self_hub'] = (df['h3_district_name'] == df['hub_district']).astype(int)

    # ── 벡터화된 이웃 집계 ──
    df = df.sort_values(['h3_district_name', 'date']).copy()
    g = df.groupby('h3_district_name')

    # 거리 가중 행렬 (역거리 가중법)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        dist_weight = np.where(neighbor_mask, 1.0 / np.maximum(dist_matrix, 0.1), 0)
        row_sums = dist_weight.sum(axis=1, keepdims=True)
        dist_weight_norm = np.where(row_sums > 0, dist_weight / row_sums, 0)

    for col, avg_col, weighted_col, max_col in [
        ('rides_per_open', 'neighbor_avg_rpo', 'neighbor_weighted_rpo', 'neighbor_max_rpo'),
        ('avg_bikes_400m', 'neighbor_avg_bikes_400m', None, None),
    ]:
        if col not in df.columns:
            continue

        # 구역별 전일 값
        df[f'_prev_{col}'] = g[col].shift(1)

        # 벡터화 계산을 위해 구역×날짜 행렬로 피벗
        pivot = df.pivot_table(
            index='date', columns='h3_district_name',
            values=f'_prev_{col}', aggfunc='first'
        )
        col_order = pivot.columns.tolist()
        idx_map = [dist_to_idx[d] for d in col_order if d in dist_to_idx]
        mask_r = neighbor_mask[np.ix_(idx_map, idx_map)]

        vals = pd.DataFrame(pivot).astype(float).values
        nan_m = np.isnan(vals)
        vals_f = np.where(nan_m, 0, vals)
        cnt_v = (~nan_m).astype(float)

        # 이웃 간 단순 평균
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            nbr_sum = vals_f @ mask_r.T.astype(float)
            nbr_cnt = cnt_v @ mask_r.T.astype(float)
            nbr_avg = np.where(nbr_cnt > 0, nbr_sum / nbr_cnt, np.nan)

        avg_pivot = pd.DataFrame(nbr_avg, index=pivot.index, columns=pivot.columns)
        avg_melted = avg_pivot.reset_index().melt(
            id_vars='date', var_name='h3_district_name', value_name=avg_col)
        df = df.drop(columns=[avg_col], errors='ignore')
        df = df.merge(avg_melted, on=['date', 'h3_district_name'], how='left')

        # 거리 가중 평균
        if weighted_col:
            wm = dist_weight_norm[np.ix_(idx_map, idx_map)]
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                w_sum = vals_f @ wm.T
                w_cnt = cnt_v @ wm.T
                w_avg = np.where(w_cnt > 0, w_sum / w_cnt, np.nan)
            w_pivot = pd.DataFrame(w_avg, index=pivot.index, columns=pivot.columns)
            w_melted = w_pivot.reset_index().melt(
                id_vars='date', var_name='h3_district_name', value_name=weighted_col)
            df = df.merge(w_melted, on=['date', 'h3_district_name'], how='left')

        # 이웃 중 최댓값
        if max_col:
            max_rows = []
            for i_row in range(vals.shape[0]):
                row_maxes = []
                for j_col in range(vals.shape[1]):
                    nbr_j = np.where(mask_r[j_col])[0]
                    if len(nbr_j) > 0:
                        valid = vals[i_row, nbr_j]
                        valid = valid[~np.isnan(valid)]
                        row_maxes.append(np.max(valid) if len(valid) > 0 else np.nan)
                    else:
                        row_maxes.append(np.nan)
                max_rows.append(row_maxes)
            max_pivot = pd.DataFrame(max_rows, index=pivot.index, columns=pivot.columns)
            max_melted = max_pivot.reset_index().melt(
                id_vars='date', var_name='h3_district_name', value_name=max_col)
            df = df.merge(max_melted, on=['date', 'h3_district_name'], how='left')

        df = df.drop(columns=[f'_prev_{col}'], errors='ignore')

    # ── 허브의 전일 RPO 및 앱 오픈 ──
    hub_lookup = df[['date', 'h3_district_name']].copy()
    hub_lookup['_hub_rpo'] = g['rides_per_open'].shift(1)
    hub_lookup['_hub_opens'] = g['app_opens'].shift(1)
    hub_lookup.columns = ['date', 'hub_district', 'hub_prev_rpo', 'hub_prev_opens']
    df = df.merge(hub_lookup, on=['date', 'hub_district'], how='left')

    nc = df['neighbor_count']
    print(f"  {NEIGHBOR_RADIUS_KM}km 내 이웃: 구역당 평균 {nc.mean():.1f}개")
    return df
