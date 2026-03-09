import json
import pandas as pd
import numpy as np
from datetime import datetime
from pytz import timezone
from shapely.geometry import Point
from h3 import h3
try:
    from .utils import *
except Exception:
    from utils import *

def raw_to_hex(bike_raw, ride_raw, bike_snapshot_raw, ride_new, zone, geo_district, area_dict, district_exception, hour, isweekend):

    if int in [type(bike_raw), type(ride_raw), type(zone)] or not len(bike_raw) or not len(ride_raw):
        hex_col = ['h3_index', 'area', 'ride_cnt', 'bike_cnt', 'bike_list']
        from_hex = pd.DataFrame(columns=hex_col)
        to_hex = pd.DataFrame(columns=hex_col)
        return from_hex, to_hex
    else:
        # bike 데이터 처리
        bike_raw['area'] = bike_raw['area'].replace(area_dict)
        bike_raw['h3_index'] = bike_raw.apply(lambda row: h3.geo_to_h3(row.lat, row.long, 9), axis=1)
        # bike별 위치한 district 매핑
        bike_raw['Point'] = bike_raw.apply(lambda row: Point([row.long, row.lat]), axis=1)
        bike_raw['tmp'] = 1
        geo_district['tmp'] = 1
        bike_district = bike_raw.merge(geo_district, how='outer', on='tmp')
        bike_district['intersect'] = bike_district.apply(lambda row: row.Point.intersects(row.geometry), axis=1)
        bike_raw = bike_district[bike_district['intersect'] == True].drop(columns=['tmp', 'district_id', 'geometry', 'intersect'])
        # district_exception에 있는 district 제외
        bike_raw = bike_raw.query("district_name not in @district_exception").reset_index(drop=True)
        
        # 250724 Central PoC 재배치 브랜치 로직
        poc_area_list = ['Region_Central_1', 'Region_Central_2', 'Region_Central_3']
        area_list = bike_raw['area'].unique()
        if any(area in poc_area_list for area in area_list):
            bike_raw = bike_raw.query("leftover < 35").reset_index(drop=True)
        print(len(bike_raw.query("leftover < 35")))

        bike = pd.concat([
            bike_raw.groupby('h3_index').size(),
            bike_raw.groupby('h3_index')['id'].apply(lambda id: list(id)),
            bike_raw.groupby('h3_index')['area'].first()
        ], axis=1).reset_index().rename(columns={'id': 'bike_list', 0: 'bike_cnt'})

        # ride 데이터 처리
        ride_raw['area'] = ride_raw['area'].replace(area_dict)
        ride_raw['h3_index'] = ride_raw.apply(lambda row: h3.geo_to_h3(row.lat, row.long, 9), axis=1)
        if isweekend:
            ride_raw['weight'] = ride_raw.apply(lambda row: 2/9 if row.date.weekday() > 4 else 1/9, axis=1)
        else:
            ride_raw['weight'] = ride_raw.apply(lambda row: 1/12 if row.date.weekday() > 4 else 2/12, axis=1)
        ride = pd.concat([
            np.round(ride_raw.groupby('h3_index')['weight'].sum(), 2),
            ride_raw.groupby('h3_index')[['area']].first()
        ], axis=1).reset_index().rename(columns={'weight': 'ride_cnt'})

        # bike, ride를 병합
        all_hex = pd.merge(ride, bike, how='outer').fillna(0)
        # fillna(0)을 하면서 bike_list가 0인 것을 빈 리스트로 바꾸어주는 작업
        all_hex['bike_list'] = all_hex['bike_list'].replace({0: '[]'})
        all_hex['bike_list'] = all_hex['bike_list'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
        all_hex['Point'] = all_hex['h3_index'].apply(lambda x: Point(h3.h3_to_geo(x)[1], h3.h3_to_geo(x)[0]))

        bike_snapshot_raw['h3_index'] = bike_snapshot_raw.apply(lambda row: h3.geo_to_h3(row.lat, row.long, 9), axis=1)
        merge_df = bike_snapshot_raw.merge(ride_new, on='bike_id', how='left')
        merge_df = merge_df.query("(start_time >= date_criteria_time) & (start_time <= date_criteria_time1)")
        # h3_value = merge_df.groupby(['h3_index', 'date', 'bike_id'])['fee'].sum().reset_index().groupby(['h3_index', 'date'])['fee'].mean().groupby('h3_index').mean().reset_index()
        h3_value = merge_df.groupby(['date', 'h3_index','bike_id'])['fee'].sum().reset_index().groupby(['h3_index'])['fee'].mean().reset_index()
        h3_value['fee'] = np.round(h3_value['fee'])
        # h3_ride_48 = merge_df.groupby(['h3_index', 'date', 'bike_id']).size().reset_index().groupby(['h3_index', 'date']).mean().groupby('h3_index').mean().reset_index()
        h3_ride_48 = merge_df.groupby(['h3_index', 'date', 'bike_id']).size().reset_index()
        h3_ride_48 = h3_ride_48.rename(columns = {0 : 'ride_48h'})
        h3_ride_48 = h3_ride_48.groupby('h3_index')['ride_48h'].mean().reset_index()
        h3_value = h3_value.merge(h3_ride_48, on ='h3_index')
        # h3_value['bike_avg'] = np.round(h3_value['bike_id'], 2)
        bike_avg=merge_df.groupby(['h3_index','date'])['bike_id'].nunique().reset_index().groupby('h3_index')['bike_id'].mean().reset_index()
        h3_value=bike_avg.merge(h3_value, on='h3_index', how='left')
        h3_value.rename(columns = {'bike_id' : 'bike_avg'},inplace=True)
        h3_value['fpb'] = h3_value['fee']/h3_value['bike_avg']

        # h3_value = h3_value[['h3_index', 'fpb']]

        all_hex = all_hex.merge(h3_value, on='h3_index', how='left').fillna(0)
        all_hex['rpb'] = all_hex['ride_48h']/(all_hex['bike_cnt'] + 4)

        return all_hex

def get_algorithm_hex(arg, all_hex, ride_raw, to_hex_neighbor_list, debug, hour):
    hex_col = ['h3_index', 'area', 'ride_cnt', 'bike_cnt', 'bike_list', 'Point', 'fee']
    # 병합된 table에서 from_hex
    from_hex = pd.DataFrame(columns=hex_col)

    # gamma 정의 (gamma: 재배치 매칭 건수)
    from_hex_cnt = int(arg * 0.1)

    if debug == False:
        area_list = all_hex['area'].unique()
        # 핸들러 일감에 의한 라이딩은 ride_cnt에서 제외
        ride_raw = ride_raw.query("riding_type != 3")

        # 동탄 피크타임 재배치용 브랜치 로직
        if ('동탄1신도시권역' in  area_list) & (hour >= 21):
            # 임의 수거 hex 지정
            from_hex_list = [
                '8930e02cc5bffff',
                '8930e02cc43ffff',
                '8930e02cc53ffff',
                '8930e02cc57ffff',
                '8930e02cccfffff',
                '8930e02cc1bffff',
                '8930e02ccc7ffff',
                '8930e02ccc3ffff',
                '8930e02ea6fffff',
                '8930e02ccd3ffff',
                '8930e02c1a7ffff',
                '8930e02ea6bffff',
                '8930e02ea6fffff',
                '8930e02ea5bffff',
                '8930e02ea53ffff',
                '8930e02c1b7ffff',
                '8930e02c1b3ffff',
                '8930e02c517ffff',
                '8930e02c507ffff',
                '8930e02c5abffff',
                '8930e02c513ffff',
                '8930e02c503ffff',
                '8930e02c533ffff',
                '8930e02c1a3ffff',
                '8930e02c5c7ffff',
                '8930e02c51bffff',
                '8930e02ccd7ffff',
                '8930e396597ffff',
                '8930e02c98fffff',
                '8930e15965bffff',
                '8930e02c913ffff',
                '8930e02c917ffff',
                '8930e02cc13ffff',
                '8930e02c903ffff',
                '8930e02c90bffff',
                '8930e02cb27ffff',
                '8930e02cb2fffff'
            ]
            from_hex = all_hex[all_hex['h3_index'].isin(from_hex_list)]
            # 야간 시간대 ride_cnt 집계
            night_ride_agg = (ride_raw.query("hour >= 22 or hour <= 6").groupby("h3_index").size()/7).reset_index().rename(columns={0: 'night_ride_cnt'})
            from_hex = from_hex.merge(night_ride_agg, how='left')
            from_hex['night_ride_cnt'] = from_hex['night_ride_cnt'].fillna(0)
            from_hex['night_ride_cnt'] = from_hex['night_ride_cnt'].fillna(0)
            from_hex['surplus_bike_cnt'] = np.ceil(from_hex['bike_cnt'] - from_hex['night_ride_cnt'] + 2).astype(int)
            from_hex = from_hex.sort_values(by='surplus_bike_cnt', ascending=False).reset_index(drop=True)
            from_hex = from_hex.query("surplus_bike_cnt > 0")
            from_hex = from_hex.query("bike_cnt >= surplus_bike_cnt")
            from_hex['bike_cnt_cumsum'] = from_hex['surplus_bike_cnt'].cumsum()
            call_count = int(arg)
            from_hex = from_hex[from_hex['bike_cnt_cumsum'].shift(fill_value=0) <= call_count]
            # from_hex.to_csv(f'result_table/from_hex_info.csv', encoding='cp949', index=False)

        # 천안 브랜치 로직
        if ('Region_Central_2' in  area_list):
            for area in area_list:
                tmp_hex = all_hex.query('area == @area')
                tmp_hex['ride_cnt_log'] = np.log(tmp_hex['ride_cnt'] + 1)
                tmp_hex['ride_distance'] = standardScaler(tmp_hex[['ride_cnt_log', 'ride_48h']]).apply(sum_of_squares, axis = 1)
                #from_hex - bike_cnt 조건을 1로 격하
                tmp_from = tmp_hex.query('bike_cnt >= 1 & fee != 0') 
                tmp_from = tmp_from.sort_values(by = ['ride_distance'], ascending = True)
                tmp_from = tmp_from[tmp_from['h3_index'].isin(to_hex_neighbor_list)==False]
                from_hex_list = hex_getter(tmp_from['h3_index'], from_hex_cnt, 0)
                tmp_from = tmp_from[tmp_from['h3_index'].isin(from_hex_list)]

                from_hex = pd.concat([from_hex, tmp_from], axis=0)
                print(f"선정된 from_hex 수: {len(from_hex)}")

        #일반로직
        else:
            for area in area_list:
                tmp_hex = all_hex.query('area == @area')
                tmp_hex['ride_cnt_log'] = np.log(tmp_hex['ride_cnt'] + 1)
                tmp_hex['ride_distance'] = standardScaler(tmp_hex[['ride_cnt_log', 'ride_48h']]).apply(sum_of_squares, axis = 1)
                #from_hex
                tmp_from = tmp_hex.query('bike_cnt >= 3 & fee != 0')
                tmp_from = tmp_from.sort_values(by = ['ride_distance'], ascending = True)
                tmp_from = tmp_from[tmp_from['h3_index'].isin(to_hex_neighbor_list)==False]
                from_hex_list = hex_getter(tmp_from['h3_index'], from_hex_cnt, 0)
                tmp_from = tmp_from[tmp_from['h3_index'].isin(from_hex_list)]
                # tmp_from = tmp_from[tmp_from['h3_index'].isin(to_hex_list)==False]

                from_hex = pd.concat([from_hex, tmp_from], axis=0)
                print(f"선정된 from_hex 수: {len(from_hex)}")
    return from_hex
