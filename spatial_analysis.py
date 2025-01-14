'''
업체 위치와 시군구 경계면 매칭 및 분석
- matching_boundary : 시군구 경계 데이터와 업체 위치 데이터를 매칭하여 각 업체의 시군구코드명을 할당하는 함수
- calcul_area : 시군구코드명별로 평균 가중 낙찰률을 계산하고, 각 클래스(S, A, B, C, D)별 업체 수를 집계하는 함수
- area_merge : 시군구코드명별 통계 데이터와 시군구 경계 데이터를 병합하여 시각화에 필요한 GeoDataFrame을 생성하는 함수
- save_analysis_result: 시군구별 업체 데이터 저장하는 함수
'''

import os
import errno
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

### 시군구 경계면과 업체 위치 매칭 함수
def matching_boundary(polygon_df, df):
    # 전국 시군구 경계면을 GeoDataFrame으로 변환
    gdf = gpd.GeoDataFrame(polygon_df, geometry=gpd.GeoSeries.from_wkt(polygon_df['geometry']))
    df['시군구코드명'] = None

    # 각 점에 대해 포함되는 폴리곤의 시군구코드명 찾기
    for idx, row in df.iterrows():
        point = Point(row['경도'], row['위도'])
        gdf['is_within'] = gdf['geometry'].contains(point)
        matching_rows = gdf[gdf['is_within']]
        
        if not matching_rows.empty:
            # df.loc[idx, '시군구코드명'] = matching_rows['SIG_KOR_NM'].values[0]
            df.loc[idx, '시군구코드명'] = matching_rows['시군구코드명'].values[0]

    # 결과 확인
    return df


### 시군구코드명별로 평균 가중 낙찰률 계산, 각 클래스별 업체 수 집계 함수
def calcul_area(df):
    # 시군구코드명 별로 가중낙찰률 평균 구하기
    df1 = df.groupby('시군구코드명')['가중 낙찰률'].mean()
    
    # 시군구코드명 별로 클래스 개수 구하기
    df2 = df.groupby('시군구코드명')['가중낙찰률 클래스'].value_counts().unstack().fillna(0)

    # 필요한 클래스(S, A, B, C, D) 열이 없을 경우 추가하여 0으로 채움
    df2 = df2.reindex(columns=['S', 'A', 'B', 'C', 'D'], fill_value=0).astype(int)
    
    # 평균 가중 낙찰률과 클래스별 개수를 병합
    merge_df = pd.concat([df1, df2], axis=1)

    # 시군구별 총 업체개수
    merge_df['업체개수'] = merge_df['S'] + merge_df['A'] + merge_df['B'] +merge_df['C'] +merge_df['D']

    merge_df.insert(0, '시군구코드명', merge_df.index)
    merge_df = merge_df.reset_index(drop=True)

    merge_df = merge_df.rename(columns={'가중 낙찰률': '평균 가중 낙찰률','S': 'S개수','A': 'A개수','B': 'B개수','C': 'C개수','D': 'D개수'})
    return merge_df


### 시각화에 필요한 GeoDataFrame 생성 함수
def area_merge(merge_df, polygon_df):
    polygon_df = polygon_df.rename(columns={'SIG_KOR_NM':'시군구코드명'})
    sigunguboundary_df = pd.merge(merge_df, polygon_df, on='시군구코드명', how='left')

    sigunguboundary_df.fillna(0, inplace=True)
    return sigunguboundary_df


### 시군구별 업체 데이터 저장하는 함수
def save_analysis_result(sigunguboundary_df, data_dir, file_prefix):
    # 결과 저장 파일 경로 생성
    sigunguboundary_file = os.path.join(data_dir, f'{file_prefix}_sigunguboundary_df.csv')
    
    try:
        # 데이터 저장
        sigunguboundary_df.to_csv(sigunguboundary_file, index=False, encoding='utf-8-sig')
        print(f"✅ 시군구 경계 분석 결과가 저장되었습니다: {sigunguboundary_file}")
        print("✅ 분석이 완료되었습니다. 업데이트된 파일로 Kepler 시각화를 진행해주세요.")
        
    except PermissionError as e:
        if e.errno == errno.EACCES:
            print(f"⚠️ 파일 접근 권한 오류: {e.filename}에 접근할 수 없습니다.")
            print("⚠️ 다른 프로그램에서 파일을 열고 있는지 확인한 후 다시 시도하세요.")
        else:
            print(f"⚠️ 파일 저장 중 오류 발생: {e}")
    
    except Exception as e:
        print(f"⚠️ 파일 저장 중 오류 발생: {e}")