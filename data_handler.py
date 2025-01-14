'''
나라장터 개찰 데이터 분석 처리 파일
- calcul_winrate : 개찰 데이터에서 업체별 참여 횟수와 낙찰 횟수를 기반으로 낙찰률과 가중 낙찰률을 계산하는 함수
- filtering_underone : 낙찰 횟수가 0보다 큰 업체만 필터링하는 함수
- rankclass : 가중 낙찰률을 기반으로 업체에 클래스(S, A, B, C, D)를 할당하고 순위를 부여하는 함수
- get_final_df : 기존 데이터와 새로운 업체 정보를 통합하여 주소, 사업 형태, 위치 정보를 업데이트하고 최종 데이터 프레임을 생성하는 함수
'''

import pandas as pd
import numpy as np

### 낙찰률 계산 함수
def calcul_winrate(result_df):
    result_filter = result_df[(result_df['순위'] == 1) | (result_df['순위'] == '1')] # 낙찰에 성공한 데이터만 추출

    df1 = pd.DataFrame(result_df[['업체명','사업자등록번호']].value_counts()) # 참여횟수
    df2 = pd.DataFrame(result_filter[['업체명','사업자등록번호']].value_counts()) # 낙찰횟수

    winrate_df = pd.merge(df1, df2, on=['업체명','사업자등록번호'], how='outer') # 아우터조인으로 새로운 데이터프레임 생성
    winrate_df.columns = ['참여횟수', '낙찰횟수']

    winrate_df.fillna(0, inplace=True)  # 결측치를 0으로 대체
    winrate_df = winrate_df.astype(int)  # 모든 열을 정수형으로 변환
    winrate_df['낙찰률(%)'] = ((winrate_df['낙찰횟수']/winrate_df['참여횟수'])*100).round(3) # 낙찰률 계산 후 반올림
    winrate_df = winrate_df.sort_values(by='낙찰률(%)', ascending=False) # 낙찰률 기준으로 내림차순 정렬

    # mean1 = np.mean(winrate_df['낙찰률(%)'] /100) # 평균 낙찰률
    # mean2 = np.mean(winrate_df['참여횟수']) # 평균 참여횟수

    winrate_df['가중 낙찰률'] = ((winrate_df['낙찰횟수']/winrate_df['참여횟수']) * np.log(winrate_df['참여횟수']+1)).round(3)
    # winrate_df['성과 정규화 지수'] = ((winrate_df['낙찰횟수']/winrate_df['참여횟수']) / np.sqrt(winrate_df['참여횟수'])).round(3)
    # winrate_df['상대적 성과 지수'] = ((winrate_df['낙찰률(%)'] /100)/mean1) * (mean2/winrate_df['참여횟수'])

    winrate_df = winrate_df.reset_index()

    return winrate_df


### 가중 낙찰률 필터링
def filtering_underone(winrate_df):
    return winrate_df[winrate_df['낙찰횟수'] > 0]


### 가중 낙찰률 기반 클래스 할당
def rankclass(filtered_df):
    # filtered_df의 명시적 복사본 생성
    filtered_df = filtered_df.copy()

    percentiles = np.percentile(filtered_df['가중 낙찰률'].values, [20, 40, 60, 80])
    labels = ['D', 'C', 'B', 'A', 'S']

    def classify(weight, percentiles):
        for idx, percentile in enumerate(percentiles):
            if weight <= percentile:
                return labels[idx]
        return labels[-1]

    filtered_df.loc[:, '가중낙찰률 클래스'] = filtered_df['가중 낙찰률'].apply(lambda x: classify(x, percentiles))
    filtered_df.loc[:, '순위'] = filtered_df.groupby('가중낙찰률 클래스')['가중 낙찰률'].rank(method='min', ascending=False).astype(int)
    filtered_df.loc[:, 'rank_class'] = filtered_df['가중낙찰률 클래스'] + filtered_df['순위'].astype(str)
    filtered_df = filtered_df.drop(columns=['순위'])
    
    return filtered_df


def get_final_df(ranked_df, old_df , newcompany_info):
    ranked_df['사업자등록번호'] = ranked_df['사업자등록번호'].astype(str)
    old_df['사업자등록번호'] = old_df['사업자등록번호'].astype(str)
    newcompany_info['사업자등록번호'] = newcompany_info['사업자등록번호'].astype(str)
    
    if not old_df.empty:
        updated_df = ranked_df.merge(old_df[['사업자등록번호', '주소', '사업형태', '위도', '경도', '전화번호']], on='사업자등록번호', how='left')
    else:
        updated_df = ranked_df.copy()

    if newcompany_info.empty:
        return updated_df
    
    for idx, row in newcompany_info.iterrows():
        biznonumber = row['사업자등록번호']
        mask = updated_df['사업자등록번호'] == biznonumber

        if mask.any():
            updated_df.loc[mask, '주소'] = row['주소']
            updated_df.loc[mask, '사업형태'] = row['사업형태']
            updated_df.loc[mask, '위도'] = row['위도']
            updated_df.loc[mask, '경도'] = row['경도']
            updated_df.loc[mask, '전화번호'] = row['전화번호']

    updated_df = updated_df.sort_values(by='가중 낙찰률', ascending=False) # 가중낙찰률 기준 내림차순 정렬

    return updated_df