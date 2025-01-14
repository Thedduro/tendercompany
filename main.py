'''
나라장터 개찰 데이터 분석 메인 실행 파일
- main : 나라장터 데이터를 크롤링하고 처리하여 분석 결과를 저장하는 전체 흐름을 관리하는 함수
'''

from nara_auto.crawler import check_and_select_mode, update_mode
from nara_auto.data_handler import calcul_winrate, filtering_underone, rankclass, get_final_df
from nara_auto.company_info import get_companyinfo, filtering_data
from nara_auto.spatial_analysis import matching_boundary, calcul_area, area_merge, save_analysis_result
import os
import sys
import pandas as pd
import shutil
from tabulate import tabulate


def main():
    while True:
        # 키워드 폴더 확인 및 모드 선택
        search_word, selected_mode = check_and_select_mode()
        if search_word == '#': continue  # 다시 키워드 입력 받음
        elif search_word is None: return  # 프로그램 종료

        # update_mode 함수 호출
        result = update_mode(search_word, selected_mode)

        # update_mode에서 None이 반환된 경우 처리
        if result is None:
            print("↩️데이터 처리 중 오류가 발생했습니다. 다시 시도하세요.")
            continue

        new_bid_df, pass_list, new_result_df = result
    
        if new_bid_df is not None and not new_bid_df.empty:
            print(f"✅ 크롤링 된 개찰 공고: {len(new_bid_df)}")
            break
        else:
            print("❌ 새로 추가된 데이터가 없습니다")
            return

    # 키워드 폴더 접근
    file_prefix = search_word.replace(" ", "_")
    data_dir = os.path.join('data', file_prefix)

    try:
        # 신규 및 업데이트 된 파일 전처리(낙찰률 및 보조지표 계산)
        bid_file = os.path.join(data_dir, f'{file_prefix}_개찰결과_result.csv')
        update_result = pd.read_csv(bid_file)
        new_winrate = calcul_winrate(update_result)
        filtered_df = filtering_underone(new_winrate)
        ranked_df = rankclass(filtered_df)

        # 신규 업체 신상정보 업데이트
        keplergl_file = os.path.join(data_dir, f'{file_prefix}_keplergl_df.csv')
        old_col = ['업체명', '사업자등록번호', '참여횟수', '낙찰횟수', '낙찰률(%)', '가중 낙찰률', '가중낙찰률 클래스', 'rank_class', '주소', '사업형태', '위도', '경도', '전화번호']
        old_df = pd.read_csv(keplergl_file) if os.path.exists(keplergl_file) else pd.DataFrame(columns=old_col)
        
        new_company = filtering_data(old_df, ranked_df)
        newcompany_info = get_companyinfo(new_company)
        final_df = get_final_df(ranked_df, old_df, newcompany_info)
        final_df.to_csv(keplergl_file, index=False, encoding='utf-8-sig')
        print(f"✅ 신규 업체 정보가 업데이트되었습니다: {len(new_company)} 개")
        print(tabulate(new_company, headers='keys', tablefmt='grid'))

        # 시군구 매칭 및 경계 데이터 처리
        polygon_file = os.path.join('data', 'polygon.csv')
        polygon_df = pd.read_csv(polygon_file)
        polygon_df = polygon_df[['시군구코드명', 'geometry']]

        city_df = matching_boundary(polygon_df, final_df)
        cityrank_df = calcul_area(city_df)
        sigunguboundary_df = area_merge(cityrank_df, polygon_df)

        # 결과 저장 함수 호출
        save_analysis_result(sigunguboundary_df, data_dir, file_prefix)

    except Exception as e:
        print(f"⚠️ 오류 발생: {e}")
        
        # selected_mode가 0인 경우 폴더 삭제 처리
        if selected_mode == 0:
            try:
                shutil.rmtree(data_dir)
                # print(f"⚠️ 오류로 인해 {data_dir} 디렉토리를 삭제하였습니다.")
            except OSError as error:
                print(f"⚠️ 디렉토리 삭제 중 오류가 발생하였습니다: {error}")

if __name__ == "__main__":
    main()
    os.system('pause') # 콘솔창 자동꺼짐 방지