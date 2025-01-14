'''
나라장터 개찰 데이터 크롤러 파일
- get_max_page : 나라장터 페이지 내비게이션에서 최대 페이지 번호를 찾는 함수
- nara_crawler : 나라장터에서 검색 키워드와 날짜 범위에 따른 입찰 공고 번호를 크롤링하는 함수
- update_existing_data : 기존 데이터와 새로운 데이터를 비교하여 업데이트하는 함수
- save_to_csv : 업데이트된 개찰 데이터를 CSV 파일로 저장하는 함수

- process_bids : 입찰공고 번호를 기반으로 개찰 결과를 크롤링하는 함수
- check_and_select_mode : 키워드 검색 모드를 선택하고 기존 데이터의 유무를 확인하는 함수
- get_most_date : 파일에서 가장 최근 또는 가장 오래된 개찰 일시를 가져오는 함수
- update_mode : 설정된 모드에 따라 새로운 데이터를 추가하거나 기존 데이터를 업데이트하는 함수

# 유효성 검사 함수
- validate_keyword_input : 입력된 키워드의 유효성을 검사하는 함수
- validate_date_format : 입력된 날짜가 올바른 형식(YYYYMMDD)인지 확인하는 함수
- validate_date_range : 시작 날짜가 종료 날짜보다 나중인지 확인하는 함수
'''

import os
import sys
import time
import random
import re
import urllib.parse
import pandas as pd
import requests
from datetime import datetime
import errno
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.common.by import By
from tqdm import tqdm
from selenium.common.exceptions import NoSuchElementException


### 페이지 네비게이션 영역에서 최대 페이지 번호를 찾는 함수
def get_max_page(driver):
    try:
        # 페이지 네비게이션의 페이지 번호 리스트를 찾음
        page_elements = driver.find_elements(By.CSS_SELECTOR, ".pagination .page a")

        try:
            # 마지막 페이지 번호 확인 (페이지 네비게이션 마지막 부분에 있는 'page_last' 클래스를 기준으로 최대 페이지 구함)
            last_page_element = driver.find_element(By.CSS_SELECTOR, ".pagination .page_last")
            max_page = int(last_page_element.get_attribute("href").split("'")[1])
            return max_page
        except NoSuchElementException:
            # 'page_last' 요소가 없는 경우 page_elements의 길이만큼 페이지를 반환
            return len(page_elements)

    except Exception as e:
        print(f"페이지 탐색 중 오류 발생: {e}")
        return 1


### 나라장터에서 검색 결과(개찰 공고) 확인 후 입찰공고번호 크롤링하는 함수
def nara_crawler(search_word, start_date, end_date):
    bidno = [] # 식별번호 저장 리스트
    search_query = search_word
    file_prefix = search_word.replace(" ", "_")
    data_dir = os.path.join('data', file_prefix)
    existing_list_file_path = os.path.join(data_dir, f'{file_prefix}_개찰결과_목록.csv')

    # 데이터 디렉토리 존재 여부 확인 및 생성
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # 기존 데이터에서 입찰공고번호 불러오기 (기존 파일이 있는지 확인)
    if not os.path.exists(existing_list_file_path):
        # print(f"'{existing_list_file_path}' 파일이 없습니다. 새로운 데이터를 수집합니다.")
        existing_bidno = []
    else:
        try:
            existing_list_df = pd.read_csv(existing_list_file_path)
            # 기존 입찰공고번호 리스트에서 '-00'을 제거한 번호로 저장
            existing_bidno = existing_list_df['입찰공고번호'].apply(lambda x: x.split('-')[0]).tolist()
        except Exception as e:
            print(f"기존 데이터를 불러오는 중 오류 발생: {e}")
            raise

    euc_kr_encoded = search_query.encode('euc-kr') # 문자열을 EUC-KR로 인코딩
    query = urllib.parse.quote(euc_kr_encoded) # URL 인코딩

    #헤더 변경으로 크롤링 차단 우회
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "TE": "Trailers",
        "DNT": "1",
    }
    url = 'https://www.g2b.go.kr:8340/search.do?kwd=' + query + f'&category=GC&subCategory=ALL&detailSearch=true&reSrchFlag=false&pageNum=1&sort=ODD&srchFd=ALL&date=&startDate={start_date}&endDate={end_date}'
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(3)

    # 팝업창이 뜨면 닫기
    main = driver.window_handles
    for i in main:
        if i != main[0]:
            driver.switch_to.window(i)
            driver.close()

    # 개찰결과 검색 결과가 0건인 경우 예외처리
    result_element = driver.find_element(By.CSS_SELECTOR, 'h3.tit')
    if result_element:
        result_text = result_element.text
        # 검색 결과 개수 추출
        match = re.search(r'\((\d+)건\)', result_text)
        if match:
            result_count = int(match.group(1))
            if result_count == 0:
                print("⚠️검색 결과가 없습니다.")
                driver.quit()  # 드라이버 종료
                return None
    else:
        print("⚠️검색 결과 개수를 확인할 수 없습니다.")
        driver.quit()  # 드라이버 종료
        return None


    # 실제 검색 결과의 최대 페이지 수 탐색
    max_page = get_max_page(driver)

    for i in tqdm(range(1,max_page+1), desc="페이지 크롤링 진행"):

        url = 'https://www.g2b.go.kr:8340/search.do?kwd=' + query + f'&category=GC&subCategory=ALL&detailSearch=true&reSrchFlag=false&pageNum={i}&sort=ODD&srchFd=ALL&date=&startDate={start_date}&endDate={end_date}'
        driver.get(url)
        time.sleep(random.randint(2, 3))

        # 팝업창이 뜨면 닫기
        main = driver.window_handles
        for i in main:
            if i != main[0]:
                driver.switch_to.window(i)
                driver.close()

        # 한 페이지의 결과 리스트
        ul_element = driver.find_element(By.XPATH, '//*[@id="contents"]/div[1]/ul')
        pattern = re.compile(r'\[([^\]]+)\]')

        # 리스트에서 각각의 결과 요소 추출
        li_elements = ul_element.find_elements(By.CSS_SELECTOR, 'ul.search_list > li')

        for li in li_elements:
            text = li.text
            match = pattern.search(text) # findall -> search 변경
            split_values = match.group(1).split('-')
            if len(split_values) == 2:
                value1, value2 = split_values
                bidno.append(value1.strip())
            else:
                pass

    # 중복되는 입찰공고번호 제거 및 정렬 (기존 데이터와 중복되는 공고 제거)
    unique_bidno = [int(b) for b in bidno if b not in existing_bidno]
    # unique_bidno_sorted = sorted(unique_bidno, reverse=True)

    # 새로운 입찰공고가 없을 경우 None 반환
    if not unique_bidno:
        return None

    driver.quit()  # 드라이버 종료

    return unique_bidno


### 기존 데이터와 새로운 데이터를 비교하여 업데이트하는 함수
def update_existing_data(existing_bid_df, existing_result_df, new_bid_df, new_result_df, latest_mode=0):
    try:
        # 최신화 모드: 새로운 데이터를 기존 데이터 앞에 추가
        if latest_mode == 1:
            # 기존 데이터의 인덱스를 새로 추가된 데이터 수만큼 밀어서 할당
            existing_bid_df['Index'] = existing_bid_df['Index'] + len(new_bid_df)
            existing_result_df['Index'] = existing_result_df['Index'] + len(new_bid_df)

            updated_bid_df = pd.concat([new_bid_df, existing_bid_df], ignore_index=True)
            updated_result_df = pd.concat([new_result_df, existing_result_df], ignore_index=True)

        # 과거 추가 모드: 새로운 데이터를 기존 데이터 뒤에 추가
        elif latest_mode == 2:
            last_index_bid = existing_bid_df['Index'].max() if not existing_bid_df.empty else -1 # 기존 데이터의 마지막 인덱스 찾기
             # 새로운 데이터의 인덱스를 기존 데이터 마지막 인덱스부터 시작하도록 조정
            new_bid_df['Index'] += (last_index_bid + 1)
            new_result_df['Index'] += (last_index_bid + 1)

            updated_bid_df = pd.concat([existing_bid_df, new_bid_df], ignore_index=True)
            updated_result_df = pd.concat([existing_result_df, new_result_df], ignore_index=True)

        return updated_bid_df, updated_result_df
    
    except Exception as e:
        print(f"데이터 업데이트 중 오류 발생: {e}")
        return existing_bid_df, existing_result_df


### CSV 저장 함수 (신규 생성 또는 업데이트 모두 처리)
def save_to_csv(data_dir, file_prefix, new_bid_df, new_result_df, latest_mode=0):
    bid_file = os.path.join(data_dir, f'{file_prefix}_개찰결과_목록.csv')
    result_file = os.path.join(data_dir, f'{file_prefix}_개찰결과_result.csv')

    # 데이터 디렉토리 존재 여부 확인 및 생성
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    try:
        # 기존 파일이 있는지 확인하여 업데이트 또는 새로 저장
        if os.path.exists(bid_file) and os.path.exists(result_file): # 기존 파일을 읽어서 업데이트
            existing_bid_df = pd.read_csv(bid_file)
            existing_result_df = pd.read_csv(result_file)
            updated_bid_df, updated_result_df = update_existing_data(existing_bid_df, existing_result_df, new_bid_df, new_result_df, latest_mode)
        else: # 파일이 없으면 새로운 데이터로 저장
            updated_bid_df, updated_result_df = new_bid_df, new_result_df

        # 업데이트된 데이터를 CSV 파일에 저장
        updated_bid_df.to_csv(bid_file, index=False, encoding='utf-8-sig')
        updated_result_df.to_csv(result_file, index=False, encoding='utf-8-sig')
    
    except PermissionError as e:
        if e.errno == errno.EACCES:
            print(f"⚠️파일 접근 권한 오류: {e.filename}에 접근할 수 없습니다.")
            print("⚠️다른 프로그램에서 파일을 열고 있는지 확인한 후 다시 시도하세요.")
        else:
            print(f"⚠️파일 저장 중 오류 발생: {e}")
    
    except Exception as e:
        print(f"⚠️파일 저장 중 오류 발생: {e}")


### 입찰공고 번호 조회하여 개찰 결과를 크롤링하는 함수
def process_bids(bidno, search_word, latest_mode=0):
    bid_list = []
    result_list = []
    pass_list = []  # 유찰된 데이터 개수 확인용

    for index, bid in tqdm(enumerate(bidno), desc="개찰 결과 크롤링 진행", total=len(bidno)):
        detail_url = f'https://www.g2b.go.kr:8101/ep/result/serviceBidResultDtl.do?bidno={bid}&bidseq=00&whereAreYouFrom=piser'  # 개찰결과 상세조회 url
        r = requests.get(detail_url)
        soup = bs(r.text, "html.parser")

        try:
            bid_number = soup.find('th', string="입찰공고번호").find_next('td').get_text(strip=True)
            bid_name = soup.find('th', string="공고명").find_next('td').get_text(strip=True)
            bid_where = soup.find('th', string="수요기관").find_next('td').get_text(strip=True)
            bid_who = soup.find('th', string="집행관").find_next('td').get_text(strip=True)
            bid_when = soup.find('th', string="실제개찰일시").find_next('td').get_text(strip=True)
            bid_list.append([index, bid_number, bid_name, bid_where, bid_who, bid_when])

            rows = soup.find_all('tr')  # 결과 테이블 데이터
            for row in rows:
                row_data = [cell.get_text(strip=True) for cell in row.find_all('td')]
                if len(row_data) >= 9:  # 개찰 순위 데이터만 추가
                    row_data.insert(0, index)  # 리스트 맨 앞에 인덱스 추가
                    result_list.append(row_data)

        except Exception as e:  # 투찰한 모든 업체가 낙찰하한선 미달일 경우 예외처리
            pass_list.append(bid)

    new_bid_df = pd.DataFrame(bid_list, columns=['Index', '입찰공고번호', '공고명', '수요기관', '집행관', '실제개찰일시'])
    new_result_df = pd.DataFrame(result_list, columns=['Index', '순위', '사업자등록번호', '업체명', '대표자명', '입찰금액', '투찰률(%)', '추첨번호', '투찰일시', '비고'])

    # 파일 저장 함수 호출 (latest_mode 전달)
    file_prefix = search_word.replace(" ", "_")
    data_dir = os.path.join('data', file_prefix)
    save_to_csv(data_dir, file_prefix, new_bid_df, new_result_df, latest_mode)

    return new_bid_df, pass_list, new_result_df


### 키워드 입력 검증 및 형식 확인 함수
def validate_keyword_input(keyword):
    # 키워드의 앞뒤 공백 제거
    keyword = keyword.strip()

    # 1. 키워드가 한 글자만 입력된 경우
    if len(keyword) < 2:
        print("키워드는 최소 두 글자 이상이어야 합니다.")
        return False

    # 2. 키워드가 공백만으로 이루어진 경우
    if not keyword:
        print("키워드는 공백만 입력할 수 없습니다.")
        return False

    # 3. 키워드 형식이 올바르지 않은 경우 (한글, 영문, 숫자, 공백만 허용)
    if not re.match(r"^[가-힣a-zA-Z0-9\s]+$", keyword):
        print("키워드는 한글, 영문, 숫자, 공백만 포함해야 합니다.")
        return False

    # 모든 조건을 통과하면 True 반환
    return True


### 키워드 폴더 존재 여부 확인 및 모드 선택 함수
def check_and_select_mode():
    while True:
        search_word = input(">>> 찾으시는 키워드를 입력하세요 (종료는 *): ").strip()

        if search_word == '*':
            print("\n프로그램을 종료합니다.")
            return None, None

        # 입력된 키워드에 대한 예외 처리
        if not validate_keyword_input(search_word):
            continue  # 키워드가 유효하지 않으면 다시 입력받음

        file_prefix = search_word.replace(" ", "_")
        data_dir = os.path.join('data', file_prefix)

        if os.path.exists(data_dir):
            # 최신 및 오래된 일시 둘 다 가져오기
            bid_file = os.path.join(data_dir, f'{file_prefix}_개찰결과_목록.csv')
            recent_date, oldest_date = get_most_date(bid_file, mode=999)
            print(f"\n'{data_dir}' 폴더가 존재합니다. 다음 메뉴에서 선택하세요:")
            print(f"해당 키워드는 현재 {oldest_date}부터 {recent_date}까지 수집되어있습니다.")
            print("1: 최신 데이터 추가")
            print("2: 기존 데이터의 과거 데이터 추가")
            print("*: 종료")
            print("#: 키워드 재입력")
            
            while True:
                menu_choice = input(">>> 메뉴를 선택하세요 (1, 2, *, #): ").strip()

                if menu_choice == '*':
                    print("\n프로그램을 종료합니다.")
                    return None, None
                elif menu_choice == '#':
                    break  # 재입력 시 다시 키워드 입력으로 돌아감
                elif menu_choice in ['1', '2']:
                    return search_word, int(menu_choice)
                else:
                    print("올바른 메뉴를 선택하세요.")
        else:
            # 폴더가 없으면 새로운 데이터 크롤링 여부 물어보기
            print(f"'{data_dir}' 폴더가 존재하지 않습니다.")
            user_input = input(f">>> '{search_word}'에 대한 데이터를 새로 크롤링하시겠습니까? (Y/N): ").strip().upper()

            if user_input in ['Y', 'y']:
                return search_word, 0  # 0번 모드로 실행
            else:
                print("프로그램을 종료합니다.")
                return None, None


### 파일을 읽어 가장 최근/오래된 개찰일시를 가져오는 함수
def get_most_date(file_name, mode):
    if os.path.exists(file_name):
        # 파일이 존재하면 데이터를 읽어 가장 최근/오래된 일시 확인
        df = pd.read_csv(file_name)
        df['실제개찰일시'] = pd.to_datetime(df['실제개찰일시'])  # 실제개찰일시를 datetime 타입으로 변환

        if mode == 1:
            # 최신 데이터 (가장 최근 일시)
            return df['실제개찰일시'].max().strftime('%Y%m%d')
        elif mode == 2:
            # 오래된 데이터 (가장 오래된 일시)
            return df['실제개찰일시'].min().strftime('%Y%m%d')
        elif mode == 999: #모드가 아님
            return df['실제개찰일시'].max().strftime('%Y%m%d'), df['실제개찰일시'].min().strftime('%Y%m%d')
    else:
        return None if mode in [1, 2] else (None, None)


### 날짜 형식 검증 함수
def validate_date_format(date_str):
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        print("⚠️ 잘못된 날짜 형식입니다. 'YYYYMMDD' 형식으로 입력해 주세요.")
        return False


### 날짜 범위 검증 함수 (start_date가 end_date보다 이전인지 확인)
def validate_date_range(start_date, end_date):
    if start_date > end_date:
        print("⚠️ 시작 날짜는 종료 날짜보다 나중일 수 없습니다.")
        return False
    return True


### 파일 업데이트 모드 설정
def update_mode(search_word, latest_mode=0):
    # 기본 경로 설정
    file_prefix = search_word.replace(" ", "_")
    data_dir = os.path.join('data', file_prefix)
    bid_file = os.path.join(data_dir, f'{file_prefix}_개찰결과_목록.csv')

    # 오늘 날짜 계산
    today = datetime.now().strftime('%Y%m%d')

    try:
        # 0번 모드: 새로운 키워드에 대한 데이터 크롤링
        if latest_mode == 0:
            print("새로운 키워드에 대한 데이터를 생성합니다.")
            start_date = input(">>> 시작 날짜를 입력하세요 (YYYYMMDD 형식): ").strip()
            end_date = input(">>> 종료 날짜를 입력하세요 (YYYYMMDD 형식): ").strip()

            # 날짜 형식과 범위 검증
            if not (validate_date_format(start_date) and validate_date_format(end_date)) or not validate_date_range(start_date, end_date):
                return None
            
            print(f"새로운 키워드 수집: {start_date}부터 {end_date}까지 데이터를 크롤링합니다.")
            bidno = nara_crawler(search_word, start_date, end_date)

            # 검색 결과가 없을 경우 폴더 삭제 및 None 반환
            if bidno is None:
                if os.path.exists(data_dir):
                    os.rmdir(data_dir)  # 폴더 삭제
                    print(f"⚠️검색 결과가 없습니다.")
                return None

        # 1번 모드: 가장 최근 데이터 이후부터 오늘까지 추가 크롤링
        elif latest_mode == 1:
            most_recent_date = get_most_date(bid_file, latest_mode)

            if not most_recent_date:
                print(f"'{bid_file}' 파일이 비어있거나 데이터에 문제가 있습니다.")
                return None
            # start_date = most_recent_date.strftime('%Y%m%d')
            print(f"최신 데이터 추가: {most_recent_date}부터 {today}까지 크롤링합니다.")
            bidno = nara_crawler(search_word, most_recent_date, today)

            # 검색 결과가 없을 경우 예외처리
            if bidno is None:
                print("⚠️이미 최신화 상태입니다.")
                return None
        
        # 2번 모드: 기존 데이터의 과거 데이터 추가 크롤링
        elif latest_mode == 2:
            most_old_date = get_most_date(bid_file, latest_mode)
            if not most_old_date:
                print(f"'{bid_file}' 파일이 비어있거나 데이터에 문제가 있습니다.")
                return None
            # end_date = most_old_date.strftime('%Y%m%d')
            start_date = input(">>> 추가할 데이터의 시작 날짜를 입력하세요 (YYYYMMDD 형식): ").strip()

            # 날짜 형식과 범위 검증
            if not validate_date_format(start_date) or not validate_date_range(start_date, most_old_date):
                return None

            print(f"기존보다 오래된 데이터 추가: {start_date}부터 {most_old_date}까지 크롤링합니다.")
            bidno = nara_crawler(search_word, start_date, most_old_date)

            # 검색 결과가 없을 경우 예외처리
            if bidno is None:
                print("⚠️해당하는 기간의 데이터가 없습니다.")
                return None

        else:
            print(f"'{latest_mode}'는 올바른 모드가 아닙니다.")
            return None
        
        # 새로운 데이터 처리 및 반환
        new_bid_df, pass_list, new_result_df = process_bids(bidno, search_word, latest_mode)
        return new_bid_df, pass_list, new_result_df
        
    except Exception as e:
        # 검색 결과가 없을 경우 폴더 삭제
        if latest_mode == 0:
            if os.path.exists(data_dir):
                os.rmdir(data_dir)  # 폴더 삭제
        print(f"오류 발생: {e}")
        return None