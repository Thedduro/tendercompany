'''
신규 업체 정보 크롤링 및 위경도 변환
- filtering_data : 기존 데이터와 비교하여 신규 업체 데이터를 필터링하는 함수
- get_api_info : 공공 API를 통해 업체의 사업자등록번호, 주소, 사업형태, 전화번호 등의 기본 정보를 가져오는 함수
- translocation : 주소를 위도와 경도로 변환하는 함수
- get_companyinfo : 신규 업체 리스트에서 기본 정보와 위경도 정보를 수집하여 데이터 프레임으로 반환하는 함수
'''

import json
import requests
import pandas as pd
from tqdm import tqdm
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup as bs
import time
import random
import re

### 기존 데이터와 새로운 데이터 비교 후 신규 업체 필터링
def filtering_data(old_df, new_df):
    return new_df[~new_df['사업자등록번호'].isin(old_df['사업자등록번호'])]

### 공공 API를 통해 업체의 기본 정보를 가져오는 함수
def get_api_info(number):
    company_info = []
   
    servicekey = 'YOUR_SERVICEKEY'
    url = f'http://apis.data.go.kr/1230000/UsrInfoService/getPrcrmntCorpBasicInfo?serviceKey={servicekey}&numOfRows=10&inqryDiv=3&bizno={number}&type=json&pageNo=1'
   
    response = requests.get(url)
    contents = response.text
 
    json_ob = json.loads(contents) # 문자열 JSON형태로 변경
    body = json_ob['response']['body']['items'][0] # 필요한 정보가 담겨있는 텍스트만 딕셔너리로
 
    bizno = body['bizno']
    corpBsnsDivNm = body['corpBsnsDivNm']
    telNo = body['telNo']
 
    # 주소 전처리
    address1 = body['adrs']
    address2 = body['dtlAdrs'].split(',') # 상세주소 제거
    address2= re.sub(r'\(.*?\)', '',  address2[0]).strip() #괄호가 있다면 안에 값까지 제거
 
    address = address1 + ' ' +address2
 
    company_info.append(bizno)
    company_info.append(address)
    company_info.append(corpBsnsDivNm)
    company_info.append(telNo)
 
    return company_info


### 주소 위경도 변환 함수
def translocation(address): 
    # Nominatim 객체 생성
    geo_local = Nominatim(user_agent='South Korea')
    
    # RateLimiter 사용
    geocode = RateLimiter(geo_local.geocode, min_delay_seconds=1)
    location = geocode(address)

    # 첫 번째 시도: 전체 주소로 위치 데이터 요청
    location = geocode(address)
    if location:
        return location.latitude, location.longitude

    # 두 번째 시도: 호수만 제거하고 위치 데이터 재요청
    simplified_address = re.sub(r'\s\d+층?\s?\d*호$', '', address)  # 예: '1120호' 또는 '11층 120호' 제거
    location = geocode(simplified_address)
    if location:
        return location.latitude, location.longitude
    
    # 위치를 찾을 수 없는 경우
    return None, None


### 신규 업체 정보 수집 함수
def get_companyinfo(new_company):
    companyinfo_list = []

    # if len(new_company) == 0:
    #     return pd.DataFrame(columns=['사업자등록번호', '주소', '사업형태', '전화번호','위도','경도'])

    for idx, row in tqdm(new_company.iterrows(), desc='신규 업체 정보 수집 진행', total=new_company.shape[0]):  # 새로운 업체 정보를 반복문으로 처리
        number = row['사업자등록번호']
        company_info = get_api_info(number)  # 크롤링 작동
        
        lat, lng = translocation(company_info[1])  # 위경도 반환 함수를 호출해 위도, 경도 반환 (input은 주소)
        company_info.append(lat)
        company_info.append(lng)
        companyinfo_list.append(company_info)

    newcompany_info = pd.DataFrame(companyinfo_list, columns=['사업자등록번호', '주소', '사업형태', '전화번호','위도','경도'])
   
    return newcompany_info
