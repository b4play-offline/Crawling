import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDrivermanager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

import time

#크롤링 코드 작성
#크롤링 코드 안정화
#크롤링해올 내용 수정
#크롤링한 자료별 차트 수치화/시각화
#자동화 코드 
#아오

def get_meta_rev(appname_list:list)->pd.DataFrame:
    '''
    메타크리틱 '유저' 리뷰 수집 코드
    
    Args: 
        appname_list : 게임 이름 리스트
    Return: 
        result: 메타크리틱 리뷰 데이터프레임    
            ===================================================
            name: 게임 이름
            user: 유저 닉네임
            date: 리뷰 작성 날짜
            review_body: 리뷰 본문
            metascore_w: 평점(0~10)
            total_ups:  좋아요 수
            total_thumbs: 평가받은 수(total_ups를 빼면 싫어요 수)
            ====================================================
            
    문제: 유저 리뷰만 수집 중, 언어 필터가 없음, pc 리뷰만 수집 중
    특징: 리뷰를 쓰러 온 유저만 있음(뻘글x), 게임을 해봐야 리뷰를 적을 수 있는 건 아님
    '''
    result = pd.DataFrame()
    for app in appname_list:
        result_app = pd.DataFrame()
        app_url = app.lower().replace(' ', '-') #요구 url에 맞게 변형
        i=0
        
        while True:
            url = f"https://www.metacritic.com/game/pc/{app_url}/user-reviews?page={i}" #콘솔도 고려?
            
            result_page = get_meta_page(url) #모든 페이지 탐색 후 이탈
            if not result_page:break
            result_page = pd.DataFrame(result_page)
            result_app = pd.concat([result_app, result_page])
            i+=1
            if i>1:break #임시 코드
        
        result_app = pd.DataFrame(result_app)
        result_app.insert(0, "name", app)
        result = pd.concat([result, result_app])
    return result

def get_meta_page(url:str)->pd.DataFrame: 
    '''
    메타크리틱 한 페이지당 수집 코드
    '''
    
    driver = webdriver.Chrome(service=Service(ChromeDrivermanager().install()))
    
    class_list = ["name",
          "date",
          "review_body",
          "metascore_w",
          "total_ups",
          "total_thumbs",]
    
    driver.get(url)
    driver.implicitly_wait(10)
    
    revs = driver.find_element(by = By.CLASS_NAME, value="product_reviews") #리뷰 섹션 탐색
    
    endchk = revs.find_element(by = By.CLASS_NAME, value="review_top_l").text #페이지 유효 체크
    if endchk[:25] == "There are no user reviews":return False
    
    btns = driver.find_elements(by=By.CLASS_NAME, value="toggle_expand") #리뷰 확장버튼 클릭
    for btn in btns:
        btn.click()
        
    revs = revs.find_elements(by = By.CLASS_NAME, value="review_top_l") #이후 리뷰데이터 수집

    result = [] 
    
    for rev in revs:
        rev_data = {}
        for cls in class_list:
            if cls =='name':
                rev_data["user"]=rev.find_element(by=By.CLASS_NAME, value=cls).text #column 이름 중복 피하기: 비효율적일 수도
            else:
                rev_data[cls]=rev.find_element(by=By.CLASS_NAME, value=cls).text
        result+=[rev_data]
        
    return result