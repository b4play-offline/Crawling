import pandas as pd
import datetime
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

import time

def get_meta_rev(app_dict:list, stop_time:int = 0) ->pd.DataFrame:
    '''
    메타크리틱 '유저' 리뷰 수집 코드
    
    Args: 
        app_dict: 게임 id & 이름 딕셔너리
        stop_time: 리뷰의 수집 일자를 제한하는 Timestamp 형식의 시간입니다.
        
    Return: 
        result: 메타크리틱 리뷰 데이터프레임    
            ===================================================
            appid: 게임 id
            name: 게임 이름
            user: 유저 닉네임
            date: 리뷰 작성 날짜
            review_body: 리뷰 본문
            metascore_w: 평점(0~10)
            total_ups:  좋아요 수
            total_thumbs: 평가받은 수(total_ups를 빼면 싫어요 수)
            ====================================================
            
    문제: 유저 리뷰만 수집 중, 언어 필터가 없음, pc 리뷰만 수집 중, 에러 대처 안했음
    특징: 리뷰를 쓰러 온 유저만 있음(뻘글x), 게임을 해봐야 리뷰를 적을 수 있는 건 아님
    '''
    
    result = pd.DataFrame()
    for app in app_dict.values():
        result_app = pd.DataFrame()
        app_url = app.lower().replace(' ', '-') #요구 url에 맞게 변형
        i=0
        esc = 0
        
        while not esc:
            url = f"https://www.metacritic.com/game/pc/{app_url}/user-reviews?page={i}" #콘솔도 고려?   
            result_page = get_meta_page(url) #모든 페이지 탐색 후 이탈
            
            if not result_page:esc=1
            
            result_page = pd.DataFrame(result_page)
            if result_page[-1]["date"]<stop_time:
                result_page = result_page[result_page.date>=stop_time]
                esc=1
            
            result_app = pd.concat([result_app, result_page])
            i+=1
            
            
            if i>1:break #임시 코드
        
        result_app = pd.DataFrame(result_app)
        result_app.insert(0, "name", app)
        result_app.insert(0, "appid", app_dict[app]) #임시 - 돌아가면 좋겠다..
        result = pd.concat([result, result_app])
    return result

def get_meta_page(url:str)->pd.DataFrame: 
    '''
    메타크리틱 한 페이지당 수집 코드
    '''
    opts = webdriver.ChromeOptions()
    opts.add_argument('headless')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    
    class_list = ["name",
          "date",
          "blurb_expanded",
          "metascore_w",
          "total_ups",
          "total_thumbs",]
    
    for rt in range(1,10): #page is unstable, needs to retry code
        try:
            driver.get(url)
            driver.implicitly_wait(10)

            revs = driver.find_element(by = By.CLASS_NAME, value="product_reviews") #리뷰 섹션 탐색
        except:
            time.sleep(rt*rt)
            if rt==9:print("last retry")
        
    endchk = revs.find_element(by = By.CLASS_NAME, value="review_top_l").text #페이지 유효 체크
    if endchk[:25] == "There are no user reviews":return False
        
    revs = revs.find_elements(by = By.CLASS_NAME, value="review_top_l") #이후 리뷰데이터 수집

    result = [] 
    
    for rev in revs:
        rev_data = {}
        for cls in class_list:
            rev_data[cls]=rev.find_element(by=By.CLASS_NAME, value=cls).text
        try:
            rev_data["user"] = rev_data.pop("name")
            rev_data["date"] = int(datetime.datetime.strptime(rev_data["date"], "%b %d, %Y").timestamp())
        except:
            print("clmconv failed")
        result+=[rev_data]
        driver.close()
        
    return result
    
    