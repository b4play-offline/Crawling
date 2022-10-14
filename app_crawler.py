import pandas as pd
import requests
import json
import datetime
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import logging

#Steam store game infos by Steamspy API
#Check param infos in https://steamspy.com/api.php 

def get_recent_revdate(id:int)->int:
    '''
    return a timestamp minus the date of 20th review written from the present
    '''

    url = f"https://store.steampowered.com/appreviews/{id}?json=1"
    opts = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

    driver.get(f"https://store.steampowered.com/app/{282070}")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    driver.implicitly_wait(10)
    m = driver.find_element(by=By.ID, value="user_reviews_offtopic_activity_menu")
    ActionChains(driver, 3).move_to_element(m).perform()
    driver.find_element(by=By.ID, value="reviews_offtopic_activity_checkbox").click()
    date = driver.find_element(by = By.CLASS_NAME, value="shortcol").find_element(by = By.CLASS_NAME, value="postedDate").text

    try:
        date = datetime.strptime(date, "POSTED: %d %B, %Y")
    except ValueError:
        date = datetime.strptime(date+f", {datetime.now().year}", "POSTED: %d %B, %Y") 
    except:
        1/0
    diff = datetime.datetime.now().timestamp() - date.timestamp()    
    return diff


def is_valid(app:dict):
    '''
    Validation that games are analysis worthy.    
    Checking list: Amount of English reviews, 24H player peak, Recent review date

    Input:
        app: dict of game data received by steamspy API.

    Output:
        bool: result of validation. True is Okay.
        app: dict of game data inclued Evaluation standard
    '''

    #get tags in steam store page
    url = f"https://store.steampowered.com/app/{app['appid']}/?l=english"
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    app["tags"] = [r.text.strip() for r in soup.find_all(class_='app_tag')][:-1]
    if "Software" in app["tags"]:
            app["why_banned"] = "Not game"
            return True, app

    #eng-review counts(if needs)
    counts_rv_eng = soup.find_all(class_="user_reviews_count")[-1].text
    for c in ["(", ")", ","]:
        counts_rv_eng = counts_rv_eng.replace(c,"")

    app["counts_rv_eng"] = int(counts_rv_eng)
    if app["counts_rv_eng"]<2500:
        app["why_banned"] = "Lack of eng reviews"
        return True, app

    #get peak players in steamcharts.com
    url = f"https://steamcharts.com/app/{app['appid']}/"
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    try:
        app["all_time_peak"] = int(soup.find_all("span", class_="num")[2].text)
    except: #may net error but usually region lock
        app["why_banned"] = "(Maybe)Region lock"
        return True, app
    
    #f2p multiplayer game/low 24 peeks may means that game is no longer to can play nomally 
    app["24h_peak"] = int(soup.find_all("span", class_="num")[1].text)
    if app["24h_peak"]<30:
        app["why_banned"] = "Too low players"
        return True, app
    elif "Free to Play" in app["tags"] and "Multiplayer" in app["tags"] and app["24h_peak"]<300:
        app["why_banned"] = "PvP no longer works"
        return True, app
    
    ## issue: hidden reviews by Steam not appares in recent reviews
    
    #stats of constantly played: is there any recent reviews? 
    if get_recent_revdate(app["appid"]) > 2592000:
        app["why_banned"] = "Recent review too old"
        return True, app

    return False, app

    

def get_appinfo(params:dict = {"request":"all"})->pd.DataFrame:  
    
    #게임 보유자순 상위 1000개의 게임의 세부정보를 반환합니다.
    #게임이 아닌 소프트웨어는 자동으로 순위에서 배제됩니다.
    logger = logging.getLogger("all_file")
    
    app_dict = []
    app_df = []
    passed_df = []
    blocked = []
    blacklist=[]
    blocked=0
    
    try:
        blacklist_df = pd.read_csv("games_list_passed.csv")
        blacklist = blacklist_df["appid"].to_list()
    except FileNotFoundError:
        logger.info("Blacklist not found, making new blacklist")
        
    #get Ranking by owner top 2000 games
    for i in range(2):
        params["page"]=f'{i}'
        res = requests.get("https://steamspy.com/api.php", params).text
        if res=="":
            logger.error("Steamspy API not working: try after 12PM")
            return 0
        res = json.loads(res)
        app_dict += res.values()
        
    #validation that games are analysis worthy
    for app in app_dict:
        ispass=0
        
        if app in blacklist:
            blocked+=1
            continue        
        
        ispass, app = is_valid(app)
        
        #make blacklist
        if ispass:passed_df+=[app]
        else: 
            app_df+=[app]
            
            if len(app_df)%200==0:
                print(len(app_df)) #progress check
        
        if len(app_df)>999:break
        
    app_df = pd.DataFrame(app_df)
    passed_df = pd.DataFrame(passed_df)
    return app_df, passed_df



#test code  
if __name__=="__main__":
    app_list, passed_list = get_appinfo({"request":"all"})
    app_list.to_csv("games_list.csv", index=False)
    passed_list.to_csv("games_list_passed.csv", index=False)
    
    
    
#1, 반복 많이 되면 
#2, 읽기 힘들게 길어지면
#3, 서로 다른 작업을 한다면