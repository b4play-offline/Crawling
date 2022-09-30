import pandas as pd
import requests
import json
import datetime
from bs4 import BeautifulSoup
#import time

#Steam store game infos by Steamspy API
#Check param infos in https://steamspy.com/api.php 

def get_recent_revdate(id:int)->int:
    '''
    return a timestamp minus the date of 20th review written from the present
    '''
    
    url = f"https://store.steampowered.com/appreviews/{id}?json=1"
    params = {"filter":"recent",
          "language":"english",
          "cursor":"*",
          "review_type":"all",
          "purchase_type":"all",
          "num_per_page":"20" # 100?
        }
    res = requests.get(url, params=params)
    res.encoding = 'utf-8-sig'
    res = json.loads(res.text) 
    
    diff = datetime.datetime.now().timestamp() - res["reviews"][-1]['timestamp_created']
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
            return False, app
        
    
    #eng-review counts(if needs)
    counts_rv_eng = soup.find_all(class_="user_reviews_count")[-1].text
    for c in ["(", ")", ","]:
        counts_rv_eng = counts_rv_eng.replace(c,"")
        
    app["counts_rv_eng"] = int(counts_rv_eng)
    if app["counts_rv_eng"]<2500:
        app["why_banned"] = "Lack of eng reviews"
        return False, app
    
    #get peak players in steamcharts.com
    url = f"https://steamcharts.com/app/{app['appid']}/"
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    
    try:
        app["all_time_peak"] = int(soup.find_all("span", class_="num")[2].text)
    except: #may net error but usually region lock
        return False, app
    
    #f2p multiplayer game/low 24 peeks may means that game is no longer to can play nomally 
    app["24h_peak"] = int(soup.find_all("span", class_="num")[1].text)
    if app["24h_peak"]<30:
        app["why_banned"] = "Too low players"
        return False, app
    elif "Free to Play" in app["tags"] and "Multiplayer" in app["tags"] and app["24h_peak"]<300:
        app["why_banned"] = "PvP no longer works"
        return False, app
    
    #stats of constantly played: is there any recent reviews?
    if get_recent_revdate(app["appid"]) > 2592000:
        app["why_banned"] = "Recent review too old"
        return False, app
    
    return True, app

    

def get_appinfo(params:dict)->pd.DataFrame:  
    
    #게임 보유자순 상위 1000개의 게임의 세부정보를 반환합니다.
    #게임이 아닌 소프트웨어는 자동으로 순위에서 배제됩니다.
    
    app_dict = []
    app_df = []
    passed_df = []
    blocked = []
    blocked=0
    
    try:
        blacklist_df = pd.read_csv("games_list_passed.csv")
        blacklist = blacklist_df["appid"]
    except:
        print("info: Blacklist not found")
        
    #get Ranking by owner top 2000 games
    for i in range(2):
        params["page"]=f'{i}'
        res = requests.get("https://steamspy.com/api.php", params).text
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