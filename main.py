import pandas as pd
import json
from steam_crawler import get_steam_rev
from steam_crawler import split_applist
import meta_crawler
from app_crawler import get_appinfo
import os

def main():
    print()
    
    
    if os.path.isfile("./games_list.csv"):
        pd.read_csv("games_list.csv")
    else:
        get_appinfo()
        
    
     
    try:
        with open ("./splited_game_list.json","r")as f:
            game_dict = json.load(f)
            game_dict = json.loads(game_dict)[3]
            game_dict = dict(list(game_dict.items())[20:27])
    except FileNotFoundError:
        pass

    
    get_steam_rev(game_dict,0, "test")
    #each_game_list = split_applist(game_df["appid"])
    
if __name__=="__main__":
    main()

# 게임 리스트의 생성
# 블랙리스트의 생성
# 패킷 생성
# 갱신 여부(자동 병합?)
