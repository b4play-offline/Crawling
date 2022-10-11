import pandas as pd
import json
from steam_crawler import get_steam_rev
from steam_crawler import split_applist
import meta_crawler
from app_crawler import get_appinfo
import os
import logging
from datetime import datetime

def set_logger():
    logger = logging.getLogger("all_file")
    printer = logging.getLogger("all_console")
    
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(funcName)s:%(lineno)s] >> %(message)s')
    
    if not os.path.isdir("./log"):
        os.makedirs("./log")        
    filehandler = logging.FileHandler(f"./log/{datetime.now().strftime('%y-%m-%d')}_Steamlog")
    streamhandler = logging.StreamHandler()
    filehandler.setFormatter(formatter)

    logger.addHandler(filehandler)
    printer.addHandler(streamhandler)
    printer.propagate=False

    logger.setLevel("DEBUG")
    printer.setLevel("DEBUG")

def get_game_list(renew = False):
    logger = logging.getLogger("all_file")
    printer = logging.getLogger("all_console")
    
    if not renew:
        game_list = pd.read_csv("games_list.csv")
    else:
        game_list = get_appinfo()
        if not game_list:
            logger.critical("Game list loading failed")
            printer.critical("Can't creating game list, try after 12 PM.")
            return 0
    return game_list
    

def main():
    #mode = all or new
    #renew list = true for false
    
    #new-game check
    
    set_logger()
    logger = logging.getLogger("all_file")
    printer = logging.getLogger("all_console")
    
    if os.path.isfile("./games_list.csv"):
        pass
    #printer = logging.getLogger("all_console")
    renew = input("renew game list?(y/n): ")
    if renew=="y":renew=True
    else:renew=False
    
    game_list = get_game_list(renew)
    
    game_dict = {}
    for k in game_list["appid"].values:
        game_dict[k] = game_list[game_list["appid"]==k]["name"].values[0]
    
    #1, get new game
    #2, get recent date
    #3, save new reviews as file
    #4, combind...how?
    #4-1, make 90-day cashe to update like count
    
    #5, calc scraping amount
    #5, ping server to scrap
    #5, get finish massege from server
    
    #6, combind in local
    
    
    get_steam_rev(game_dict, 0) 
     
    #try:
    #    with open ("./splited_game_list.json","r")as f:
    #        game_dict = json.load(f)
    #        game_dict = json.loads(game_dict)[3]
    #        game_dict = dict(list(game_dict.items())[20:27])
    #except FileNotFoundError:
    #    pass

    
    
    #each_game_list = split_applist(game_df["appid"])
    
if __name__=="__main__":
    main()

# 게임 리스트의 생성
# 블랙리스트의 생성
# 패킷 생성
# 갱신 여부(자동 병합?)
