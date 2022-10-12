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
    '''
    Setting log files.
    '''
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

def get_gamelist(renew = True): 
    '''
    Returns list of Steam games top owner 1000.
    In:
        renew: if True, Returns updated game list.
    Out:
        gamelist_old: dict of pre-crawled games' id and name.
        gamelist_new: dict of newly chart-in games' id and name. 
    '''
    logger = logging.getLogger("all_file")
    printer = logging.getLogger("all_console")
    
    if renew=="keep":
        gamelist = pd.read_csv("games_list.csv")
        gamelist_old = list(gamelist.appid.values)
        gamelist_new = [] 
    else:
        gamelist, gamelist_passed = get_appinfo()
        
        if not gamelist:
            logger.critical("Game list loading failed")
            printer.critical("Can't creating game list, try after 12 PM.")
            return 0
        
        if renew=="new":
            gamelist_new = list(gamelist.appid.values)
            gamelist_old = []
        elif renew=="renew":
            pre_li = pd.read_csv("games_list.csv").appid.values
            gamelist_new = [app for app in gamelist.appid.values if app not in pre_li]
            gamelist_old = [app for app in gamelist.appid.values if app not in gamelist_new]
        else:
            logger.warning("Got unexpected arg in this func. return empty list")
            gamelist_new = []
            gamelist_old = []
        
        gamelist.to_csv("games_list.csv", index=False)
        gamelist_passed.to_csv("games_list_passed.csv", index=False)    
        
    return get_game_dict(gamelist_old, gamelist), get_game_dict(gamelist_new, gamelist)
    
def get_game_dict(game_li, game_df):
    '''
    make dict of game id and name.
    In:
        game_li: list of game id.
        game_df: dict of original game list.
    Out:
        game_dict: id as key, name as value
    '''
    game_dict = {}
    for k in game_li:
        game_dict[k] = game_df[game_df["appid"]==k]["name"].values[0]
    return game_dict

def get_last_date(log_list):
    '''
    return recent Crawled date refer to log.
    In:
        log_list: list of file names in log dir.
    Out:
        last_date: tiemstamp of last Crawling code activated.
    '''
    logger = logging.getLogger("all_file")
    printer = logging.getLogger("all_console")
    last_date = 0
    for i in range(len(log_list)-1,-1,-1):
        try:
            last_date = datetime.strptime(log_list[i], "%y-%m-%d_Steamlog").timestamp()
            break
        except: pass
    if last_date:
        logger.info(f"Crawling starts at {last_date}")
    else: 
        logger.warning("last log not found: crawling entire date")
    return last_date


def main():
    set_logger()
    if os.path.isfile("./games_list.csv"):
        if input("renew game list?(y/n): ")=="y":renew="renew"
        else:renew="keep"
    else:
        print("First launch detected. making game list...")
        renew="new"        
    gamedict, newgamedict = get_gamelist(renew)
    
    logs = os.listdir("./log")
    
    
    get_steam_rev(newgamedict, 0)
    get_steam_rev(gamedict, get_last_date(logs)) 
    

    #3, save new reviews as file
    #4, combind...how?
    #4-1, make 90-day cashe to update like count
    
    #5, calc scraping amount
    #5, ping server to scrap
    #5, get finish massege from server
    
    #6, combind in local
    
    
     
    #try:
    #    with open ("./splited_gamelist.json","r")as f:
    #        game_dict = json.load(f)
    #        game_dict = json.loads(game_dict)[3]
    #        game_dict = dict(list(game_dict.items())[20:27])
    #except FileNotFoundError:
    #    pass

    
    
    #each_gamelist = split_applist(game_df["appid"])
    
if __name__=="__main__":
    main()

# 게임 리스트의 생성
# 블랙리스트의 생성
# 패킷 생성
# 갱신 여부(자동 병합?)
