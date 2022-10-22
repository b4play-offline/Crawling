import pandas as pd
import json
import localfuncs as lf
import sshfuncs 
from steam_crawler import get_steam_rev
from steam_crawler import split_applist
from app_crawler import get_appinfo
import os
import logging
import threading

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
        
    return lf.get_game_dict(gamelist_old, gamelist), lf.get_game_dict(gamelist_new, gamelist)

class Crawlmanager():
    def __init__(self):
        pass

def main():
    lf.set_logger()
    logger = logging.getLogger("all_file")
    printer = logging.getLogger("all_console")
    
    logs = os.listdir("./log")
    lastdate = lf.get_last_date(logs)
    
    '''
    #checking launch mode
    if os.path.isfile("./games_list.csv"):
        if input("renew game list?(y/n): ")=="y":renew="renew"
        else:renew="keep"
    else:
        printer.info("First launch detected. making game list...")
        renew="new"       
        setup = input("Install requirements before scraping?(y/n)")
    gamedict, newgamedict = get_gamelist(renew)
    '''
    #test
    renew="new"
    gamedict, newgamedict = get_gamelist("keep")
    newgamedict = dict(list(gamedict.items())[-3:])
    setup="n"
    
    if os.path.isfile("./Steamrev_temp.csv"):os.remove("./Steamrev_temp.csv") #reset
    
    hosts = ['43.201.27.201']
    username = "ec2-user"
    keypath='C:/Users/Ion/.ssh/team19-key.pem'  
    pw = ''
    trd_list = []
    
    if renew=="new":
        newgamedict = split_applist(newgamedict)
        if len(newgamedict)>len(hosts):
            logger.error("Host not enough to crawl. Excluded games saved as excluded.json")
            with open("excluded.json","w", encoding="utf-8")as f:
                json.dump(newgamedict[len(hosts):],f,ensure_ascii=False)
            newgamedict = newgamedict[:len(hosts)]
            
        #crawling
        for i in range(len(newgamedict)):
            host = sshfuncs.Sshclass(hosts[i], username, pw, keypath, i)   
            if setup=="y":
                host.first_setup()
            trd = threading.Thread(target=sshfuncs.run, args=(host, [newgamedict[i],0]))
            trd.start()
            trd_list.append(trd)
        for t in trd_list:
            t.join()
            
        #combind separeted datas
        filename = ["Steamrev_temp", "Steamrev_base", "Steamrev_summary"]
        for i in range(len(newgamedict)):
            for j in range(len(filename)):
                lf.merge(f"{filename[j]}",f"./sshcache/{filename[j]}_{i}", ".csv") #datas
            lf.merge(f"{os.listdir('./log')[-1]}",f"{os.listdir('./log')[-1]}_{i}","") #log
        
        for f in os.listdir("./sshcache"):
            os.remove(f)
            
    else:
        get_steam_rev(newgamedict, 0) 
        get_steam_rev(gamedict, lastdate) #갱신 오래됐을 경우?


    #Todo 
    #1, 어느 서버를 쓸 것인지 설정   
    #3, 다중 크롤링 요청. 
    # 오류 전달받기
    #5, 완성본으로 합치고, 완료 메시지/캐시 삭제
    
    #모든 단계에서 오류 체크
     
if __name__=="__main__":
    main()


