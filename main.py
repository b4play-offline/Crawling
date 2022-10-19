import pandas as pd
import json
from steam_crawler import get_steam_rev
from steam_crawler import split_applist
import meta_crawler
from app_crawler import get_appinfo
import os
import logging
from datetime import datetime
import paramiko

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

def push_to_server(data:list):
    with open("./query.json", "w", encoding="utf-8") as f:
        json.dump(data,f,ensure_ascii=False)
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('43.201.27.201', port='22', username='ec2-user', password='', key_filename='C:/Users/Ion/.ssh/team19-key.pem')

    sftp = ssh.open_sftp()
    sftp.put('./query.json', '/home/ec2-user/Crawling/query.json')
    
    ssh.exec_command('nohup python3 /home/ec2-user/Crawling/steam_crawler.py')
    ssh.close()
    
def check_status():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('43.201.27.201', port='22', username='ec2-user', password='', key_filename='C:/Users/Ion/.ssh/team19-key.pem')
    
    sftp = ssh.open_sftp()
    sftp.put('/home/ec2-user/Crawling/Steamrev_base.csv', './Steamrev_base_1.csv')

def pull_from_server():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('43.201.27.201', port='22', username='ec2-user', password='', key_filename='C:/Users/Ion/.ssh/team19-key.pem')
    
    sftp = ssh.open_sftp()
    sftp.put('/home/ec2-user/Crawling/Steamrev_base.csv', './Steamrev_base_1.csv')
    
    
def main():
    set_logger()
    #if os.path.isfile("./games_list.csv"):
    #    if input("renew game list?(y/n): ")=="y":renew="renew"
    #    else:renew="keep"
    #else:
    #    printer.info("First launch detected. making game list...")
    #    renew="new"        #테스트를 위해 비활성
    renew="keep"
    gamedict, newgamedict = get_gamelist(renew)
    
    logs = os.listdir("./log")
    
    mode = "local"
    if mode=="local":
        get_steam_rev(dict(list(gamedict.items())[-3:]), 0) #현재 테스트 코드
    #    get_steam_rev(gamedict, get_last_date(logs)) #현재 테스트 코드
    if mode=="ssh":
        
        push_to_server([dict(list(gamedict.items())[-3:]), 0])

    

    
    #Todo
    
    #
    
    
    # 2, 손실된 파일 데이터 체크하기
    # 2-2, 불러오기 실패하면 로그에 저장(나중에 업데이트?)
    # 서버와 연동
    #1, 스크랩 양 예측하기-문제는 갱신의 규모는 예측하기 힘듦(비율로 예상하던지, 서버에서 자체 계산하던지, 빼던지)
    
    #1, 어느 서버를 쓸 것인지 설정
    #서버에 코드 업로드
    
    #모드: 로컬에서 돌리기
    # - 90일 이내 갱신인 경우
    # - 예정 크롤링 리스트가 하나인 경우

    #모드 2: 타 서버 여러개에서 돌리기
    # - 크롤링 예정 리스트를 쿼리로 만들어서 서버에 올리기
    # - 서버 갯수가 부족한 경우 2일에 나누어서 돌려야 함
    
    #3, 다중 크롤링 요청. 그리고 완료 전달받기
    #4, 다 긁어와서 하나로 합치기
    #5, 완성본으로 합치고, 완료 메시지
    #6, 패킷 압축 저장?
        
    #each_gamelist = split_applist(game_df["appid"])
    
if __name__=="__main__":
    main()
