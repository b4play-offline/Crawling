import logging
import os
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
    if not os.path.isdir("./sshcache"):
        os.makedirs("./sshcashe")        
    filehandler = logging.FileHandler(f"./log/{datetime.now().strftime('%y-%m-%d')}_Steamlog")
    streamhandler = logging.StreamHandler()
    filehandler.setFormatter(formatter)

    logger.addHandler(filehandler)
    printer.addHandler(streamhandler)
    printer.propagate=False

    logger.setLevel("DEBUG")
    printer.setLevel("DEBUG")

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

def get_last_date(log_list:list = os.listdir("./log")):
    '''
    return recent Crawled date refer to log.
    In:
        log_list: list of file names in log dir.
    Out:
        last_date: tiemstamp of last Crawling code activated.
    '''
    logger = logging.getLogger("all_file")\
        
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


def merge(origin, cache, ftype):
    with open(origin+ftype, "a", encoding="utf-8")as f1: 
        with open(cache+ftype, "r", encoding="utf-8")as f2:
            f1.write("\n")
            f1.write(f2.read())

