import pandas as pd
import requests
import json
import os
import time
import logging
from datetime import datetime

def set_logger_aws():
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

def split_applist(app_dict:dict)->dict:
  '''
  Split steam applist by day query limit(100,000)
  In just case, split at 99500 query.
  
  Input:
    app_dict: dict of all appids.
  
  Output:
    sum_list: list of splited appid dict.
  '''
  
  rv_lim = 0
  sum_list = [] 
  splited_list = []  
  
  for k, v in app_dict.items():
    url = f"https://store.steampowered.com/appreviews/{k}?json=1"
    params = {"filter":"recent",
              "language":"english",
              "cursor":"*",
              "review_type":"all",
              "purchase_type":"all",
              "num_per_page":"1"
            }
    res = requests.get(url, params=params)
    res.encoding = 'utf-8-sig'
    res = json.loads(res.text)
    
    tot_rv = res['query_summary']['total_reviews']
    tot_rv = (tot_rv//100)+int(bool(tot_rv))
    
    if rv_lim+tot_rv>99499: #500 = margin
      sum_list.append(splited_list)
      splited_list = []
      rv_lim = tot_rv
    
    rv_lim+=tot_rv
    splited_list.append({k:v})
    
  if len(splited_list):
    sum_list.append(splited_list)
      
  return sum_list

def get_query(url:str, params:dict, is_end:bool=False):
  '''
  get query data from api
  
  Input:
    url: steamapi url
    params: steamapi parameter
    is_end: crawling progress of current game
    
  Output:
    res: result of query
  '''
  printer = logging.getLogger("Steam_console")
  logger = logging.getLogger("Steam_console")
  for tr in range(1,7): #if error, retry
    res = requests.get(url, params=params)
    res.encoding = 'utf-8-sig'
    res = json.loads(res.text) 
    
    n_rv = res['query_summary']['num_reviews']
    
    if not res['success']:  
      logger.warning(f"Api conect Failed x{tr}")
    elif params["cursor"]=='*' and n_rv==0: 
      logger.warning(f"Wrong app id x{tr}")
    elif not is_end and n_rv==0: #200 = accepted error
      logger.warning(f"Unexpected endpoint x{tr}")
    else:
      res['success'] = True
      return res
    printer(f"Loading Error, wait until {tr*tr*5} sec.")
    time.sleep(tr*tr*5)
    if tr==6:
      logger.warning(f"Retrying failed and Sleep")
      printer.warning(f"Retrying failed and Sleep 30 min")
      time.sleep(3600)
  logger.error(f"Query {params['cursor']} Failed.") 
  printer.error(f"Query {params['cursor']} Failed.") 
  res['success'] = False
  return res

def get_game_rev(appid:str, stop_time:int):
  '''
  Get total reviews of single game. returns reviews, summary, counts as list 
  '''
  
  logger = logging.getLogger("all_file")
  printer = logging.getLogger("all_console")
  url = f"https://store.steampowered.com/appreviews/{appid}?json=1"
  params = {"filter":"recent",
          "language":"english",
          "cursor":"*",
          "review_type":"all",
          "purchase_type":"all",
          "num_per_page":"100"
        }
  revs = []
  n_rv = 400
  tot_rv = 400
   
  while n_rv!=0:
    try:
      naive_endchk = bool(len(revs)>=tot_rv-200)
      res = get_query(url, params, naive_endchk)
      n_rv = res['query_summary']['num_reviews']
      
      if not res['success']: #prevent reset
          exmsg = input("exit process and save?")
          if exmsg == "1":n_rv=0
      
      if params['cursor']=='*':
          summary =res["query_summary"] 
          tot_rv = res["query_summary"]['total_reviews']
            
      revs += res['reviews'] 
      if n_rv and res['reviews'][-1]['timestamp_created'] <stop_time : n_rv=0 #outdated
      params['cursor'] = res['cursor']
    
    except ConnectionError: #if net err, handle yourself and insert any key
      logger.critical("Network Connections Lost")
      input("Net error: restore connections and press any key ")  
    except BaseException as e:
      logger.critical(f"Unknown Error {type(e)} occured: {e}")
      printer.critical(f"{type(e)}: {e}")
      time.sleep(3000)
  return [revs, summary, tot_rv]

def get_steam_rev(app_dict:dict, stop_time:int = 0, filename:str = f"Steamrev"):
  '''
  Collect Steam reviews by using steamapi. 10m rev per day, 1.2m rev per hour(0.5 query/s)
  
  Input:
      app_dict: Dict made of game id and name.
      stop_time: Timestamp time that set scrap limit.
      filename: Name of saving file.
  Output:
      none(save review and summary as filename.csv)
      check collected columns info at https://partner.steamgames.com/doc/store/getreviews  
  '''
  logger = logging.getLogger("all_file")
  printer = logging.getLogger("all_console")
  
  summaries = []
  if stop_time:stop_time-=24*60*60*90
  first_base=1
  first_early=1
  
  for app in app_dict.keys():       
    app_res = get_game_rev(app, stop_time)
    isum = {"app_id":app, "name":app_dict[app]}
    isum.update(app_res[1]) 
    summaries += [isum]
          
     
    rev_df = pd.DataFrame(app_res[0])
    rev_df = rev_df[rev_df.timestamp_created>=stop_time] #kill outdated
    
    rev_df.insert(0, "appid", app) 
    rev_df.insert(1, "name", app_dict[app]) 
    
    today_tstamp = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    
    rev_df_early = rev_df[rev_df.timestamp_created >= today_tstamp - 60*60*24*9] #early 90d revs
    rev_df = rev_df[rev_df.timestamp_created<today_tstamp - 60*60*24*9]
    
    #csv append code  
    if first_early and len(rev_df_early):
      if not os.path.isfile(f"./{filename}_temp.csv"):
        rev_df_early.to_csv(f"{filename}_temp.csv", index=False, mode='a')
      first_early=0
    else:
      rev_df.to_csv(f"{filename}_temp.csv",index=False,mode='a', header=False)
      
      
    if first_base and len(rev_df_early):
        if not os.path.isfile(f"./{filename}_base.csv"):
          rev_df.to_csv(f"{filename}_base.csv",index=False,mode='a')
        first_base=0
    else:
      rev_df.to_csv(f"{filename}_base.csv",index=False,mode='a', header=False)
    
    
    logger.info(f"APP {app} Done, {len(app_res[0])} of {app_res[2]} loaded.")
    printer.info(f"APP {app} Done, {len(app_res[0])} of {app_res[2]} loaded.")
         
  summaries_df = pd.DataFrame(summaries)
  summaries_df.to_csv(f"{filename}_summary.csv",index=False)
  logger.info("Scrapping Compleated")
  return True #임시, 로그파일 반환? 

if __name__=="__main__":
  set_logger_aws()
  with open("./query.json", "r", encoding="utf-8")as f:
    inp = json.load(f)
  get_steam_rev(inp[0],inp[1])

