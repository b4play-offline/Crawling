import pandas as pd
import requests
import json
import time
import logging
from datetime import datetime



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

def get_steam_rev(app_dict:dict, stop_time:int = 0, filename:str = f"{int(datetime.now().timestamp())}_Steamrev")->tuple[pd.DataFrame, pd.DataFrame]:
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
  for app in app_dict.keys():    #crwal all game in list
    url = f"https://store.steampowered.com/appreviews/{app}?json=1"
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
            isum = {"app_id":app, "name":app_dict[app]}
            isum.update(res['query_summary']) 
            summaries += [isum]
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
    if len(revs):  
      rev_df = pd.DataFrame(revs)
      rev_df = rev_df[rev_df.timestamp_created>=stop_time] #kill outdated
      
      rev_df.insert(0, "appid", app) 
      rev_df.insert(1, "name", app_dict[app]) 
      rev_df.to_csv(f"{filename}.csv",index=False,mode='a')
      
      logger.info(f"APP {app} Done, {len(revs)} of {tot_rv} loaded.")
      printer.info(f"APP {app} Done, {len(revs)} of {tot_rv} loaded.")
       
  summaries_df = pd.DataFrame(summaries)
  summaries_df.to_csv(f"{filename}_summary.csv",index=False,mode='a')
  logger.info("Scrapping Compleated")
  return True #임시, 로그파일 반환? 