import pandas as pd
import requests
import json

#Steam store game infos by Steamspy API
#Check param infos in https://steamspy.com/api.php 

def get_appinfo(params:dict)->pd.DataFrame:  
    
    #게임 보유자순 상위 1000개의 게임의 세부정보를 반환합니다.
    #게임이 아닌 소프트웨어는 자동으로 순위에서 배제됩니다.
    
    app_ids = []
    app_df = []
    
    for i in range(2):
        params["page"]=f'{i}'
        res = requests.get("https://steamspy.com/api.php", params).text
        res = json.loads(res)
        app_ids += res.keys()
    
    for id in app_ids:
        params = {"request":"appdetails", "appid":id}
        res = requests.get("https://steamspy.com/api.php", params).text
        res = json.loads(res)
        
        if "Software" in res["tags"].keys():continue
        
        app_df+=[res]
        
        if len(app_df)>999:break
        
    app_df = pd.DataFrame(app_df).transpose().reset_index(drop=True)

    return app_df

