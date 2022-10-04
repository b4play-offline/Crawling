import pandas as pd
import json
from steam_crawler import get_steam_rev
import meta_crawler

def main():
    with open ("splited_game_list.json","r")as f:
        game_dict = json.load(f)
        game_dict = json.loads(game_dict)[3]
        game_dict = dict(list(game_dict.items())[20:27])
    
    get_steam_rev(game_dict,0, "test")
    #each_game_list = split_applist(game_df["appid"])
    
if __name__=="__main__":
    main()
