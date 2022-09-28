import pandas as pd

from steam_crawler import get_steam_rev
import meta_crawler

def main():
    game_df = pd.read_csv("games_list.csv")
    
    test_summary, test_df = get_steam_rev(game_df["appid"][0:100], 0)
    
    test_summary.to_csv("1_100_summary.csv", index=False)
    test_df.to_csv("1_100_review.csv", index=False)

if __name__=="__main__":
    main()
