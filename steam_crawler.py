import pandas as pd
import requests
import json
import time

def get_steam_rev(app_list:list, stop_time:int = 0)->tuple[pd.DataFrame, pd.DataFrame]:
    '''
    스팀 API 활용 리뷰 수집 코드!!

    사용한 방법: steamworks api, 하루 최대 1000만 건, 시간당 약 120만건 수집 가능(0.5 query/s)

    Input:
        app_list: 수집할 게임의 id 리스트입니다.
        stop_time: 리뷰의 수집 일자를 제한하는 Timestamp 형식의 시간입니다.

    Output:
        sumaries_df: 수집한 게임별 리뷰 통계입니다.

          ========================================================================
          num_reviews: 리뷰의 쿼리 당 수집량
          review_score: 수집한 리뷰들의 긍정평가 비율
          review_score_desc: 스팀에서 표시되는 평가 텍스트(예시: 대체로 긍정적)
          total_positive: 긍정적 평가의 수
          total_negative: 부정적 평가의 수
          total_reviews: 수집된 리뷰의 총량
          =========================================================================



        rev_df_all: 수집한 게임들의 리뷰들 Dataframe입니다.

          =========================================================================
          recommendationid: 리뷰의 고유 id
          author: 리뷰 작성자에 대한 정보 (플레이타임 등)
          language: 작성된 리뷰의 언어
          review: 리뷰 본문
          timestamp_created: 리뷰 작성 시각(unix Timestamp)
          timestamp_updated: 리뷰 최종 수정 시각
          voted_up: True일 시, 긍정 평가
          votes_up: 유저들에게서 받은 추천수
          votes_funny: 유저들에게서 받은 "재미있음" 수
          weighted_vote_score: 스팀에서 평가한 리뷰의 "유용함" 지표
          comment_count: 달린 댓글의 수
          steam_purchase: True일 시, 작성자의 게임은 스팀 직접 구매입니다.(False는 선물 혹은 CD키 구매입니다)
          received_for_free: True일 시, 작성자의 게임은 'Steam 선물'을 통해 받은 게임입니다.
          written_during_early_access: True일 시, 게임의 '앞서 해보기' 기간동안 작성된 리뷰입니다. '앞서 해보기'란 일종의 유료 베타를 의미합니다.
          hidden_in_steam_china: True일 시, 중국에서 금지된 게임입니다.
          =========================================================================
    '''



    rev_df_all = pd.DataFrame()
    #daylim = 0
    for app in app_list:    #crwal all game in list
        url = f"https://store.steampowered.com/appreviews/{app}?json=1"
        params = {"filter":"recent",
                "language":"english",
                "cursor":"*",
                "review_type":"all",
                "purchase_type":"all",
                "num_per_page":"100"
              }
        revs = []
        summaries = []
        n_rv = 200

        while n_rv!=0:
          n_rv_prev = n_rv
          for tr in range(5): #if error, retry
            res = requests.get(url, params=params)
            res.encoding = 'utf-8-sig'
            res = json.loads(res.text) 
            
            n_rv = res['query_summary']['num_reviews']
            if not res['success']:
              print("connect Failed:")
              err_input = input() #failue
            elif params["cursor"]=='*' and n_rv==0: 
              print("load Failed:")
              err_input = input()
            elif n_rv_prev!=100 and n_rv==0:
              print(f"API went wrong. rtry after {tr*tr*5}sec.")
              time.sleep(tr*tr*5)
            else:break
            
          if tr==5:
            exmsg = input("exit process and save?")
            if exmsg == "1":break
          
            
          #파라미터 착각. 다시 조정할 필요 있음
          #if not res['query_summary']['num_reviews'] == 100: print(str(app)+": done")
        
          if params['cursor']=='*': 
                isum = {"app_id":app}
                isum.update(res['query_summary']) #get summary
                summaries += [sum]

          revs += res['reviews'] #get reviews


          if n_rv and res['reviews'][-1]['timestamp_created'] <stop_time : break #outdated

          params['cursor'] = res['cursor']
          
        rev_df = pd.DataFrame(revs)
        rev_df = rev_df[rev_df.timestamp_created>=stop_time] #kill outdated
        rev_df.insert(0, "app_id", app) #insert game id
        print(f"app {app}: Done! loaded Reviews: {len(rev_df)}")  
        rev_df_all = pd.concat([rev_df_all, rev_df]) 
        

    summaries_df = pd.DataFrame(summaries)
    return summaries_df, rev_df_all 


