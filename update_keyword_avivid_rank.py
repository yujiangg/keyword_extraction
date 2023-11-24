import time
from tqdm import tqdm
import pandas as pd
import requests
from db import DBhelper
from bs4 import BeautifulSoup
import json
import datetime

def get_web_id_dict():
    query = """SELECT web_id,url FROM google_search_console_url"""
    return {i:v for i,v in DBhelper('jupiter_new').ExecuteSelect(query)}

web_id_dict = get_web_id_dict()

def find_avivid_rank(keyword):
    for i in range(1, 100, 10):
        google = requests.get(f"https://www.googleapis.com/customsearch/v1/?cx=46d551baeb2bc4ead&key=AIzaSyDF-Rpzr2flF0l7bQL565SbajE7Tz4Ovwg&start={i}&q={keyword}")
        result = google.json().get('items')
        time.sleep(0.66)
        if result:
            for j, v in enumerate(result):
                if 'avivid' in v.get('link'):
                    return j+1 + (i-1)
        else:
            return None

# query = f"""SELECT g1.web_id, g1.date
# FROM google_search_console_website g1
# JOIN (
#     SELECT web_id, MAX(date) AS max_date
#     FROM google_search_console_website
#     GROUP BY web_id
# ) g2 ON g1.web_id = g2.web_id AND g1.date = g2.max_date
# WHERE g1.web_id in ('{"','".join(web_id_dict.keys())}') """
#df = pd.DataFrame(DBhelper('jupiter_new').ExecuteSelect(query))
date = (datetime.datetime.utcnow() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
df = pd.DataFrame(columns=['web_id', 'date', 'keyword_rank'])
rank_all = []
print(date)
for web_id, url in web_id_dict.items():
    curr = {}
    cu = []
    repose = requests.get(url+'/'+date)
    if repose.status_code != 200 or repose.text == 'null':
        print(f"{web_id}未取得排名")
        continue
    BL = eval(repose.text)
    for i in tqdm(BL[:10]):
        keyword = i['keyword']
        imp = i['impressions']
        rank = find_avivid_rank(keyword)
        page = rank//10 + 1 if type(rank) == int else float('inf')
        topk = rank % 10 if type(rank) == int else float('inf')
        topk = topk if topk else 10
        cu.append((page, topk, imp, keyword))
    for j, v in enumerate(sorted(cu)):
        rank = v[1] if v[1] != float('inf') else None
        page = v[0] if v[0] != float('inf') else None
        curr[j] = {'keyword': v[3], 'rank': rank, 'page': page, 'imp': v[2]}
    print([web_id,date,json.dumps(curr)])
    df.loc[len(df)] = [web_id,date,json.dumps(curr)]
DBhelper.ExecuteUpdatebyChunk(df, db='jupiter_new', table='google_search_console_website')