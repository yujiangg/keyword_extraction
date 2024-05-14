from db import DBhelper
import pandas as pd
import datetime


def get_web_id_list():
    q = "SELECT web_id  FROM dione.missoner_web_id_table x WHERE ai_article_enable =1"
    web_id_list = DBhelper('dione').ExecuteSelect(q)
    return [i[0] for i in web_id_list]


def get_data(web_id, dateint):
    q = f"""SELECT
        web_id,
        keyword,
        COUNT(article_id) AS article_count,
        GROUP_CONCAT(article_id SEPARATOR ', ') AS article_ids
        FROM
            missoner_keyword_article_new
        WHERE 
            dateint >={dateint} and web_id ='{web_id}'
        GROUP BY
        web_id,
        keyword;"""
    data =DBhelper('dione_2').ExecuteSelect(q)
    return pd.DataFrame(data)


if __name__ == '__main__':
    query = f"TRUNCATE TABLE missoner_article_ids"
    DBhelper('dione_2').ExecuteSelect(query)
    web_id_list = get_web_id_list()
    type_name = {1: 'yesterday', 7: 'week', 30: 'month'}
    now_date = int(datetime.datetime.utcnow().strftime("%Y%m%d"))
    for web_id in web_id_list:
        df = pd.DataFrame(columns=['web_id', 'keyword', 'article_count', 'article_ids', 'types', 'date'])
        for day in [1, 7, 30]:
            dateint = int((datetime.datetime.utcnow() - datetime.timedelta(days=day)).strftime("%Y%m%d"))
            df_ = get_data(web_id, dateint)
            df_['types'] = type_name[day]
            df_['date'] = now_date
            df = pd.concat([df, df_, ], axis=0, ignore_index=True)
        DBhelper.ExecuteUpdatebyChunk(df, db='dione_2', table=f'missoner_article_ids', is_ssh=False)
