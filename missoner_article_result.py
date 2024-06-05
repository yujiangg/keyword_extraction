from db import DBhelper
import pandas as pd
import datetime


def get_total_pageviews(web_id, dateint, now_date):
    query = f"""SELECT web_id,sum(pageviews) as pageviews,sum(google) as google ,sum(likr) as likr,sum(facebook) as facebook ,sum(xuite) as xuite,sum(yahoo) as yahoo,sum(line) as line,sum(yt) as youtube 
    FROM dione.missoner_article_source_domain 
    WHERE `date`  BETWEEN {str(dateint)} and {str(now_date)} 
    and web_id ='{web_id}'"""
    data =DBhelper('dione').ExecuteSelect(query)
    return pd.DataFrame(data)


def get_data(web_id, table_name, dateint, now_date):
    query = f"""SELECT web_id,article_id,title,SUM(pageviews) AS total_pageviews_rate,
                       SUM(landings)/SUM(pageviews) AS total_landings_rate,
                       SUM(bounce)/SUM(pageviews) AS total_bounce_rate 
                       FROM {table_name} 
                       WHERE date BETWEEN {str(dateint)} AND {str(now_date)} 
                       and web_id = '{web_id}' 
                       GROUP BY article_id 
                       ORDER BY total_pageviews_rate 
                       DESC LIMIT 100"""
    data = DBhelper('dione').ExecuteSelect(query)
    return pd.DataFrame(data)

def get_web_id_list():
    query = f"""SELECT web_id FROM dione.missoner_web_id_table x WHERE article_pre_enable  = 1"""
    data = DBhelper('dione').ExecuteSelect(query)
    return [i[0] for i in data]




if __name__ == '__main__':
    web_id_list = get_web_id_list()
    type_name = {1: 'yesterday', 7: 'week', 30: 'month'}
    now_date = int((datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime("%Y%m%d"))
    yes_date = int((datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(days=1)).strftime("%Y%m%d"))
    query = f"TRUNCATE TABLE article_metrics_pre"
    DBhelper('lrmn_tag_media').ExecuteSelect(query)
    query = f"TRUNCATE TABLE article_pageview_statistics"
    DBhelper('lrmn_tag_media').ExecuteSelect(query)

    for web_id in web_id_list:
        df = pd.DataFrame(
            columns=['web_id', 'title', 'total_pageviews_rate', 'total_landings_rate', 'total_bounce_rate', 'domain',
                     'types', 'date'])
        df_pgs = pd.DataFrame(columns=['web_id', 'pageviews', 'google', 'likr', 'facebook', 'xuite', 'yahoo',
                                       'line', 'youtube', 'types', 'date'])
        for day in [1, 7, 30]:
            dateint = int(
                (datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(days=day)).strftime(
                    "%Y%m%d"))
            domain_pag_df = get_total_pageviews(web_id, dateint, yes_date)
            for domain in ['all', 'google', 'likr', 'facebook', 'xuite', 'yahoo', 'line', 'youtube']:
                pgs_count = int(domain_pag_df[domain][0]) if domain != 'all' else int(domain_pag_df['pageviews'][0])
                if pgs_count == 0:
                    continue
                table_name = 'missoner_article'
                if domain not in ['all', 'youtube']:
                    table_name += '_' + domain
                elif domain == 'youtube':
                    table_name += '_' + 'yt'
                print(f"{web_id}+{str(day)}+{domain}")
                df_data = get_data(web_id, table_name, dateint, yes_date)
                df_data['total_pageviews_rate'] = df_data.apply(lambda x: round(x['total_pageviews_rate'] / pgs_count, 3),
                                                                axis=1)
                df_data['domain'] = domain
                df_data['types'] = type_name[day]
                df_data['date'] = now_date
                df = pd.concat([df, df_data, ], axis=0, ignore_index=True)
            domain_pag_df['types'] = type_name[day]
            domain_pag_df['date'] = now_date
            df_pgs = pd.concat([df_pgs, domain_pag_df, ], axis=0, ignore_index=True)
        DBhelper.ExecuteUpdatebyChunk(df, db='lrmn_tag_media', table=f'article_metrics_pre', is_ssh=False)
        DBhelper.ExecuteUpdatebyChunk(df_pgs, db='lrmn_tag_media', table=f'article_pageview_statistics', is_ssh=False)