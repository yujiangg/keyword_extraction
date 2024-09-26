from keyword_util import *
import time
from db import DBhelper
import datetime
from slackwarningletter import slack_warning
import pandas as pd
import concurrent.futures
from tqdm import tqdm
import random
import numpy as np

def fetch_web_id_list():
    q = f"SELECT web_id,web_id_type,eng FROM missoner_web_id_table WHERE enable = 1 and keyword_enable = 1"
    data = DBhelper('dione').ExecuteSelect(q)
    web_id_dict = {web_id: {"lang": eng, "type": web_id_type} for web_id, web_id_type,eng in data}
    return web_id_dict


def fetch_pageview_hot_df(web_id, dateint, n):
    qurey = f"SELECT web_id,article_id,source_domain,SUM(pageviews) as pageviews, SUM(landings) as landings, SUM(exits) as exits,SUM(bounce) as bounce, SUM(timeOnPage) as timeOnPage,date FROM pageviews_report_hour_missoner where date = '{dateint}' and web_id = '{web_id}'group by article_id,source_domain order by pageviews desc limit {n}"
    data = DBhelper('dione').ExecuteSelect(qurey)
    columns = ['web_id', 'article_id','source_domain','pageviews', 'landings', 'exits', 'bounce', 'timeOnPage', 'date']
    df_hot = pd.DataFrame(data=data, columns=columns)
    if web_id == 'kfan':
        df_hot['article_id'] = df_hot.apply(lambda x: x.article_id.split('&')[0], axis=1)
        df_hot = df_hot.groupby(['web_id', 'article_id', 'source_domain','date'])[['pageviews','landings','exits','bounce','timeOnPage']].sum().reset_index()
    return df_hot


def fetch_article_df(web_id):
    qurey = f"SELECT web_id,signature,title,content,keywords,url,image From article_list where web_id = '{web_id}' order by id desc limit 10000"
    data = DBhelper('dione').ExecuteSelect(qurey)
    columns = ['web_id', 'article_id','title','content', 'keywords','url','image']
    df_hot = pd.DataFrame(data=data, columns=columns)
    return df_hot


def fetch_ecom_df(web_id):
    qurey = f"SELECT web_id,signature,title,content,url,image From ecom_table where web_id = '{web_id}'"
    data = DBhelper('dione_2').ExecuteSelect(qurey)
    columns = ['web_id', 'article_id','title','content','url','image']
    df_hot = pd.DataFrame(data=data, columns=columns)
    df_hot['keywords'] ='_'
    if web_id =='kfan':
        df_hot['article_id'] = df_hot.apply(lambda x:x.article_id.split('&')[0],axis=1)
        df_hot.drop_duplicates(['article_id'],inplace=True)
    return df_hot


def fetch_df_hot(web_id, types, dateint, n):
    df_hot_1 = fetch_pageview_hot_df(web_id, dateint, n)
    if df_hot_1.size == 0:
        return None
    if types == 1:
        df_hot_2 = fetch_ecom_df(web_id)
    else:
        df_hot_2 = fetch_article_df(web_id)
    if df_hot_2.size == 0:
        return None
    df_hot = pd.merge(df_hot_1, df_hot_2)
    return df_hot


def collect_pageviews_by_source(keyword_dict, keyword, row, source_domain_mapping, params, is_cut,dm):
    ## save each keyword from a article ##
    if keyword not in keyword_dict:
        ## process internal and external source loop and save to popular keyword dict
        if dm in source_domain_mapping:  # internal case
            keyword_dict[keyword] = np.append(params, [0, row['pageviews'], is_cut])
        else:  # external case
            keyword_dict[keyword] = np.append(params, [row['pageviews'], 0, is_cut])
    else:
        ## process internal and external source loop and add to popular keyword dict
        if dm in source_domain_mapping:  # internal case
            ## add to internal source count
            keyword_dict[keyword][:-1] += np.append(params, [0, row['pageviews']])
        else:  # external case
            ## add to external source count
            keyword_dict[keyword][:-1] += np.append(params, [row['pageviews'], 0])
    return keyword_dict


def collect_source_article_pageviews_by_source(article_dict, row, params_all,params):
    ## save each keyword from a article ##
    if row['article_id'] not in article_dict:
        article_dict[row['article_id']] = params_all.copy()
    else:
        article_dict[row['article_id']][3:] += params
    return article_dict

def fetch_now_keywords_by_web_id(web_id,date_int):
    query = f"SELECT keyword, pageviews FROM missoner_keyword WHERE date={date_int} and web_id='{web_id}'"
    data = DBhelper('dione_2').ExecuteSelect(query)
    df = pd.DataFrame(data, columns=['keyword', 'pageviews'])
    return df

def fetch_now_article_by_web_id(web_id,date_int):
    query = f"SELECT article_id, pageviews FROM missoner_article WHERE date={date_int} and web_id='{web_id}'"
    data = DBhelper('dione').ExecuteSelect(query)
    df = pd.DataFrame(data, columns=['article_id', 'pageviews'])
    return df


def collect_source_keyword_pageviews_by_source(article_dict, params, keyword):
    ## save each keyword from a article ##
    if keyword not in article_dict:
        article_dict[keyword] = params.copy()
    else:
        article_dict[keyword] += params
    return article_dict

def fetch_now_source_keywords_by_web_id(web_id,date_int ,source):
    query = f"SELECT keyword, pageviews FROM missoner_keyword_{source} WHERE date={date_int} and web_id='{web_id}'"
    data = DBhelper('dione_2').ExecuteSelect(query)
    df = pd.DataFrame(data, columns=['keyword', 'pageviews'])
    return df

def compute_trend_from_df(df_pageviews_last, df_pageviews_now):
    ## make pageviews to be int32 and math operation by index:keyword
    df_pageviews_last = df_pageviews_last[['keyword', 'pageviews']].set_index('keyword').astype({'pageviews': 'int32'})
    df_pageviews_now = df_pageviews_now[['keyword', 'pageviews']].set_index('keyword').astype({'pageviews': 'int32'})
    df_trend = ((df_pageviews_now - df_pageviews_last)/df_pageviews_last).fillna(0).rename(columns={'pageviews': 'trend'})
    df_trend = pd.concat([df_trend, df_pageviews_last], axis=1).rename(columns = {'pageviews': 'pageviews_last'})
    df_trend['keyword'] = df_trend.index
    return df_trend

def get_domain_df(domain_dict,_index,web_id,date_int):
    domain_df = pd.DataFrame.from_dict(domain_dict, 'index')
    domain_df[_index] = domain_df.index
    domain_df['web_id'] = web_id
    domain_df['date'] = date_int
    domain_df.reset_index(drop=True, inplace=True)
    pageviews = domain_df.iloc[:, :-3].apply(np.sum, axis=1)
    domain_df['pageviews'] = pageviews
    # mean = sum(pageviews) // len(domain_df)
    # domain_df = domain_df[domain_df['pageviews'] > mean]
    return domain_df

def compute_trend_article_from_df(df_pageviews_last, df_pageviews_now):
    ## make pageviews to be int32 and math operation by index:keyword
    df_pageviews_last = df_pageviews_last[['article_id', 'pageviews']].set_index('article_id').astype({'pageviews': 'int32'})
    df_pageviews_now = df_pageviews_now[['article_id', 'pageviews']].set_index('article_id').astype({'pageviews': 'int32'})
    df_trend = ((df_pageviews_now - df_pageviews_last)/df_pageviews_now*100).fillna(0).rename(columns = {'pageviews': 'trend'})

    df_trend = pd.concat([df_trend, df_pageviews_last], axis=1).rename(columns = {'pageviews': 'pageviews_last'})
    df_trend['article_id'] = df_trend.index
    return df_trend

def compute_hour_diff(df_article_last,df_article,name):
    diff = df_article[[name, 'pageviews']].set_index(name).astype({'pageviews': 'int32'}) - df_article_last.set_index(name).astype({'pageviews': 'int32'})
    diff = diff.fillna(0).astype(int).reset_index().rename({'pageviews':'pageviews_hour'},axis='columns')
    df_article = pd.merge(df_article, diff, on=name, how='left')
    return df_article

def fetch_last_hour_article(web_id,hour,aok,col,week,date):
    hour = hour - 1
    if aok =='keyword':
        db_name = 'dione_2'
    else:
        db_name = 'dione'
    query = f"SELECT {col}, pageviews FROM missoner_{aok}_hour_{week} WHERE hour='{hour}' and web_id='{web_id}' and date='{date}'"
    data = DBhelper(db_name).ExecuteSelect(query)
    df = pd.DataFrame(data, columns=[col, 'pageviews'])
    return df

def collect_article_pageviews_by_source(article_dict,row,source_domain_mapping,params_all,params,dm):
    ## save each keyword from a article ##
    if row['article_id'] not in article_dict.keys():
        ## process internal and external source loop and save to popular keyword dict
        if dm in source_domain_mapping:  # internal case
            article_dict[row['article_id']] = np.append(params_all, [0, row['pageviews']])
        else:  # external case
            article_dict[row['article_id']] = np.append(params_all, [row['pageviews'],0])
    else:
        ## process internal and external source loop and add to popular keyword dict
        if dm in source_domain_mapping:  # internal case
            ## add to internal source count
            article_dict[row['article_id']][3:]+= np.append(params, [0, row['pageviews']])
        else:  # external case
            ## add to external source count
            article_dict[row['article_id']][3:] += np.append(params, [row['pageviews'], 0])
    return article_dict


def main(web_id, web_id_type, web_id_lang, tw_date, tw_hour, weekday, n=5000):
    try:
        print(f"*****start_{web_id}*****")
        source_list = ['google', 'likr', 'facebook', 'xuite', 'yahoo', 'line', 'youtube']
        dateint = int(tw_date.replace('-', ''))
        update_df = {}
        df_hot = fetch_df_hot(web_id, web_id_type, dateint, n)
        if df_hot is None or df_hot.size == 0:
            print(f'no {web_id} valid data in dione.report_hour')
            return update_df

        dict_keyword_article = {}
        i = 0
        keyword_dict = {}
        article_dict = {}
        article_domain_dict = {}
        keyword_domain_dict = {}
        source_dict_article = collections.defaultdict(dict)
        source_dict_keyword = collections.defaultdict(dict)
        for index, row in df_hot.iterrows():
            # ## process keyword ##
            if web_id_lang == 0:  ## 中文
                if row['keywords'] == '_' or row['keywords'] == '':
                    keyword_list, keywords = get_chinese(row['title'],web_id)
                    is_cut = 1
                else:
                    keyword_list = [k.strip() for k in row['keywords'].split(',')]
                    keywords = row['keywords']
                    is_cut = 0
            elif web_id_lang == 1:   ## 英文
                if row['keywords'] == '_' or row['keywords'] == '':
                    keyword_list, keywords = get_eng(row['title'])
                    is_cut = 1
                else:
                    keyword_list = [k.strip() for k in row['keywords'].split(',')]
                    keywords = row['keywords']
                    is_cut = 0
            elif web_id_lang == 2:   ## 越南
                if row['keywords'] == '_' or row['keywords'] == '':
                    keyword_list, keywords = get_vina(row['title'])
                    is_cut = 1
                else:
                    keyword_list = [k.strip() for k in row['keywords'].split(',')]
                    keywords = row['keywords']
                    is_cut = 0
            elif web_id_lang == 3:  ##印度與
                if row['keywords'] == '_' or row['keywords'] == '':
                    keyword_list, keywords = get_ind(row['title'])
                    is_cut = 1
                else:
                    keyword_list = [k.strip() for k in row['keywords'].split(',')]
                    keywords = row['keywords']
                    is_cut = 0
            elif web_id_lang == 4:
                if row['keywords'] == '_' or row['keywords'] == '':
                    keyword_list, keywords = get_thai(row['title'])
                    is_cut = 1
                else:
                    keyword_list = [k.strip() for k in row['keywords'].split(',')]
                    keywords = row['keywords']
                    is_cut = 0

            params = np.array(row[['pageviews', 'landings', 'exits', 'bounce', 'timeOnPage']]).astype('int')
            params_data = np.array(row[['web_id', 'title', 'content']])
            params_all = np.append(params_data, params)
            dm = row['source_domain']
            if dm == "google search":
                dm = 'google'
            elif dm == "fb":
                dm = 'facebook'
            url = row['url']
            image = row['image']
            article_dict = collect_article_pageviews_by_source(article_dict, row, [web_id], params_all, params,dm)
            ## separate keyword_list to build dictionary ##
            if row['article_id'] not in article_domain_dict:
                article_domain_dict[row['article_id']] = {'internal': 0, 'google': 0, 'facebook': 0, 'yahoo': 0, 'likr': 0,
                                                  'xuite': 0, 'youtube': 0, 'line': 0, 'feed_related': 0, 'dcard': 0,
                                                  'ptt': 0, 'edm': 0, 'other': 0}
            if dm == web_id:
                article_domain_dict[row['article_id']]['internal'] += int(row['pageviews'])
            elif dm in article_domain_dict[row['article_id']]:
                article_domain_dict[row['article_id']][dm] += int(row['pageviews'])
            else:
                article_domain_dict[row['article_id']]['other'] += int(row['pageviews'])

            for keyword in keyword_list:
                ## keyword and articles mapping, for table, missoner_keyword_article

                dict_keyword_article[i] = {'web_id': web_id, 'article_id': row['article_id'], 'keyword': keyword,
                                           'is_cut': is_cut, 'url': url, 'image': image}
                i += 1
                ## compute pageviews by external and internal sources, for table, missoner_keyword
                keyword_dict = collect_pageviews_by_source(keyword_dict, keyword, row, [web_id], params,
                                                           is_cut, dm)
                if keyword not in keyword_domain_dict:
                    keyword_domain_dict[keyword] = {'internal': 0, 'google': 0, 'facebook': 0, 'yahoo': 0, 'likr': 0,
                                                    'xuite': 0, 'youtube': 0, 'line': 0, 'feed_related': 0, 'dcard': 0,
                                                    'ptt': 0, 'edm': 0, 'other': 0}
                if dm == web_id:
                    keyword_domain_dict[keyword]['internal'] += int(row['pageviews'])
                elif dm in keyword_domain_dict[keyword]:
                    keyword_domain_dict[keyword][dm] += int(row['pageviews'])
                else:
                    keyword_domain_dict[keyword]['other'] += int(row['pageviews'])

            if dm in source_list:
                source_dict_article[dm] = collect_source_article_pageviews_by_source(source_dict_article[dm], row, params_all, params)
                for keyword in keyword_list:
                    source_dict_keyword[dm] = collect_source_keyword_pageviews_by_source(source_dict_keyword[dm], params, keyword)
            #print(f"index: {index},article_id:{row['article_id']} ,keywords: {keywords}")
        if not keyword_dict or not article_dict:
            return update_df


        for name, source_data in source_dict_article.items():
            if name == 'youtube':
                name = 'yt'
            db_source_article_name = f'missoner_article_{name}'

            source_data_df = pd.DataFrame.from_dict(source_data, 'index',
                                                    columns=['web_id', 'title', 'content', 'pageviews', 'landings',
                                                             'exits', 'bounce', 'timeOnPage'])
            source_data_df = source_data_df.reset_index().rename(columns={'index': 'article_id'})
            source_data_df['date'] = dateint
            update_df[('dione', db_source_article_name)] = source_data_df.copy()
            #DBhelper.ExecuteUpdatebyChunk(source_data_df, db='dione', table=db_source_article_name, chunk_size=100000,

        for name, source_data in source_dict_keyword.items():
            if name == 'youtube':
                name = 'yt'
            ################
            last_keyword_pageviews = fetch_now_source_keywords_by_web_id(web_id, dateint, name)
            ################
            db_source_article_name = f'missoner_keyword_{name}'
            source_data_df = pd.DataFrame.from_dict(source_data, 'index',
                                                    columns=['pageviews', 'landings', 'exits', 'bounce', 'timeOnPage'])
            source_data_df = source_data_df.reset_index().rename(columns={'index': 'keyword'})
            df_trend = compute_trend_from_df(last_keyword_pageviews, source_data_df)
            source_data_df = pd.concat([source_data_df.set_index('keyword'), df_trend.set_index('keyword')],
                                       axis=1).reset_index(level=0)
            source_data_df.dropna(axis=0, subset=["pageviews"], inplace=True)
            source_data_df = source_data_df.fillna(0)
            source_data_df['date'] = dateint
            source_data_df['web_id'] = web_id
            update_df[('dione_2', db_source_article_name)] = source_data_df.copy()
            # DBhelper.ExecuteUpdatebyChunk(source_data_df, db='dione_2', table=db_source_article_name, chunk_size=100000,
            #                               is_ssh=False)

        data_save, data_trend = {}, {}
        i = 0
        for key, value in keyword_dict.items():
            data_save[i] = {'web_id': web_id, 'keyword': key, 'pageviews': value[0], 'external_source_count': value[5],
                            'internal_source_count': value[6], 'landings': value[1], 'exits': value[2],
                            'bounce': value[3], 'timeOnPage': value[4], 'is_cut': value[7], 'date': dateint}
            data_trend[i] = {'web_id': web_id, 'keyword': key, 'pageviews': value[0], 'hour': tw_hour, 'date': dateint}
            i += 1

        data_save_article, data_trend_article = {}, {}
        i = 0
        for key, value in article_dict.items():
            data_save_article[i] = {'web_id': web_id, 'article_id': key, 'title': value[1], 'content': value[2],
                                    'pageviews': value[3], 'external_source_count': value[8],
                                    'internal_source_count': value[9], 'landings': value[4], 'exits': value[5],
                                    'bounce': value[6], 'timeOnPage': value[7], 'date': dateint}
            data_trend_article[i] = {'web_id': web_id, 'article_id': key, 'pageviews': value[3], 'hour': tw_hour,
                                     'date': dateint}
            #print(f'{data_trend_article[i]}')
            i += 1

        df_pageviews_last = fetch_now_keywords_by_web_id(web_id, dateint)
        #####
        df_pageviews_now = pd.DataFrame.from_dict(data_trend, "index")[['keyword', 'pageviews']]
        df_trend = compute_trend_from_df(df_pageviews_last, df_pageviews_now)
        ## article
        df_pageviews_last_article = fetch_now_article_by_web_id(web_id, dateint)
        df_pageviews_now_article = pd.DataFrame.from_dict(data_trend_article, "index")[['article_id', 'pageviews']]
        df_trend_article = compute_trend_article_from_df(df_pageviews_last_article, df_pageviews_now_article)

        article_domain_df = get_domain_df(article_domain_dict, 'article_id', web_id, dateint)
        article_domain_df = article_domain_df.rename({'youtube': 'yt'}, axis='columns')
        update_df[('dione', 'missoner_article_source_domain')] = article_domain_df


        keyword_domain_df = get_domain_df(keyword_domain_dict, 'keyword', web_id, dateint)
        keyword_domain_df = keyword_domain_df.rename({'youtube': 'yt'}, axis='columns')
        update_df[('dione_2', 'missoner_keyword_source_domain')] = keyword_domain_df

        df_keyword = pd.DataFrame.from_dict(data_save, "index")
        df_keyword = pd.concat([df_keyword.set_index('keyword'), df_trend.set_index('keyword')], axis=1).reset_index(
            level=0)
        pageviews_array = np.array(df_keyword['pageviews']).astype('int')
        df_keyword = df_keyword.dropna(subset=['web_id'])
        df_keyword = df_keyword.fillna(0)

        update_df[('dione_2', 'missoner_keyword')] = df_keyword.copy()

        df_keyword['hour'] = tw_hour
        if tw_hour < 1:
            df_keyword['pageviews_hour'] = df_keyword['pageviews']
        else:
            df_keyword_last = fetch_last_hour_article(web_id, tw_hour, 'keyword', 'keyword', weekday, dateint)
            df_keyword = compute_hour_diff(df_keyword_last, df_keyword, 'keyword')
        try:
            df_keyword['pageviews_hour'] = df_keyword.apply(lambda x: x['pageviews'] if (
                        x['pageviews_last'] == 0 and x['pageviews_hour'] == 0 and x['pageviews'] != 0) else x[
                'pageviews_hour'], axis=1)
        except:
            df_keyword['pageviews_hour'] = df_keyword['pageviews']

        update_df[('dione_2', f"missoner_keyword_hour_{weekday}")] = df_keyword

        df_article = pd.DataFrame.from_dict(data_save_article, "index")
        df_article = pd.concat([df_article.set_index('article_id'), df_trend_article.set_index('article_id')],
                               axis=1).reset_index(level=0)
        df_article = df_article.fillna(0)
        update_df[('dione', f"missoner_article")] = df_article.copy()

        df_article['hour'] = tw_hour
        if tw_hour < 1:
            df_article['pageviews_hour'] = df_article['pageviews']
        else:
            df_article_last = fetch_last_hour_article(web_id, tw_hour, 'article', 'article_id', weekday, dateint)
            df_article = compute_hour_diff(df_article_last, df_article, 'article_id')
        try:
            df_article['pageviews_hour'] = df_article.apply(lambda x: x['pageviews'] if (
                        x['pageviews_last'] == 0 and x['pageviews_hour'] == 0 and x['pageviews'] != 0) else x[
                'pageviews_hour'], axis=1)
        except:
            df_article['pageviews_hour'] = df_article['pageviews']

        # article_list_dict = df_article.to_dict('records')
        update_df[('dione', f"missoner_article_hour_{weekday}")] = df_article

        df_keyword_article = pd.DataFrame.from_dict(dict_keyword_article, "index")
        df_keyword_article['dateint'] = dateint
        update_df[('dione_2', f"missoner_keyword_article_new")] = df_keyword_article
        update_df[('dione', f"missoner_keyword_article_new")] = df_keyword_article.copy()
        print(f"*****計算完成{web_id}*****")
        return update_df
    except Exception as e:
        print(f"""{web_id}錯誤:{e}""")
        return {}











if __name__ == '__main__':
    t_start = time.time()
    utc_now = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    utc_date, utc_hour = utc_now.strftime('%Y-%m-%d,%H').split(',')
    tw_now = utc_now + datetime.timedelta(hours=8)
    tw_date, tw_hour = tw_now.strftime('%Y-%m-%d,%H').split(',')
    slack_letter = slack_warning()
    tw_hour = int(tw_hour)
    temp = pd.Timestamp((datetime.datetime.utcnow() - datetime.timedelta(hours=1) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d'))
    weekday = str(temp.dayofweek + 1)
    if tw_hour == 0:
        table_name = f"missoner_keyword_hour_{weekday}"
        query = f"TRUNCATE TABLE {table_name}"
        DBhelper('dione_2').ExecuteSelect(query)

        table_name = f"missoner_article_hour_{weekday}"
        query = f"TRUNCATE TABLE {table_name}"
        DBhelper('dione').ExecuteSelect(query)

    web_id_dict = fetch_web_id_list()

    batch_size = 100000
    df_dict = collections.defaultdict(pd.DataFrame)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(main, web_id,info['type'], info['lang'], tw_date, tw_hour, weekday, 5000) for web_id,info in web_id_dict.items()]
        for future in tqdm(concurrent.futures.as_completed(futures)):
            try:
                update_dict = future.result()
                if not update_dict:
                    continue
                for table_name, df in update_dict.items():
                    df_dict[table_name] = pd.concat([df_dict[table_name], df], axis=0, ignore_index=True)
                    if len(df_dict[table_name]) > batch_size:
                        DBhelper.ExecuteUpdatebyChunk(df_dict[table_name], db=table_name[0], table=table_name[1],
                                                      chunk_size=100000,
                                                      is_ssh=False)
                        df_dict[table_name] = pd.DataFrame()
                        print(f"""{table_name}釋放""")
                print({i: len(v) for i, v in df_dict.items()})
            except Exception as e:
                print(f"An error occurred: {e}")
                # 如果发生错误，可以选择退出或中止所有线程
                slack_letter.send_letter_test(f'文案大師關鍵字,{str(tw_date)}-{str(tw_hour)}時,發生錯誤')
                executor.shutdown(wait=True)  # 中止所有线程
                break  # 跳出循环
    for table_name, df in df_dict.items():
        DBhelper.ExecuteUpdatebyChunk(df_dict[table_name], db=table_name[0], table=table_name[1],
                                      chunk_size=100000,
                                      is_ssh=False)
        df_dict[table_name] = pd.DataFrame()
        print(f"""{table_name}釋放""")

    t_end = time.time()
    t_spent = t_end - t_start
    if t_spent >= 3600:
        slack_letter.send_letter_test(f'文案大師關鍵字,{str(tw_date)}-{str(tw_hour)}時,本次執行時間為{t_spent}s,已超過50分鐘,請檢查問題')
    slack_letter.send_letter_test(f'文案大師關鍵字,{str(tw_date)}-{str(tw_hour)}時,執行成功')
    del_modle()
