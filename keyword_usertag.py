import pandas as pd
import datetime
from db import DBhelper
from basic import get_date_shift, get_yesterday, to_datetime, get_today, check_is_UTC0, timing, logging_channels, date_range, datetime_to_str
from jieba_based import Composer_jieba
from keyword_usertag_report import keyword_usertag_report, delete_expired_rows
import jieba.analyse
import paramiko
from ecom_usertag import update_ec_usertag
from keyword_missoner import fetch_all_dict,fetch_while_list_keywords
from mallbrands_custom import update_usertag_member
def clean_keyword_list(keyword_list, stopwords, stopwords_missoner):
    keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords)  ## remove stopwords
    keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords_missoner)  ## remove stopwords
    keyword_list = Composer_jieba().filter_quantifier(keyword_list)  ## remove number+quantifier, ex: 5.1萬
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9]{2}")  ## remove 2 digit number
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9.]*")  ## remove floating
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[a-z]{1,4}|[A-Z]{2}")  ## remove 1-4 lowercase letter and 2 Upper
    keyword_list = [keyword for keyword in keyword_list if keyword != ''] ## remove blank
    return keyword_list

@timing
def fetch_white_list_keywords():
    query = f"""SELECT name FROM BW_list where property=1"""
    print(query)
    data = DBhelper('missioner', is_ssh=True).ExecuteSelect(query)
    white_list = [d[0] for d in data]
    return white_list


@timing
def fetch_usertag_web_id_ex_day():
    query = "SELECT web_id, usertag_keyword_expired_day FROM web_id_table where usertag_keyword_enable=1"
    print(query)
    data = DBhelper('dione').ExecuteSelect(query)
    web_id_all = [d[0] for d in data]
    expired_day_all = [d[1] for d in data]
    return web_id_all, expired_day_all

def fetch_token(web_id):
    query = f"""SELECT registation_id,uuid,os_platform,is_fcm FROM web_gcm_reg WHERE web_id='{web_id}' order by id desc limit 10000"""
    data = DBhelper('cloud_subscribe', is_ssh=True).ExecuteSelect(query)
    data = pd.DataFrame(data, columns=['token','uuid','os_platform','is_fcm'])
    return data

@timing
def fetch_browse_record_yesterday_join(web_id, is_df=False, is_UTC0=False):
    date_start = get_yesterday(is_UTC0=is_UTC0)
    date_end = get_today(is_UTC0=is_UTC0) - datetime.timedelta(seconds=1)
    query = \
        f"""
            SELECT 
            s.uuid,
            t.code,
            t.registation_id AS token,
            s.article_id,
            l.title,
            l.content,
            l.keywords
        FROM
            subscriber_browse_record s
                INNER JOIN
            article_list l ON s.article_id = l.signature                
                AND s.web_id = '{web_id}'                
                AND s.click_time BETWEEN '{date_start}' AND '{date_end}'
                AND l.web_id = '{web_id}'
                INNER JOIN         
            token_index t ON t.uuid = s.uuid
                AND t.invalid = 0
                AND t.web_id = '{web_id}'            
        """
    print(query)
    data = DBhelper('dione').ExecuteSelect(query)
    if is_df:
        df = pd.DataFrame(data, columns=['web_id', 'uuid', 'token', 'article_id', 'title', 'content', 'keywords'])
        return df
    else:
        return data
def str_to_timetamp(s):
    return datetime.datetime.timestamp(datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'))

@timing
def fetch_browse_record_join(web_id, date, is_df=False):
    date_start = to_datetime(date)
    date_end = date_start - datetime.timedelta(days=-1, seconds=1)  ## pixnet, upmedia, ctnews, cmoney,
    date_end = str_to_timetamp(str(date_end))
    date_start = str_to_timetamp(str(date_start))

    query = \
        f"""
            SELECT 
            s.web_id,
            s.uuid,
            s.article_id,
            l.title,
            l.content,
            l.keywords
        FROM
            subscriber_browse_record_new s
                INNER JOIN
            article_list l ON s.article_id = l.signature                
                AND s.web_id = '{web_id}'                
                AND s.timetamps BETWEEN '{date_start}' AND '{date_end}'
                AND l.web_id = '{web_id}'        
        """
    print(query)
    data = DBhelper('dione').ExecuteSelect(query)
    if is_df:
        df = pd.DataFrame(data, columns=['web_id','uuid', 'article_id', 'title', 'content', 'keywords'])
        return df
    else:
        return data

@logging_channels(['clare_test'])
@timing
def main_update_subscriber_usertag(web_id, date, is_UTC0, jump2gcp, expired_day, jieba_base, stopwords, stopwords_usertag,all_dict_set,
                                   is_save=False, delete_expired_report=False):
    ## fetch subscribed browse record
    # data = fetch_browse_record_yesterday_join(web_id, is_df=False, is_UTC0=is_UTC0)
    expired_date = get_date_shift(date_ref=date, days=-expired_day, to_str=True,
                                  is_UTC0=is_UTC0)  ## set to today + 3 (yesterday+4), preserve 4 days
    data = fetch_browse_record_join(web_id, date=date, is_df=True)
    n_data = len(data)
    if n_data == 0:
        print('no valid data in dione.subscriber_browse_record')
        return pd.DataFrame(), pd.DataFrame()
    print('token_df_start!')
    token_df = fetch_token(web_id)
    print('token_df_ok!')
    data = data.merge(token_df, on='uuid', how='left').dropna()
    n_data = len(data)
    if n_data == 0:
        print('no valid data in dione.subscriber_browse_record')
        return pd.DataFrame(), pd.DataFrame()
    data['code'] = data['os_platform'] + data['is_fcm'].astype('int').astype('str')
    ## build usertag DataFrame
    j, data_save = 0, {}
    for row in data.iterrows():
        web_id,uuid, article_id, title, content, keywords, token, _, _,code = row[-1]
        news = title + ' ' + content
        ## pattern for removing https
        news_clean = jieba_base.filter_str(news, pattern="https:\/\/([0-9a-zA-Z.\/]*)")
        ## pattern for removing symbol, -,+~.
        news_clean = jieba_base.filter_symbol(news_clean)
        if (keywords == '') | (keywords == '_'):
            keyword_list = jieba.analyse.extract_tags(news_clean, topK=80)
            keyword_list = [i for i in keyword_list if i in all_dict_set]
            keyword_list = clean_keyword_list(keyword_list, stopwords, stopwords_usertag)[:8]
            keywords = ','.join(keyword_list)  ## add keywords
            is_cut = 1
        else:
            keyword_list = [k.strip() for k in keywords.split(',')]
            keyword_list = clean_keyword_list(keyword_list, stopwords, stopwords_usertag)
            is_cut = 0
        for keyword in keyword_list:
            data_save[j] = {'web_id': web_id, 'uuid': uuid, 'code': code, 'token': token, 'cert_web_id': web_id,
                            'news': news_clean, 'keywords': keywords, 'usertag': keyword, 'article_id': article_id,
                            'expired_date': expired_date, 'is_cut': is_cut}
            j += 1

    ## build DataFrame
    df_map = pd.DataFrame.from_dict(data_save, "index")
    ## filter nonsense data
    df_map = df_map[df_map.usertag != '']

    ## drop unused columns and drop duplicates, and save to db
    df_map_save = df_map.drop(columns=['news', 'keywords']).drop_duplicates(subset=['web_id','usertag','uuid','article_id'])
    if is_save:
        DBhelper.ExecuteUpdatebyChunk(df_map_save, db='missioner', table='usertag', chunk_size=100000, is_ssh=jump2gcp)
    ## delete expired data
    # delete_expired_rows(web_id, table='usertag', is_UTC0=is_UTC0, jump2gcp=jump2gcp)
    ### prepare keyword_usertag_report
    df_freq_token = keyword_usertag_report(web_id, expired_date, usertag_table='usertag', report_table='usertag_report',
                                           is_UTC0=is_UTC0, jump2gcp=jump2gcp, is_save=is_save,
                                           delete_expired_report=delete_expired_report)

    return df_map_save, df_freq_token



if __name__ == '__main__':
    ## set is in UTC+0 or UTC+8
    is_UTC0 = check_is_UTC0()
    jump2gcp = True
    date = get_yesterday(is_UTC0=is_UTC0) ## compute all browsing record yesterday ad 3:10 o'clock
    date = date -datetime.timedelta(1)
    date_list = [date]
    # date_list = [datetime_to_str(date) for date in date_range('2022-03-03', 4)]
    # date_list = ['2022-02-21', '2022-02-22', '2022-02-23', '2022-02-24']
    ## set up config (add word, user_dict.txt ...)
    jieba_base = Composer_jieba()
    jieba_base.set_config() ## add all user dictionary (add_words, google_trend, all_hashtag)
    #  add white list keyword
    white_list = fetch_white_list_keywords()
    jieba_base.add_words(white_list)

    ## set up media
    white_dict, all_keyword_list = fetch_while_list_keywords()
    all_dict_set = fetch_all_dict(jieba_base ,all_keyword_list)
    #  get stopwords
    stopwords = jieba_base.get_stopword_list()
    stopwords_usertag = jieba_base.read_file('./jieba_based/stop_words_usertag.txt')

    web_id_all, expired_day_all = fetch_usertag_web_id_ex_day()
    # web_id_all = ['btnet'] #btnet
    # expired_day_all = [4]
    ## get expired_date
    for date in date_list:
        for web_id, expired_day in zip(web_id_all, expired_day_all):
            main_update_subscriber_usertag(web_id, date, is_UTC0,
                                            jump2gcp, expired_day,
                                            jieba_base, stopwords,
                                            stopwords_usertag,
                                            all_dict_set,
                                            is_save=True,
                                            delete_expired_report=True)


    update_ec_usertag(jieba_base,stopwords,stopwords_usertag,all_dict_set)
    update_usertag_member()

    # connect to server
    config = DBhelper._read_config()["mysql"]["missoner_screen"]
    con = paramiko.SSHClient()
    con.load_system_host_keys()

    con.connect(config["MYSQL_HOST"], username=config["MYSQL_USER"], password=config["MYSQL_PASSWORD"])
    stdin, stdout, stderr = con.exec_command("screen -dmS wrapping_token bash -c 'python3 /var/www/html/cron_job/process_ta_token_json_csv.py'")