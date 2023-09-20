import pandas as pd
import datetime
from db import DBhelper
from basic import get_date_shift, get_yesterday, to_datetime, get_today, check_is_UTC0, timing, logging_channels, date_range, datetime_to_str
from jieba_based import Composer_jieba
import jieba.analyse
from keyword_usertag_report import keyword_usertag_report, delete_expired_rows
import paramiko
from urllib import parse
import re
from tqdm import tqdm
import hashlib
from ecom_usertag import update_ec_usertag
from keyword_missoner import fetch_all_dict
from update_pageview_hour_report import pageveiw_hour


class usertag_only_uuid():

    def __init__(self):
        self.setting_jieba()
        self.today = str(datetime.datetime.utcnow() + datetime.timedelta(hours=8) - datetime.timedelta(hours=24))
        self.yesterday = str(datetime.datetime.utcnow() + datetime.timedelta(hours=8 - 48))

        self.usertag_web_id_dict = self.fetch_missoner_web_id_dict()
        self.fetch_webid_rule_usertag(list(self.usertag_web_id_dict.keys()))
    def fetch_missoner_web_id_dict(self):
        qurey = f"SELECT web_id FROM ecom_web_id_table x WHERE uuid_enable = 1"
        data = DBhelper('missioner',is_ssh=False).ExecuteSelect(qurey)
        return {i[0]:1 for i in data}

    def str_to_timetamp(self,s):
        return datetime.datetime.timestamp(datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'))
    def fetch_browse_record_join(self,web_id, date, is_df=False):
        date_start = to_datetime(date)
        date_end = date_start - datetime.timedelta(days=-1, seconds=1)  ## pixnet, upmedia, ctnews, cmoney,
        date_end = self.str_to_timetamp(str(date_end))
        date_start = self.str_to_timetamp(str(date_start))

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

    def fetch_ecom_history(self,web_id):
        query=f"""SELECT uuid,timestamp,url_now FROM tracker.clean_event_load
            where date_time between '{self.yesterday}' and '{self.today}' AND web_id='{web_id}'  
            """
        data = DBhelper('cdp').ExecuteSelect(query)
        return pd.DataFrame(data,columns=['uuid','timetamps','url_now'])
    def get_report(self,report,web_id):
        report['product_id'] = report.apply(lambda x: self.fetch_url_encoder(web_id, x['url_now']), axis=1)
        report = report[report['product_id'] != '_']
        report = report[report['product_id'] != web_id]
        report.drop_duplicates('product_id', inplace=True)
        return report
    def fetch_webid_rule_usertag(self, web_id_list):
        qurey = f"""SELECT web_id, url_split, pattern, url_modify_rule, filter_rule, condition_rule, signature_rule FROM web_id_url_encoder_rule WHERE web_id IN ('{"','".join(web_id_list)}')"""
        url_en = DBhelper('jupiter_new').ExecuteSelect(qurey)
        self.web_id_to_pattern_usertag_dict = {web_id: {'pattern': pattern, 'url_modify_rule':url_modify_rule, 'filter_rule': filter_rule,
                     'signature_rule': signature_rule, 'condition_rule': condition_rule, 'url_split': url_split} for
            web_id, url_split, pattern, url_modify_rule, filter_rule, condition_rule, signature_rule in url_en}
        return


    def fetch_url_encoder(self, web_id, url):
        if self.usertag_web_id_dict.get(web_id) == 1:
            url = parse.unquote(url)
        try:
            finding = re.findall(self.web_id_to_pattern_usertag_dict[web_id]['pattern'], url)
        except:
            print(f'{web_id}_no_rule')
            return '_'
        find = re.findall(web_id, url)
        if not finding:
            if find:
                return web_id
            else:
                return '_'
        try:
            signature = eval(self.web_id_to_pattern_usertag_dict[web_id]['signature_rule'])
        except:
            if find:
                return web_id
            else:
                return '_'
            return '_'

        if len(signature.encode()) > 60:
            encoder = hashlib.sha256()
            encoder.update((signature).encode())
            ecoded_signature = web_id + '_' + encoder.hexdigest()
        else:
            ecoded_signature = web_id + '_' + signature
        return ecoded_signature

    def clean_keyword_list(self,keyword_list):
        keyword_list = Composer_jieba().clean_keyword(keyword_list, self.stopwords)  ## remove stopwords
        keyword_list = Composer_jieba().clean_keyword(keyword_list, self.stopwords_missoner)  ## remove stopwords
        keyword_list = Composer_jieba().filter_quantifier(keyword_list)  ## remove number+quantifier, ex: 5.1萬
        keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9]{2}")  ## remove 2 digit number
        keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9.]*")  ## remove floating
        keyword_list = Composer_jieba().filter_str_list(keyword_list,pattern="[a-z]{1,4}|[A-Z]{2}")  ## remove 1-4 lowercase letter and 2 Upper
        keyword_list = [keyword for keyword in keyword_list if keyword != '']  ## remove blank
        return keyword_list
    def fetch_ecom_df(self,web_id):
        qurey = f"SELECT web_id,signature,title,content From ecom_table where web_id = '{web_id}'"
        data = DBhelper('jupiter_new').ExecuteSelect(qurey)
        columns = ['web_id', 'product_id', 'title', 'content']
        df_hot = pd.DataFrame(data=data, columns=columns)
        df_hot['keywords'] = '_'
        if web_id == 'kfan':
            df_hot['article_id'] = df_hot.apply(lambda x: x.article_id.split('&')[0], axis=1)
            df_hot.drop_duplicates(['article_id'], inplace=True)
        return df_hot

    def generate_keyword_list(self,row):
        ## process keyword ##
        keywords = row['keywords']
        # news = row['title'] + ' ' + row['content']
        news = row['title'] + row['content']
        news_clean = self.jieba_base.filter_str(news, pattern="https:\/\/([0-9a-zA-Z.\/]*)")  ## pattern for https
        news_clean = self.jieba_base.filter_symbol(news_clean)
        if (keywords == '') | (keywords == '_') | len(keywords.split(',')) < 2:
            keyword_list = jieba.analyse.extract_tags(news_clean, topK=10)
            keyword_list = [i for i in keyword_list if i in self.all_dict_set]
            keyword_list = self.clean_keyword_list(keyword_list)[:5]
            keywords = ','.join(keyword_list)  ## add keywords
            is_cut = 1
        else:
            keyword_list = [k.strip() for k in keywords.split(',')]
            keyword_list = [i for i in keyword_list if i in self.all_dict_set]
            keyword_list = self.clean_keyword_list(keyword_list)
            is_cut = 0
        return keywords, keyword_list, is_cut
    def setting_jieba(self):
        self.jieba_base = Composer_jieba()
        self.jieba_base.set_config()
        self.stopwords = self.jieba_base.get_stopword_list()
        self.stopwords_missoner = self.jieba_base.read_file('./jieba_based/stop_words_usertag.txt')
        self.all_dict_set = fetch_all_dict(self.jieba_base)
    def update_usertag_only(self):
        for web_id,web_id_type in tqdm(self.usertag_web_id_dict.items()):
            df = pd.DataFrame(columns=['web_id','uuid','usertag','article_id','timetamps','date'])
            if web_id_type == 1:
                data = self.fetch_ecom_history(web_id)
                if len(data) == 0:
                    print(f'{web_id}_no_data')
                    continue
                data = self.get_report(data,web_id)
                article_data = self.fetch_ecom_df(web_id)
                df_hot = pd.merge(data, article_data)
                for index, row in df_hot.iterrows():
                    keywords, keyword_list, is_cut = self.generate_keyword_list(row)
                    for keyword in keyword_list:
                        df.loc[len(df)] = [web_id,row['uuid'],keyword,row['product_id'],row['timetamps'],self.today]
                DBhelper.ExecuteUpdatebyChunk(df, db='missioner', table='usertag_uuid_new', chunk_size=100000,is_ssh=False)

if __name__ == '__main__':
    usertag_uuid = usertag_only_uuid()
    usertag_uuid.update_usertag_only()
