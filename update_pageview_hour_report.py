from AmazonS3 import AmazonS3
import json
from tqdm import tqdm
import os
import datetime
import pickle
from basic import date2int
from db import DBhelper
import collections
import pandas as pd
import re
from tqdm import tqdm
import hashlib
from slackwarningletter import slack_warning

class pageveiw_hour:
    def __init__(self):
        self.utc_now = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
        self.utc_date, self.utc_hour = self.utc_now.strftime('%Y-%m-%d,%H').split(',')
        self.tw_now = self.utc_now + datetime.timedelta(hours=8)
        self.tw_date, self.tw_hour = self.tw_now.strftime('%Y-%m-%d,%H').split(',')

        self.awsS3 = AmazonS3('elephants3')
        self.web_id_list = self.fetch_missoner_web_id_list()
        self.web_id_to_pattern_dict = self.fetch_webid_rule(self.web_id_list)
        self.domain_dict = self.fetch_domain_dict()
        self.domain_list = self.get_domain_list()
        self.objects = list(self.awsS3.getDateHourObjects(self.utc_date, int(self.utc_hour)))

    def main(self):
        data_dic = self.count_timepage_landing_bounce_exit(self.bulid_data_dic(self.objects))
        df = self.data_to_df(data_dic)
        return df

    def fetch_missoner_web_id_list(self):
        qurey = "SELECT web_id FROM missoner_web_id_table WHERE enable = 1"
        data = DBhelper('dione_2').ExecuteSelect(qurey)
        return [i[0] for i in data]

    def fetch_webid_rule(self, web_id_list):
        qurey = f"""SELECT web_id, url_split, pattern, url_modify_rule, filter_rule, condition_rule, signature_rule FROM web_id_url_encoder_rule WHERE web_id IN ('{"','".join(web_id_list)}')"""
        url_en = DBhelper('jupiter_new').ExecuteSelect(qurey)
        web_id_to_pattern_dict = {
            web_id: {'pattern': pattern, 'url_modify_rule': url_modify_rule, 'filter_rule': filter_rule,
                     'signature_rule': signature_rule, 'condition_rule': condition_rule, 'url_split': url_split} for
            web_id, url_split, pattern, url_modify_rule, filter_rule, condition_rule, signature_rule in url_en}
        return web_id_to_pattern_dict

    def fetch_url_encoder(self, web_id, url):
        finding = re.findall(self.web_id_to_pattern_dict[web_id]['pattern'], url)
        find = re.findall(web_id, url)
        if not finding:
            if find:
                return web_id
            else:
                return '_'
        try:
            signature = eval(self.web_id_to_pattern_dict[web_id]['signature_rule'])
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

    def fetch_source_domain_mapping(self, web_id):
        query = f"SELECT website_web_id FROM missoner_web_id_table where web_id='{web_id}' and enable ='1'"
        print(query)
        data = DBhelper('dione_2').ExecuteSelect(query)
        source_domain_mapping = [d[0] for d in data]
        return source_domain_mapping

    def fetch_domain_dict(self):
        domain_dict = {}
        for web_id in self.web_id_list:
            domain_dict[web_id] = self.fetch_source_domain_mapping(web_id)
        return domain_dict

    def get_domain_list(self):
        query = f"SELECT web_id FROM missoner_web_id_table where web_id_type = '3'"
        print(query)
        data = DBhelper('dione_2').ExecuteSelect(query)
        return [d[0] for d in data]

    def check_domain(self, url, web_id):
        # source_domain = fetch_source_domain_mapping(web_id)
        if not url:
            return 'None'

        for domain in self.domain_list:
            dm = re.findall(domain, url)
            if dm:
                return dm[0]
        for inter in self.domain_dict[web_id]:
            dm = re.findall(inter, url)
            if dm:
                return web_id
        return 'other'

    def str_to_timetamp(self, s):
        return datetime.datetime.timestamp(datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'))

    def bulid_data_dic(self, obj):
        data_dic = {i: collections.defaultdict(list) for i in self.web_id_list}
        for o in tqdm(obj):
            k = json.loads(self.awsS3.Read(o.key))
            for i in k:
                if i['web_id'] not in self.web_id_list:
                    continue
                if 'behavior_type' not in i and i['behavior_type'] != 'landing':
                    continue
                if 'uuid' not in i or i['uuid'] == '_':
                    continue
                if 'datetime' not in i:
                    continue
                if i['current_url'] == i['referrer_url']:
                    continue
                if i['title'] == '_':
                    continue
                ecoded_signature = self.fetch_url_encoder(i['web_id'], i['current_url'])
                if ecoded_signature == '_':
                    continue
                data_dic[i['web_id']][i['uuid']].append(
                    [i['web_id'], i['uuid'], ecoded_signature, i['current_url'], i['referrer_url'], i['datetime'],
                     self.check_domain(i['referrer_url'], i['web_id']), 0, 0, 0, 0])
        return data_dic

    def count_timepage_landing_bounce_exit(self, data_dic):
        ##[web_id,uuid,ecoded_signature,current_url,referrer_url,datetime,domain,time_on_page,lading,boune,exit]
        for web_id, web_data_dic in data_dic.items():
            if not web_data_dic:
                continue
            for uuid, pageview in web_data_dic.items():
                pageview = sorted(pageview, key=lambda x: self.str_to_timetamp(x[5]))
                L = len(pageview) - 1
                for i, view in enumerate(pageview):
                    if i == 0:
                        source = view[6]
                        view[-3] = 1
                        last_time = self.str_to_timetamp(view[5])
                        if i == L:
                            view[-1] = 1
                        continue
                    view[6] = source if source != 'None' else view[6]
                    now_time = self.str_to_timetamp(view[5])
                    pageview[i - 1][-4] = now_time - last_time
                    last_time = now_time
                    if i == L:
                        view[-1] = 1
                        continue
                    view[-2] = 1
        return data_dic

    def data_to_df(self, data_dic, hour=None):
        data = []
        for data_1 in data_dic.values():
            if not data_1:
                continue
            for data_2 in data_1.values():
                if not data_2:
                    continue
                for data_3 in data_2:
                    data.append(data_3)
        df = pd.DataFrame(data,
                          columns=['web_id', 'uuid', 'article_id', 'current_url', 'referrer_url', 'datetime',
                                   'source_domain', 'timeOnPage', 'landings', 'bounce', 'exits'])
        df1 = df.groupby(['web_id', 'article_id', 'source_domain']).sum()
        df2 = df.groupby(['web_id', 'article_id', 'source_domain'])['uuid'].count()
        df = pd.concat([df1, df2], axis=1).rename(columns={'uuid': 'pageviews'}).reset_index()
        df = df[df.source_domain != 'None']
        df['hour'] = self.tw_hour
        df['date'] = int(''.join(self.tw_date.split('-')))
        return df


if __name__ == '__main__':
    try:
        pageveiw = pageveiw_hour()
        df = pageveiw.main()
        DBhelper.ExecuteUpdatebyChunk(df, db='dione_2', table='pageviews_report_hour', chunk_size=100000,is_ssh=False)
    except:
        slack_letter = slack_warning()
        slack_letter.send_letter_test(f'pageviews_{datetime.datetime.utcnow()+datetime.timedelta(hours=8)}執行失敗')