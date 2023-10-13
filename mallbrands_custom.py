import pandas as pd
from db import DBhelper
import datetime

def count_unique(data_dict):
    for key, value in data_dict.items():
        data_dict[key] = len(set(value))
    return data_dict
def update_usertag_member():
    web_id_list = ['mallbrands']
    expired_date = datetime.date.today() + datetime.timedelta(days=7)
    for web_id in web_id_list:
        query = f"""SELECT web_id,registation_id,uuid,os_platform,is_fcm,newmember FROM web_gcm_reg where web_id = '{web_id}' and memo = '0'"""
        data = DBhelper('cloud_subscribe', is_ssh=True).ExecuteSelect(query)
        data = pd.DataFrame(data, columns=['web_id','token','uuid','os_platform','is_fcm','member'])
        data = data[data.apply(lambda x:x.member !='_',axis=1)]
        data['code'] = data['os_platform'] + data['is_fcm'].astype('int').astype('str')
        data['usertag'] = data.apply(lambda x:'新客' if x.member == '1' else '舊客', axis=1)
        data['expired_date'] = expired_date
        data.drop(['os_platform','is_fcm','member'],axis=1,inplace=True)
        DBhelper.ExecuteUpdatebyChunk(data, db='missioner', table='usertag_member', chunk_size=100000, is_ssh=True)
        usertag_dict, token_dict, uuid_dict = {}, {}, {}
        usertags, tokens, uuids = list(data['usertag']), list(data['token']), list(data['uuid'])
        L = len(usertags)
        i = 0
        for usertag, token, uuid in zip(usertags, tokens, uuids):
            if usertag not in usertag_dict.keys():  # add a set
                usertag_dict[usertag] = 1
                token_dict[usertag] = [token]
                uuid_dict[usertag] = [uuid]
            else:
                usertag_dict[usertag] += 1
                token_dict[usertag] += [token]
                uuid_dict[usertag] += [uuid]
            i += 1
            if i % 10000 == 0:
                print(f"finish add counting, {i}/{L}")
        token_dict = count_unique(token_dict)
        uuid_dict = count_unique(uuid_dict)
        ## build a dict to save to Dataframe (faster version for adding components)
        data_save = {}
        i = 0
        for usertag, term_freq in usertag_dict.items():
            data_save[i] = {'web_id': web_id, 'usertag': usertag, 'term_freq': term_freq,
                            'token_count': token_dict[usertag], 'uuid_count': uuid_dict[usertag],
                            'expired_date': expired_date, 'enable': 1}
            i += 1
        df_freq_token = pd.DataFrame.from_dict(data_save, "index")
        df_freq_token[['term_freq', 'token_count', 'uuid_count']] = df_freq_token[['term_freq', 'token_count', 'uuid_count']].astype('int')
        DBhelper.ExecuteUpdatebyChunk(df_freq_token, db='missioner', table='usertag_report', chunk_size=100000, is_ssh=True)

    return

if __name__ == '__main__':
    update_usertag_member()