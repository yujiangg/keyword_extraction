import pandas as pd
from db import DBhelper
import datetime


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
    return

if __name__ == '__main__':
    update_usertag_member()