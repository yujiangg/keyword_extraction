import datetime
import searchconsole
from db import DBhelper
import re
import pandas as pd

def fetch_url_encoder(web_id, url,web_id_to_pattern_dict):
    finding = re.findall(web_id_to_pattern_dict[web_id]['pattern'].lower(), url.lower())
    find = re.findall(web_id, url)
    if not finding:
        if find:
            return web_id
        else:
            return '_'
    try:
        signature = eval(web_id_to_pattern_dict[web_id]['signature_rule'])
    except:
        if find:
            return web_id
        else:
            return '_'
        return '_'
    if len(signature.encode()) > 60:
        encoder = hashlib.sha256()
        encoder.update((signature).encode())
    return signature

def fetch_webid_rule(web_id_list):
    qurey = f"""SELECT web_id, url_split, pattern, url_modify_rule, filter_rule, condition_rule, signature_rule FROM web_id_url_encoder_rule WHERE web_id IN ('{"','".join(web_id_list)}')"""
    url_en = DBhelper('jupiter_new').ExecuteSelect(qurey)
    web_id_to_pattern_dict = {
        web_id: {'pattern': pattern, 'url_modify_rule': url_modify_rule, 'filter_rule': filter_rule,
                 'signature_rule': signature_rule, 'condition_rule': condition_rule, 'url_split': url_split} for
        web_id, url_split, pattern, url_modify_rule, filter_rule, condition_rule, signature_rule in url_en}
    return web_id_to_pattern_dict

def find_similarity_product(web_id,product_id_list):
    df = pd.DataFrame(columns=['product_id','sim1','sim2','sim3'])
    for i,v in enumerate(product_id_list):
        if len(df) >= 20:
            return df
        query = f""" SELECT similarity_product_id  FROM web_push.item_similarity_table WHERE web_id ='{web_id}' and main_product_id ='{v}'  and text_similarity !=1 order by text_similarity desc limit 3 """
        url_en = DBhelper('zz',is_ssh=True).ExecuteSelect(query)
        if not url_en:
            continue
        df.loc[i] = [v] + [i[0] for i in url_en]
    return df


def get_report(webproperty,web_id,web_id_to_pattern_dict):
    report = webproperty.query.range('today', days=-7).dimension('query', 'page').get().to_dataframe()
    report = report[report['clicks'] > 1]
    report['product_id'] = report.apply(lambda x: fetch_url_encoder(web_id, x['page'],web_id_to_pattern_dict), axis=1)
    report = report[report['product_id'] != '_']
    report = report[report['product_id'] != web_id]
    report.drop_duplicates('product_id', inplace=True)
    return report

def get_web_id_url_pair():
    query = f""" SELECT web_id,web_id_site  FROM web_push.google_search_console_id_table"""
    data = DBhelper('zz',is_ssh=True).ExecuteSelect(query)
    return {web_id:url for web_id,url in data}

if __name__ == '__main__':
    account = searchconsole.authenticate(client_config='client_secrets.json', credentials='credentials.json')
    web_id_dict = get_web_id_url_pair()
    web_id_to_pattern_dict = fetch_webid_rule(web_id_dict.keys())
    date = str(datetime.date.today())
    for web_id,url in web_id_dict.items():
        print(web_id)
        webproperty = account[url]
        report = get_report(webproperty,web_id)
        report_sim = find_similarity_product(web_id,list(report.product_id))
        data = report.merge(report_sim, how='left', on='product_id')
        data.dropna(inplace=True)
        data['web_id'] = web_id
        data['rank'] = [i+1 for i in range(len(data))]
        data['date'] = date
        DBhelper.ExecuteUpdatebyChunk(data, db='zz', table='google_keyword_product_similer_rank', chunk_size=100000,is_ssh=True)
