from db import DBhelper
import pickle
from jieba_based.jieba_utils import Composer_jieba


def fetch_google_ads_keyword():
    qurey = "SELECT keyword FROM keyword_value WHERE low_price != 0"
    data = DBhelper('gads',is_ssh = True).ExecuteSelect(qurey)
    return [i[0] for i in data]


if __name__ == '__main__':
    jieba = Composer_jieba()
    google_ads_keyword = fetch_google_ads_keyword()
    with open(f'{jieba.ROOT_DIR}/jieba_based/google_ads_keyword.pickle', 'wb') as f:
        pickle.dump(google_ads_keyword, f)