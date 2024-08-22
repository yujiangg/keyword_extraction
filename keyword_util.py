from jieba_based.jieba_utils import Composer_jieba
from pyvi import ViTokenizer, ViPosTagger
import yake
from pythainlp.tokenize import word_tokenize
from pythainlp.corpus import thai_stopwords
from pythainlp.tag import pos_tag
from nltk.corpus import stopwords
from db import DBhelper
import re
import os
import nltk
import stanza
import jieba.analyse
import collections

#  中文切詞
jieba_base = Composer_jieba()
jieba_base.set_config()
chinese_stopwords = jieba_base.get_stopword_list()
stopwords_missoner = jieba_base.read_file('./jieba_based/stop_words_usertag.txt')



def clean_keyword_list(keyword_list,stopwotds_db):
    keyword_list = Composer_jieba().clean_keyword(keyword_list, chinese_stopwords)  ## remove stopwords
    keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords_missoner)  ## remove stopwords
    keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwotds_db)
    keyword_list = Composer_jieba().filter_quantifier(keyword_list)  ## remove number+quantifier, ex: 5.1萬
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9]{2}")  ## remove 2 digit number
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9.]*")  ## remove floating
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[a-z]{1,4}|[A-Z]{2}")  ## remove 1-4 lowercase letter and 2 Upper
    keyword_list = [keyword for keyword in keyword_list if keyword != ''] ## remove blank
    return keyword_list


def fetch_google_ads_keyword():
    qurey = "SELECT keyword FROM keyword_value WHERE low_price != 0"
    data = DBhelper('gads', is_ssh=True).ExecuteSelect(qurey)
    return [i[0] for i in data]


def fetch_while_list_keywords():
    while_dict = collections.defaultdict(list)
    query = f"""SELECT name,web_id FROM BW_list where property=1"""
    data = DBhelper('dione').ExecuteSelect(query)
    all_keyword = []
    for key, web_id in data:
        if web_id == 'all':
            all_keyword.append(key)
            continue
        while_dict[web_id].append(key)
    return while_dict,all_keyword

def fetch_black_list_keywords():
    black_dict = collections.defaultdict(list)
    query = f"""SELECT name,web_id FROM BW_list where property=0"""
    data = DBhelper('dione').ExecuteSelect(query)
    for key,web_id in data:
        black_dict[web_id].append(key)
    q = f"SELECT web_id,name FROM missoner_web_id_table WHERE enable = 1"
    data = DBhelper('dione').ExecuteSelect(q)
    for web_id, key in data:
        black_dict[web_id].extend(eval(key))
    return black_dict

white_dict, all_keyword_list = fetch_while_list_keywords()
black_dict = fetch_black_list_keywords()

def fetch_all_dict():
    f1 = open('./jieba_based/add_words.txt')
    text1 = set()
    for line in f1:
        text1.add(line.split('\n')[0])
    f2 = open('./jieba_based/user_dict.txt')
    text2 = set()
    for line in f2:
        text2.add(line.split('\n')[0])
    text3 = jieba_base.fetch_gtrend_keywords()
    text3 = set(text3)
    text5 = fetch_google_ads_keyword()
    text5 = set(text5)
    all_keyword_set = set.union(text1,text3,text5)
    all_keyword_set = set.union(all_keyword_set, set(all_keyword_list))
    return all_keyword_set


all_dict_set = fetch_all_dict()


def get_chinese(text, web_id):
    white_list = white_dict.get(web_id, [])
    black_list = black_dict.get(web_id, [])
    all_dict = set.union(all_dict_set, set(white_list))
    news_clean = jieba_base.filter_str(text, pattern="https:\/\/([0-9a-zA-Z.\/]*)")  ## pattern for https
    news_clean = jieba_base.filter_symbol(news_clean)
    keyword_list = jieba.analyse.extract_tags(news_clean, topK=10)
    keyword_list = [i for i in keyword_list if i in all_dict]
    keyword_list = clean_keyword_list(keyword_list, black_list)[:5]
    keywords = ','.join(keyword_list)
    return keyword_list, keywords



# 英文切詞

custom_kw_extractor = yake.KeywordExtractor(lan="en", n=2, dedupLim=0.1, top=20, features=None)


def get_eng(text):
    keyword_res = custom_kw_extractor.extract_keywords(text)
    keyword_list = [i[0] for i in keyword_res]
    keywords = ','.join(keyword_list)
    return keyword_list, keywords


#  越南切詞
def get_vina(text):
    tokens = ViTokenizer.tokenize(text)
    pos_tags_vi = ViPosTagger.postagging(tokens)
    keyword_list = [word for word, pos in zip(pos_tags_vi[0], pos_tags_vi[1]) if pos in ['N', 'Np', 'A']]
    keyword_list = list(set(keyword_list))
    keywords = ','.join(keyword_list)
    return keyword_list, keywords


# 印尼文切詞
def initialize_stanza():
    stanza_model_dir = './stanza_resources'
    stanza_dir = os.path.abspath(stanza_model_dir)
    if not os.path.exists(stanza_model_dir):
        try:
            stanza.download('id', model_dir=stanza_model_dir)
        except Exception as e:
            print(f"Error downloading Stanza model: {e}")
            raise
    nlp = stanza.Pipeline('id', model_dir=stanza_model_dir, processors='tokenize,pos')
    return nlp


indonesian_stopwords = set(stopwords.words('indonesian'))
nlp = initialize_stanza()


def extract_keywords_with_pos(pos_tags, target_pos=['NOUN']):
    keywords = [word for word, pos in pos_tags if pos in target_pos and word not in indonesian_stopwords]
    freq_dist = nltk.FreqDist(keywords)
    top_keywords = freq_dist.most_common(10)  # 取前10个高频词
    return top_keywords


def pos_tagging(text):
    doc = nlp(text)
    pos_tags = [(word.text, word.upos) for sentence in doc.sentences for word in sentence.words]
    return pos_tags


def get_ind(text):
    pos_tags = pos_tagging(text)
    keyword_list = extract_keywords_with_pos(pos_tags)
    keyword_list = list(set([i.lower() for i, v in keyword_list]))
    keywords = ','.join(keyword_list)
    return keyword_list, keywords


# 泰文切詞
thai_stopwords = set(thai_stopwords())


def get_thai(text):
    tokens = word_tokenize(text, engine='newmm')
    # 使用正則表達式過濾掉非泰文字
    thai_text = [word for word in tokens if re.fullmatch(r'[\u0E00-\u0E7F]+', word)]
    keyword_list = [word for word in thai_text if word not in thai_stopwords and word.strip() != '']
    tagged_words = pos_tag(keyword_list, engine='perceptron')
    keyword_list = list(set(a for a, b in tagged_words if b in ("NPRP", "NCMN", "VACT")))
    keywords = ','.join(keyword_list)
    return keyword_list, keywords









# tag_ec_pid_rel_list = []
# batch_size = 5000
# with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
#     futures = [executor.submit(process_row, v, product_ids_with_tags, tag_tensor, ecom_tag_embeding, id_type)
#                for i, v in df_order_detail.iterrows()]
#     for future in tqdm(concurrent.futures.as_completed(futures)):
#         result = future.result()
#         if result:
#             tag_ec_pid_rel_list.append(result)
#             if len(tag_ec_pid_rel_list) == batch_size:
#                 print("update_type_tag_5000")
#                 insert_into_db(tag_ec_pid_rel_list)
#                 tag_ec_pid_rel_list = []
# if tag_ec_pid_rel_list:
#     insert_into_db(tag_ec_pid_rel_list)