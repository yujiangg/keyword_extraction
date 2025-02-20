from definitions import ROOT_DIR
from openai import AzureOpenAI
#from jieba_utils import Composer_jieba
import jieba
import jieba.analyse
import json
import re, os
from dotenv import load_dotenv
load_dotenv()


class Jieba_ChatGPT:
    def __init__(self): ##
        # self.web_id = web_id
        self.web_id_Vietnam = ['thanhnien2021', 'tuoitrexahoi']
        self.web_id_with_hash_tag = ['ctnews', 'mirrormedia', 'upmedia', 'btnet', 'bnext',
                                        'dailyview', 'moneyweekly', 'nongnong', 'newscts', 'newtalk',
                                        'setn', 'nownews','ftvnews']
        # self.ROOT_DIR = ROOT_DIR
        self.AZURE_client = self.initialize_azure_openai()

    def get_client(self):
        return self.AZURE_client

    def initialize_azure_openai(self):
        AZURE_client = AzureOpenAI(
            azure_endpoint=os.getenv('azure_endpoint'),
            api_key=os.getenv('api_key'),
            api_version=os.getenv('api_version')
        )
        return AZURE_client

    # use chatGPT to find new word 
    def find_new_words(self, original_text, cut_text, model="chat-cs-canada-4o-mini", temperature=0, top_p=0.2, presence_penalty=0, max_tokens = 500, frequency_penalty = 0.5):
        prompt = (
            f"以下是作為網路新聞的原文：\n\"{original_text}\"\n"
            f"跟一個已經被分詞的中文詞彙列表：\n\"{cut_text}\"\n"
            "提取原文的關鍵字，將被分詞的中文詞彙列表中不合理的詞彙替換掉。"
            "最後統整成純關鍵字的list，不要解釋。"
            "返回格式必須是json且與以下示例一致："
            "{\"keyword_list\": [\"日本語\", \"ください\"]}"
        )
        response = self.AZURE_client.chat.completions.create(
            model = model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "你是一個專業的中文文本分析助手，專門進行關鍵字提取與詞彙修正。"},
                {"role": "user", "content": prompt}
            ],
            temperature = temperature,  # 降低創造性，確保結果更精確
            top_p = top_p,  # 降低隨機性，使輸出更加穩定
            presence_penalty = presence_penalty,  # 不鼓勵引入新的、不在原文中的詞彙
            # max_tokens = max_tokens,  # 關鍵字列表通常不需要太長
            # frequency_penalty = frequency_penalty,  # 避免某些詞重複出現過多
        )
        result = json.loads(response.choices[0].message.content)['keyword_list']
        
        # if result == None:
        #     return result
        # else:
        #     # 使用正則表達式去除數字和點
        #     result = [re.sub(r"^(?:\d+\.\s*|-+\s*)", "", line) for line in result.split("\n")]
        return result
    

        
if __name__ == '__main__':
    
    jieba_chatgpt = Jieba_ChatGPT()
    #jieba_base = Composer_jieba()
    
    allow_pos = [
            # 名詞
            'n',  # 普通名詞（如：學校、電腦）
            'nr',  # 人名（如：李小龍、馬斯克）
            'nrfg',  # 古代人名
            'nrt',  # 轉譯人名（如：特朗普）
            'ns',  # 地名（如：北京、台北）
            'nt',  # 機構團體（如：聯合國、微軟公司）
            'nz',  # 其他專有名詞（如：蘋果手機）
            'nl',  # 名詞性慣用語（如：百花齊放）
            'ng',  # 名詞性語素（如：文化中的“文”）

            # 動詞
            'v',  # 普通動詞（如：跑、寫、吃）
            'vd',  # 動作動詞（如：喜愛、思考）
            'vn',  # 名動詞（如：發展、學習）
            'vshi',  # 動詞“是”
            'vyou',  # 動詞“有”
            'vf',  # 趨向動詞（如：起來、下去）
            'vx',  # 非謂語動詞
            'vi',  # 不及物動詞（如：睡、走）
            'vl',  # 動詞性慣用語（如：將就、依靠）
            'vg',  # 動詞性語素（如：購於“購物”）

            # 形容詞
            'a',  # 普通形容詞（如：美麗、巨大）
            'ad',  # 副形

            'x',  # 非語素字（如：哦、嗯）
        ]


    news = '卵巢癌完善治療 多部科協同是關鍵 | 健康 | NOWnews今日新聞,【健康醫療網／記者曾正豪報導】50歲王小姐因腹痛6個月，而且一直有解便不順的困擾，經超音波檢查後發現是卵巢長了4公分的巧克力囊腫，於是接受了腹腔鏡患側卵巢輸卵管...'

    extract_words = "/".join(jieba.analyse.extract_tags(news, allowPOS=allow_pos))
    words = jieba_chatgpt.find_new_words(news, extract_words)

    # add new words 
    #jieba_base.add_words(words)

    ########################### Test start ###########################
    new_news = '提升晚期卵巢癌5年存活率完整手術是重要關鍵。【記者伧是旺報導】卵巢癌是婦癌中最為棘手的疾病，素有沉默殺手的稱號。新診斷個案75%屬於晚期疾病(第三期或第四期)，最有效的治療方式為完善的切除手術，婦癌專科醫師專精的手術技術、以及跨團隊外科醫師的協助；盡力將腫瘤切除乾淨，是晚期卵巢癌之5年存活率提升重要關鍵。'
    c = jieba.lcut(new_news, cut_all = False)
    result_1 = "/".join(jieba.analyse.extract_tags(new_news))
    
    print(f"修正後的分詞: \n{c}")
    print(f"修正後的關鍵字: {result_1}")
    ###########################  Test end  ###########################