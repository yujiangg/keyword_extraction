from jieba_based.jieba_utils import Composer_jieba
import pickle





if __name__ == '__main__':
    jieba = Composer_jieba()
    all_hashtags = jieba.fetch_all_hashtags()
    with open(f'{jieba.ROOT_DIR}/jieba_based/all_hashtag.pickle', 'wb') as f:
        pickle.dump(all_hashtags, f)







