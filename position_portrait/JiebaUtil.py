import os
import re
import jieba
import jieba.analyse
import jieba.posseg as pseg
import codecs


def segmentation(partition):
    JIEBA_PATH = 'words'

    load_keywords(JIEBA_PATH)  # 加载关键词
    stopwords_list = get_stopwords_list(JIEBA_PATH)  # 加载停用词

    for row in partition:
        words = cut_sentence(row.description, stopwords_list)
        yield row.id, row.district, row.positionfirstcategory, words


def load_keywords(root):
    if not jieba.dt.initialized:
        keyword_path = os.path.join(root, "ITKeywords.txt")
        jieba.load_userdict(keyword_path)


def get_stopwords_list(root):
    stopwords_path = os.path.join(root, "stopwords.txt")
    return [i.strip() for i in codecs.open(stopwords_path, encoding='UTF-8').readlines()]


def cut_sentence(sentence, stopwords_list):
    seg_list = pseg.lcut(sentence)
    seg_list = [i for i in seg_list if i.flag not in stopwords_list]
    filtered_words_list = []
    for seg in seg_list:
        if len(seg.word) <= 1:
            continue
        elif seg.flag == "eng":
            if len(seg.word) <= 2:
                continue
            else:
                filtered_words_list.append(seg.word)
        elif seg.flag.startswith("n"):
            filtered_words_list.append(seg.word)
        elif seg.flag in ["x", "eng"]:
            filtered_words_list.append(seg.word)
    return filtered_words_list
