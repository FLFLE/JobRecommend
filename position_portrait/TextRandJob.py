# coding:utf-8
from jieba.analyse import TextRank, textrank, extract_tags
from pyspark.ml.feature import CountVectorizer, HashingTF, IDF, CountVectorizerModel, IDFModel
from pyspark.sql.types import *
import JiebaUtil
import jieba
from jieba import posseg
from pyspark.sql.functions import when, col
from SparkSession import SparkSessionBase
from pyspark.sql import HiveContext
from pyspark.sql import functions as F, types as T


class TextRandJob(SparkSessionBase):
    SPARK_URL = "local"
    SPARK_APP_NAME = 'TextRandJob'
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

    def start(self):
        hcx = HiveContext(self.spark.sparkContext)
        df_position = hcx.table('talents.ods_position')
        words_df = df_position.filter(col('description').isNotNull())
        textrank_df = words_df.rdd.mapPartitions(textrank).toDF(
                    ['id', 'region', 'category', 'keyword', 'textrank'])
        textrank_df.show()
        textrank_df.write.insertInto('talents.position_textrank_keywords_result')


def textrank(partition):
    textrank_model = TextRank(window=5, word_min_len=2)
    allowPOS = ('n', "x", 'eng', 'nr', 'ns', 'nt', "nw", "nz", "c")
    for row in partition:
        tags = textrank_model.textrank(row.description, withWeight=True, allowPOS=allowPOS, withFlag=False)
        for tag in tags:
            yield row.id, row.district, row.positionfirstcategory, tag[0], tag[1]


class TextRank(jieba.analyse.TextRank):
    def __init__(self, window=20, word_min_len=2):
        super(TextRank, self).__init__()
        self.span = window  # 窗口大小
        self.word_min_len = word_min_len  # 单词的最小长度
        self.pos_filt = frozenset(('n', 'x', 'eng', 'f', 's', 't', 'nr', 'ns', 'nt', "nw", "nz", "PER", "LOC", "ORG"))
        self.stopwords_list = JiebaUtil.get_stopwords_list('words')

    def pairfilter(self, wp):
        if wp.flag == "eng":
            if len(wp.word) <= 2:
                return False
        if wp.flag in self.pos_filt and len(
                wp.word.strip()) >= self.word_min_len and wp.word.lower() not in self.stopwords_list:
            return True


if __name__ == '__main__':
    TextRandJob().start()
