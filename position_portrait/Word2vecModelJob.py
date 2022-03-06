# coding:utf-8
import os

from pyspark.sql import HiveContext
from pyspark.ml.feature import Word2Vec
from pyspark.sql.functions import *

import JiebaUtil
from SparkSession import SparkSessionBase


class Word2vecModelJob(SparkSessionBase):
    SPARK_URL = "local"
    SPARK_APP_NAME = 'Word2vecModelJob'
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

    def start(self):
        hcx = HiveContext(self.spark.sparkContext)
        position_df = hcx.table('talents.ods_position')

        words_df = position_df \
            .filter(col('description').isNotNull()) \
            .rdd \
            .mapPartitions(JiebaUtil.segmentation) \
            .toDF(['id', 'region', 'category', 'words'])

        words_df.show()
        words2vec = Word2Vec(vectorSize=100, inputCol='words', outputCol='feature', minCount=3)
        words2vec_model = words2vec.fit(words_df)
        words2vec_model.write().overwrite().save('model/words2vec.model')


if __name__ == '__main__':
    Word2vecModelJob().start()