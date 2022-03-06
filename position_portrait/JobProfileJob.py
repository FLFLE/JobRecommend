# coding:utf-8

from pyspark.sql import HiveContext
from pyspark.sql.functions import *

from SparkSession import SparkSessionBase


class JobProfileJob(SparkSessionBase):
    SPARK_URL = "local"
    SPARK_APP_NAME = 'JobProfileJob'
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

    def start(self):
        hcx = HiveContext(self.spark.sparkContext)
        idf_keyword_df = hcx.table('talents.ods_keywords')
        textrank_keyword_df = hcx.table('talents.position_textrank_keywords_result')

        position_weights_df = textrank_keyword_df.join(idf_keyword_df,
                                                       textrank_keyword_df['keyword'] == idf_keyword_df['keyword']) \
            .withColumn('weights', textrank_keyword_df['textrank'] * idf_keyword_df['idf']) \
            .select(textrank_keyword_df['position_id'], textrank_keyword_df['region'], textrank_keyword_df['keyword'],
                    'weights') \
            .groupby(col('position_id')) \
            .agg(min('region').alias('region'), collect_list('keyword').alias('keywords'),
                 collect_list('weights').alias('weights'))

        position_keywords_idf = position_weights_df.rdd.map(
            lambda row: (row.position_id, row.region, dict(zip(row.keywords, row.weights)))) \
            .toDF(['position_id', 'region', 'keywords_idf'])

        position_keywords_idf.show()

        position_keywords_df = idf_keyword_df.join(textrank_keyword_df,
                                                   idf_keyword_df.keyword == textrank_keyword_df.keyword, 'inner') \
            .groupby(textrank_keyword_df.position_id) \
            .agg(collect_set(textrank_keyword_df.keyword).alias('keywords'))

        result = position_keywords_idf.join(position_keywords_df,
                                            position_keywords_idf['position_id'] == position_keywords_df['position_id']) \
            .select(position_keywords_idf['position_id'], position_keywords_idf['region'],
                    position_keywords_idf['keywords_idf'], position_keywords_df['keywords'])

        result.write.insertInto('talents.position_profile')


if __name__ == '__main__':
    JobProfileJob().start()