from pyspark.sql import HiveContext
from pyspark.ml.feature import CountVectorizer, IDF, CountVectorizerModel, IDFModel
from pyspark.sql.functions import col


import JiebaUtil
from SparkSession import SparkSessionBase


class PositionTFIDFWriter(SparkSessionBase):
    SPARK_URL = "local"
    SPARK_APP_NAME = 'PositionTFIDFWriter'
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

    def process(self):
        hcx = HiveContext(self.spark.sparkContext)
        df_position = hcx.table('talents.ods_position')

        words_df = df_position \
            .filter(col('description').isNotNull()) \
            .rdd \
            .mapPartitions(JiebaUtil.segmentation) \
            .toDF(['id', 'region', 'category', 'words'])

        cv_model = CountVectorizerModel.load('model/cv.model')
        cv_result = cv_model.transform(words_df)

        idf_model = IDFModel.load('model/idf.model')
        idf_result = idf_model.transform(cv_result)

        position_index_idf = idf_result.rdd.mapPartitions(top).toDF(['id', 'region', 'category', 'index', 'tfidf'])

        keywords_idf = hcx.table('talents.ods_keywords')

        position_keyword_idf = position_index_idf.join(keywords_idf,
                                                       keywords_idf['index'] == position_index_idf['index']) \
            .select(position_index_idf['id'], position_index_idf['region'], keywords_idf['keyword'],
                    position_index_idf['tfidf'])

        position_keyword_idf.write.insertInto('talents.position_tfidf_keywords_result')


def top(partition):
    TOPN = 20
    for row in partition:
        idf_features = list(zip(row.idfFeatures.indices, row.idfFeatures.values))
        sorted(idf_features, key=lambda x: x[1], reverse=True)

        top_idf = idf_features[:TOPN]
        for idx, tfidf in top_idf:
            yield row.id, row.region, row.category, int(idx), round(float(tfidf), 4)


if __name__ == '__main__':
    PositionTFIDFWriter().process()
