from pyspark.sql import HiveContext
from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.sql.functions import col

import JiebaUtil
from SparkSession import SparkSessionBase


class TFIDFJob(SparkSessionBase):
    SPARK_URL = "local"
    SPARK_APP_NAME = 'TFIDFJob'
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

    def process(self):
        hcx = HiveContext(self.spark.sparkContext)
        df_position = hcx.table('talents.ods_position')

        words_df = df_position \
            .filter(col('description').isNotNull()) \
            .select('id', 'district', 'positionfirstcategory', 'description') \
            .rdd \
            .mapPartitions(JiebaUtil.segmentation) \
            .toDF(['id', 'region', 'category', 'words'])
        # 训练CV模型
        cv = CountVectorizer(inputCol='words', outputCol='countFeatures', vocabSize=1000 * 300, minDF=1.0)
        cv_model = cv.fit(words_df)
        cv_model.write().overwrite().save('model\\cv.model')

        cv_result = cv_model.transform(words_df)
        cv_result.show()

        # 训练IDF模型
        idf = IDF(inputCol='countFeatures', outputCol='idfFeatures')
        idf_model = idf.fit(cv_result)
        idf_model.write().overwrite().save('model\\idf.model')


if __name__ == '__main__':
    TFIDFJob().process()
