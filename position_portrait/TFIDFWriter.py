from pyspark.sql import HiveContext
from pyspark.ml.feature import CountVectorizer, IDF, CountVectorizerModel, IDFModel
from SparkSession import SparkSessionBase



class TFIDFWriter(SparkSessionBase):
    SPARK_URL = "local"
    SPARK_APP_NAME = 'TFIDFWriter'
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

    def process(self):
        cv_model = CountVectorizerModel.load('model/cv.model')
        idf_model = IDFModel.load('model/idf.model')

        keywords_with_idf = list(zip(cv_model.vocabulary, idf_model.idf.toArray()))
        print(keywords_with_idf)

        for index in range(len(keywords_with_idf)):
            keywords_with_idf[index] = list(keywords_with_idf[index])
            keywords_with_idf[index].append(index)
            keywords_with_idf[index][1] = float(keywords_with_idf[index][1])

        rdd = self.spark.sparkContext.parallelize(keywords_with_idf)
        result = rdd.toDF(['keyword', 'idf', 'index'])
        result.write.insertInto('talents.ods_keywords')


if __name__ == '__main__':
    TFIDFWriter().process()
