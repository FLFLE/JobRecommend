# coding:utf-8

from pyspark.sql import HiveContext
from pyspark.ml.feature import Word2VecModel
from pyspark.sql import functions as F

from SparkSession import SparkSessionBase


# CREATE TABLE position_vector(
# position_id String comment "position_id",
# region String comment "region",
# vectors ARRAY<double> comment "vectors"
# );
class PositionVectorJob(SparkSessionBase):
    SPARK_URL = "local"
    SPARK_APP_NAME = 'PositionVectorJob'
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

    def start(self):
        hcx = HiveContext(self.spark.sparkContext)
        pp_df = hcx.table('talents.position_profile')
        p_idf_weight_df = pp_df.select(F.col('position_id'), F.col('region'),
                                       F.explode('keywords_idf').alias('word', 'weight'))

        vec_model = Word2VecModel.load('model/words2vec.model')
        vectors = vec_model.getVectors()

        position_vector_df = p_idf_weight_df.join(vectors, p_idf_weight_df['word'] == vectors['word'], 'inner') \
            .rdd.map(lambda row: (row.position_id, row.region, row.word, float(row.weight) * row.vector)) \
            .toDF(['position_id', 'region', 'word', 'vector'])

        def vector_avg(row):
            summary = 0
            for v in row.vectors:
                summary += v
            vectors = [float(vector) for vector in (summary / len(row.vectors))]
            return row.position_id, row.region, vectors

        position_vector_df = position_vector_df.groupBy('position_id') \
            .agg(F.min('region').alias('region'), F.collect_set('vector').alias('vectors')) \
            .rdd.map(vector_avg).toDF(['position_id', 'region', 'vectors'])

        position_vector_df.show()
        position_vector_df.write.insertInto('talents.position_vector')


if __name__ == '__main__':
    PositionVectorJob().start()