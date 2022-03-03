from pyspark.sql import HiveContext

from driver.SparkSessionBase import SparkSessionBase


class SparkLoader(SparkSessionBase):
    SPARK_APP_NAME = 'Tag'
    SPARK_URL = 'local'
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()
        self.hcx = HiveContext(self.spark.sparkContext)
