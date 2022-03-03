from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from driver.SparkSessionBase import SparkSessionBase


class UserGenderTag(SparkSessionBase):
    SPARK_APP_NAME = 'UserGenderTag'
    SPARK_URL = 'local'
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

    def process(self):
        hcx = HiveContext(self.spark.sparkContext)
        df = hcx.table('talents.ods_account')
        df.show()


if __name__ == '__main__':
    UserGenderTag().process()
