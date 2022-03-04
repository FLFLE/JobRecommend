# coding=utf-8
from typing import List

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
        hcx.table('talents.ods_position')
        hcx.table('talents.ods_company')
        hcx.table('talents.ods_deliver')
        # df.select('id',when(col('sex') == '男', '男').when(col('sex') == '女', '女').otherwise('未知').alias('性别')
        #           ).groupby('性别').agg(count('性别').alias('count')).show()

        # df.select('id', when(col('age') >= 63, '50后').when(col('age') >= 53, '60后') \
        #         .when(col('age') >= 43, '70后').when(col('age') >= 33, '80后') \
        #         .when(col('age') >= 23, '90后').otherwise('00后').alias('年龄段')) \
        #     .groupBy('年龄段').count().orderBy(col('年龄段').desc()).show()

        # df.select(col('expectcity').alias('期望城市')) \
        #     .groupBy('期望城市').agg(count('期望城市').alias('count')).show()

        # df.select(col('status').alias('在职状态'))\
        #     .groupby('在职状态').agg(count('在职状态').alias('count')).where('count >=10').show()

        # df.select(col('expectpositionname').alias('期望职位')) \
        #     .groupBy('期望职位').agg(count('期望职位').alias('count')).where('count > 10').show()

        # df.select(col('is_famous_enterprise').alias('是否名企')) \
        #     .groupBy('是否名企').agg(count('是否名企').alias('count')).where('count > 10').show()

        # df.select(col('is_famous_enterprise').alias('是否名企')) \
        # .groupBy('是否名企').agg(count('是否名企').alias('count')).where('count > 10').show()

        df.dropna('any').select(col('work_year').alias('工作年限')) \
            .groupBy('工作年限').agg(count('工作年限').alias('count')).where('count > 10').show()


if __name__ == '__main__':
    UserGenderTag().process()
