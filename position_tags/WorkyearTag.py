from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *

hcx = SparkLoader().hcx

df = hcx.table('talents.ods_position')
result = df.select('workyear', when(col('workyear') == '3-5年', '3-5年')
                   .when(col('workyear') == '1-3年', '1-3年')
                   .when(col('workyear') == '不限', '不限')
                   .when(col('workyear') == '5-10年', '5-10年')
                   .when(col('workyear') == '应届毕业生', '应届毕业生')
                   .when(col('workyear') == '1年以下', '1年以下')
                   .when(col('workyear') == '10年以上', '10年以上')
                   .alias('工作年限')
                   ) \
    .na.drop('any') \
    .groupBy('工作年限') \
    .agg(count('工作年限').alias('number')) \
    .orderBy(desc('number'))
result.show(truncate=False)
