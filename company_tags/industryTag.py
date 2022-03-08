from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *

# 行业领域
hcx = SparkLoader().hcx
df = hcx.table('talents.ods_company')

result = df.select('industryfield') \
    .groupBy('industryfield') \
    .agg(count('industryfield').alias('number')) \
    .orderBy(desc('number'))

result.show(truncate=False)
