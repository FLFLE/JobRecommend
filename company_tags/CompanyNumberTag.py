from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *

# 公司的城市分布
hcx = SparkLoader().hcx
df = hcx.table('talents.ods_company')
result = df.select('city')\
    .groupBy('city')\
    .agg(count('city').alias('number'))\
    .orderBy(desc('number'))

result.show(truncate=False)