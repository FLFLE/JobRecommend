from driver.SparkLoader import SparkLoader
from pyspark.sql import HiveContext
from pyspark.sql.functions import *


hcx = SparkLoader().hcx
df = hcx.table('talents.ods_position')
result = df.select('city')\
    .groupBy('city')\
    .agg(count('city').alias('number'))\
    .where("number > 10")\
    .orderBy(desc('number'))
result.show(truncate=False)

