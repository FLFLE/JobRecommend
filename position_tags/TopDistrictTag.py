from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *

hcx = SparkLoader().hcx
df = hcx.table('talents.ods_position')

result = df.select('district') \
    .groupBy('district') \
    .agg(count('district').alias('number')) \
    .orderBy(desc('number')) \
    .limit(5)

result.show(truncate=False)
