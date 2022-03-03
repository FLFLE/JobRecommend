from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *

hcx = SparkLoader().hcx
df = hcx.table('talents.ods_position')

result = df.select('positionname') \
    .groupBy('positionname') \
    .agg(count('positionname').alias('number')) \
    .orderBy(desc('number')) \
    .limit(10)

result.show(truncate=False)
