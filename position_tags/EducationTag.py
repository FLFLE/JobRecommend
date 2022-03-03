from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *

hcx = SparkLoader().hcx
df = hcx.table('talents.ods_position')

result = df.select('education')\
    .groupBy('education')\
    .agg(count('education').alias('number'))\
    .orderBy(desc('number'))\
    .where("number > 16")

result.show(truncate=False)

