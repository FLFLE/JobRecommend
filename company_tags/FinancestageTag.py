from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *

hcx = SparkLoader().hcx
df = hcx.table('talents.ods_company')

result = df.select('financestage') \
    .groupBy('financestage') \
    .agg(count('financestage').alias('number')) \
    .orderBy(desc('number'))

result.show(truncate=False)
