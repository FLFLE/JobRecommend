from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *

hcx = SparkLoader().hcx
df = hcx.table('talents.ods_company')
result = df.select('city', 'is_famous_enterprise') \
    .where("is_famous_enterprise == 1")\
    .groupBy('city') \
    .agg(count('city').alias('famous_enterprise_number')) \
    .orderBy(desc('famous_enterprise_number'))

result.show(truncate=False)
