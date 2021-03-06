from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *

# 企业规模
hcx = SparkLoader().hcx
df = hcx.table('talents.ods_company')

result = df.select('companysize') \
    .groupBy('companysize') \
    .agg(count('companysize').alias('number')) \
    .orderBy(desc('number'))

result.show(truncate=False)
