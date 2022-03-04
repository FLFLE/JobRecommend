from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *

hcx = SparkLoader().hcx
df = hcx.table('talents.ods_position')

result1 = df.select('positionfirstcategory', 'positionsecondcategory') \
    .groupBy('positionfirstcategory', 'positionsecondcategory') \
    .agg(count('*').alias('number')) \
    .where("number > 10") \
    .orderBy(desc('number'))
result1.show()

result2 = df.select('positionsecondcategory', 'positionthirdcategory') \
    .groupBy('positionsecondcategory', 'positionthirdcategory') \
    .agg(count('*').alias('number')) \
    .where("number > 10") \
    .orderBy(desc('number'))
result2.show()

