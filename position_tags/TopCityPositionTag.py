from pyspark.sql import Window
from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *

hcx = SparkLoader().hcx
df = hcx.table('talents.ods_position')

tmp_df = df.select('city', 'positionname') \
    .groupBy('city', 'positionname') \
    .agg(count('positionname').alias('number'))\
    .orderBy(desc('number'))
# tmp_df.show()

window = Window.partitionBy('city').orderBy(desc('number'))
result = tmp_df.select('city', 'positionname', 'number', dense_rank().over(window).alias('rank'))\
    .where("rank<=5").where('number > 3')
result.show()
