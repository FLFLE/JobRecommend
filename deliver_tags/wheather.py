## running on zepplin

# %pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# from pyspark.sql.functions import *
# from pyspark.sql import HiveContext
# hcx = HiveContext(spark.sparkContext)
# account_df = hcx.table('talents.ods_account')
# deliver_df = hcx.table('talents.ods_deliver')
# result0 = account_df.agg(countDistinct('id').alias('count'))
# result0 = result0.rdd.map(lambda x: x[0]).collect()
# result0 = list(map(int, result0))
# result1 = deliver_df.agg(countDistinct('account_id').alias('count'))
# result1 = result1.rdd.map(lambda x: x[0]).collect()
# result1 = list(map(int, result1))
# result = spark.createDataFrame([('yes', result1[0]), ('no', result0[0] - result1[0])], ['是否投递', '人数'])
# z.show(result)