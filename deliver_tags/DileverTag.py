# coding=utf-8
from pyspark.sql.functions import *
from driver.SparkLoader import SparkLoader

spark = SparkLoader().spark
hcx = SparkLoader().hcx
account_df = hcx.table('talents.ods_account')
position_df = hcx.table('talents.ods_position')
hcx.table('talents.ods_company')
deliver_df = hcx.table('talents.ods_deliver')

# 添加各期望城市投递数标签
cleaned_account = account_df.dropna('any', col('expectcity'))
cleaned_account.join(position_df, account_df['expectcity'] == position_df['city']) \
    .join(deliver_df, position_df['id'] == deliver_df['position_id'], 'right_outer') \
    .select(account_df['expectcity'], deliver_df['position_id']) \
    .groupby(account_df['expectcity']).agg(count(deliver_df['position_id']).alias('count'))

# 添加是否投递占比标签
# deliver_num=deliver_df.join(account_df,deliver_df['account_id']==account_df['id']).select('account_id').groupby('account_id').count()
# deliver_partition_df=deliver_df.dropna().groupBy('id').agg((deliver_num.alias("是"))
# 添加每日投递数量标签
# deliver_df.select(dayofyear('deliver_time').alias('day')).groupBy('day')\
#     .agg(count('day')).show()
# 投递用户Top10
# deliver_df.select(col('account_id').alias('投递用户')) \
#             .groupBy('投递用户').agg(count('投递用户').alias('count')).orderBy(col('count').desc()).show(10,truncate=False)
# highesteducation_df=df.select(col('highesteducation').alias('学历信息')) \
#             .groupBy('学历信息').agg(count('学历信息').alias('count')).where('count > 10')
# 投递学历层次占比
deliver_df.join(account_df, deliver_df['account_id'] == account_df['id']) \
    .select(account_df['highesteducation']) \
    .groupBy('highesteducation').agg(count('highesteducation').alias('count')).where('count > 10').show()
