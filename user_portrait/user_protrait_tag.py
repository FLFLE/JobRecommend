#coding=utf-8
from pyspark.sql.functions import *
from driver.SparkLoader import SparkLoader
#初始化驱动

spark=SparkLoader().spark
hcx=SparkLoader().hcx

#获取数据
account_df = hcx.table('talents.ods_account')
position_df = hcx.table('talents.ods_position')
company_df=hcx.table('talents.ods_company')
deliver_df=hcx.table('talents.ods_deliver')

#添加用户标签
insert_user_id = 10000913

#用户画像性别标签
# user_sex=account_df.select('sex').where(col('id').like('{}'.format(insert_user_id)))
# user_sex=user_sex.rdd.map(lambda x: x[0]).collect()
#print(user_sex)

# # 用户画像工作类型标签
# temp_df=account_df.withColumn('expectpositionnametype',concat(col('expectpositionnametype1'),col('expectpositionnametype2')))
# user_expectpositionnametype=temp_df.select('expectpositionnametype').where(col('id').like('{}'.format(insert_user_id)))
# user_expectpositionnametype=user_expectpositionnametype.rdd.map(lambda x: x[0]).collect()
# print(user_expectpositionnametype)
#
# # 用户画像年龄段标签
# user_age=account_df.select('age').where(col('id').like('{}'.format(insert_user_id)))
# user_age=user_age.rdd.map(lambda x: x[0]).collect()
# print(user_age)
#
# # 用户画像学历标签
# user_highesteducation=account_df.select('highesteducation').where(col('id').like('{}'.format(insert_user_id)))
# user_highesteducation=user_highesteducation.rdd.map(lambda x: x[0]).collect()
# print(user_highesteducation)
#
# # 用户画像期望工作标签
# user_expectpositionname=account_df.select('expectpositionname').where(col('id').like('{}'.format(insert_user_id)))
# user_expectpositionname=user_expectpositionname.rdd.map(lambda x: x[0]).collect()
# print(user_expectpositionname)
#
# # 用户画像期望城市标签
# user_expectcity=account_df.select('expectcity').where(col('id').like('{}'.format(insert_user_id)))
# user_expectcity=user_expectcity.rdd.map(lambda x: x[0]).collect()
# print(user_expectcity)
#
# # 用户画像期望薪资标签
# user_expectsalarys=account_df.select('expectsalarys').where(col('id').like('{}'.format(insert_user_id)))
# user_expectsalarys=user_expectsalarys.rdd.map(lambda x: x[0]).collect()
# print(user_expectsalarys)
#
# # 用户画像在职状态标签
# user_status=account_df.select('status').where(col('id').like('{}'.format(insert_user_id)))
# user_status=user_status.rdd.map(lambda x: x[0]).collect()
# print(user_status)
#
# # 用户画像校友会标签
# user_latest_schoolname=account_df.select('latest_schoolname').where(col('id').like('{}'.format(insert_user_id)))
# user_latest_schoolname=user_latest_schoolname.rdd.map(lambda x: x[0]).collect()
# print(user_latest_schoolname)

# 用户画像最近投递标签

user_latest_deliver=deliver_df.select('deliver_time','company_id').where(col('account_id').like('{}'.format(insert_user_id))).orderBy(col('deliver_time')).limit(5)
user_latest_deliver=user_latest_deliver.rdd.map(lambda x: (x[0],x[1])).collect()
print(user_latest_deliver)




