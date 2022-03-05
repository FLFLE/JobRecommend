# coding=utf-8
from pyspark.sql.functions import *
from driver.SparkLoader import SparkLoader

# 初始化驱动

spark = SparkLoader().spark
hcx = SparkLoader().hcx

# 获取数据
account_df = hcx.table('talents.ods_account')
position_df = hcx.table('talents.ods_position')
company_df = hcx.table('talents.ods_company')
deliver_df = hcx.table('talents.ods_deliver')

# 添加职位标签
insert_position_id = 1007336

# #职位画像城市标签
# position_city=position_df.select('city').where(col('id').like('{}'.format(insert_position_id)))
# position_city=position_city.rdd.map(lambda x: x[0]).collect()
# print(position_city)

# 职位画像薪资标签
# temp_df=position_df.withColumn('salaryrange',concat_ws('-',col('salarymin'),col('salarymax')))
# position_salary=temp_df.select('salaryrange').where(col('id').like('{}'.format(insert_position_id)))
# position_salary=position_salary.rdd.map(lambda x: x[0]).collect()
# position_salary[0] =position_salary[0]+'k'
# print(position_salary)

# #职位画像工作年限标签
# position_workyear=position_df.select('workyear').where(col('id').like('{}'.format(insert_position_id)))
# position_workyear=position_workyear.rdd.map(lambda x: x[0]).collect()
# print(position_workyear)
#
# #职位画像工作类型标签
temp_df = position_df.withColumn('positioncategory', concat(col('positionfirstcategory'), col('positionsecondcategory'),
                                                            col('positionthirdcategory')))
position_category = temp_df.select('positioncategory').where(col('id').like('{}'.format(insert_position_id)))
position_category = position_category.rdd.map(lambda x: x[0]).collect()
print(position_category)
#
#
# 职位画像学历标签
position_education = position_df.select('education').where(col('id').like('{}'.format(insert_position_id)))
position_education = position_education.rdd.map(lambda x: x[0]).collect()
print(position_education)
