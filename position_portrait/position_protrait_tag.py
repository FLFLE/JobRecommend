# coding=utf-8
from pyspark.sql.functions import *
from driver.SparkLoader import SparkLoader

# 初始化驱动

spark = SparkLoader().spark
hcx = SparkLoader().hcx

# 获取数据
position_df = hcx.table('talents.ods_position')
position_profile_df = hcx.table('talents.position_profile')


# 职位画像城市标签
def get_position_city(position_id):
    position_city = position_df.select('city').where(f'id == {position_id}')
    position_city = position_city.collect()
    position_city = position_city[0][0]
    return position_city


# 职位画像薪资标签
def get_position_salary(position_id):
    tmp_df = position_df.withColumn('salaryrange', concat_ws('-', col('salarymin'), col('salarymax')))
    position_salary = tmp_df.select('salaryrange').where(f'id == {position_id}')
    position_salary = position_salary.collect()
    position_salary = position_salary[0][0] + 'k'
    return position_salary


# 职位画像工作年限标签
def get_position_workyear(position_id):
    position_workyear = position_df.select('workyear').where(f'id == {position_id}')
    position_workyear = position_workyear.collect()
    position_workyear = position_workyear[0][0]
    return position_workyear


# 职位画像工作类型标签
def get_position_category(position_id):
    temp_df = position_df.withColumn('positioncategory',
                                     concat(col('positionfirstcategory'), col('positionsecondcategory'),
                                            col('positionthirdcategory')))
    position_category = temp_df.select('positioncategory').where(f'id == {position_id}')
    position_category = position_category.collect()
    position_category = position_category[0][0]
    return position_category


# 职位画像学历标签
def get_position_education(position_id):
    position_education = position_df.select('education').where(f'id == {position_id}')
    position_education = position_education.collect()
    position_education = position_education[0][0]
    return position_education


def get_position_keywords(position_id):
    position_keywords = position_profile_df.select('keywords').where(f'position_id == {position_id}')
    position_keywords = position_keywords.collect()
    position_keywords = position_keywords[0][0]
    return position_keywords


class Position():
    def __init__(self, position_id):
        self.position_id = position_id

    def get_position_protrait(self):
        position_city = get_position_city(self.position_id)
        position_salary = get_position_salary(self.position_id)
        position_workyear = get_position_workyear(self.position_id)
        position_category = get_position_category(self.position_id)
        position_education = get_position_education(self.position_id)
        position_keywords = get_position_keywords(self.position_id)
        return position_city, position_salary, position_workyear, position_category, position_education, position_keywords
