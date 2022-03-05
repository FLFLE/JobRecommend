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


# 性别
def get_user_sex(user_id):
    user_sex = account_df.select('sex').where(f'id=={user_id}')
    user_sex = user_sex.collect()
    user_sex = user_sex[0][0]
    return user_sex


# 工作类型
def get_position_type(user_id):
    temp_df = account_df.withColumn('expectpositionnametype',
                                    concat(col('expectpositionnametype1'), col('expectpositionnametype2')))
    user_expectpositionnametype = temp_df.select('expectpositionnametype').where(f'id=={user_id}')
    user_expectpositionnametype = user_expectpositionnametype.collect()
    user_expectpositionnametype = user_expectpositionnametype[0][0]
    return user_expectpositionnametype


# 用户年龄
def get_age(user_id):
    user_age = account_df.select('age').where(f'id=={user_id}')
    user_age = user_age.collect()
    user_age = user_age[0][0]
    return user_age


# 用户学历
def get_education(user_id):
    user_highesteducation = account_df.select('highesteducation').where(f'id=={user_id}')
    user_highesteducation = user_highesteducation.collect()
    user_highesteducation = user_highesteducation[0][0]
    return user_highesteducation


# 期望职位
def get_expectposition(user_id):
    user_expectpositionname = account_df.select('expectpositionname').where(f'id=={user_id}')
    user_expectpositionname = user_expectpositionname.collect()
    user_expectpositionname = user_expectpositionname[0][0]
    return user_expectpositionname


# 期望城市
def get_expectcity(user_id):
    user_expectcity = account_df.select('expectcity').where(f'id=={user_id}')
    user_expectcity = user_expectcity.collect()
    user_expectcity = user_expectcity[0][0]
    return user_expectcity


# 期望薪资
def get_expectsalarys(user_id):
    user_expectsalarys = account_df.select('expectsalarys').where(f'id=={user_id}')
    user_expectsalarys = user_expectsalarys.collect()
    user_expectsalarys = user_expectsalarys[0][0]
    return user_expectsalarys


# 在职状态
def get_status(user_id):
    user_status = account_df.select('status').where(f'id=={user_id}')
    user_status = user_status.collect()
    user_status = user_status[0][0]
    return user_status


# 校友会
def get_latest_schoolname(user_id):
    user_latest_schoolname = account_df.select('latest_schoolname').where(f'id=={user_id}')
    user_latest_schoolname = user_latest_schoolname.collect()
    user_latest_schoolname = user_latest_schoolname[0][0]
    return user_latest_schoolname


# 最近投递
# def get_latest_deliver(user_id):
#     user_latest_deliver = deliver_df.select('deliver_time', 'company_id'). \
#         where(f'account_id=={user_id}').orderBy(col('deliver_time')).limit(5)
#     user_latest_deliver = user_latest_deliver.rdd.map(lambda x: (x[0], x[1])).collect()
#     return user_latest_deliver


class User:
    def __init__(self, user_id):
        self.user_id = user_id

    def get_protrait(self):
        # user_sex = get_user_sex(self.user_id)
        # user_position_type = get_position_type(self.user_id)
        # user_age = get_age(self.user_id)
        # user_education = get_education(self.user_id)
        # user_expectposition = get_expectposition(self.user_id)
        # user_expectcity = get_expectcity(self.user_id)
        # user_expectsalarys = get_expectsalarys(self.user_id)
        # user_status = get_status(self.user_id)
        # user_latest_schoolname = get_latest_schoolname(self.user_id)
        # user_latest_deliver = get_latest_deliver(self.user_id)
        # return user_sex, user_position_type, user_age, user_education, user_expectposition, user_expectcity, \
        #        user_expectsalarys, user_status, user_latest_schoolname
                # ,user_latest_deliver

if __name__=='__main__':
    print(User(2710).get_protrait())