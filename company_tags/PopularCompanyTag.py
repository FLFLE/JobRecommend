from driver.SparkLoader import SparkLoader
from pyspark.sql.functions import *
from pyspark.sql import Window

hcx = SparkLoader().hcx
company_df = hcx.table('talents.ods_company')
deliver_df = hcx.table('talents.ods_deliver')
account_df = hcx.table('talents.ods_account')
position_df = hcx.table('talents.ods_position')
#
tmp_df = company_df.join(deliver_df, company_df['cid'] == deliver_df['company_id']) \
    .select('city', 'companyname') \
    .groupBy('city', 'companyname') \
    .agg(count('companyname').alias('number'))

window = Window.partitionBy('city').orderBy(desc('number'))

result = tmp_df.select('city', 'companyname', 'number', dense_rank().over(window).alias('rank')) \
    .where('rank < 5').where('number > 5')
result.show(truncate=False)

account_df.join(position_df, account_df['expectcity'] == position_df['city']) \
    .join(deliver_df, position_df['id'] == deliver_df['position_id'], 'right_outer') \
    .select(account_df['expectcity'], deliver_df['position_id']) \
    .groupby(account_df['expectcity']).agg(count(deliver_df['position_id']).alias('count')).show()
