from pyspark.sql.functions import *
from pyspark.sql import functions
from driver.SparkLoader import SparkLoader
from pyspark.sql.types import IntegerType

spark = SparkLoader().spark
hcx = SparkLoader().hcx
df = hcx.table('talents.ods_position')
salary = df.select('salarymin', 'salarymax')
salary1 = salary.rdd.map(lambda x: x[0]).collect()
salary2 = salary.rdd.map(lambda x: x[1]).collect()
n = 0
num1, num2, num3, num4, num5 = 0, 0, 0, 0, 0
for i in salary1:
    i = i.encode('utf-8').replace('本科', '0').replace('大专', '0').replace('不限', '0').replace('员工关系', '0') \
        .replace('数值', '0').replace('硕士', '0')
    i = int(i)
    if i < 8:
        num1 = num1 + 1
    if (i >= 8) and (i < 12):
        num2 = num2 + 1
    if (i >= 12) and (i < 20):
        num3 = num3 + 1
    if (i >= 20) and (i < 50):
        num4 = num4 + 1
    else:
        num5 = num5 + 1
    n = n + 1
n = 0
num_1, num_2, num_3, num_4, num_5 = 0, 0, 0, 0, 0
for i in salary2:
    i = i.encode('utf-8').replace('游戏策划', '0').replace('HRBP', '0')
    i = int(i)
    if i < 8:
        num_1 = num_1 + 1
    if (i >= 8) and (i < 12):
        num_2 = num_2 + 1
    if (i >= 12) and (i < 20):
        num_3 = num_3 + 1
    if (i >= 20) and (i < 50):
        num_4 = num_4 + 1
    else:
        num_5 = num_5 + 1
    n = n + 1
salary0 = [('1k-8k', num1 + num_1), ('8k-12k', num2 + num_2), ('12k-20k', num3 + num_3), ('20k-50k', num4 + num_4),
           ('50k+', num5 + num_5)]
salary_0 = spark.createDataFrame(salary0, ['薪资水平', '人数'])
