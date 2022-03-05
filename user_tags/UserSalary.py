## running on zepplin

# %pyspark
# #coding = 'utf-8'
# hcv = HiveContext(spark.sparkContext)
# df = hcv.table('talents.ods_account')
# salary_1 = df.withColumn('expectsalarys', explode(split(col('expectsalarys'), '-'))).select('expectsalarys')
# salary1 = salary_1.rdd.map(lambda x: x[0]).collect()
# n = 0
# num1, num2, num3, num4, num5 = 0, 0, 0, 0, 0
# for i in salary1:
#     i = i.replace('k', '').encode('utf-8').replace('以上', '')
#     i = int(i)
#     if i < 8:
#         num1 = num1 + 1
#     if (i >= 8) and (i < 12):
#         num2 = num2 + 1
#     if (i >= 12) and (i < 20):
#         num3 = num3 + 1
#     if (i >= 20) and (i < 50):
#         num4 = num4 + 1
#     else:
#         num5 = num5 + 1
#     n = n + 1
# salary2 = [('1k-8k', num1), ('8k-12k', num2), ('12k-20k', num3), ('20k-50k', num4), ('50k+', num5)]
# salary_2 = spark.createDataFrame(salary2, ['薪资水平', '人数'])
# z.show(salary_2)