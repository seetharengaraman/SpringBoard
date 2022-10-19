'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month,broadcast

import os


spark = SparkSession.builder.appName('Optimize I').getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions",30)

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-1]) 

answers_input_path = os.path.join(project_path, 'data/answers')

questions_input_path = os.path.join(project_path, 'data/questions')

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()

'''
Answers aggregation

Here we : get number of answers per question per month
'''

answers_month = answersDF.repartition('question_id').withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt')).cache()

resultDF = answers_month.join(broadcast(questionsDF), 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt').cache()

resultDF.orderBy('question_id', 'month').show()

answers_month.unpersist()
resultDF.unpersist()

'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
'''
