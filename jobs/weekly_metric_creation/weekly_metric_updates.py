# Databricks notebook source
import pickle
import boto3
import re
import json
import pandas as pd
pd.set_option('display.max_rows', 100)
import numpy as np
import heroku3

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, FloatType, ArrayType, DoubleType, StructType, StructField,LongType

# COMMAND ----------

# MAGIC %md #### Load Secrets

# COMMAND ----------

def get_secret(secret_name="prod/psqldb/conn_string"):

    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    
    get_secret_value_response = client.get_secret_value(
            SecretId=secret_name)

    # Decrypts secret using the associated KMS key.
    secret_string = get_secret_value_response['SecretString']
    
    secret = json.loads(secret_string)
    return secret

def shutdown_dynos(heroku_secret_token, dynos_to_stop):
    heroku_conn = heroku3.from_key(heroku_secret_token)
    app = heroku_conn.app("openalex-guts")
    current_quantities = []

    for dyno_name in dynos_to_stop:
        current_quantities.append(app.process_formation()[dyno_name].quantity)
        app.process_formation()[dyno_name].scale(0)
    return current_quantities

def restart_dynos(heroku_secret_token, dynos_to_stop, old_quantities):
    heroku_conn = heroku3.from_key(heroku_secret_token)
    app = heroku_conn.app("openalex-guts")

    for dyno_name, dyno_quantity in zip(dynos_to_stop, old_quantities):
        app.process_formation()[dyno_name].scale(dyno_quantity)

# COMMAND ----------

secret = get_secret()
heroku_secret = get_secret(secret_name = "prod/heroku/oauth")
buckets = get_secret("prod/aws/buckets")

# COMMAND ----------

database_copy_save_path = f"{buckets['database_copy_save_path']}"

# COMMAND ----------

# MAGIC %md ## Topics and Topic Share

# COMMAND ----------

num_papers_by_primary_topic = spark.read.parquet(f"{database_copy_save_path}/mid/work_topic") \
    .groupBy('topic_id').count() \
    .select(F.col('topic_id').alias('primary_topic_id'), F.col('count').alias('num_papers'))

num_papers_by_primary_topic.cache().count()

# COMMAND ----------

work_topics = spark.read.parquet(f"{database_copy_save_path}/mid/work_topic").dropDuplicates().repartition(256)
work_topics.cache().count()

# COMMAND ----------

topics = spark.read.parquet(f"{database_copy_save_path}/mid/topic")
topics.cache().count()

# COMMAND ----------

# MAGIC %md ##### author topics

# COMMAND ----------

w1 = Window.partitionBy('author_id').orderBy(F.col('topic_count').desc(), F.col('rand_num'))
w2 = Window.partitionBy('author_id').orderBy(F.col('topic_share').desc(), F.col('rand_num'))

author_topics = spark.read.parquet(f"{database_copy_save_path}/mid/affiliation") \
    .filter(F.col('author_id').isNotNull()) \
    .filter(F.col('paper_id').isNotNull()) \
    .select('author_id','paper_id').dropDuplicates() \
    .join(work_topics, how='inner', on='paper_id') \
    .groupBy(['author_id','topic_id']).count() \
    .select('author_id','topic_id',F.col('count').alias('topic_count')) \
    .join(num_papers_by_primary_topic.select('num_papers', F.col('primary_topic_id').alias('topic_id')), 
          how='inner', on='topic_id') \
    .withColumn('topic_share', F.col('topic_count')/F.col('num_papers')) \
    .join(topics, how='inner', on='topic_id') \
    .withColumn('rand_num', F.rand(seed=1)) \
    .withColumn('topic_count_rank', F.row_number().over(w1)) \
    .withColumn('topic_share_rank', F.row_number().over(w2)) \
    .filter((F.col('topic_count_rank') <= 30) | (F.col('topic_share_rank') <= 30)) \
    .select('author_id','topic_id','display_name','topic_count','topic_share')

author_topics.cache().count()

# COMMAND ----------

dynos_to_shutdown = ['fast_store_authors']
curr_q = shutdown_dynos(heroku_secret['heroku_token'], dynos_to_shutdown)
curr_q

# COMMAND ----------

author_topics \
    .repartition(36) \
    .write.format("jdbc") \
    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") \
    .option("dbtable", 'counts.author_topic_for_api_full') \
    .option("user", secret['username']) \
    .option("password", secret['password']) \
    .option("driver", "org.postgresql.Driver") \
    .option("truncate", True) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

_ = restart_dynos(heroku_secret['heroku_token'], dynos_to_shutdown, curr_q)

# COMMAND ----------

# MAGIC %md ##### source topics

# COMMAND ----------

w1 = Window.partitionBy('journal_id').orderBy(F.col('topic_count').desc(), F.col('rand_num'))
w2 = Window.partitionBy('journal_id').orderBy(F.col('topic_share').desc(), F.col('rand_num'))

source_topics = spark.read.parquet(f"{database_copy_save_path}/mid/work") \
    .filter(F.col('journal_id').isNotNull()) \
    .filter(F.col('paper_id').isNotNull()) \
    .select('journal_id','paper_id').dropDuplicates() \
    .join(work_topics, how='inner', on='paper_id') \
    .groupBy(['journal_id','topic_id']).count() \
    .select('journal_id','topic_id',F.col('count').alias('topic_count')) \
    .join(num_papers_by_primary_topic.select('num_papers', F.col('primary_topic_id').alias('topic_id')), 
          how='inner', on='topic_id') \
    .withColumn('topic_share', F.col('topic_count')/F.col('num_papers')) \
    .join(topics, how='inner', on='topic_id') \
    .withColumn('rand_num', F.rand(seed=1)) \
    .withColumn('topic_count_rank', F.row_number().over(w1)) \
    .withColumn('topic_share_rank', F.row_number().over(w2)) \
    .filter((F.col('topic_count_rank') <= 30) | (F.col('topic_share_rank') <= 30)) \
    .select('journal_id','topic_id','display_name','topic_count','topic_share')

source_topics.cache().count()

# COMMAND ----------

dynos_to_shutdown = ['fast_store_sources']
curr_q = shutdown_dynos(heroku_secret['heroku_token'], dynos_to_shutdown)
curr_q

# COMMAND ----------

source_topics \
    .repartition(6) \
    .write.format("jdbc") \
    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") \
    .option("dbtable", 'counts.source_topic_for_api_full') \
    .option("user", secret['username']) \
    .option("password", secret['password']) \
    .option("driver", "org.postgresql.Driver") \
    .option("truncate", True) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

_ = restart_dynos(heroku_secret['heroku_token'], dynos_to_shutdown, curr_q)

# COMMAND ----------

# MAGIC %md ##### institution topics

# COMMAND ----------

w1 = Window.partitionBy('affiliation_id').orderBy(F.col('topic_count').desc(), F.col('rand_num'))
w2 = Window.partitionBy('affiliation_id').orderBy(F.col('topic_share').desc(), F.col('rand_num'))

institution_topics = spark.read.parquet(f"{database_copy_save_path}/mid/affiliation") \
    .filter(F.col('affiliation_id').isNotNull()) \
    .filter(F.col('paper_id').isNotNull()) \
    .select('affiliation_id','paper_id').dropDuplicates() \
    .join(work_topics, how='inner', on='paper_id') \
    .groupBy(['affiliation_id','topic_id']).count() \
    .select('affiliation_id','topic_id',F.col('count').alias('topic_count')) \
    .join(num_papers_by_primary_topic.select('num_papers', F.col('primary_topic_id').alias('topic_id')), 
          how='inner', on='topic_id') \
    .withColumn('topic_share', F.col('topic_count')/F.col('num_papers')) \
    .join(topics, how='inner', on='topic_id') \
    .withColumn('rand_num', F.rand(seed=1)) \
    .withColumn('topic_count_rank', F.row_number().over(w1)) \
    .withColumn('topic_share_rank', F.row_number().over(w2)) \
    .filter((F.col('topic_count_rank') <= 30) | (F.col('topic_share_rank') <= 30)) \
    .select('affiliation_id','topic_id','display_name','topic_count','topic_share')

institution_topics.cache().count()

# COMMAND ----------

dynos_to_shutdown = ['fast_store_institutions']
curr_q = shutdown_dynos(heroku_secret['heroku_token'], dynos_to_shutdown)
curr_q

# COMMAND ----------

institution_topics \
    .repartition(12) \
    .write.format("jdbc") \
    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") \
    .option("dbtable", 'counts.institution_topic_for_api_full') \
    .option("user", secret['username']) \
    .option("password", secret['password']) \
    .option("driver", "org.postgresql.Driver") \
    .option("truncate", True) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

_ = restart_dynos(heroku_secret['heroku_token'], dynos_to_shutdown, curr_q)

# COMMAND ----------


