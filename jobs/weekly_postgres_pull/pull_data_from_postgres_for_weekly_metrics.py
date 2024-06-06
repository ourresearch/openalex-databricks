# Databricks notebook source
import pickle
import boto3
import re
import json
import pandas as pd
pd.set_option('display.max_rows', 100)
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, FloatType, ArrayType, DoubleType, StructType, StructField,LongType

# COMMAND ----------

# MAGIC %md ##### Load Secret

# COMMAND ----------

def get_secret(secret_name = "prod/psqldb/conn_string"):

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

# COMMAND ----------

secret = get_secret()
buckets = get_secret("prod/aws/buckets")

# COMMAND ----------

database_copy_save_path = f"{buckets['database_copy_save_path']}"

# COMMAND ----------

# MAGIC %md #### Tables to pull

# COMMAND ----------

# MAGIC %md ##### currently mid.work, mid.affiliation, mid.citation, and mid.work_concept are taken care of in AND (if more columns are needed from those tables, change the code in the AND postgres data pull)

# COMMAND ----------

# Getting predicates for reading tables
df = (spark.read
    .format("postgresql")
    .option("dbtable", "(SELECT histogram_bounds::text::bigint[] FROM pg_stats WHERE tablename = 'affiliation' AND attname = 'paper_id') new_table")
    .option("host", secret['host'])
    .option("port", secret['port'])
    .option("database", secret['dbname'])
    .option("user", secret['username'])
    .option("password", secret['password'])
    .load())

work_id_predicates = df.collect()[0][0]

if len(work_id_predicates) == 125:
    final_predicates = []
    final_predicates.append(f"paper_id >= 0 and paper_id < {work_id_predicates[2]}")
    for i in range(2, len(work_id_predicates[:-3]), 3):
        final_predicates.append(f"paper_id >= {work_id_predicates[i]} and paper_id < {work_id_predicates[i+3]}")
    final_predicates.append(f"paper_id >= {work_id_predicates[-2]}")
print(len(final_predicates))

testing_new_predicates = final_predicates[:-1].copy()
testing_new_predicates.append(f'paper_id >= {work_id_predicates[-3]} and paper_id < {work_id_predicates[-2]}')
testing_new_predicates.append(f'paper_id >= {work_id_predicates[-2]} and paper_id < 4395018129')
testing_new_predicates.append('paper_id >= 4395018129 and paper_id < 4395318129')
testing_new_predicates.append('paper_id >= 4395318129 and paper_id < 4395718129')
testing_new_predicates.append('paper_id >= 4395718129 and paper_id < 4396018129')
testing_new_predicates.append('paper_id >= 4396018129 and paper_id < 4396118129')
testing_new_predicates.append('paper_id >= 4396118129')
print(len(testing_new_predicates))

# COMMAND ----------

testing_new_predicates

# COMMAND ----------

# mid.work_topic

df = (spark.read
      .jdbc(
              url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
              table="(select distinct paper_id, topic_id, topic_rank, score from mid.work_topic) as new_table", 
              properties={"user": secret['username'],
                          "password": secret['password']}, 
              predicates=testing_new_predicates))

df \
    .repartition(384).write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/mid/work_topic")

# COMMAND ----------

# mid.journal

df = (spark.read
.format("postgresql")
.option("dbtable", 
        f"(select distinct journal_id, display_name, type, merge_into_id from mid.journal) as new_table")
.option("host", secret['host'])
.option("port", secret['port'])
.option("database", secret['dbname'])
.option("user", secret['username'])
.option("password", secret['password'])
.load())

df \
.repartition(64) \
.write.mode('overwrite') \
.parquet(f"{database_copy_save_path}/mid/journal")

# COMMAND ----------

# mid.topic

df = (spark.read
.format("postgresql")
.option("dbtable", 
        f"(select distinct topic_id, display_name, subfield_id, field_id, domain_id from mid.topic) as new_table")
.option("host", secret['host'])
.option("port", secret['port'])
.option("database", secret['dbname'])
.option("user", secret['username'])
.option("password", secret['password'])
.load())

df \
.repartition(64) \
.write.mode('overwrite') \
.parquet(f"{database_copy_save_path}/mid/topic")

# COMMAND ----------

# mid.subfield

df = (spark.read
.format("postgresql")
.option("dbtable", 
        f"(select distinct subfield_id, display_name from mid.subfield) as new_table")
.option("host", secret['host'])
.option("port", secret['port'])
.option("database", secret['dbname'])
.option("user", secret['username'])
.option("password", secret['password'])
.load())

df \
.write.mode('overwrite') \
.parquet(f"{database_copy_save_path}/mid/subfield")

# COMMAND ----------

# mid.field

df = (spark.read
.format("postgresql")
.option("dbtable", 
        f"(select distinct field_id, display_name from mid.field) as new_table")
.option("host", secret['host'])
.option("port", secret['port'])
.option("database", secret['dbname'])
.option("user", secret['username'])
.option("password", secret['password'])
.load())

df \
.write.mode('overwrite') \
.parquet(f"{database_copy_save_path}/mid/field")

# COMMAND ----------

#mid.work_extra_ids

df = (spark.read
      .jdbc(
              url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
              table="(select distinct paper_id, attribute_type, attribute_value from mid.work_extra_ids) as new_table", 
              properties={"user": secret['username'],
                          "password": secret['password']}, 
              predicates=testing_new_predicates))

df \
    .repartition(384).write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/mid/work_extra_ids")
