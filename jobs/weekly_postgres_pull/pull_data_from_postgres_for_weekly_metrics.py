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

def get_secret(secret_name = "postgres-works"):

    if secret_name == "postgres-works":
        secret = {'username': dbutils.secrets.get(scope = "postgres-works", key = "user"),
                'password': dbutils.secrets.get(scope = "postgres-works", key = "password"),
                'host': dbutils.secrets.get(scope = "postgres-works", key = "host"),
                'dbname': dbutils.secrets.get(scope = "postgres-works", key = "dbname"),
                'port': dbutils.secrets.get(scope = "postgres-works", key = "port"),
                'engine': dbutils.secrets.get(scope = "postgres-works", key = "engine")}
    elif secret_name == "author-disambiguation-buckets":
        secret = {'and_save_path': dbutils.secrets.get(scope = "author-disambiguation-buckets", key = "and_save_path"),
                  'database_copy_save_path': dbutils.secrets.get(scope = "author-disambiguation-buckets", key = "database_copy_save_path"),
                  'temp_save_path': dbutils.secrets.get(scope = "author-disambiguation-buckets", key = "temp_save_path"),
                  'orcid_save_path': dbutils.secrets.get(scope = "author-disambiguation-buckets", key = "orcid_save_path")}
    elif secret_name == "heroku-creds":
        secret = {'heroku_id': dbutils.secrets.get(scope = "heroku-creds", key = "heroku_id"),
                  'heroku_token': dbutils.secrets.get(scope = "heroku-creds", key = "heroku_token")}

    return secret

# COMMAND ----------

secret = get_secret()
buckets = get_secret("author-disambiguation-buckets")

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
testing_new_predicates.append(f'paper_id >= {work_id_predicates[-2]} and paper_id < {work_id_predicates[-1]}')
testing_new_predicates.append(f'paper_id >= {work_id_predicates[-1]}')
# testing_new_predicates.append(f'paper_id >= {work_id_predicates[-3]} and paper_id < {work_id_predicates[-2]}')
# testing_new_predicates.append(f'paper_id >= {work_id_predicates[-2]} and paper_id < 4395018129')
# testing_new_predicates.append('paper_id >= 4395018129 and paper_id < 4395318129')
# testing_new_predicates.append('paper_id >= 4395318129 and paper_id < 4395718129')
# testing_new_predicates.append('paper_id >= 4395718129 and paper_id < 4396018129')
# testing_new_predicates.append('paper_id >= 4396018129 and paper_id < 4396118129')
# testing_new_predicates.append('paper_id >= 4396118129')
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

# # mid.citation_unmatched

# df = (spark.read
#       .jdbc(
#               url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
#               table="(select distinct paper_id, reference_sequence_number, raw_json from mid.citation_unmatched) as new_table", 
#               properties={"user": secret['username'],
#                           "password": secret['password']}, 
#               predicates=testing_new_predicates))

# df \
#     .repartition(384).write.mode('overwrite') \
#     .parquet(f"{database_copy_save_path}/mid/citation_unmatched")

# COMMAND ----------

# mid.journal

df = (spark.read
.format("postgresql")
.option("dbtable", 
        f"(select * from mid.journal) as new_table")
.option("host", secret['host'])
.option("port", secret['port'])
.option("database", secret['dbname'])
.option("user", secret['username'])
.option("password", secret['password'])
.load())

df \
.repartition(64) \
.dropDuplicates() \
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

# COMMAND ----------

# mid.institution
df = (spark.read
    .format("postgresql")
    .option("dbtable", f"mid.institution")
    .option("host", secret['host'])
    .option("port", secret['port'])
    .option("database", secret['dbname'])
    .option("user", secret['username'])
    .option("password", secret['password'])
    .load())

df \
    .coalesce(64) \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/mid/institution")

spark.read.parquet(f"{database_copy_save_path}/mid/institution") \
    .coalesce(1) \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/mid/institution_single_file")

# COMMAND ----------

# ins.ror_base
df = (spark.read
    .format("postgresql")
    .option("dbtable", f"ins.ror_base")
    .option("host", secret['host'])
    .option("port", secret['port'])
    .option("database", secret['dbname'])
    .option("user", secret['username'])
    .option("password", secret['password'])
    .load())

df \
    .coalesce(64) \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/ins/ror_base")

# COMMAND ----------

# ins.ror_types
df = (spark.read
    .format("postgresql")
    .option("dbtable", f"ins.ror_types")
    .option("host", secret['host'])
    .option("port", secret['port'])
    .option("database", secret['dbname'])
    .option("user", secret['username'])
    .option("password", secret['password'])
    .load())

df \
    .coalesce(64) \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/ins/ror_types")

# COMMAND ----------

# ins.ror_aliases
df = (spark.read
    .format("postgresql")
    .option("dbtable", f"ins.ror_aliases")
    .option("host", secret['host'])
    .option("port", secret['port'])
    .option("database", secret['dbname'])
    .option("user", secret['username'])
    .option("password", secret['password'])
    .load())

df \
    .coalesce(64) \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/ins/ror_aliases")

# COMMAND ----------

# ins.ror_acronyms
df = (spark.read
    .format("postgresql")
    .option("dbtable", f"ins.ror_acronyms")
    .option("host", secret['host'])
    .option("port", secret['port'])
    .option("database", secret['dbname'])
    .option("user", secret['username'])
    .option("password", secret['password'])
    .load())

df \
    .coalesce(64) \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/ins/ror_acronyms")

# COMMAND ----------

# ins.ror_labels
df = (spark.read
    .format("postgresql")
    .option("dbtable", f"ins.ror_labels")
    .option("host", secret['host'])
    .option("port", secret['port'])
    .option("database", secret['dbname'])
    .option("user", secret['username'])
    .option("password", secret['password'])
    .load())

df \
    .coalesce(64) \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/ins/ror_labels")

# COMMAND ----------

# # ins.recordthresher_record

# if len(work_id_predicates) == 125:
#     final_predicates = []
#     final_predicates.append(f"work_id >= 0 and work_id < {work_id_predicates[2]}")
#     for i in range(2, len(work_id_predicates[:-3]), 3):
#         final_predicates.append(f"work_id >= {work_id_predicates[i]} and work_id < {work_id_predicates[i+3]}")
#     final_predicates.append(f"work_id >= {work_id_predicates[-2]}")
# print(len(final_predicates))

# testing_new_predicates = final_predicates[:-1].copy()
# testing_new_predicates.append(f'work_id >= {work_id_predicates[-3]} and work_id < {work_id_predicates[-2]}')
# testing_new_predicates.append(f'work_id >= {work_id_predicates[-2]} and work_id < {work_id_predicates[-1]}')
# testing_new_predicates.append(f'work_id >= {work_id_predicates[-1]}')
# # testing_new_predicates.append(f'work_id >= {work_id_predicates[-3]} and work_id < {work_id_predicates[-2]}')
# # testing_new_predicates.append(f'work_id >= {work_id_predicates[-2]} and work_id < 4395018129')
# # testing_new_predicates.append('work_id >= 4395018129 and work_id < 4395318129')
# # testing_new_predicates.append('work_id >= 4395318129 and work_id < 4395718129')
# # testing_new_predicates.append('work_id >= 4395718129 and work_id < 4396018129')
# # testing_new_predicates.append('work_id >= 4396018129 and work_id < 4396118129')
# # testing_new_predicates.append('work_id >= 4396118129')
# print(len(testing_new_predicates))

# df = (spark.read
#       .jdbc(
#               url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
#               table="(select work_id, record_type, authors, citations, id, doi from ins.recordthresher_record where record_type = 'mag_location') as new_table", 
#               properties={"user": secret['username'],
#                           "password": secret['password']}, 
#               predicates=testing_new_predicates))

# df \
# .repartition(256) \
# .write.mode('overwrite') \
# .parquet(f"{database_copy_save_path}/ins/recordthresher_record")

# COMMAND ----------

# df = (spark.read
#       .jdbc(
#               url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
#               table="(select work_id, authors, citations from ins.mag_authors) as new_table", 
#               properties={"user": secret['username'],
#                           "password": secret['password']}, 
#               predicates=testing_new_predicates))

# df \
# .repartition(256) \
# .write.mode('overwrite') \
# .parquet(f"{database_copy_save_path}/ins/mag_authors")
