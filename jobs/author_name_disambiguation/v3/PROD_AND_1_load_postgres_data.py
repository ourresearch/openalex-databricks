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

# 40 minutes

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, FloatType, ArrayType, DoubleType, StructType, StructField,LongType

# COMMAND ----------

# MAGIC %md ### Load Secret

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

prod_save_path = f"{buckets['and_save_path']}/V3/PROD"
orcid_save_path = f"{buckets['orcid_save_path']}"
database_copy_save_path = f"{buckets['database_copy_save_path']}"
temp_save_path = f"{buckets['temp_save_path']}/latest_tables_for_AND/"

# COMMAND ----------

# MAGIC %md ### Tables to load

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
# testing_new_predicates.append(f'paper_id >= {work_id_predicates[-3]} and paper_id < 4395018129')
# testing_new_predicates.append('paper_id >= 4395018129 and paper_id < 4395318129')
# testing_new_predicates.append('paper_id >= 4395318129 and paper_id < 4395718129')
# testing_new_predicates.append('paper_id >= 4395718129 and paper_id < 4396018129')
# testing_new_predicates.append('paper_id >= 4396018129 and paper_id < 4396118129')
# testing_new_predicates.append('paper_id >= 4396118129')
print(len(testing_new_predicates))

# COMMAND ----------

testing_new_predicates

# COMMAND ----------

# mid.work
df = (spark.read
      .jdbc(
              url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
              table="(select distinct paper_id, original_title, doi_lower, oa_status, journal_id, merge_into_id, publication_date, type, type_crossref, arxiv_id, is_paratext, best_url, best_free_url, unpaywall_normalize_title, created_date from mid.work) new_table", 
              properties={"user": secret['username'],
                          "password": secret['password']}, 
              predicates=testing_new_predicates))

df \
        .repartition(384)\
        .dropDuplicates()\
        .write.mode('overwrite') \
        .parquet(f"{database_copy_save_path}/mid/work")

# COMMAND ----------

# mid.affiliation

df = (spark.read
      .jdbc(
              url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
              table="(select distinct paper_id,author_id,affiliation_id,author_sequence_number,original_author,original_orcid,original_affiliation,is_corresponding_author from mid.affiliation) new_table", 
              properties={"user": secret['username'],
                          "password": secret['password']}, 
              predicates=testing_new_predicates))

df \
        .repartition(384)\
        .dropDuplicates()\
        .write.mode('overwrite') \
        .parquet(f"{database_copy_save_path}/mid/affiliation")

# COMMAND ----------

# mid.work_topic (not needed yet for AND)

# df = (spark.read
#       .jdbc(
#               url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
#               table="(select distinct paper_id, topic_id from mid.work_topic) as new_table", 
#               properties={"user": secret['username'],
#                           "password": secret['password']}, 
#               predicates=final_predicates))

# df \
#         .write.mode('overwrite') \
#         .parquet(f"{temp_save_path}work_topic_temp")

# spark.read.parquet(f"{temp_save_path}work_topic_temp")\
#     .repartition(256).write.mode('overwrite') \
#     .parquet(f"{temp_save_path}work_topic")

# COMMAND ----------

# mid.topic (not needed yet for AND)

# df = (spark.read
# .format("postgresql")
# .option("dbtable", 
#         f"(select distinct topic_id, display_name from mid.topic) as new_table")
# .option("host", secret['host'])
# .option("port", secret['port'])
# .option("database", secret['dbname'])
# .option("user", secret['username'])
# .option("password", secret['password'])
# .load())

# df \
# .select('topic_id','display_name') \
# .repartition(64) \
# .write.mode('overwrite') \
# .parquet(f"{temp_save_path}topic")

# COMMAND ----------

# mid.work_concept

df = (spark.read
      .jdbc(
              url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
              table="(select distinct paper_id, field_of_study, score from mid.work_concept where field_of_study not in (17744445,138885662,162324750,144133560,15744967,33923547,71924100,86803240,41008148,127313418,185592680,142362112,144024400,127413603,205649164,95457728,192562407,121332964,39432304) and score > 0.3) as new_table", 
              properties={"user": secret['username'],
                          "password": secret['password']}, 
              predicates=testing_new_predicates))

df \
        .repartition(384)\
        .dropDuplicates()\
        .write.mode('overwrite') \
        .parquet(f"{database_copy_save_path}/mid/work_concept")

# COMMAND ----------

# # mid.work_keyword_concept

# df = (spark.read
#       .jdbc(
#               url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
#               table="(select distinct paper_id from mid.work_keyword_concept) as new_table", 
#               properties={"user": secret['username'],
#                           "password": secret['password']}, 
#               predicates=testing_new_predicates))

# df \
#         .repartition(384)\
#         .dropDuplicates()\
#         .write.mode('overwrite') \
#         .parquet(f"{database_copy_save_path}/mid/work_keyword_concept")

# COMMAND ----------

# mid.citation

df = (spark.read
      .jdbc(
              url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
              table="(select distinct paper_id, paper_reference_id from mid.citation where paper_reference_id is not null) as new_table", 
              properties={"user": secret['username'],
                          "password": secret['password']}, 
              predicates=testing_new_predicates))

df \
        .repartition(384)\
        .dropDuplicates()\
        .write.mode('overwrite') \
        .parquet(f"{database_copy_save_path}/mid/citation")

# COMMAND ----------

# Getting predicates for reading tables
df = (spark.read
    .format("postgresql")
    .option("dbtable", "(SELECT histogram_bounds::text::bigint[] FROM pg_stats WHERE tablename = 'author' AND attname = 'author_id') new_table")
    .option("host", secret['host'])
    .option("port", secret['port'])
    .option("database", secret['dbname'])
    .option("user", secret['username'])
    .option("password", secret['password'])
    .load())

author_id_predicates = df.collect()[0][0]
author_id_predicates = [x for x in author_id_predicates if x >= 5000000000]

if len(author_id_predicates) == 125:
    final_predicates = []
    final_predicates.append(f"author_id >= 5000000000 and author_id < {author_id_predicates[2]}")
    for i in range(2, len(author_id_predicates[:-3]), 3):
        final_predicates.append(f"author_id >= {author_id_predicates[i]} and author_id < {author_id_predicates[i+3]}")
    final_predicates.append(f"author_id >= {author_id_predicates[-2]}")
elif len(author_id_predicates) <= 35:
    final_predicates = []
    final_predicates.append(f"author_id >= 5000000000 and author_id < {author_id_predicates[0]}")
    for i in range(len(author_id_predicates[:-1])):
        final_predicates.append(f"author_id >= {author_id_predicates[i]} and author_id < {author_id_predicates[i+1]}")
    final_predicates.append(f"author_id >= {author_id_predicates[-1]}")
print(len(final_predicates))

# COMMAND ----------

final_predicates

# COMMAND ----------

# mid.author
df = (spark.read
      .jdbc(
              url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
              table="(select distinct author_id, display_name, merge_into_id from mid.author where author_id >= 5000000000) as new_table", 
              properties={"user": secret['username'],
                          "password": secret['password']}, 
              predicates=final_predicates))

df \
        .repartition(384)\
        .dropDuplicates()\
        .write.mode('overwrite') \
        .parquet(f"{database_copy_save_path}/mid/author")

# COMMAND ----------

# orcid.openalex_authorships
df = (spark.read
        .format("postgresql")
        .option("dbtable", f"(select distinct paper_id, author_sequence_number, orcid, random_num from orcid.openalex_authorships) as new_table")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("partitionColumn", "random_num")
        .option("lowerBound", "0")
        .option("upperBound", "50")
        .option("numPartitions", "26")
        .option("fetchsize", "200").load())

df \
        .repartition(384).write.mode('overwrite') \
        .parquet(f"{database_copy_save_path}/orcid/openalex_authorships")

# COMMAND ----------

# orcid.add_orcid
df = (spark.read
        .format("postgresql")
        .option("dbtable", f"(select distinct work_author_id, new_orcid, request_date from orcid.add_orcid) as new_table")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password']).load())

df \
        .write.mode('overwrite') \
        .parquet(f"{database_copy_save_path}/orcid/add_orcid")

# COMMAND ----------

# MAGIC %md #### ORCID and extended attributes tables can be configured later

# COMMAND ----------

# mid.author_orcid

# df = (spark.read
#       .jdbc(
#               url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
#               table="(select distinct author_id, orcid, evidence from mid.author_orcid where author_id > 5000000000) as new_table", 
#               properties={"user": secret['username'],
#                           "password": secret['password']}, 
#               predicates=final_predicates))

# df \
#         .write.mode('overwrite') \
#         .parquet(f"{temp_save_path}author_orcid_temp")

# spark.read.parquet(f"{temp_save_path}author_orcid_temp")\
#     .repartition(256).write.mode('overwrite') \
#     .parquet(f"{temp_save_path}author_orcid")

# COMMAND ----------

# legacy.mag_main_author_extended_attributes

# df = (spark.read
#       .jdbc(
#               url=f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}",
#               table="(select author_id, attribute_value as alternate_name from legacy.mag_main_author_extended_attributes where author_id > 5000000000 and attribute_type=1) as new_table", 
#               properties={"user": secret['username'],
#                           "password": secret['password']}, 
#               predicates=final_predicates))

# df \
#         .write.mode('overwrite') \
#         .parquet(f"{temp_save_path}author_alternate_names_temp")

# spark.read.parquet(f"{temp_save_path}author_alternate_names_temp")\
#     .repartition(256).write.mode('overwrite') \
#     .parquet(f"{temp_save_path}author_alternate_names")

# COMMAND ----------

# MAGIC %md #### Creating new features table

# COMMAND ----------

work =spark.read.parquet(f"{database_copy_save_path}/mid/work").dropDuplicates().filter(F.col('merge_into_id').isNull()).select('paper_id')
work.cache().count()

# COMMAND ----------

affiliations = spark.read.parquet(f"{database_copy_save_path}/mid/affiliation").dropDuplicates() \
    .join(work, how='inner', on='paper_id') \
    .withColumn('work_author_id', F.concat_ws("_", F.col('paper_id'), F.col('author_sequence_number'))) \
    .fillna(-1, subset=['author_id']) \
    .filter(F.col('author_id')==-1)
affiliations.cache().count()

# COMMAND ----------

w1 = Window.partitionBy('work_author_id').orderBy(F.col('author_name_len').desc())

# COMMAND ----------

coauthors = affiliations.dropDuplicates() \
    .dropDuplicates(subset=['work_author_id','paper_id','original_author']) \
    .withColumn('author_name_len', F.length(F.col('original_author'))) \
    .withColumn('work_author_rank', F.row_number().over(w1)) \
    .filter(F.col('work_author_rank')==1) \
    .groupBy('paper_id').agg(F.collect_set(F.col('original_author')).alias('coauthors'))
coauthors.cache().count()

# COMMAND ----------

work_concept = spark.read.parquet(f"{database_copy_save_path}/mid/work_concept").dropDuplicates() \
    .join(affiliations.select('paper_id').dropDuplicates(), on='paper_id', how='inner') \
    .groupby('paper_id').agg(F.collect_set(F.col('field_of_study')).alias('concepts'))
work_concept.cache().count()

# COMMAND ----------

citation = spark.read.parquet(f"{database_copy_save_path}/mid/citation").dropDuplicates()\
    .join(affiliations.select('paper_id').dropDuplicates(), on='paper_id', how='inner') \
    .groupBy('paper_id').agg(F.collect_set(F.col('paper_reference_id')).alias('citations'))
citation.cache().count()

# COMMAND ----------

# MAGIC %md #### Looking at current authors table

# COMMAND ----------

# author = spark.read.parquet(f"{temp_save_path}author")
# author.cache().count()

# COMMAND ----------

# author_orcid = spark.read.parquet(f"{temp_save_path}author_orcid").dropDuplicates()
# author_orcid.cache().count()

# COMMAND ----------

# alternate_names = spark.read.parquet(f"{temp_save_path}author_alternate_names") \
#     .groupBy('author_id').agg(F.collect_set(F.col('alternate_name')).alias('alternate_names'))
# alternate_names.cache().count()

# COMMAND ----------

# MAGIC %md #### Getting authors that need to go through AND

# COMMAND ----------

@udf(returnType=StringType())
def get_orcid_from_list(orcid_list):
    if isinstance(orcid_list, list):
        if orcid_list:
            orcid = orcid_list[0]
        else:
            orcid = ''
    elif isinstance(orcid_list, set):
        orcid_list = list(orcid_list)
        if orcid_list:
            orcid = orcid_list[0]
        else:
            orcid = ''
    else:
        orcid = ''
    return orcid

# COMMAND ----------

# work_author_id, original_author, orcid, concepts, institutions, citations, coauthors, created_date
affiliations \
    .groupBy('work_author_id','original_author','author_id','paper_id')\
    .agg(F.collect_set(F.col('affiliation_id')).alias('institutions'), 
         F.collect_set(F.col('original_orcid')).alias('orcid')) \
    .withColumn('final_orcid', get_orcid_from_list(F.col('orcid'))) \
    .withColumn('author_name_len', F.length(F.col('original_author'))) \
    .withColumn('work_author_rank', F.row_number().over(w1)) \
    .filter(F.col('work_author_rank')==1) \
    .join(work_concept, how='left', on='paper_id') \
    .join(citation, how='left', on='paper_id') \
    .join(coauthors, how='left', on='paper_id') \
    .withColumn("created_date", F.current_timestamp()) \
    .select("work_author_id", "original_author", F.col("final_orcid").alias('orcid'), "concepts", "institutions", "citations", "coauthors", "created_date") \
    .write.mode('overwrite')\
    .parquet(f"{prod_save_path}/input_data_for_AND")

# COMMAND ----------


