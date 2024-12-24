# Databricks notebook source
import pickle
import boto3
import datetime
import re
import json
import pandas as pd
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_colwidth', None)
import numpy as np
import unicodedata
import matplotlib.pyplot as plt
from unidecode import unidecode
import random
import heroku3
import psycopg2

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, FloatType, ArrayType, DoubleType, StructType, StructField,LongType,TimestampType

# COMMAND ----------

from changing_affs import matching_based_on_current_affs
from institution_string_matching import string_matching_function

# create udfs
matching_based_on_current_affs_udf = F.udf(matching_based_on_current_affs, ArrayType(LongType()))
string_matching_function_udf = F.udf(string_matching_function, ArrayType(LongType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting all data

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

def connect_to_db():
    secret = get_secret()
    conn = psycopg2.connect( 
        host=secret['host'],
        port=secret['port'],
        user=secret['username'],
        password=secret['password'],
        database=secret['dbname']
    )
    return conn

# COMMAND ----------

secret = get_secret()
buckets = get_secret("prod/aws/buckets")
heroku_secret = get_secret(secret_name = "prod/heroku/oauth")

# COMMAND ----------

database_copy_save_path = f"{buckets['database_copy_save_path']}"
start_datetime = datetime.datetime.now()
curr_date = start_datetime.strftime("%Y_%m_%d_%H_%M")
# curr_date = '2024_07_14_23_18'
iteration_save_path = f"{buckets['temp_save_path']}/institution_fixes_{curr_date}/"

# COMMAND ----------

institutions = spark.read.parquet(f"{database_copy_save_path}/mid/institution")\
    .filter(F.col('merge_into_id').isNull()).filter(F.col('created_date').startswith('2024-')) \
    .select('affiliation_id','ror_id','display_name')

# COMMAND ----------

spark.read.parquet(f"{database_copy_save_path}/mid/institution").filter(F.col('merge_into_id').isNull()).select('affiliation_id','ror_id','display_name') \
    .toPandas().to_parquet("institutions_current.parquet")

# COMMAND ----------

affiliations = spark.read.parquet(f"{database_copy_save_path}/mid/affiliation") \
    .select('paper_id','affiliation_id').dropDuplicates() \
    .join(institutions, how='inner', on='affiliation_id')

# COMMAND ----------

# MAGIC %md #### Functions

# COMMAND ----------

@udf(returnType=ArrayType(LongType()))
def join_aff_id_cols(aff_ids, over_aff_ids):
    if isinstance(aff_ids, list):
        if isinstance(over_aff_ids, list):
            if (-1 in over_aff_ids) & (len(over_aff_ids) > 1):
                return [x for x in over_aff_ids if x != -1].copy()
            elif -1 in over_aff_ids:
                return over_aff_ids.copy()
            elif (len(over_aff_ids) > 0) & (isinstance(over_aff_ids[0], int)):
                return over_aff_ids.copy()
            else:
                return aff_ids
        else:
            return aff_ids
    else:
        return [-1]

# COMMAND ----------

# MAGIC %md #### Downloading table for fixing

# COMMAND ----------

testing = spark.read \
    .format("redshift") \
    .option("url", "jdbc:redshift://default-workgroup.639657227068.us-east-1.redshift-serverless.amazonaws.com:5439/dev") \
    .option("user", "awsuser") \
    .option("password", "y74AFxqNwD9r9n5") \
    .option("dbtable", "public.affiliation_string_v2") \
    .option("tempdir", f"{buckets['temp_save_path']}/temp_data_for_redshift") \
    .option("forward_spark_s3_credentials", "true").load()

# COMMAND ----------

testing.count()

# COMMAND ----------

testing \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/mid/affiliation_string_v2")

# COMMAND ----------

aff_string_table = spark.read.parquet(f"{database_copy_save_path}/mid/affiliation_string_v2") \
        .na.fill('[-1]', subset=['affiliation_ids']) \
    .na.fill('[]', subset=['affiliation_ids_override']) \
    .withColumn('new_affiliation_ids', F.regexp_replace(F.col("affiliation_ids"), " ", "")) \
    .withColumn('new_affiliation_ids', F.split(F.split(F.col('new_affiliation_ids'), '\[').getItem(1), '\]').getItem(0)) \
    .withColumn('new_affiliation_ids', F.split(F.col('new_affiliation_ids'), ',')) \
    .withColumn('new_affiliation_ids_override', 
                F.regexp_replace(F.col("affiliation_ids_override"), " ", "")) \
    .withColumn('new_affiliation_ids_override', 
                F.split(F.split(F.col('new_affiliation_ids_override'), '\[').getItem(1), '\]').getItem(0)) \
    .withColumn('new_affiliation_ids_override', F.split(F.col('new_affiliation_ids_override'), ',')) \
    .select('original_affiliation',F.col('new_affiliation_ids').cast(ArrayType(LongType())).alias('affiliation_ids'),
            F.col('new_affiliation_ids_override').cast(ArrayType(LongType())).alias('affiliation_ids_override')) \
    .dropDuplicates(subset=['original_affiliation']) \
    .withColumn('current_affs', join_aff_id_cols(F.col('affiliation_ids'), 
                                                             F.col('affiliation_ids_override')))
    
aff_string_table.cache().count()

# COMMAND ----------

# MAGIC %md #### Running the string matching functions

# COMMAND ----------

@udf(returnType=IntegerType())
def check_for_change(aff_ids, over_aff_ids):
    curr_set = set(aff_ids)
    new_set = set(over_aff_ids)
    if curr_set == new_set:
        return 0
    elif (len(curr_set) == 1) and (len(new_set) == 1) and (not isinstance(list(curr_set)[0], int)) and (list(over_aff_ids)[0]==-1):
        return 0
    else:
        return 1

# COMMAND ----------

@udf(returnType=ArrayType(LongType()))
def remove_negative_1(aff_ids):
    new_aff_ids = [x for x in aff_ids if isinstance(x, int)].copy()
    if (-1 in new_aff_ids) & (len(new_aff_ids) > 1):
        return [x for x in new_aff_ids if x != -1].copy()
    elif not new_aff_ids:
        return [-1]
    else:
        return new_aff_ids.copy()

# COMMAND ----------

aff_string_table \
    .withColumn('add_affs_1', string_matching_function_udf(F.col('original_affiliation'))) \
    .withColumn('temp_affs', F.array_distinct(F.array_union(F.col('add_affs_1'), F.col('current_affs')))) \
    .withColumn('temp_affs_1', remove_negative_1(F.col('temp_affs'))) \
    .withColumn('affs_removed', matching_based_on_current_affs_udf(F.col('temp_affs_1'), F.col('original_affiliation'))) \
    .withColumn('temp_affs_2', remove_negative_1(F.col('affs_removed'))) \
    .withColumn('row_changed', check_for_change(F.col('current_affs'), F.col('temp_affs_2'))) \
    .filter(F.col('row_changed')==1) \
    .select('original_affiliation', 'current_affs', F.col('temp_affs_2').alias('affiliation_ids_override')) \
    .write.mode('overwrite')\
    .parquet(f"{iteration_save_path}strings_for_v2_table_dist/")

# COMMAND ----------

final_counts_data = spark.read.parquet(f"{iteration_save_path}strings_for_v2_table_dist/")
final_counts_data.cache().count()

# COMMAND ----------

display(final_counts_data.sample(0.01))

# COMMAND ----------

# MAGIC %md ### Updating override table and putting works in most_things queue

# COMMAND ----------

@udf(returnType=StringType())
def process_block_affiliation_change(block):
    conn_part = connect_to_db()

    cur_part = conn_part.cursor()

    for row in block:
        _ = process_row_affiliation_change(conn_part, cur_part, row[0], row[1])

    cur_part.close()
    conn_part.close()

def process_row_affiliation_change(connection, curs_update, aff_string, aff_ids):
    dt = datetime.datetime.now(datetime.timezone.utc)
    val = (dt, aff_string)
    sql_string = f"""UPDATE mid.affiliation_string_v2 SET affiliation_ids_override = jsonb_build_array({aff_ids}), updated = %s WHERE original_affiliation = %s"""

    try:
        curs_update.execute(sql_string, val)
        connection.commit()
    except:
        print("Error while updating row in PostgreSQL:", val)
        connection.rollback()

# COMMAND ----------

strings_to_change = spark.read.parquet(f"{iteration_save_path}strings_for_v2_table_dist/")

# COMMAND ----------

strings_to_change_rand_int = int(strings_to_change.count()/200)

# COMMAND ----------

display(strings_to_change \
    .select('original_affiliation', F.concat_ws(",", F.col('affiliation_ids_override')).alias('affiliation_ids_override')))

# COMMAND ----------

strings_changed = strings_to_change \
    .select('original_affiliation', F.concat_ws(",", F.col('affiliation_ids_override')).alias('affiliation_ids_override')) \
    .select(F.array([F.col('original_affiliation'), F.col('affiliation_ids_override')]).alias('data_to_update')) \
    .withColumn('random_int', (F.rand()*strings_to_change_rand_int+1).cast(IntegerType())) \
    .groupBy('random_int').agg(F.collect_list(F.col('data_to_update')).alias('data_to_update')) \
    .repartition(80) \
    .withColumn('changes_done', process_block_affiliation_change(F.col('data_to_update')))

strings_changed.cache().count()

# COMMAND ----------

current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

test_query = \
    f"""INSERT INTO queue.run_once_work_add_most_things(work_id, rand)
    SELECT DISTINCT paper_id, random()
    FROM mid.affiliatio
    JOIN mid.affiliation_string_v2
    ON mid.affiliation.original_affiliation = mid.affiliation_string_v2.original_affiliation
    WHERE mid.affiliation_string_v2.updated>'{current_date} 00:00' ON CONFLICT DO NOTHING;"""

# COMMAND ----------

connection = connect_to_db()
curs_update = connection.cursor()

# COMMAND ----------

try:
    curs_update.execute(test_query)
    connection.commit()
except:
    print("Error while updating row in PostgreSQL")
    connection.rollback()

# COMMAND ----------

curs_update.execute("SELECT COUNT(*) FROM queue.run_once_work_add_most_things;")
results = curs_update.fetchone() 
print(results)

# COMMAND ----------

curs_update.close()
connection.close()

# COMMAND ----------


