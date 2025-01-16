# Databricks notebook source
import psycopg2
import boto3
import json
import heroku3
import time

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, FloatType, ArrayType, DoubleType, StructType, StructField, LongType, TimestampType

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
heroku_secret = get_secret(secret_name = "heroku-creds")
buckets = get_secret("author-disambiguation-buckets")

# COMMAND ----------

prod_save_path = f"{buckets['and_save_path']}/V3/PROD"

# COMMAND ----------

# MAGIC %md ### Update postgres tables

# COMMAND ----------

def connect_to_db():
    conn = psycopg2.connect( 
        host=secret['host'],
        port=secret['port'],
        user=secret['username'],
        password=secret['password'],
        database=secret['dbname']
    )
    return conn

# COMMAND ----------

# Establish a connection to the PostgreSQL database
conn = connect_to_db()

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# COMMAND ----------

# MAGIC %md #### Creating temp_authorships_authors_modified table

# COMMAND ----------

cur.execute("DROP TABLE if exists tmp_authorships_authors_modified")

# COMMAND ----------

update_query = \
f"""create temp table tmp_authorships_authors_modified as (select *, split_part(work_author_id, '_', 1)::bigint as paper_id, 
split_part(work_author_id, '_', 2)::integer as author_sequence_number from authorships.authors_modified where modified_date > now() - interval '6 hours');
create index on tmp_authorships_authors_modified (author_id_changed);
create index on tmp_authorships_authors_modified (author_id);"""

cur.execute(update_query)
conn.commit()

# COMMAND ----------

# update_query = \
# f"""create temp table tmp_authorships_authors_modified as (select *, split_part(work_author_id, '_', 1)::bigint as paper_id, 
# split_part(work_author_id, '_', 2)::integer as author_sequence_number from authorships.authors_modified where modified_date > now() - interval '6 hours');
# alter table tmp_authorships_authors_modified add column paper_id bigint generated always as (split_part(work_author_id, '_', 1)::bigint) stored;
# alter table tmp_authorships_authors_modified add column author_sequence_number integer generated always as (split_part(work_author_id, '_', 2)::integer) stored;
# create index on tmp_authorships_authors_modified (author_id_changed);
# create index on tmp_authorships_authors_modified (author_id);"""

# cur.execute(update_query)
# conn.commit()

# COMMAND ----------

# MAGIC %md #### Update mid.affiliation

# COMMAND ----------

dynos_to_shutdown = ['run_once_work_add_most_things','run_once_work_add_some_things','run_once_work_add_everything']
curr_q = shutdown_dynos(heroku_secret['heroku_token'], dynos_to_shutdown)
curr_q

# COMMAND ----------

@udf(returnType=StringType())
def process_block_affiliation_change(block):
    conn_part = connect_to_db()

    cur_part = conn_part.cursor()

    for row in block:
        _ = process_row_affiliation_change(conn_part, cur_part, row[0], row[1], row[2])

    cur_part.close()
    conn_part.close()

def process_row_affiliation_change(connection, curs_update, paper_id, author_sequence_number, author_id):
    val = (author_id, paper_id, int(author_sequence_number))
    sql_string = """UPDATE mid.affiliation SET author_id = %s,  updated_date = now() WHERE paper_id = %s AND author_sequence_number = %s"""

    try:
        curs_update.execute(sql_string, val)
        connection.commit()
    except:
        print("Error while updating row in PostgreSQL:", val)
        connection.rollback()

# COMMAND ----------

authors_modified = spark.read.parquet(f"{prod_save_path}/current_authors_modified_table") \
    .select(F.split(F.col('work_author_id'), '_').alias('work_author_id'), 'author_id','display_name','alternate_names','orcid',
            'author_id_changed','created_date','modified_date') \
    .filter(F.size(F.col('work_author_id')) == 2) \
    .select(F.col('work_author_id').getItem(0).cast(LongType()).alias('paper_id'), 
            F.col('work_author_id').getItem(1).cast(IntegerType()).alias('author_sequence_number'),
            'author_id','display_name','alternate_names','orcid',
            'author_id_changed','created_date','modified_date')

# COMMAND ----------

authors_modified \
    .filter(F.col('author_id_changed')).count()

# COMMAND ----------

authors_modified_rand_int = int(authors_modified.filter(F.col('author_id_changed')).count()/200)

# COMMAND ----------

authors_changed = authors_modified \
    .filter(F.col('author_id_changed')) \
    .select(F.array([F.col('paper_id'),F.col('author_sequence_number').cast(LongType()), F.col('author_id')]).alias('data_to_update')) \
    .withColumn('random_int', (F.rand()*authors_modified_rand_int+1).cast(IntegerType())) \
    .groupBy('random_int').agg(F.collect_list(F.col('data_to_update')).alias('data_to_update')) \
    .repartition(80) \
    .withColumn('changes_done', process_block_affiliation_change(F.col('data_to_update')))

authors_changed.cache().count()

# COMMAND ----------

_ = restart_dynos(heroku_secret['heroku_token'], dynos_to_shutdown, curr_q)

# COMMAND ----------

fast_dynos_to_shutdown = ['fast_store_works_authors_changed']
fast_curr_q = shutdown_dynos(heroku_secret['heroku_token'], fast_dynos_to_shutdown)
fast_curr_q

# COMMAND ----------

curr_work_author_store_queue = \
    (spark.read
        .format("postgresql")
        .option("dbtable", "queue.work_authors_changed_store")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password'])
        .load())

# COMMAND ----------

(authors_modified \
    .filter(F.col('author_id_changed'))
    .select('paper_id')
    .join(curr_work_author_store_queue.select(F.col('id').alias('paper_id')), on='paper_id', how='leftanti')
    .select(F.col('paper_id').alias('id')).dropDuplicates().withColumn('rand', F.rand())
    .withColumn('started', F.lit(None).cast(TimestampType()))
    .withColumn('finished', F.lit(None).cast(TimestampType()))
    .repartition(40)
    .write.format("jdbc")
    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
    .option("dbtable", 'queue.work_authors_changed_store')
    .option("user", secret['username'])
    .option("password", secret['password'])
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save())

# COMMAND ----------

# MAGIC %md #### Update author properties

# COMMAND ----------

dynos_to_shutdown = ['fast_store_authors']
curr_q = shutdown_dynos(heroku_secret['heroku_token'], dynos_to_shutdown)
curr_q

# COMMAND ----------

cur.execute("DROP TABLE if exists tmp_authorships_authors_modified_properties")

# COMMAND ----------

cur.execute("DROP TABLE if exists authors_info")

# COMMAND ----------

(authors_modified \
    .dropDuplicates(subset=['author_id']) \
    .select('author_id','display_name','alternate_names','orcid','created_date','modified_date','author_id_changed')
    .repartition(40)
    .write.format("jdbc")
    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
    .option("dbtable", 'authorships.authors_modified_single_row')
    .option("user", secret['username'])
    .option("password", secret['password'])
    .option("driver", "org.postgresql.Driver")
    .option("truncate", True)
    .mode("overwrite")
    .save())

# COMMAND ----------

update_query = \
"""create temp table tmp_authorships_authors_modified_properties as (
    select a.author_id, a.display_name, a.alternate_names, a.orcid, a.created_date, a.modified_date, a.author_id_changed from (select *
        from authorships.authors_modified_single_row) a);

create unique index on tmp_authorships_authors_modified_properties (author_id);
analyze tmp_authorships_authors_modified_properties;

-- upsert authors

create temp table authors_info as (
  select
    author_id,
    display_name,
    coalesce(modified_date, now()) as modified_date
  from
    tmp_authorships_authors_modified_properties);

analyze authors_info;

update mid.author 
set display_name = authors_info.display_name,
updated_date = greatest(
  coalesce(author.updated_date, '1970-01-01'::timestamp without time zone),
  authors_info.modified_date)
from authors_info
where author.author_id = authors_info.author_id
and (
  author.display_name is distinct from authors_info.display_name
  or authors_info.modified_date > coalesce(author.updated_date, '1970-01-01'::timestamp without time zone));

insert into mid.author (author_id, display_name, updated_date, created_date) (
  select authors_info.*, now() from authors_info left join mid.author using (author_id) where author.author_id is null);

-- update alternate names
delete from legacy.mag_main_author_extended_attributes
where author_id in (select author_id from tmp_authorships_authors_modified_properties);

insert into legacy.mag_main_author_extended_attributes (
    select author_id, 1 as attribute_type, unnest(alternate_names) as attribute_value 
    from tmp_authorships_authors_modified_properties);

-- update orcids
delete from mid.author_orcid 
where author_id in (select author_id from tmp_authorships_authors_modified_properties);

delete from mid.author_orcid 
where orcid in (select orcid from tmp_authorships_authors_modified_properties);

insert into mid.author_orcid (
    select author_id, orcid, now(), 'author_dismabiguation' 
    from tmp_authorships_authors_modified_properties 
    where orcid is not null and trim(orcid) != '');"""

cur.execute(update_query)
conn.commit()

# COMMAND ----------

# MAGIC %md ##### Looking at authors to delete due to merged/deleted works

# COMMAND ----------

@udf(returnType=StringType())
def process_block_author_merge(block):
    conn_part = connect_to_db()

    cur_part = conn_part.cursor()

    for row in block:
        _ = process_row_author_merge(conn_part, cur_part, row[0], row[1])

    cur_part.close()
    conn_part.close()

def process_row_author_merge(connection, curs_update, old_author_id, new_author_id):
    val = (new_author_id, old_author_id)
    sql_string = \
        """UPDATE mid.author set merge_into_id = %s, merge_into_date = now(),  updated_date = now() WHERE author_id = %s"""

    try:
        curs_update.execute(sql_string, val)
        connection.commit()
    except:
        print("Error while updating row in PostgreSQL:", val)
        connection.rollback()

# COMMAND ----------

authors_to_merge = spark.read.parquet(f"{prod_save_path}/author_merges_to_combine_orcid/")

# COMMAND ----------

authors_to_merge_rand_int = int(authors_to_merge.count()/200)

# COMMAND ----------

authors_merged = authors_to_merge \
    .select(F.array([F.col('author_id'),F.col('new_author_id')]).alias('data_to_update')) \
    .withColumn('random_int', (F.rand()*authors_to_merge_rand_int+1).cast(IntegerType())) \
    .groupBy('random_int').agg(F.collect_list(F.col('data_to_update')).alias('data_to_update')) \
    .repartition(40) \
    .withColumn('changes_done', process_block_author_merge(F.col('data_to_update')))

authors_merged.cache().count()

# COMMAND ----------

merge_author_query = f"""
UPDATE mid.author ma
SET merge_into_id = a.merge_to_id,
    merge_into_date = now(),
    updated_date = now()
FROM authorships.author_id_merges a
WHERE ma.author_id = a.merge_from_id
AND ma.merge_into_id is null;"""

cur.execute(merge_author_query)
conn.commit()

# COMMAND ----------

delete_author_query = f"""
update mid.author 
set merge_into_id = 5317838346,
merge_into_date = now(),
updated_date = now()
where author.author_id in (select author_id from authorships.authors_to_delete) and author.merge_into_id is null;"""

cur.execute(delete_author_query)
conn.commit()

# COMMAND ----------

cur.close()
conn.close()

# COMMAND ----------

_ = restart_dynos(heroku_secret['heroku_token'], dynos_to_shutdown, curr_q)

# COMMAND ----------

_ = restart_dynos(heroku_secret['heroku_token'], fast_dynos_to_shutdown, fast_curr_q)
