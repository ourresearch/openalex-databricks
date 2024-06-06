# Databricks notebook source
import psycopg2
import boto3
import json
import heroku3

# COMMAND ----------

# MAGIC %md ### Load Secret

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

# COMMAND ----------

secret = get_secret()
heroku_secret = get_secret(secret_name = "prod/heroku/oauth")
buckets = get_secret("prod/aws/buckets")

# COMMAND ----------

prod_save_path = f"{buckets['and_save_path']}/V3/PROD"

# COMMAND ----------

# MAGIC %md ### Update postgres tables

# COMMAND ----------

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

# Establish a connection to the PostgreSQL database
conn = connect_to_db()

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# COMMAND ----------

# MAGIC %md #### Creating temp_authorships_authors_modified table

# COMMAND ----------

update_query = \
f"""create temp table tmp_authorships_authors_modified as (select * from authorships.authors_modified 
where modified_date > now() - interval '6 hours');
alter table tmp_authorships_authors_modified add column paper_id bigint generated always as (split_part(work_author_id, '_', 1)::bigint) stored;
alter table tmp_authorships_authors_modified add column author_sequence_number integer generated always as (split_part(work_author_id, '_', 2)::integer) stored;
create index on tmp_authorships_authors_modified (author_id_changed);
create index on tmp_authorships_authors_modified (author_id);"""

cur.execute(update_query)
conn.commit()

# COMMAND ----------

# MAGIC %md #### Update mid.affiliation

# COMMAND ----------

dynos_to_shutdown = ['run_once_work_add_most_things','run_once_work_add_some_things','run_once_work_add_everything']
curr_q = shutdown_dynos(heroku_secret['heroku_token'], dynos_to_shutdown)
curr_q

# COMMAND ----------

update_query = \
"""-- update affiliation author_ids
update mid.affiliation 
set author_id = t.author_id,
updated_date = now()
from tmp_authorships_authors_modified t 
where t.paper_id = affiliation.paper_id 
and t.author_sequence_number = affiliation.author_sequence_number 
and t.author_id_changed 
and affiliation.author_id is distinct from t.author_id;

-- store works with updated authors
insert into authorships.works_to_enqueue (paper_id) (
  select distinct paper_id from tmp_authorships_authors_modified
) on conflict do nothing;"""

cur.execute(update_query)
conn.commit()

# COMMAND ----------

_ = restart_dynos(heroku_secret['heroku_token'], dynos_to_shutdown, curr_q)

# COMMAND ----------

# MAGIC %md #### Update author properties

# COMMAND ----------

dynos_to_shutdown = ['fast_store_authors']
curr_q = shutdown_dynos(heroku_secret['heroku_token'], dynos_to_shutdown)
curr_q

# COMMAND ----------

update_query = \
"""create temp table tmp_authorships_authors_modified_properties as (
    select author_id, display_name, alternate_names, orcid, created_date, modified_date, author_id_changed from (
        select *, row_number() over (partition by author_id) as rn from tmp_authorships_authors_modified) x 
    where rn = 1);

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
where author_id in (select author_id from tmp_authorships_authors_modified_properties) 
and evidence = 'AND_v3';

insert into mid.author_orcid (
    select author_id, orcid, now(), 'AND_v3' 
    from tmp_authorships_authors_modified_properties 
    where orcid is not null and trim(orcid) != '');"""

cur.execute(update_query)
conn.commit()

# COMMAND ----------

# MAGIC %md ##### Looking at authors to delete due to merged/deleted works

# COMMAND ----------

delete_author_query = f"""
update mid.author 
set merge_into_id = 5317838346,
merge_into_date = now(),
updated_date = now()
where author.author_id in (select author_id from authorships.authors_to_delete);"""

cur.execute(delete_author_query)
conn.commit()

# COMMAND ----------

_ = restart_dynos(heroku_secret['heroku_token'], dynos_to_shutdown, curr_q)

# COMMAND ----------

cur.close()
conn.close()

# COMMAND ----------


