# Databricks notebook source
import pickle
import boto3
import re
import json
import random
import unicodedata
from unidecode import unidecode
import datetime
from statistics import mode
from nameparser import HumanName
from collections import Counter
import pandas as pd
import numpy as np
import xgboost as xgb

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, FloatType, ArrayType, DoubleType, StructType, StructField, LongType

# COMMAND ----------

# MAGIC %md #### Load Secrets

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

start_datetime = datetime.datetime.now()
curr_date = start_datetime.strftime("%Y_%m_%d_%H_%M")
# curr_date = '2024_04_24_19_06'
prod_save_path = f"{buckets['and_save_path']}/V3/PROD"
temp_save_path = f"{buckets['temp_save_path']}/{curr_date}"
orcid_save_path = f"{buckets['orcid_save_path']}"
database_copy_save_path = f"{buckets['database_copy_save_path']}"
name_of_stats_to_track = []
stats_to_track = []
print(curr_date)

# COMMAND ----------

# MAGIC %md #### Load Disambiguator Model

# COMMAND ----------

with open("./disambiguator/model.pkl", "rb") as f:
    disambiguator_model = pickle.load(f)

broadcast_disambiguator_model = spark.sparkContext.broadcast(disambiguator_model)

# COMMAND ----------

# MAGIC %md #### Get Latest Data to Disambiguate

# COMMAND ----------

# df = (spark.read
#     .format("postgresql")
#     .option("dbtable", "(select * from authorships.for_disambiguation_mv order by created_date DESC limit 300000) new_table")
#     .option("host", secret['host'])
#     .option("port", secret['port'])
#     .option("database", secret['dbname'])
#     .option("user", secret['username'])
#     .option("password", secret['password'])
#     .option("partitionColumn", "partition")
#     .option("lowerBound", 0)
#     .option("upperBound", 21)
#     .option("numPartitions", 6)
#     .option("fetchSize", "15")
#     .load()
# )

# df.write.mode('overwrite') \
#     .parquet(f"{temp_save_path}/raw_data_to_disambiguate/")

# COMMAND ----------

# created_date_df = (spark.read
#     .format("postgresql")
#     .option("dbtable", "(select max(created_date) as created_date from authorships.for_disambiguation_mv) new_table")
#     .option("host", secret['host'])
#     .option("port", secret['port'])
#     .option("database", secret['dbname'])
#     .option("user", secret['username'])
#     .option("password", secret['password'])
#     .option("fetchSize", "15")
#     .load()
# )

# print(created_date_df.select('created_date').collect()[0][0])

# COMMAND ----------

total_count = spark.read.parquet(f"{prod_save_path}/input_data_for_AND").count()
total_count

# COMMAND ----------

sample_size_for_each_stage = 700000 # this is the sample size for each round of AND
sample_per = sample_size_for_each_stage/total_count

# 425,000 rows per run, 2 hr 15 min, r5d.4xlarge (driver), r5d.12xlarge (executor) (x10)
# 450,000 rows per run, 2 hr 15 min, r5d.4xlarge (driver), r5d.12xlarge (executor) (x10)
# 600,000 rows per run, 2 hr 31 min, r5d.4xlarge (driver), r5d.12xlarge (executor) (x10)
# 700,000 rows per run, 2 hr 41 min, r5d.4xlarge (driver), r5d.12xlarge (executor) (x10)

# COMMAND ----------

sample_per

# COMMAND ----------

if sample_per < 1.0:
    spark.read.parquet(f"{prod_save_path}/input_data_for_AND") \
        .sample(sample_per) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/raw_data_to_disambiguate/")
else:
    spark.read.parquet(f"{prod_save_path}/input_data_for_AND") \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/raw_data_to_disambiguate/")

# COMMAND ----------

raw_count = spark.read.parquet(f"{temp_save_path}/raw_data_to_disambiguate/").count()
name_of_stats_to_track.append('raw_data_count')
stats_to_track.append(raw_count)
print(raw_count)

# COMMAND ----------

# MAGIC %md ### FOR CREATING NEW CLUSTERS
# MAGIC
# MAGIC #### 1. For removing works from cluster
# MAGIC * Add to the remove from cluster folder (should mostly already be set up)
# MAGIC
# MAGIC #### 2. For claiming works for a cluster (that already exists)
# MAGIC * Create a table of work_author_ids to cluster_id
# MAGIC
# MAGIC #### 3. For merging authors
# MAGIC * One column is 'merge_from' author_id and one column is "merge_into" author id
# MAGIC
# MAGIC #### 4. For separating one author cluster out of another (overmerged cluster)
# MAGIC * Create a table where one work_author_id is in one column and list of all work_author_ids is in another column
# MAGIC * Process will be similar to duplicate authors: 
# MAGIC   1. Single row gets a new author ID
# MAGIC   2. Join on work_author_id and explode list of work_author_ids
# MAGIC   3. Join author table using the exploded work_author_ids

# COMMAND ----------

# MAGIC %md #### Functions

# COMMAND ----------

@udf(returnType=StringType())
def transform_author_name(author):
    if author.startswith("None "):
        author = author.replace("None ", "")
    elif author.startswith("Array "):
        author = author.replace("Array ", "")

    author = unicodedata.normalize('NFKC', author)
    
    author_name = HumanName(" ".join(author.split()))

    if (author_name.title == 'Dr.') | (author_name.title == ''):
        temp_new_author_name = f"{author_name.first} {author_name.middle} {author_name.last}"
    else:
        temp_new_author_name = f"{author_name.title} {author_name.first} {author_name.middle} {author_name.last}"

    new_author_name = " ".join(temp_new_author_name.split())

    author_names = new_author_name.split(" ")
    
    if (author_name.title != '') : 
        final_author_name = new_author_name
    else:
        if len(author_names) == 1:
            final_author_name = new_author_name
        elif len(author_names) == 2:
            if (len(author_names[1]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        elif len(author_names) == 3:
            if (len(author_names[1]) == 1) & (len(author_names[2]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[2]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[2]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]==".") & (author_names[2][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[2]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        elif len(author_names) == 4:
            if (len(author_names[1]) == 1) & (len(author_names[2]) == 1) & (len(author_names[3]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[2]} {author_names[3]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[2]) == 2) & (len(author_names[3]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]==".") & (author_names[2][1]==".") & (author_names[3][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[2]} {author_names[3]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        else:
            final_author_name = new_author_name
    return final_author_name


@udf(returnType=ArrayType(StringType()))
def transform_coauthors(coauthors):
    return [transform_author_name_reg(x) for x in coauthors]

def transform_author_name_reg(author):
    if author.startswith("None "):
        author = author.replace("None ", "")
    elif author.startswith("Array "):
        author = author.replace("Array ", "")

    author = unicodedata.normalize('NFKC', author)
    
    author_name = HumanName(" ".join(author.split()))

    if (author_name.title == 'Dr.') | (author_name.title == ''):
        temp_new_author_name = f"{author_name.first} {author_name.middle} {author_name.last}"
    else:
        temp_new_author_name = f"{author_name.title} {author_name.first} {author_name.middle} {author_name.last}"

    new_author_name = " ".join(temp_new_author_name.split())

    author_names = new_author_name.split(" ")
    
    if (author_name.title != '') : 
        final_author_name = new_author_name
    else:
        if len(author_names) == 1:
            final_author_name = new_author_name
        elif len(author_names) == 2:
            if (len(author_names[1]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        elif len(author_names) == 3:
            if (len(author_names[1]) == 1) & (len(author_names[2]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[2]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[2]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]==".") & (author_names[2][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[2]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        elif len(author_names) == 4:
            if (len(author_names[1]) == 1) & (len(author_names[2]) == 1) & (len(author_names[3]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[2]} {author_names[3]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[2]) == 2) & (len(author_names[3]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]==".") & (author_names[2][1]==".") & (author_names[3][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[2]} {author_names[3]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        else:
            final_author_name = new_author_name
    return final_author_name

@udf(returnType=ArrayType(StringType()))  
def remove_current_author(author, coauthors):
    return [x for x in coauthors if x!=author][:250]

@udf(returnType=ArrayType(StringType()))
def transform_list_col_for_nulls_string(col_with_nulls):
    if isinstance(col_with_nulls, list):
        return col_with_nulls[:250]
    else:
        return []

@udf(returnType=ArrayType(LongType()))
def transform_list_col_for_nulls_long(col_with_nulls):
    if isinstance(col_with_nulls, list):
        return col_with_nulls[:250]
    else:
        return []

@udf(returnType=ArrayType(StringType()))
def remove_current_author(author, coauthors):
    return [x for x in coauthors if x!=author][:250]

@udf(returnType=ArrayType(StringType()))
def coauthor_transform(coauthors):
    final_coauthors = []
    skip_list = [" ", "," ,"." ,"-" ,":" ,"/"]

    for coauthor in coauthors:
        split_coauthor = coauthor.split(" ")
        if len(split_coauthor) > 1:
            temp_coauthor = f"{split_coauthor[0][0]}_{split_coauthor[-1]}".lower()
            final_coauthors.append("".join([i for i in temp_coauthor if i not in skip_list]))
        else:
            final_coauthors.append("".join([i for i in coauthor if i not in skip_list]))

    return list(set(final_coauthors))

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

def length_greater_than_6(x):
    return (F.length(x) > 6)

def concept_L0_removed(x):
    return ~x.isin([17744445,138885662,162324750,144133560,15744967,33923547,71924100,86803240,41008148,127313418,185592680,142362112,144024400,127413603,205649164,95457728,192562407,121332964,39432304])

# COMMAND ----------

@udf(returnType=IntegerType())
def get_random_int_udf(block_id):
    return random.randint(0, 1000000)

def length_greater_than_6(x):
    return (F.length(x) > 6)

def concept_L0_removed(x):
    return ~x.isin([17744445,138885662,162324750,144133560,15744967,33923547,71924100,86803240,41008148,127313418,185592680,142362112,144024400,127413603,205649164,95457728,192562407,121332964,39432304])

@udf(returnType=StringType())
def only_get_last(all_names):
    all_names = all_names.split(" ")
    if len(all_names) > 1:
        return all_names[-1]
    else:
        return all_names[0]
    
@udf (returnType=ArrayType(ArrayType(StringType())))
def score_data(full_arr):
    full_arr = np.array(full_arr)
    data_arr = full_arr[:,2:].astype('float')
    block_arr = full_arr[:,0]
    label_arr = full_arr[:,1]
    model_preds = broadcast_disambiguator_model.value.predict_proba(data_arr)[:,1]
    return np.vstack([block_arr[model_preds>0.05], label_arr[model_preds>0.05], model_preds[model_preds>0.05].astype('str')]).T.tolist()

@udf(returnType=StringType())
def get_starting_letter(names):
    temp_letters = [x[0] for x in names.split(" ") if x]
    return temp_letters[0] if temp_letters else ""

# COMMAND ----------

@udf(returnType=ArrayType(StringType()))
def group_non_latin_characters(text):
    groups = []
    text = text.replace(".", "").replace(" ", "")
    for char in text:
        try:
            script = unicodedata.name(char).split(" ")[0]
            if script == 'LATIN':
                pass
            else:
                if script not in groups:
                    groups.append(script)
        except:
            if "UNK" not in groups:
                groups.append("UNK")
    return groups

@udf(returnType=IntegerType())
def name_to_keep_ind(groups):
    groups_to_skip = ['HIRAGANA', 'CJK', 'KATAKANA','ARABIC', 'HANGUL', 'THAI','DEVANAGARI','BENGALI',
                      'THAANA','GUJARATI']
    
    if any(x in groups_to_skip for x in groups):
        return 0
    else:
        return 1

# COMMAND ----------

@udf(returnType=IntegerType())
def check_block_vs_block(block_1_names_list, block_2_names_list):
    
    # check first names
    first_check, _ = match_block_names(block_1_names_list[0], block_1_names_list[1], block_2_names_list[0], 
                                    block_2_names_list[1])
    # print(f"FIRST {first_check}")
    
    if first_check:
        last_check, _ = match_block_names(block_1_names_list[-2], block_1_names_list[-1], block_2_names_list[-2], 
                                           block_2_names_list[-1])
        # print(f"LAST {last_check}")
        if last_check:
            m1_check, more_to_go = match_block_names(block_1_names_list[2], block_1_names_list[3], block_2_names_list[2], 
                                           block_2_names_list[3])
            if m1_check:
                if not more_to_go:
                    return 1
                m2_check, more_to_go = match_block_names(block_1_names_list[4], block_1_names_list[5], block_2_names_list[4], 
                                                block_2_names_list[5])
                
                if m2_check:
                    if not more_to_go:
                        return 1
                    m3_check, more_to_go = match_block_names(block_1_names_list[6], block_1_names_list[7], block_2_names_list[6], 
                                                block_2_names_list[7])
                    if m3_check:
                        if not more_to_go:
                            return 1
                        m4_check, more_to_go = match_block_names(block_1_names_list[8], block_1_names_list[8], block_2_names_list[8], 
                                                block_2_names_list[9])
                        if m4_check:
                            if not more_to_go:
                                return 1
                            m5_check, _ = match_block_names(block_1_names_list[10], block_1_names_list[11], block_2_names_list[10], 
                                                block_2_names_list[11])
                            if m5_check:
                                return 1
                            else:
                                return 0
                        else:
                            return 0
                    else:
                        return 0
                else:
                    return 0
            else:
                return 0
        else:
            return 0
    else:
        swap_check = check_if_last_name_swapped_to_front_creates_match(block_1_names_list, block_2_names_list)
        # print(f"SWAP {swap_check}")
        if swap_check:
            return 1
        else:
            return 0
        
def get_name_from_name_list(name_list):
    name = []
    for i in range(0,12,2):
        if name_list[i]:
            name.append(name_list[i][0])
        elif name_list[i+1]:
            name.append(name_list[i+1][0])
        else:
            break
    if name_list[-2]:
        name.append(name_list[-2][0])
    elif name_list[-1]:
        name.append(name_list[-1][0])
    else:
        pass

    return name
        
def check_if_last_name_swapped_to_front_creates_match(block_1, block_2):
    name_1 = get_name_from_name_list(block_1)
    if len(name_1) != 2:
        return False
    else:
        name_2 = get_name_from_name_list(block_2)
        if len(name_2)==2:
            if " ".join(name_1) == " ".join(name_2[-1:] + name_2[:-1]):
                return True
            else:
                return False
        else:
            return False
    
def match_block_names(block_1_names, block_1_initials, block_2_names, block_2_initials):
    if block_1_names and block_2_names:
        if any(x in block_1_names for x in block_2_names):
            return True, True
        else:
            return False, True
    elif block_1_names and not block_2_names:
        if block_2_initials:
            if any(x in block_1_initials for x in block_2_initials):
                return True, True
            else:
                return False, True
        else:
            return True, True
    elif not block_1_names and block_2_names:
        if block_1_initials:
            if any(x in block_1_initials for x in block_2_initials):
                return True, True
            else:
                return False, True
        else:
            return True, True
    elif block_1_initials and block_2_initials:
        if any(x in block_1_initials for x in block_2_initials):
            return True, True
        else:
            return False, True
    else:
        return True, False

@udf(returnType=ArrayType(ArrayType(StringType())))
def get_name_match_list(name):
    name_split_1 = name.replace("-", "").split()
    name_split_2 = ""
    if "-" in name:
        name_split_2 = name.replace("-", " ").split()

    fn = []
    fni = []
    
    m1 = []
    m1i = []
    m2 = []
    m2i = []
    m3 = []
    m3i = []
    m4 = []
    m4i = []
    m5 = []
    m5i = []

    ln = []
    lni = []
    for name_split in [name_split_1, name_split_2]:
        if len(name_split) == 0:
            pass
        elif len(name_split) == 1:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[0]) > 1:
                ln.append(name_split[0])
                lni.append(name_split[0][0])
            else:
                lni.append(name_split[0][0])
            
        elif len(name_split) == 2:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 3:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 4:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 5:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])
                
            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 6:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])
            
            if len(name_split[4]) > 1:
                m4.append(name_split[4])
                m4i.append(name_split[4][0])
            else:
                m4i.append(name_split[4][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 7:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])
            
            if len(name_split[4]) > 1:
                m4.append(name_split[4])
                m4i.append(name_split[4][0])
            else:
                m4i.append(name_split[4][0])

            if len(name_split[5]) > 1:
                m5.append(name_split[5])
                m5i.append(name_split[5][0])
            else:
                m5i.append(name_split[5][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        else:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])
                
            if len(name_split[4]) > 1:
                m4.append(name_split[4])
                m4i.append(name_split[4][0])
            else:
                m4i.append(name_split[4][0])

            joined_names = " ".join(name_split[5:-1])
            m5.append(joined_names)
            m5i.append(joined_names[0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
            

    return [list(set(x)) for x in [fn,fni,m1,m1i,m2,m2i,m3,m3i,m4,m4i,m5,m5i,ln,lni]]

@udf(returnType=StringType())
def transform_author_name(author):
    if author.startswith("None "):
        author = author.replace("None ", "")
    elif author.startswith("Array "):
        author = author.replace("Array ", "")

    author = unicodedata.normalize('NFKC', author)
    
    author_name = HumanName(" ".join(author.split()))

    if (author_name.title == 'Dr.') | (author_name.title == ''):
        temp_new_author_name = f"{author_name.first} {author_name.middle} {author_name.last}"
    else:
        temp_new_author_name = f"{author_name.title} {author_name.first} {author_name.middle} {author_name.last}"

    new_author_name = " ".join(temp_new_author_name.split())

    author_names = new_author_name.split(" ")
    
    if (author_name.title != '') : 
        final_author_name = new_author_name
    else:
        if len(author_names) == 1:
            final_author_name = new_author_name
        elif len(author_names) == 2:
            if (len(author_names[1]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        elif len(author_names) == 3:
            if (len(author_names[1]) == 1) & (len(author_names[2]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[2]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[2]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]==".") & (author_names[2][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[2]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        elif len(author_names) == 4:
            if (len(author_names[1]) == 1) & (len(author_names[2]) == 1) & (len(author_names[3]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[2]} {author_names[3]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[2]) == 2) & (len(author_names[3]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]==".") & (author_names[2][1]==".") & (author_names[3][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[2]} {author_names[3]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        else:
            final_author_name = new_author_name
    return final_author_name

@udf(returnType=ArrayType(StringType()))  
def remove_current_author(author, coauthors):
    return [x for x in coauthors if x!=author][:250]

@udf(returnType=StringType())
def transform_name_for_search(name):
    name = unidecode(unicodedata.normalize('NFKC', name))
    name = name.lower().replace(" ", " ").replace(".", " ").replace(",", " ").replace("|", " ").replace(")", "").replace("(", "")\
        .replace("-", "").replace("&", "").replace("$", "").replace("#", "").replace("@", "").replace("%", "").replace("0", "") \
        .replace("1", "").replace("2", "").replace("3", "").replace("4", "").replace("5", "").replace("6", "").replace("7", "") \
        .replace("8", "").replace("9", "").replace("*", "").replace("^", "").replace("{", "").replace("}", "").replace("+", "") \
        .replace("=", "").replace("_", "").replace("~", "").replace("`", "").replace("[", "").replace("]", "").replace("\\", "") \
        .replace("<", "").replace(">", "").replace("?", "").replace("/", "").replace(";", "").replace(":", "").replace("\'", "") \
        .replace("\"", "")
    name = " ".join(name.split())
    return name

@udf(returnType=ArrayType(ArrayType(StringType())))
def create_author_name_list_from_list(name_lists):
    if not isinstance(name_lists, list):
        name_lists = name_lists.tolist()
    
    name_list_len = len(name_lists[0])
    
    temp_name_list = [[j[i] for j in name_lists] for i in range(name_list_len)]
    temp_name_list_2 = [[j[0] for j in i if j] for i in temp_name_list]
    
    return [list(set(x)) for x in temp_name_list_2]

@udf(returnType=ArrayType(ArrayType(StringType())))
def get_name_match_from_alternate_names(alt_names):
    trans_names = list(set([transform_name_for_search_reg(x) for x in alt_names]))
    name_lists = [get_name_match_list_reg(x) for x in trans_names]
    return create_author_name_list_from_list_reg(name_lists)

def create_author_name_list_from_list_reg(name_lists):
    if not isinstance(name_lists, list):
        name_lists = name_lists.tolist()
    
    name_list_len = len(name_lists[0])
    
    temp_name_list = [[j[i] for j in name_lists] for i in range(name_list_len)]
    temp_name_list_2 = [[j[0] for j in i if j] for i in temp_name_list]
    
    return [list(set(x)) for x in temp_name_list_2]

def transform_name_for_search_reg(name):
    name = unidecode(unicodedata.normalize('NFKC', name))
    name = name.lower().replace(" ", " ").replace(".", " ").replace(",", " ").replace("|", " ").replace(")", "").replace("(", "")\
        .replace("-", "").replace("&", "").replace("$", "").replace("#", "").replace("@", "").replace("%", "").replace("0", "") \
        .replace("1", "").replace("2", "").replace("3", "").replace("4", "").replace("5", "").replace("6", "").replace("7", "") \
        .replace("8", "").replace("9", "").replace("*", "").replace("^", "").replace("{", "").replace("}", "").replace("+", "") \
        .replace("=", "").replace("_", "").replace("~", "").replace("`", "").replace("[", "").replace("]", "").replace("\\", "") \
        .replace("<", "").replace(">", "").replace("?", "").replace("/", "").replace(";", "").replace(":", "").replace("\'", "") \
        .replace("\"", "")
    name = " ".join(name.split())
    return name

def get_name_match_list_reg(name):
    name_split_1 = name.replace("-", "").split()
    name_split_2 = ""
    if "-" in name:
        name_split_2 = name.replace("-", " ").split()

    fn = []
    fni = []
    
    m1 = []
    m1i = []
    m2 = []
    m2i = []
    m3 = []
    m3i = []
    m4 = []
    m4i = []
    m5 = []
    m5i = []

    ln = []
    lni = []
    for name_split in [name_split_1, name_split_2]:
        if len(name_split) == 0:
            pass
        elif len(name_split) == 1:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[0]) > 1:
                ln.append(name_split[0])
                lni.append(name_split[0][0])
            else:
                lni.append(name_split[0][0])
            
        elif len(name_split) == 2:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 3:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 4:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 5:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])
                
            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 6:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])
            
            if len(name_split[4]) > 1:
                m4.append(name_split[4])
                m4i.append(name_split[4][0])
            else:
                m4i.append(name_split[4][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 7:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])
            
            if len(name_split[4]) > 1:
                m4.append(name_split[4])
                m4i.append(name_split[4][0])
            else:
                m4i.append(name_split[4][0])

            if len(name_split[5]) > 1:
                m5.append(name_split[5])
                m5i.append(name_split[5][0])
            else:
                m5i.append(name_split[5][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        else:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])
                
            if len(name_split[4]) > 1:
                m4.append(name_split[4])
                m4i.append(name_split[4][0])
            else:
                m4i.append(name_split[4][0])

            joined_names = " ".join(name_split[5:-1])
            m5.append(joined_names)
            m5i.append(joined_names[0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
            

    return [list(set(x)) for x in [fn,fni,m1,m1i,m2,m2i,m3,m3i,m4,m4i,m5,m5i,ln,lni]]

@udf(returnType=StringType())
def get_most_frequent_name(x):
    return mode(x)

@udf(returnType=StringType())
def get_unique_orcid_for_author_table(list_of_orcids):
    if not isinstance(list_of_orcids, list):
        try:
            list_of_orcids = list_of_orcids.tolist()
        except:
            list_of_orcids = list(list_of_orcids)
        
    orcids = [x for x in list_of_orcids if x]
    
    if orcids:
        return orcids[0]
    else:
        return ""
    
@udf(returnType=IntegerType())
def check_for_unique_orcid_live_clustering(list_of_orcids):
    if not isinstance(list_of_orcids, list):
        try:
            list_of_orcids = list_of_orcids.tolist()
        except:
            list_of_orcids = list(list_of_orcids)
        
    orcids = [x for x in list_of_orcids if x]
    
    if len(orcids) > 1:
        return 0
    else:
        return 1

# COMMAND ----------

def get_data_features_scored(df, prefix):
    w1 = Window.partitionBy(['work_author_id', 'author_id']).orderBy(F.col('total_per').desc())
    w2 = Window.partitionBy('work_author_id').orderBy(F.col('total_per').desc())

    df \
        .withColumn('row_label', F.concat_ws("|", F.col('work_author_id'), F.col('work_author_id_2'))) \
        .withColumn('work_in_citations_2', F.array_contains(F.col('citations_2'), F.col('paper_id')).cast(IntegerType())) \
        .withColumn('work_2_in_citations', F.array_contains(F.col('citations'), F.col('paper_id_2')).cast(IntegerType())) \
        .withColumn('citation_work_match', F.when((F.col('work_2_in_citations')==1) | 
                                                  (F.col('work_in_citations_2')==1), 1).otherwise(0)) \
        .withColumn('insts_inter', F.size(F.array_intersect(F.col('institutions'), F.col('institutions_2')))) \
        .withColumn('coauths_inter', F.size(F.array_intersect(F.col('coauthors_shorter'), F.col('coauthors_shorter_2')))) \
        .withColumn('concps_inter', F.size(F.array_intersect(F.col('concepts_shorter'), F.col('concepts_shorter_2')))) \
        .withColumn('cites_inter', F.size(F.array_intersect(F.col('citations'), F.col('citations_2')))) \
        .withColumn('coauths_union', F.size(F.array_union(F.col('coauthors_shorter'), F.col('coauthors_shorter_2')))) \
        .withColumn('concps_union', F.size(F.array_union(F.col('concepts_shorter'), F.col('concepts_shorter_2')))) \
        .withColumn('cites_union', F.size(F.array_union(F.col('citations'), F.col('citations_2')))) \
        .withColumn('inst_per', F.when(F.col('insts_inter')>0, 1).otherwise(0)) \
        .withColumn('coauthors_shorter_per', F.round(F.when(F.col('coauths_union')>0, 
                                                            F.col('coauths_inter')/F.col('coauths_union')).otherwise(0.0), 4)) \
        .withColumn('concepts_shorter_per', F.round(F.when(F.col('concps_union')>0, 
                                                           F.col('concps_inter')/F.col('concps_union')).otherwise(0.0), 4)) \
        .withColumn('citation_per', F.round(F.when(F.col('cites_union')>0, 
                                                   F.col('cites_inter')/F.col('cites_union')).otherwise(0.0), 4)) \
        .withColumn('total_per', F.col('inst_per') + F.col('concepts_shorter_per') + F.col('coauthors_shorter_per') + F.col('citation_per') + F.col('citation_work_match')) \
        .filter(F.col('total_per')>0) \
        .withColumn('total_per_rank_author', F.row_number().over(w1)) \
        .filter(F.col('total_per_rank_author')==1) \
        .withColumn('total_per_rank', F.row_number().over(w2)) \
        .filter(F.col('total_per_rank')<=25) \
        .withColumn('exact_match', F.when(F.col('author')==F.col('author_2'), 1).otherwise(0)) \
        .withColumn('name_len', F.length(F.col('author'))) \
        .withColumn('name_spaces', F.size(F.split(F.col('author'), " "))) \
        .select(F.col('work_author_id').alias('block'),'row_label', 'inst_per','concepts_shorter_per', 'coauthors_shorter_per', 
            (F.col('exact_match')*F.col('name_len')).alias('exact_match_len'),
            (F.col('exact_match')*F.col('name_spaces')).alias('exact_match_spaces'), 'citation_per', 'citation_work_match') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}{prefix}all_features/")

    print(f"{datetime.datetime.now().strftime('%H:%M')}: features saved: ", spark.read.parquet(f"{temp_save_path}{prefix}all_features/").count())
        
    spark.read.parquet(f"{temp_save_path}{prefix}all_features/")\
        .withColumn('random_int', get_random_int_udf(F.col('block'))) \
        .withColumn('concat_cols', F.array(F.col('block'), F.col('row_label').cast(StringType()), 
                                            F.col('inst_per').cast(StringType()), 
                                            F.col('concepts_shorter_per').cast(StringType()), 
                                            F.col('coauthors_shorter_per').cast(StringType()), 
                                            F.col('exact_match_len').cast(StringType()), 
                                            F.col('exact_match_spaces').cast(StringType()), 
                                            F.col('citation_per').cast(StringType()), 
                                            F.col('citation_work_match').cast(StringType()))) \
        .groupby('random_int') \
        .agg(F.collect_list(F.col('concat_cols')).alias('data_to_score')) \
        .withColumn('scored_data', score_data(F.col('data_to_score'))) \
        .select('scored_data') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}{prefix}data_scored/")

# COMMAND ----------

def live_clustering_algorithm(scored_data_prefix):
    w1 = Window.partitionBy('work_author_id').orderBy(F.col('score').desc())
    w2 = Window.partitionBy('author_id').orderBy(F.col('score').desc())

    
    spark.read.parquet(f"{temp_save_path}{scored_data_prefix}data_scored/") \
        .select(F.explode('scored_data').alias('scored_data')) \
        .select(F.col('scored_data').getItem(0).alias('work_author_id'),
                F.col('scored_data').getItem(1).alias('pairs'), 
                F.col('scored_data').getItem(2).alias('score').cast(FloatType())) \
        .dropDuplicates(subset=['pairs']) \
        .select('work_author_id', 
                F.split(F.col('pairs'), "\|")[1].alias('work_author_id_2'), 
                'score') \
        .repartition(250) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}{scored_data_prefix}flat_scored_data/")
    
    spark.read.parquet(f"{temp_save_path}{scored_data_prefix}flat_scored_data/") \
        .join(all_new_data.select('work_author_id','orcid','author'), 
              how='inner', on='work_author_id') \
        .join(temp_authors_table.select('work_author_id_2','author_id','orcid_2').distinct(), 
              how='inner',on='work_author_id_2') \
        .filter((F.col('orcid')==F.col('orcid_2')) | 
        (F.col('orcid')=='') | 
        (F.col('orcid_2')=='')) \
        .withColumn('rank', F.row_number().over(w1)) \
        .filter(F.col('rank')==1) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}{scored_data_prefix}potential_cluster_matches/")

    pot_cluster_matches = spark.read.parquet(f"{temp_save_path}{scored_data_prefix}potential_cluster_matches/")

    orcids_check = pot_cluster_matches\
        .groupby('author_id')\
        .agg(F.collect_set(F.col('orcid')).alias('orcids')) \
        .withColumn('orcid_good', check_for_unique_orcid_live_clustering('orcids')) \
        .select('author_id','orcid_good') \
        .alias('orcids_check')

    pot_cluster_matches \
        .join(orcids_check.filter(F.col('orcid_good')==1).select('author_id').distinct(), how='inner', on='author_id')\
        .select('work_author_id', 'author_id') \
        .dropDuplicates(subset=['work_author_id']) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}{scored_data_prefix}matched_to_cluster/orcids_good/")

    pot_cluster_matches \
        .join(orcids_check.filter(F.col('orcid_good')==0).select('author_id').distinct(), how='inner', on='author_id')\
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}{scored_data_prefix}orcids_not_good/")

    spark.read.parquet(f"{temp_save_path}{scored_data_prefix}orcids_not_good/") \
        .withColumn('rank', F.row_number().over(w2)) \
        .filter(F.col('rank')==1) \
        .select('work_author_id', 'author_id') \
        .dropDuplicates(subset=['work_author_id']) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}{scored_data_prefix}matched_to_cluster/orcids_not_good/")

# COMMAND ----------

def create_new_features_table(new_rows_location):
    new_rows = spark.read.parquet(f"{temp_save_path}/new_rows_for_author_table/{new_rows_location}/") \
        .dropDuplicates()

    temp_features_table \
        .union(all_new_data.join(new_rows.select('work_author_id').dropDuplicates(), how='inner', on='work_author_id') \
                .select(F.col('work_author_id').alias('work_author_id_2'), 
                        F.col('orcid').alias('orcid_2'),
                        F.col('citations').alias('citations_2'),
                        F.col('institutions').alias('institutions_2'),
                        F.col('author').alias('author_2'),
                        F.col('paper_id').alias('paper_id_2'),
                        'original_author',
                        F.col('concepts_shorter').alias('concepts_shorter_2'),
                        F.col('coauthors_shorter').alias('coauthors_shorter_2'))) \
        .dropDuplicates(subset=['work_author_id_2']) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/temp_features_table/{new_rows_location}/")

# COMMAND ----------

@udf(returnType=StringType())
def get_author_display_name(list_of_all_names):
    # Get counts of all unique, transformed names
    name_counts = Counter(list_of_all_names)
    
    # Check to see if there are names with spaces (preferred)
    has_space = [x for x in name_counts.most_common() if len(x[0].split(" "))>1]

    # Logic for if there is at least one name with a space
    if has_space:
        count_to_check = has_space[0][1]
        match_count = [x for x in has_space if x[1]==count_to_check]
        if len(match_count) == 1:
            display_name = match_count[0][0]
        else:
            name_len = 0
            display_name = ''
            for match in match_count:
                if len(match[0]) > name_len:
                    display_name = match[0]
                    name_len = len(match[0])
                elif len(match[0]) == name_len:
                    if match[0] > display_name:
                        display_name = match[0]
                        name_len = len(match[0])
                    else:
                        pass
                else:
                    pass
    # Logic for if there are no names with a space
    else:
        no_space = name_counts.most_common()
        count_to_check = no_space[0][1]
        match_count = [x for x in no_space if x[1]==count_to_check]
        if len(match_count) == 1:
            display_name = match_count[0][0]
        else:
            name_len = 0
            display_name = ''
            for match in match_count:
                if len(match[0]) > name_len:
                    display_name = match[0]
                    name_len = len(match[0])
                elif len(match[0]) == name_len:
                    if match[0] > display_name:
                        display_name = match[0]
                        name_len = len(match[0])
                    else:
                        pass
                else:
                    pass
                
    # check to see if there are other variations of the name
    possible_replacements = [x for x in list_of_all_names if ((unidecode(display_name)==unidecode(x)) & 
                                                              (unidecode(x) != x))]
    if possible_replacements:
        if len(possible_replacements)==1:
            if (len(set(possible_replacements[0]+unidecode(possible_replacements[0]))) > 
                len(set(display_name+unidecode(display_name)))):
                display_name = possible_replacements[0]
            else:
                pass
        else:
            replace_lens = [len(set(x+unidecode(x))) for x in possible_replacements]
            max_len = max(replace_lens)
            for name in possible_replacements:
                if len(set(name+unidecode(name))) == max_len:
                    display_name = name

                
    return display_name

@udf(returnType=ArrayType(StringType()))
def get_author_alternate_names(init_alt_names_list):
    temp_list = [x.replace("-","‐") for x in init_alt_names_list]
    temp_list = [x[:-1] if x.endswith("(") else x for x in temp_list]
    
    final_list = temp_list.copy()
    
    for name in temp_list:
        if (name.replace(".","") in temp_list) & (name.replace(".","") != name):
            try:
                final_list.remove(name.replace('.',''))
            except:
                pass
            
        if ((name.replace("."," ").replace("  ", " ") in temp_list) & 
            (name.replace("."," ").replace("  ", " ") != name)):
            try:
                final_list.remove(name.replace('.', ' ').replace('  ', ' '))
            except:
                pass
            
        if ((name.replace(".",". ").replace("  ", " ") in temp_list) & 
            (name.replace(".",". ").replace("  ", " ") != name)):
            try:
                final_list.remove(name)
            except:
                pass
            
        if (name.title() in temp_list) & (name.title() != name):
            try:
                final_list.remove(name)
            except:
                pass
    
    return list(set(final_list))

# COMMAND ----------

def create_new_author_table(new_rows_location):
    new_rows = spark.read.parquet(f"{temp_save_path}/new_rows_for_author_table/{new_rows_location}/")

    cluster_df = new_rows.union(temp_authors_table.select(F.col('work_author_id_2').alias('work_author_id'), 'author_id'))

    # need to join new rows with features table
    temp_features_table \
        .select(F.col('work_author_id_2').alias('work_author_id'),F.col('orcid_2').alias('orcid'),
                'original_author',F.col('author_2').alias('author')) \
        .join(cluster_df, how='inner', on='work_author_id') \
        .filter(F.col('original_author')!="") \
        .filter(F.col('original_author').isNotNull()) \
        .groupby('author_id') \
        .agg(F.collect_set(F.col('orcid')).alias('orcid'), 
            F.collect_set(F.col('work_author_id')).alias('work_author_id'),
            F.collect_set(F.col('author')).alias('alternate_names'),
            F.collect_set(F.col('author')).alias('names_for_list'),
            F.collect_list(F.col('author')).alias('names')) \
        .withColumn('orcid', get_unique_orcid_for_author_table(F.col('orcid'))) \
        .withColumn('display_name', get_author_display_name(F.col('names'))) \
        .withColumn('author_alternate_names', get_author_alternate_names(F.col('alternate_names'))) \
        .withColumn('name_match_list', get_name_match_from_alternate_names('names_for_list')) \
        .select(F.explode('work_author_id').alias('work_author_id_2'), 
                'author_id',
                F.col('orcid').alias('orcid_2'), 
                'display_name',
                'alternate_names',
                'author_alternate_names',
                'name_match_list') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/temp_authors_table/{new_rows_location}/")

# COMMAND ----------

@udf(returnType=IntegerType())
def check_list_vs_list(list_1, list_2):
    set_1 = set(list_1)
    set_2 = set(list_2)
    if set_1 == set_2:
        return 0
    else:
        return 1
    
@udf(returnType=LongType())
def get_paper_id_from_work_author(work_author_id):
    return int(work_author_id.split("_")[0])

# COMMAND ----------

# @udf(returnType=StringType())
# def fix_bad_orcid(curr_work_author_id, curr_orcid, work_author_id_wrong, work_author_id_correct, orcid):
#     if curr_work_author_id not in [work_author_id_wrong, work_author_id_correct]:
#         return curr_orcid
#     else:
#         if curr_work_author_id == work_author_id_wrong:
#             return ""
#         else:
#             return orcid

# COMMAND ----------

bad_author_names = ['None Anonymous','Not specified','&NA; &NA;','Creator','None Unknown',
                    'Unknown Unknown','Unknown Author','Unknown','Author Unknown',
                    'None, None','None','None None','None No authorship indicated',
                    'No authorship indicated','None &NA;','&Na; &Na;','&Na;',
                    'Occdownload Gbif.Org','GBIF.Org User','et al. et al.']

# COMMAND ----------

# save prod features table
spark.read.parquet(f"{prod_save_path}/current_features_table/") \
    .filter(~F.col('original_author').isin(bad_author_names)) \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}/current_features_table/")

######################### FOR MAKING CHANGES TO ORCID (code not complete) #########################
# spark.read.parquet(f"{prod_save_path}/current_features_table/") \
#     .filter(~F.col('original_author').isin(bad_author_names)) \
#     .withColumn('new_orcid', fix_bad_orcid(F.col('work_author_id_2'), F.col('orcid_2'), "work_author_id_wrong", 
#                                            "work_author_id_correct", orcid)) \
#     .select('work_author_id_2', F.col('new_orcid').alias('orcid_2'), 'citations_2', 'institutions_2',
#             'author_2', 'paper_id_2', 'original_author', 'concepts_shorter_2','coauthors_shorter_2') \
#     .write.mode('overwrite') \
#     .parquet(f"{temp_save_path}/current_features_table/")
###############################################################################

spark.read.parquet(f"{prod_save_path}/current_features_table/")\
    .filter(F.col('original_author').isin(bad_author_names)) \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}/null_author_rows_to_filter_out/")

spark.read.parquet(f"{temp_save_path}/current_features_table/").count()

# COMMAND ----------

curr_author_table = spark.read.parquet(f"{prod_save_path}/current_authors_table/")

null_author_rows = spark.read.parquet(f"{temp_save_path}/null_author_rows_to_filter_out/")\
    .select(F.col('work_author_id_2').alias('work_author_id')).distinct()

# save prod authors table
curr_author_table\
    .join(null_author_rows, how='leftanti', on='work_author_id') \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}/current_authors_table/")

spark.read.parquet(f"{temp_save_path}/current_authors_table/").count()

# COMMAND ----------

authors_table_last_date = curr_author_table.select(F.max('modified_date')).collect()[0][0]

authors_table_last_date

# COMMAND ----------

spark.read\
    .parquet(f"{temp_save_path}/raw_data_to_disambiguate/").select(F.min('created_date'), F.max('created_date')) \
    .show(truncate=False)

# COMMAND ----------

# FOR INIT SAVE
# (spark.read.parquet("s3://temp-prod-working-bucket/2023_09_12_01_30/new_data_to_be_given_null_value/") \
#     .select(F.col('work_author_id_2').alias('work_author_id'), 'author_id', 
#                 'display_name', 
#                 'alternate_names', 
#                 F.col('orcid_2').alias('orcid'))
#     .withColumn("created_date", F.current_timestamp()) 
#     .withColumn("modified_date", F.current_timestamp())
#     .write.mode('overwrite')
#     .parquet(f"{prod_save_path}/current_null_authors_table/"))

# COMMAND ----------

# saving null authors table
spark.read.parquet(f"{prod_save_path}/current_null_authors_table/")\
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}/current_null_authors_table/")

# COMMAND ----------

w1 = Window.partitionBy('work_author_id').orderBy(F.col('name_len').desc())

(spark.read
    .parquet(f"{temp_save_path}/raw_data_to_disambiguate/")
    .select('work_author_id', F.trim(F.col('original_author')).alias('original_author'), 'orcid', 'concepts', 
            'institutions', 'citations', 'coauthors', 'created_date')
    .filter(F.col('original_author').isNotNull())
    .filter(F.col('original_author')!='')
    .filter(~F.col('original_author').isin(bad_author_names))
    .withColumn('name_len', F.length(F.col('original_author')))
    .withColumn('rank', F.row_number().over(w1))
    .filter(F.col('rank')==1)
    .withColumn('citations', transform_list_col_for_nulls_long(F.col('citations')))
    .withColumn('coauthors', transform_list_col_for_nulls_string(F.col('coauthors')))
    .withColumn('concepts', transform_list_col_for_nulls_long(F.col('concepts')))
    .withColumn('institutions', transform_list_col_for_nulls_long(F.col('institutions')))
    .withColumn('author', transform_author_name(F.col('original_author')))
    .withColumn('coauthors', transform_coauthors(F.col('coauthors')))
    .withColumn('coauthors', remove_current_author(F.col('author'),F.col('coauthors')))
    .withColumn('coauthors', coauthor_transform(F.col('coauthors')))
    .withColumn('orcid', F.when(F.col('orcid').isNull(), '').otherwise(F.col('orcid')))
    .withColumn('paper_id', F.split(F.col('work_author_id'), "_").getItem(0).cast(LongType()))
    .withColumn('concepts', F.array_distinct(F.col('concepts')))
    .withColumn('concepts_shorter', F.filter(F.col('concepts'), concept_L0_removed))
    .withColumn('coauthors_shorter', F.filter(F.col('coauthors'), length_greater_than_6))
    .select('work_author_id','paper_id','original_author','author','orcid','coauthors_shorter','concepts_shorter',
        'institutions','citations','created_date')
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}/new_data_to_disambiguate/"))

# COMMAND ----------

spark.read.parquet(f"{temp_save_path}/new_data_to_disambiguate/").sample(0.001).show(10, truncate=True)

# COMMAND ----------

temp_new_data = spark.read.parquet(f"{temp_save_path}/new_data_to_disambiguate/")
name_of_stats_to_track.append('temp_data_count')
stats_to_track.append(temp_new_data.count())
temp_new_data.count()

# COMMAND ----------

final_new_data = temp_new_data.groupby(['original_author', 'author', 'orcid', 'coauthors_shorter', 'concepts_shorter', 
                       'institutions', 'citations']) \
    .agg(F.collect_set(F.col('work_author_id')).alias('work_author_ids'), 
         F.max(F.col('created_date')).alias('created_date'), 
         F.max(F.col('work_author_id')).alias('work_author_id')) \
    .withColumn('works_len', F.size(F.col('work_author_ids'))) \
    .withColumn('paper_id', get_paper_id_from_work_author('work_author_id')) \
    .select('work_author_ids','work_author_id', 'paper_id', 'original_author', 'author', 'orcid', 'coauthors_shorter', 
            'concepts_shorter', 'institutions', 'citations', 'created_date','works_len')

# COMMAND ----------

final_new_data.filter(F.col('works_len')>1) \
    .select('work_author_ids','work_author_id','original_author', 'author', 'orcid', 'coauthors_shorter', 
            'concepts_shorter', 'institutions', 'citations') \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}/duplicate_work_entries/")

# COMMAND ----------

name_of_stats_to_track.append('dup_work_entries_count')
stats_to_track.append(spark.read.parquet(f"{temp_save_path}/duplicate_work_entries/").count())

# COMMAND ----------

final_new_data \
    .select('work_author_id', 'paper_id', 'original_author', 'author', 'orcid', 'coauthors_shorter', 'concepts_shorter', 'institutions', 'citations', 'created_date') \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}/final_data_to_disambiguate/")

# COMMAND ----------

final_new_data.sample(0.001).show(20)

# COMMAND ----------

for_stats = spark.read.parquet(f"{temp_save_path}/final_data_to_disambiguate/")

# COMMAND ----------

# getting input data stats for tracking purposes
print("Getting stats for tracking")
name_of_stats_to_track.append('input_data_citations_null_count')
stats_to_track.append(for_stats.filter(F.size(F.col('citations'))==0).count())

name_of_stats_to_track.append('input_data_concepts_shorter_null_count')
stats_to_track.append(for_stats.filter(F.size(F.col('concepts_shorter'))==0).count())

name_of_stats_to_track.append('input_data_coauthors_shorter_null_count')
stats_to_track.append(for_stats.filter(F.size(F.col('coauthors_shorter'))==0).count())

name_of_stats_to_track.append('input_data_institutions_null_count')
stats_to_track.append(for_stats.filter(F.size(F.col('institutions'))==0).count())

name_of_stats_to_track.append('input_data_orcid_null_count')
stats_to_track.append(for_stats.filter(F.col('orcid')=='').count())

print("Done getting stats")

# COMMAND ----------

# MAGIC %md #### Adding rows to add_works and author_id_merges tables

# COMMAND ----------

# secret = get_secret()

# COMMAND ----------

# MAGIC %md ##### For add works table

# COMMAND ----------

# ids_to_change = ['2007620353_3','1965902888_2']

# features = spark.read.parquet(f"{temp_save_path}/current_features_table/") \
#     .filter(F.col('work_author_id_2').isin(ids_to_change)) \
#     .select(F.col('work_author_id_2').alias('work_author_id'), 'author_2') \
#     .withColumn('new_author_id', F.lit(5012659032)) \
#     .withColumn('request_type', F.lit('user')) \
#     .withColumn('request_date', F.current_timestamp())

# print(features.cache().count())

# if features.count() == len(ids_to_change):
#     (features.select('work_author_id','new_author_id','request_type','request_date') \
#        .write.format("jdbc") 
#        .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
#        .option("dbtable", 'authorships.add_works') 
#        .option("user", secret['username']) 
#        .option("password", secret['password']) 
#        .option("driver", "org.postgresql.Driver") 
#        .mode("append") 
#        .save())
# else:
#     print("The counts did not match up: ", features.count(), " vs ", len(ids_to_change))

# COMMAND ----------

# MAGIC %md ##### For overmerged authors table

# COMMAND ----------

# ids_to_change = ['2884227934_9']


# features = spark.read.parquet(f"{temp_save_path}/current_features_table/") \
#    .filter(F.col('work_author_id_2').isin(ids_to_change)) \
#    .withColumn('groupby_num', F.lit(101)) \
#    .groupby('groupby_num') \
#    .agg(F.max(F.col('work_author_id_2')).alias##('work_author_id_for_cluster'), 
#         F.collect_set(F.col('work_author_id_2')).alias('all_works_to_cluster')) \
#    .select('work_author_id_for_cluster', 
#            F.explode('all_works_to_cluster').alias('all_works_to_cluster')) \
#    .withColumn('request_type', F.lit('user')) \
#    .withColumn('request_date', F.current_timestamp())

# print(features.cache().count())

# (features.select('work_author_id_for_cluster','all_works_to_cluster','request_type','request_date') \
#    .write.format("jdbc") 
#    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
#    .option("dbtable", 'authorships.overmerged_authors') 
#    .option("user", secret['username']) 
#    .option("password", secret['password']) 
#    .option("driver", "org.postgresql.Driver") 
#    .mode("append") 
#    .save())

# COMMAND ----------

# MAGIC %md ### AND Code

# COMMAND ----------

if temp_new_data.filter(F.col('created_date')>authors_table_last_date).count() <= 0:
    print(f"{datetime.datetime.now().strftime('%H:%M')}: NO NEW DATA")
    pass
else:
    ########################################### INITIAL DATA PREP #####################################################

    # Read add_works table
    df = (spark.read
        .format("postgresql")
        .option("dbtable", "authorships.add_works")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("fetchSize", "15")
        .load()
    )

    df.write.mode('overwrite') \
        .parquet(f"{temp_save_path}/current_add_works_table/")

    # Read author_id_merges table
    df = (spark.read
        .format("postgresql")
        .option("dbtable", "authorships.author_id_merges")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("fetchSize", "15")
        .load()
    )

    df.write.mode('overwrite') \
        .parquet(f"{temp_save_path}/current_author_id_merges_table/")
    
    # Read orcid_add table
    df = (spark.read
        .format("postgresql")
        .option("dbtable", "authorships.add_orcid")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("fetchSize", "15")
        .load()
    )

    df.write.mode('overwrite') \
        .parquet(f"{temp_save_path}/current_add_orcid_table/")

    add_orcid_df = spark.read.parquet(f"{temp_save_path}/current_add_orcid_table/") \
        .dropDuplicates(subset=['work_author_id']).select('work_author_id','new_orcid')
    print(f"{datetime.datetime.now().strftime('%H:%M')}: Rows in add_orcid table: {add_orcid_df.cache().count()}")

    # Read change_raw_author_name table
    df = (spark.read
        .format("postgresql")
        .option("dbtable", "authorships.change_raw_author_name")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("fetchSize", "15")
        .load()
    )

    df.write.mode('overwrite') \
        .parquet(f"{temp_save_path}/current_change_raw_author_name_table/")

    change_raw_author_name_df = spark.read.parquet(f"{temp_save_path}/current_change_raw_author_name_table/") \
        .dropDuplicates(subset=['work_author_id']).select('work_author_id','new_raw_author_name')
    print(f"{datetime.datetime.now().strftime('%H:%M')}: Rows in change_raw_author_name table: {change_raw_author_name_df.cache().count()}")

    merge_authors_df = spark.read.parquet(f"{temp_save_path}/current_author_id_merges_table/") \
        .dropDuplicates(subset=['merge_from_id'])
    print(f"{datetime.datetime.now().strftime('%H:%M')}: Rows in author_ids_merges table: {merge_authors_df.cache().count()}")

    add_works_df = spark.read.parquet(f"{temp_save_path}/current_add_works_table/") \
        .select('work_author_id', F.col('new_author_id').alias('author_id')) \
        .dropDuplicates() \
        .join(merge_authors_df.select(F.col('merge_from_id').alias('author_id'), 'merge_to_id'), 
              how='left', on='author_id') \
        .withColumn('final_author_id', F.when(F.col('merge_to_id').isNull(), 
                                              F.col('author_id')).otherwise(F.col('merge_to_id'))) \
        .select('work_author_id', F.col('final_author_id').alias('author_id')) \
        .dropDuplicates(subset=['work_author_id'])
    
    print(f"{datetime.datetime.now().strftime('%H:%M')}: Rows in add_works table: {add_works_df.cache().count()}")

    all_new_data = spark.read.parquet(f"{temp_save_path}/final_data_to_disambiguate/") \
        .dropDuplicates(subset=['work_author_id']) \
        .join(add_works_df, how='leftanti', on='work_author_id')
    new_data_size = all_new_data.count()

    print(f"{datetime.datetime.now().strftime('%H:%M')}: {new_data_size} NEW ROWS TO DISAMBIGUATE")
    name_of_stats_to_track.append('new_rows_count')
    stats_to_track.append(new_data_size)
    all_new_data.cache().count()

    init_cluster_df = spark.read.parquet(f"{temp_save_path}/current_authors_table/")\
        .select('work_author_id','author_id') \
        .join(all_new_data.select('work_author_id'), how='leftanti', on='work_author_id') \
        .join(add_works_df.select('work_author_id', F.col('author_id').alias('new_author_id')), 
              how='left', on='work_author_id') \
        .withColumn('final_author_id', F.when(F.col('new_author_id').isNull(), 
                                              F.col('author_id')).otherwise(F.col('new_author_id'))) \
        .select('work_author_id', F.col('final_author_id').alias('author_id')) \
        .join(merge_authors_df.select(F.col('merge_from_id').alias('author_id'), 'merge_to_id'), 
              how='left', on='author_id') \
        .withColumn('final_author_id', F.when(F.col('merge_to_id').isNull(), 
                                              F.col('author_id')).otherwise(F.col('merge_to_id'))) \
        .select('work_author_id', F.col('final_author_id').alias('author_id')) \

    print(f"{datetime.datetime.now().strftime('%H:%M')}: Init cluster table created")

    # Create init features table
    spark.read.parquet(f"{temp_save_path}/current_features_table/") \
        .join(init_cluster_df.select(F.col('work_author_id').alias('work_author_id_2')), how='inner', on='work_author_id_2') \
        .select('work_author_id_2', 'orcid_2', F.col('citations_2').cast(ArrayType(LongType())), 
                'institutions_2', 'author_2', F.col('paper_id_2').cast(LongType()), 'original_author',
                F.col('concepts_shorter_2').cast(ArrayType(LongType())), 'coauthors_shorter_2') \
        .union(all_new_data.select(F.col('work_author_id').alias('work_author_id_2'), 
                                   F.col('orcid').alias('orcid_2'), 
                                   F.col('citations').alias('citations_2'), 
                                   F.col('institutions').alias('institutions_2'), 
                                   F.col('author').alias('author_2'), 
                                   F.col('paper_id').alias('paper_id_2'), 
                                   'original_author', 
                                   F.col('concepts_shorter').alias('concepts_shorter_2'),
                                   F.col('coauthors_shorter').alias('coauthors_shorter_2'))) \
        .join(add_orcid_df.select(F.col('work_author_id').alias('work_author_id_2'), 'new_orcid'), 
                  how='left', on='work_author_id_2') \
        .withColumn('final_orcid_id', F.when(F.col('new_orcid').isNull(), 
                                                F.col('orcid_2')).otherwise(F.col('new_orcid'))) \
        .select('work_author_id_2',F.col('final_orcid_id').alias('orcid_2'),'citations_2','institutions_2','author_2',
                'paper_id_2','original_author','concepts_shorter_2','coauthors_shorter_2') \
        .join(change_raw_author_name_df.select(F.col('work_author_id').alias('work_author_id_2'), 'new_raw_author_name'), 
                  how='left', on='work_author_id_2') \
        .withColumn('final_raw_author_name', F.when(F.col('new_raw_author_name').isNull(), 
                                                F.col('author_2')).otherwise(F.col('new_raw_author_name'))) \
        .select('work_author_id_2','orcid_2','citations_2','institutions_2',F.col('final_raw_author_name').alias('author_2'),
                'paper_id_2','original_author','concepts_shorter_2','coauthors_shorter_2') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/temp_features_table/init/")

    temp_features_table = spark.read.parquet(f"{temp_save_path}/temp_features_table/init/")

    print(f"{datetime.datetime.now().strftime('%H:%M')}: Init features table created")

    # Create init authors table
    temp_features_table \
        .select(F.col('work_author_id_2').alias('work_author_id'),F.col('orcid_2').alias('orcid'),
                'original_author',F.col('author_2').alias('author')) \
        .join(init_cluster_df, how='inner', on='work_author_id') \
        .filter(F.col('original_author')!="") \
        .filter(F.col('original_author').isNotNull()) \
        .groupby('author_id') \
        .agg(F.collect_set(F.col('orcid')).alias('orcid'), 
            F.collect_set(F.col('work_author_id')).alias('work_author_id'),
            F.collect_set(F.col('author')).alias('alternate_names'),
            F.collect_set(F.col('author')).alias('names_for_list'),
            F.collect_list(F.col('author')).alias('names')) \
        .withColumn('orcid', get_unique_orcid_for_author_table(F.col('orcid'))) \
        .withColumn('display_name', get_author_display_name(F.col('names'))) \
        .withColumn('name_match_list', get_name_match_from_alternate_names('names_for_list')) \
        .withColumn('author_alternate_names', get_author_alternate_names(F.col('alternate_names'))) \
        .select(F.explode('work_author_id').alias('work_author_id_2'), 
                'author_id',
                F.col('orcid').alias('orcid_2'), 
                'display_name',
                'alternate_names',
                'author_alternate_names',
                'name_match_list') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/temp_authors_table/init/")

    temp_authors_table = spark.read.parquet(f"{temp_save_path}/temp_authors_table/init/")

    print(f"{datetime.datetime.now().strftime('%H:%M')}: Init authors table created")

    # Get author names match data
    author_names_match = spark.read.parquet(f"{prod_save_path}/current_author_names_match/")

    # Checking for works that have already been disambiguated
    spark.read.parquet(f"{temp_save_path}/current_authors_table/")\
        .select('work_author_id','author_id') \
        .join(all_new_data.select('work_author_id'), how='inner', on='work_author_id') \
        .select('work_author_id') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/temp_authors_to_change_table/")

    if spark.read.parquet(f"{temp_save_path}/temp_authors_to_change_table/").count() == 0:
        print(f"{datetime.datetime.now().strftime('%H:%M')}: No authors have been previously disambiguated")
        pass
    else:
        # Need to remove all data from tables
        works_authors_to_remove = spark.read.parquet(f"{temp_save_path}/temp_authors_to_change_table/")

        print(f"{datetime.datetime.now().strftime('%H:%M')}: Authors have been previously disambiguated: {works_authors_to_remove.count()}")
        name_of_stats_to_track.append('previously_disambiguated_count')
        stats_to_track.append(works_authors_to_remove.count())

        # temp_features_table \
        #     .join(works_authors_to_remove.select(F.col('work_author_id').alias('work_author_id_2')), 
        #         how='leftanti', on='work_author_id_2') \
        #     .write.mode('overwrite') \
        #     .parquet(f"{temp_save_path}/temp_features_table/init_2/")

        # temp_features_table = spark.read.parquet(f"{temp_save_path}/temp_features_table/init_2/")

        temp_authors_table \
            .join(works_authors_to_remove.select(F.col('work_author_id').alias('work_author_id_2')), 
                how='leftanti', on='work_author_id_2') \
            .write.mode('overwrite') \
            .parquet(f"{temp_save_path}/temp_authors_table/init_2/")

        temp_authors_table = spark.read.parquet(f"{temp_save_path}/temp_authors_table/init_2/")

        author_names_match \
            .join(works_authors_to_remove.select(F.col('work_author_id').alias('work_author_id_2')), 
                how='leftanti', on='work_author_id_2') \
            .write.mode('overwrite') \
            .parquet(f"{temp_save_path}/temp_author_names_match/init_2/")

        author_names_match = spark.read.parquet(f"{temp_save_path}/temp_author_names_match/init_2/")

    ########################################### ORCID MATCH #####################################################

    # Getting ORCID matches
    all_new_data.filter(F.col('orcid')!='')\
        .join(temp_authors_table.select(F.col('orcid_2').alias('orcid'),'author_id'),how='inner', on='orcid') \
        .select('work_author_id', 
                'author_id') \
        .dropDuplicates(subset=['work_author_id']) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/new_rows_for_author_table/orcid_rows_for_author_table/")

    orcid_add_count = spark.read\
        .parquet(f"{temp_save_path}/new_rows_for_author_table/orcid_rows_for_author_table/").count()
    print(f"{datetime.datetime.now().strftime('%H:%M')}: ORCID added: {orcid_add_count}")
    name_of_stats_to_track.append('orcid_matched_count')
    stats_to_track.append(orcid_add_count)

    # Making all tables current
    new_loc = 'orcid_rows_for_author_table'
    # _ = create_new_features_table(new_loc)
    # print("New features table created")
    # temp_features_table = spark.read.parquet(f"{temp_save_path}/temp_features_table/{new_loc}/")

    _ = create_new_author_table(new_loc)
    print(f"{datetime.datetime.now().strftime('%H:%M')}: New authors table created")
    temp_authors_table = spark.read.parquet(f"{temp_save_path}/temp_authors_table/{new_loc}/")

    # Creating table for next round
    all_new_data \
        .join(temp_authors_table.select(F.col('work_author_id_2').alias('work_author_id')).distinct(), 
                    how='leftanti', on='work_author_id') \
        .select('work_author_id','paper_id','original_author','author','orcid','coauthors_shorter','concepts_shorter',
            'institutions','citations','created_date') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/round_2_of_clustering/")

    # Removing connections to clusters for user input
    try:
        work_to_clusters_removed = spark.read.parquet(f"{prod_save_path}/works_removed_from_clusters/")
    except:
        work_to_clusters_removed = spark.sparkContext.emptyRDD()\
            .toDF(schema=StructType([StructField("work_author_id", StringType()),
                                     StructField("author_id", LongType())]))
            
    ####################################### NAME MATCH ROUND 1 ################################################
            
    round_2_new_data = spark.read.parquet(f"{temp_save_path}/round_2_of_clustering/")

    names_match = round_2_new_data \
        .withColumn('paper_id', F.split(F.col('work_author_id'), "_").getItem(0).cast(LongType())) \
        .join(temp_authors_table.select('work_author_id_2', 
                                        'orcid_2', 
                                        'author_id',
                                        F.explode(F.col('alternate_names')).alias('author')),
            how='inner', on='author') \
        .join(work_to_clusters_removed, how='leftanti', on=['work_author_id','author_id']) \
        .filter((F.col('orcid')==F.col('orcid_2')) | 
                (F.col('orcid')=='') | 
                (F.col('orcid_2')=='')) \
        .join(temp_features_table.drop("orcid_2"), how='inner', on='work_author_id_2') \
        .repartition(480)

    # prepare data for model scoring and score
    _ = get_data_features_scored(names_match, "/names_match/")

    # send through clustering/matching algorithm
    _ = live_clustering_algorithm("/names_match/")

    # save new author table rows to file
    spark.read.parquet(f"{temp_save_path}/names_match/matched_to_cluster/*") \
        .select('work_author_id', 
                'author_id') \
        .dropDuplicates(subset=['work_author_id']) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/new_rows_for_author_table/name_match_rows_for_author_table/")

    name_match_add_count = spark.read\
        .parquet(f"{temp_save_path}/new_rows_for_author_table/name_match_rows_for_author_table/").count()
    print(f"{datetime.datetime.now().strftime('%H:%M')}: Name match rows added: {name_match_add_count}")
    name_of_stats_to_track.append('name_match_1_count')
    stats_to_track.append(name_match_add_count)

    # Making all tables current
    new_loc = 'name_match_rows_for_author_table'
    # _ = create_new_features_table(new_loc)
    # print("New features table created")
    # temp_features_table = spark.read.parquet(f"{temp_save_path}/temp_features_table/{new_loc}/")

    _ = create_new_author_table(new_loc)
    print(f"{datetime.datetime.now().strftime('%H:%M')}: New authors table created")
    temp_authors_table = spark.read.parquet(f"{temp_save_path}/temp_authors_table/{new_loc}/")

    # Creating table for next round
    all_new_data \
        .join(temp_authors_table.select(F.col('work_author_id_2').alias('work_author_id')).distinct(), 
                    how='leftanti', on='work_author_id') \
        .select('work_author_id','paper_id','original_author','author','orcid','coauthors_shorter','concepts_shorter',
            'institutions','citations','created_date') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/round_3_of_clustering/")

    ####################################### NAME MATCH ROUND 2 ###############################################

    round_3_new_data = spark.read.parquet(f"{temp_save_path}/round_3_of_clustering/")

    names_match_2 = round_3_new_data \
        .withColumn('paper_id', F.split(F.col('work_author_id'), "_").getItem(0).cast(LongType())) \
        .join(temp_authors_table.select('work_author_id_2', 
                                        'orcid_2', 
                                        'author_id',
                                        F.explode(F.col('alternate_names')).alias('author')),
            how='inner', on='author') \
        .join(work_to_clusters_removed, how='leftanti', on=['work_author_id','author_id']) \
        .filter((F.col('orcid')==F.col('orcid_2')) | 
                (F.col('orcid')=='') | 
                (F.col('orcid_2')=='')) \
        .join(temp_features_table.drop("orcid_2"), how='inner', on='work_author_id_2')

    # prepare data for model scoring and score
    _ = get_data_features_scored(names_match_2, "/names_match_2/")

    # send through clustering/matching algorithm
    _ = live_clustering_algorithm("/names_match_2/")

    # save new author table rows to file
    spark.read.parquet(f"{temp_save_path}/names_match_2/matched_to_cluster/*") \
        .select('work_author_id', 
                'author_id') \
        .dropDuplicates(subset=['work_author_id']) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/new_rows_for_author_table/name_match_rows_for_author_table_2/")

    name_match_add_count = spark.read\
        .parquet(f"{temp_save_path}/new_rows_for_author_table/name_match_rows_for_author_table_2/").count()
    print(f"{datetime.datetime.now().strftime('%H:%M')}: Name match rows added: {name_match_add_count}")
    name_of_stats_to_track.append('name_match_2_count')
    stats_to_track.append(name_match_add_count)

    # Making all tables current
    new_loc = 'name_match_rows_for_author_table_2'
    # _ = create_new_features_table(new_loc)
    # print("New features table created")
    # temp_features_table = spark.read.parquet(f"{temp_save_path}/temp_features_table/{new_loc}/")

    _ = create_new_author_table(new_loc)
    print(f"{datetime.datetime.now().strftime('%H:%M')}: New authors table created")
    temp_authors_table = spark.read.parquet(f"{temp_save_path}/temp_authors_table/{new_loc}/")

    # Creating table for next round
    all_new_data \
        .join(temp_authors_table.select(F.col('work_author_id_2').alias('work_author_id')).distinct(), 
                    how='leftanti', on='work_author_id') \
        .select('work_author_id','paper_id','original_author','author','orcid','coauthors_shorter','concepts_shorter',
            'institutions','citations','created_date') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/round_4_of_clustering/")
    
    ######################################### NO NAME MATCH ###############################################

    round_4_new_data = spark.read.parquet(f"{temp_save_path}/round_4_of_clustering/") \
        .filter(F.col('author').isNotNull()) \
        .filter(F.col('author')!='') \
        .withColumn('non_latin_groups', group_non_latin_characters(F.col('author'))) \
        .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups'))

    round_4_new_data \
        .filter(F.col('name_to_keep_ind')==1) \
        .withColumn('transformed_search_name', transform_name_for_search(F.col('author'))) \
        .withColumn('name_len', F.length(F.col('transformed_search_name'))) \
        .filter(F.col('name_len')>1) \
        .withColumn('name_match_list', get_name_match_list(F.col('transformed_search_name'))) \
        .withColumn('block', only_get_last(F.col('transformed_search_name'))) \
        .select('work_author_id','orcid','name_match_list','transformed_search_name', 'block') \
        .withColumn('block_removed', F.expr("regexp_replace(transformed_search_name, block, '')")) \
        .withColumn('new_block_removed', F.trim(F.expr("regexp_replace(block_removed, '  ', ' ')"))) \
        .withColumn('letter', get_starting_letter(F.col('new_block_removed'))) \
        .select('work_author_id','orcid','name_match_list','transformed_search_name','letter', 'block') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/for_new_authors_table/names_to_blocks/")

    no_names_match = spark.read.parquet(f"{temp_save_path}/for_new_authors_table/names_to_blocks/")
    no_names_match.cache().count()

    # join those names to authors table alternate names to get work_author_ids to check
    full_no_names_match_table = no_names_match \
        .join(author_names_match, how='inner', on=['block','letter']) \
        .withColumn('matched_names', check_block_vs_block(F.col('name_match_list'), F.col('name_match_list_2'))) \
        .filter(F.col('matched_names')==1) \
        .select('work_author_id', 'work_author_id_2') \
        .dropDuplicates() \
        .join(temp_authors_table.select('work_author_id_2','author_id'), how='left', on='work_author_id_2') \
        .join(round_4_new_data, how='inner', on='work_author_id') \
        .join(temp_features_table, how='inner', on='work_author_id_2') \
        .filter((F.col('orcid')==F.col('orcid_2')) | 
            (F.col('orcid')=='') | 
            (F.col('orcid_2')==''))
        
    # prepare data for model scoring and score
    _ = get_data_features_scored(full_no_names_match_table, "/no_names_match/")

    # send through clustering/matching algorithm
    _ = live_clustering_algorithm("/no_names_match/")

    # save new author table rows to file
    spark.read.parquet(f"{temp_save_path}/no_names_match/matched_to_cluster/*") \
        .select('work_author_id', 
                'author_id') \
        .dropDuplicates(subset=['work_author_id']) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/new_rows_for_author_table/no_name_match_rows_for_author_table/")

    no_name_match_add_count = spark.read\
        .parquet(f"{temp_save_path}/new_rows_for_author_table/no_name_match_rows_for_author_table/").count()
    print(f"{datetime.datetime.now().strftime('%H:%M')}: No name match rows added: {no_name_match_add_count}")
    name_of_stats_to_track.append('no_name_match_count')
    stats_to_track.append(no_name_match_add_count)

    # Making all tables current
    new_loc = 'no_name_match_rows_for_author_table'
    # _ = create_new_features_table(new_loc)
    # print("New features table created")
    # temp_features_table = spark.read.parquet(f"{temp_save_path}/temp_features_table/{new_loc}/")

    _ = create_new_author_table(new_loc)
    print(f"{datetime.datetime.now().strftime('%H:%M')}: New authors table created")
    temp_authors_table = spark.read.parquet(f"{temp_save_path}/temp_authors_table/{new_loc}/")

    #################################### NO MATCH: NEW CLUSTER ##########################################

    # Creating table for work_author_ids that need new cluster
    all_new_data \
        .join(temp_authors_table.select(F.col('work_author_id_2').alias('work_author_id')).distinct(), 
                    how='leftanti', on='work_author_id') \
        .select('work_author_id','paper_id','original_author','author','orcid','coauthors_shorter','concepts_shorter',
            'institutions','citations','created_date') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/end_of_clustering_leftovers/")

    new_cluster_count = spark.read\
        .parquet(f"{temp_save_path}/end_of_clustering_leftovers/").count()
    print(f"{datetime.datetime.now().strftime('%H:%M')}: New clusters added: {new_cluster_count}")
    name_of_stats_to_track.append('new_cluster_count')
    stats_to_track.append(new_cluster_count)

    # Getting max author_id to create new cluster nums
    max_id = int(temp_authors_table.select(F.max(F.col('author_id'))).collect()[0][0])

    # Create new clusters
    w1 = Window.orderBy(F.col('work_author_id'))

    spark.read.parquet(f"{temp_save_path}/end_of_clustering_leftovers/") \
        .select('work_author_id').distinct() \
        .withColumn('temp_cluster_num', F.row_number().over(w1)) \
        .withColumn('author_id', F.lit(max_id) + F.col('temp_cluster_num')) \
        .select('work_author_id','author_id') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/new_rows_for_author_table/new_author_clusters/")

    ###########################################################################

    # Check for overmerged authors to fix
    secret = get_secret()

    overmerged_clusters_raw = (spark.read
        .format("postgresql")
        .option("dbtable", "authorships.overmerged_authors")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password'])
        .load())

    try:
        overmerged_clusters_fixed = (spark.read
            .format("postgresql")
            .option("dbtable", "authorships.overmerged_authors_fixed")
            .option("host", secret['host'])
            .option("port", secret['port'])
            .option("database", secret['dbname'])
            .option("user", secret['username'])
            .option("password", secret['password'])
            .load())
    except:
        overmerged_clusters_fixed = spark.sparkContext.emptyRDD()\
            .toDF(schema=StructType([StructField("work_author_id_for_cluster", StringType()),
                                     StructField("all_works_to_cluster", StringType())]))
    

    overmerged_clusters = overmerged_clusters_raw.select('work_author_id_for_cluster','all_works_to_cluster') \
        .join(overmerged_clusters_fixed.select('work_author_id_for_cluster','all_works_to_cluster'), 
              how='leftanti', on=['work_author_id_for_cluster','all_works_to_cluster']) \
        .select(F.col('work_author_id_for_cluster').alias('work_author_id'),'all_works_to_cluster') \
        .groupBy('work_author_id') \
        .agg(F.collect_set(F.col('all_works_to_cluster')).alias('work_author_ids'))

    if overmerged_clusters.count() == 0:
        print(f"{datetime.datetime.now().strftime('%H:%M')}: No overmerges to fix!")
    elif overmerged_clusters.count() > 0:
        print(f"{datetime.datetime.now().strftime('%H:%M')}: Overmerge clusters to fix: {overmerged_clusters.count()}")

        if spark.read.parquet(f"{temp_save_path}/new_rows_for_author_table/new_author_clusters/").count() > 0:
            new_max_id = int(spark.read.parquet(f"{temp_save_path}/new_rows_for_author_table/new_author_clusters/")
                            .select(F.max(F.col('author_id'))).collect()[0][0])
        else:
            new_max_id = max_id
        
        w2 = Window.orderBy(F.col('work_author_id'))

        # Getting work_author_ids that will be in new cluster
        ids_to_skip = overmerged_clusters.select(F.explode('work_author_ids').alias('work_author_id'))\
            .rdd.flatMap(lambda x: x).collect()

        # Getting the new cluster number
        overmerged_clusters.select('work_author_id','work_author_ids')\
            .withColumn('temp_cluster_num', F.row_number().over(w2)) \
            .withColumn('author_id', F.lit(new_max_id) + F.col('temp_cluster_num')) \
            .select(F.explode('work_author_ids').alias('work_author_id'), 'author_id') \
            .write.mode('append') \
            .parquet(f"{temp_save_path}/new_rows_for_author_table/new_author_clusters/")

        # Filtering out changed work_author_ids from temp_authors_table
        temp_authors_table = temp_authors_table.filter(~F.col('work_author_id_2').isin(ids_to_skip))\
            .alias('new_temp_authors_new_clusters')

        # Writing out changed data to authorships.overmerged_authors_fixed
        (overmerged_clusters.select(F.col('work_author_id').alias('work_author_id_for_cluster'),'work_author_ids')
            .select('work_author_id_for_cluster', F.explode('work_author_ids').alias('all_works_to_cluster'))
            .withColumn("modified_date", F.current_timestamp())
            .repartition(6)
            .write.format("jdbc") 
            .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
            .option("dbtable", 'authorships.overmerged_authors_fixed') 
            .option("user", secret['username']) 
            .option("password", secret['password']) 
            .option("driver", "org.postgresql.Driver") 
            .mode("append") 
            .save())

    ###########################################################################

    # Making all tables current
    new_loc = 'new_author_clusters'
    # _ = create_new_features_table(new_loc)
    # print("New features table created")
    # temp_features_table = spark.read.parquet(f"{temp_save_path}/temp_features_table/{new_loc}/")

    _ = create_new_author_table(new_loc)
    print(f"{datetime.datetime.now().strftime('%H:%M')}: New authors table created")
    temp_authors_table = spark.read.parquet(f"{temp_save_path}/temp_authors_table/{new_loc}/")

    ################################### DUPLICATES/NULL AUTHORS ###########################################

    # Taking care of duplicate entries (if necessary)
    dup_work_entries = spark.read.parquet(f"{temp_save_path}/duplicate_work_entries/")
    if dup_work_entries.count() > 0:
        duplicate_rows_temp_authors = temp_authors_table \
            .join(dup_work_entries.select(F.col('work_author_id').alias('work_author_id_2'),
                                        'work_author_ids'), how='inner', on='work_author_id_2') \
            .select(F.explode('work_author_ids').alias('work_author_id_2'),
                    'author_id',
                    'orcid_2', 
                    'display_name',
                    'alternate_names',
                    'author_alternate_names',
                    'name_match_list').alias('dup_rows_temp_author_table')
            
        temp_authors_table.union(duplicate_rows_temp_authors.select(*temp_authors_table.columns)) \
            .dropDuplicates(subset=['work_author_id_2']) \
            .write.mode('overwrite') \
            .parquet(f"{temp_save_path}/temp_authors_table/after_dup_entries/")
    
        duplicate_rows_temp_features = temp_features_table \
            .join(dup_work_entries.select(F.col('work_author_id').alias('work_author_id_2'),
                                        'work_author_ids'), how='inner', on='work_author_id_2') \
            .select(F.explode('work_author_ids').alias('work_author_id_2'),
                    'orcid_2',
                    'citations_2',
                    'institutions_2',
                    'author_2',
                    'paper_id_2',
                    'original_author',
                    'concepts_shorter_2',
                    'coauthors_shorter_2').alias('dup_rows_temp_feature_table')
            
        temp_features_table.union(duplicate_rows_temp_features.select(*temp_features_table.columns)) \
            .dropDuplicates(subset=['work_author_id_2']) \
            .write.mode('overwrite') \
            .parquet(f"{temp_save_path}/temp_features_table/after_dup_entries/")

        print(f"{datetime.datetime.now().strftime('%H:%M')}: New features table created")
        temp_features_table = spark.read.parquet(f"{temp_save_path}/temp_features_table/after_dup_entries/")

        print(f"{datetime.datetime.now().strftime('%H:%M')}: New authors table created")
        temp_authors_table = spark.read.parquet(f"{temp_save_path}/temp_authors_table/after_dup_entries/")

    temp_aut_count = temp_features_table.count()
    temp_fea_count = temp_authors_table.count()
    print(f"{datetime.datetime.now().strftime('%H:%M')}: TESTING: rows in temp authors table: {temp_fea_count}")
    print(f"{datetime.datetime.now().strftime('%H:%M')}: TESTING: rows in temp features table: {temp_aut_count}")

    if temp_aut_count != temp_fea_count:
        import time
        time.sleep(10)
        print(end_this_script)

    # Taking care of empty/null/bad author names
    (spark.read
        .parquet(f"{temp_save_path}/raw_data_to_disambiguate/")
        .select('work_author_id', F.trim(F.col('original_author')).alias('original_author'), 'orcid', 'concepts',   
                'institutions', 'citations', 'coauthors', 'created_date')
        .join(temp_authors_table.select(F.col('work_author_id_2').alias('work_author_id'))
                .dropDuplicates(subset=['work_author_id']), how='leftanti', on='work_author_id') \
        .dropDuplicates(subset=['work_author_id'])
        .select(F.col('work_author_id').alias('work_author_id_2'), 
                F.col('orcid').alias('orcid_2'), 'original_author')
        .withColumn('author_id', F.lit(9999999999))
        .withColumn('display_name', F.lit('NULL AUTHOR_ID'))
        .withColumn('alternate_names', F.array(F.lit('NULL AUTHOR_ID')))
        .select('work_author_id_2', 
                'original_author',
                'author_id',
                'orcid_2', 
                'display_name',
                'alternate_names')
        .write.mode('overwrite')
        .parquet(f"{temp_save_path}/new_data_to_be_given_null_value/"))
    
    if spark.read.parquet(f"{temp_save_path}/null_author_rows_to_filter_out/").count() > 0:
        (spark.read.parquet(f"{temp_save_path}/null_author_rows_to_filter_out/")
            .select('work_author_id_2', 'orcid_2', 'original_author')
            .withColumn('author_id', F.lit(9999999999))
            .withColumn('display_name', F.lit('NULL AUTHOR_ID'))
            .withColumn('alternate_names', F.array(F.lit('NULL AUTHOR_ID')))
            .select('work_author_id_2', 
                    'original_author',
                    'author_id',
                    'orcid_2', 
                    'display_name',
                    'alternate_names')
            .write.mode('append')
            .parquet(f"{temp_save_path}/new_data_to_be_given_null_value/"))
    
    ############################## COMPARING FINAL VS INIT AUTHOR TABLES ######################################

    # Loading initial and final null author tables to compare against
    new_null_author_data = spark.read.parquet(f"{temp_save_path}/new_data_to_be_given_null_value/")
    old_null_author_data = spark.read.parquet(f"{temp_save_path}/current_null_authors_table/")

    null_authors_diff = new_null_author_data\
        .join(old_null_author_data.select(F.col('work_author_id').alias('work_author_id_2')).distinct(), 
                how='leftanti', on='work_author_id_2') \
        .withColumn("created_date", F.current_timestamp()) \
        .withColumn("modified_date", F.current_timestamp()) \
        .withColumn('author_id_changed', F.lit(True)) \
        .dropDuplicates(subset=['work_author_id_2'])

    # Loading initial and final tables to compare against
    init_author_table = spark.read.parquet(f"{prod_save_path}/current_authors_table/") \
        .select('work_author_id', 
                F.col('author_id').alias('author_id_1'),
                F.col('display_name').alias('display_name_1'),
                F.col('alternate_names').alias('alternate_names_1'), 
                F.col('orcid').alias('orcid_1'),
                'created_date',
                'modified_date')

    final_author_table = temp_authors_table \
            .select(F.col('work_author_id_2').alias('work_author_id'), 
                F.col('author_id').alias('author_id_2'),
                F.col('display_name').alias('display_name_2'),
                F.col('author_alternate_names').alias('alternate_names_2'), 
                'orcid_2') \
            .join(add_orcid_df.select('work_author_id', 'new_orcid'), 
                  how='left', on='work_author_id') \
            .withColumn('final_orcid_id', F.when(F.col('new_orcid').isNull(), 
                                                 F.col('orcid_2')).otherwise(F.col('new_orcid'))) \
            .select('work_author_id', 
                    'author_id_2',
                    'display_name_2',
                    'alternate_names_2', 
                    F.col('final_orcid_id').alias('orcid_2'))
            
    print(f"{datetime.datetime.now().strftime('%H:%M')}: INITIAL TABLE: {init_author_table.count()}")
    print(f"{datetime.datetime.now().strftime('%H:%M')}: FINAL TABLE: {final_author_table.count()}")

    name_of_stats_to_track.append('init_author_table_count')
    stats_to_track.append(init_author_table.count())

    name_of_stats_to_track.append('final_author_table_count')
    stats_to_track.append(final_author_table.count())

    # take final author table, compare to init table (all columns) to see if anything has been changed
    compare_tables = final_author_table.join(init_author_table, how='inner', on='work_author_id') \
        .withColumn('orcid_compare', F.when(F.col('orcid_1')==F.col('orcid_2'), 0).otherwise(1)) \
        .withColumn('display_name_compare', F.when(F.col('display_name_1')==F.col('display_name_2'), 0).otherwise(1)) \
        .withColumn('author_id_compare', F.when(F.col('author_id_1')==F.col('author_id_2'), 0).otherwise(1)) \
        .withColumn('alternate_names_compare', check_list_vs_list(F.col('alternate_names_1'), 
                                                                    F.col('alternate_names_2'))) \
        .withColumn('total_changes', F.col('orcid_compare') + F.col('display_name_compare') + 
                                F.col('author_id_compare') + F.col('alternate_names_compare'))
    
    # if not, write out those rows to a folder
    compare_tables.filter(F.col('total_changes')==0) \
        .select('work_author_id', 
                F.col('author_id_1').alias('author_id'),
                F.col('display_name_1').alias('display_name'),
                F.col('alternate_names_1').alias('alternate_names'), 
                F.col('orcid_1').alias('orcid'),
                'created_date',
                'modified_date') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/final_author_table_part/no_changes/")

    # if modified but not created, write out to different folder
    compare_tables.filter(F.col('total_changes')>0) \
        .select('work_author_id', 
                F.col('author_id_2').alias('author_id'),
                F.col('display_name_2').alias('display_name'),
                F.col('alternate_names_2').alias('alternate_names'), 
                F.col('orcid_2').alias('orcid'),
                'created_date') \
        .withColumn("modified_date", F.current_timestamp()) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/final_author_table_part/modified/")

    # if created, write out to different folder
    final_author_table.join(init_author_table.select('work_author_id'), how='leftanti', on='work_author_id') \
        .select('work_author_id', 
                F.col('author_id_2').alias('author_id'),
                F.col('display_name_2').alias('display_name'),
                F.col('alternate_names_2').alias('alternate_names'), 
                F.col('orcid_2').alias('orcid')) \
        .withColumn("created_date", F.current_timestamp()) \
        .withColumn("modified_date", F.current_timestamp()) \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/final_author_table_part/created/")

    # Writing out rows that have changed author IDs (merge table)
    if spark.read.parquet(f"{temp_save_path}/temp_authors_to_change_table/").count() > 0:
        # write out previously disambiguated rows to final locations
        compare_tables.join(works_authors_to_remove, how='inner', on='work_author_id') \
            .filter(F.col('author_id_1')!=F.col('author_id_2')) \
            .select('work_author_id', 
                    F.col('author_id_1').alias('old_author_id'),
                    F.col('author_id_2').alias('new_author_id')) \
            .withColumn("modified_date", F.current_timestamp()) \
            .write.mode('overwrite') \
            .parquet(f"{temp_save_path}/work_authors_changed_clusters/")

        print("Author cluster changes: ", spark.read.parquet(f"{temp_save_path}/work_authors_changed_clusters/").count())
    
    # Writing out rows that have changed author IDs (merge table)
    compare_tables.join(works_authors_to_remove, how='inner', on='work_author_id') \
            .filter(F.col('author_id_1').isNotNull()) \
            .filter(F.col('author_id_1')!=F.col('author_id_2')) \
            .select('work_author_id', 
                    F.col('author_id_1').alias('old_author_id'),
                    F.col('author_id_2').alias('new_author_id')) \
            .withColumn("modified_date", F.current_timestamp()) \
            .write.mode('append') \
            .parquet(f"{prod_save_path}/previously_disambiguated_author_merges/")
    
    print(f"{datetime.datetime.now().strftime('%H:%M')}: No row changes: ", spark.read.parquet(f"{temp_save_path}/final_author_table_part/no_changes/").count())
    print(f"{datetime.datetime.now().strftime('%H:%M')}: Modified rows: ", spark.read.parquet(f"{temp_save_path}/final_author_table_part/modified/").count())
    print(f"{datetime.datetime.now().strftime('%H:%M')}: New cluster rows: ", spark.read.parquet(f"{temp_save_path}/final_author_table_part/created/").count())
    print(f"{datetime.datetime.now().strftime('%H:%M')}: Total rows: ", spark.read.parquet(f"{temp_save_path}/final_author_table_part/*").count())

    name_of_stats_to_track.append('new_rows_count')
    stats_to_track.append(spark.read.parquet(f"{temp_save_path}/final_author_table_part/created/").count())

    name_of_stats_to_track.append('modified_rows_count')
    stats_to_track.append(spark.read.parquet(f"{temp_save_path}/final_author_table_part/modified/").count())

    name_of_stats_to_track.append('total_rows_count')
    stats_to_track.append(spark.read.parquet(f"{temp_save_path}/final_author_table_part/*").count())

    ############### ALL ROWS BEING WRITTEN TO POSTGRES OUTPUT TABLE (authorships.authors_modified) #################

    secret = get_secret()

    # Getting new author IDs and writing to authorships.authors_modified
    (final_author_table.join(init_author_table.select('work_author_id'), how='leftanti', on='work_author_id') 
        .select('work_author_id', F.col('author_id_2').alias('author_id'), 
                F.col('display_name_2').alias('display_name'), 
                F.col('alternate_names_2').alias('alternate_names'), 
                F.col('orcid_2').alias('orcid'))
        .withColumn("created_date", F.current_timestamp()) 
        .withColumn("modified_date", F.current_timestamp())
        .withColumn('author_id_changed', F.lit(True))
        .repartition(12)
        .write.format("jdbc") 
        .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
        .option("dbtable", 'authorships.authors_modified') 
        .option("user", secret['username']) 
        .option("password", secret['password']) 
        .option("driver", "org.postgresql.Driver") 
        .mode("overwrite") 
        .save())
    
    print(f"{datetime.datetime.now().strftime('%H:%M')}: authorships.authors_modified write 1 done")

    # Getting rows that changed author IDs and writing to authorships.authors_modified
    (compare_tables.filter(F.col('author_id_compare')==1).filter(F.col('total_changes')>0)
            .select('work_author_id', F.col('author_id_2').alias('author_id'), 
                    F.col('display_name_2').alias('display_name'), 
                    F.col('alternate_names_2').alias('alternate_names'), 
                    F.col('orcid_2').alias('orcid'), 'created_date') 
        .withColumn("modified_date", F.current_timestamp())
        .withColumn('author_id_changed', F.lit(True))
        .repartition(12)
        .write.format("jdbc")
        .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
        .option("dbtable", 'authorships.authors_modified')
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())
    
    print(f"{datetime.datetime.now().strftime('%H:%M')}: authorships.authors_modified write 2 done")

    # Getting any new null authors and writing to authorships.authors_modified
    if null_authors_diff.count() > 0:
        (null_authors_diff
            .select(F.col('work_author_id_2').alias('work_author_id'), 'author_id', 
                        'display_name', 'alternate_names', F.col('orcid_2').alias('orcid'), 
                        'created_date','modified_date','author_id_changed')
            .repartition(12)
            .write.format("jdbc") 
            .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
            .option("dbtable", 'authorships.authors_modified') 
            .option("user", secret['username']) 
            .option("password", secret['password']) 
            .option("driver", "org.postgresql.Driver") 
            .mode("append") 
            .save())
        
    print(f"{datetime.datetime.now().strftime('%H:%M')}: authorships.authors_modified write 3 done")

    # Getting any row changes to author ID metadata (but not author ID) and writing to authorships.authors_modified
    (compare_tables.filter(F.col('author_id_compare')==0).filter(F.col('total_changes')>0)
            .select('work_author_id', F.col('author_id_2').alias('author_id'), 
                    F.col('display_name_2').alias('display_name'), 
                    F.col('alternate_names_2').alias('alternate_names'), 
                    F.col('orcid_2').alias('orcid'), 'created_date') 
        .withColumn("modified_date", F.current_timestamp()) 
        .withColumn('author_id_changed', F.lit(False))
        .repartition(12)
        .write.format("jdbc")
        .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
        .option("dbtable", 'authorships.authors_modified')
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())
    
    if spark.read.parquet(f"{temp_save_path}/temp_authors_to_change_table/").count() > 0:
        prev_disam_authors = spark.read.parquet(f"{temp_save_path}/temp_authors_to_change_table/")
        
        (spark.read.parquet(f"{temp_save_path}/final_author_table_part/no_changes/")\
            .join(prev_disam_authors, how='inner', on='work_author_id').dropDuplicates(subset=['work_author_id'])
            .select('work_author_id', F.col('author_id'), 
                    F.col('display_name'), 
                    F.col('alternate_names'), 
                    F.col('orcid'), 'created_date') 
        .withColumn("modified_date", F.current_timestamp()) 
        .withColumn('author_id_changed', F.lit(True))
        .repartition(24)
        .write.format("jdbc")
        .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
        .option("dbtable", 'authorships.authors_modified')
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())


    #######################################################################################################
    #######################################################################################################
    #######################################################################################################

    # TEMP SECTION FOR CHANGING DISPLAY NAMES AND ALTERNATE NAMES (COMMENT OUT POSTGRES WRITES ABOVE?)

    # (compare_tables.filter(F.col('author_id_compare')==0).filter(F.col('total_changes')>0)
    #         .select(F.col('author_id_2').alias('author_id'), 
    #                 F.col('display_name_2').alias('display_name'), 
    #                 F.col('alternate_names_2').alias('alternate_names'), 
    #                 F.col('orcid_2').alias('orcid'))
    #     .dropDuplicates('author_id')
    #     .withColumn("modified_date", F.current_timestamp()) 
    #     .repartition(6)
    #     .write.format("jdbc")
    #     .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
    #     .option("dbtable", 'authorships.large_metadata_change')
    #     .option("user", secret['username'])
    #     .option("password", secret['password'])
    #     .option("driver", "org.postgresql.Driver")
    #     .mode("overwrite")
    #     .save())

    #######################################################################################################
    #######################################################################################################
    #######################################################################################################
    
    print(f"{datetime.datetime.now().strftime('%H:%M')}: authorships.authors_modified write 4 done")
    
    print(f"{datetime.datetime.now().strftime('%H:%M')}: All postgres tables written")

    ########################################## FINAL TABLES TO S3 ##########################################

    # Writing out final null authors table from this round
    (new_null_author_data.select(F.col('work_author_id_2').alias('work_author_id')).distinct()\
        .join(old_null_author_data, how='inner', on ='work_author_id')
        .dropDuplicates(subset=['work_author_id'])
        .select('work_author_id', 'author_id', 'display_name', 'alternate_names', 'orcid',
                'created_date','modified_date')
        .write.mode('overwrite')
        .parquet(f"{prod_save_path}/current_null_authors_table/"))

    if null_authors_diff.count() > 0:
        (null_authors_diff
            .select(F.col('work_author_id_2').alias('work_author_id'), 'author_id', 
                        'display_name', 'alternate_names', F.col('orcid_2').alias('orcid'), 
                        'created_date','modified_date','author_id_changed')
            .write.mode('append')
            .parquet(f"{prod_save_path}/current_null_authors_table/"))
    
    print(f"{datetime.datetime.now().strftime('%H:%M')}: Final null author table written to S3")

    # Writing out final authors table from this round
    spark.read.parquet(f"{temp_save_path}/final_author_table_part/*") \
        .repartition(384) \
        .write.mode('overwrite') \
        .parquet(f"{prod_save_path}/current_authors_table/")

    print(f"{datetime.datetime.now().strftime('%H:%M')}: Final authors table written to S3")

    # Writing out final features table from this round
    temp_features_table \
        .repartition(384) \
        .write.mode('overwrite') \
        .parquet(f"{prod_save_path}/current_features_table/")

    print(f"{datetime.datetime.now().strftime('%H:%M')}: Final features table written to S3")

    # Writing out new author name match table from this round
    temp_features_table \
        .select('work_author_id_2', 'orcid_2', F.col('author_2').alias('transformed_name')) \
        .filter(F.col('transformed_name')!="") \
        .filter(F.col('transformed_name').isNotNull()) \
        .withColumn('transformed_search_name', transform_name_for_search(F.col('transformed_name'))) \
        .withColumn('name_len', F.length(F.col('transformed_search_name'))) \
        .filter(F.col('name_len')>1) \
        .withColumn('name_match_list_2', get_name_match_list(F.col('transformed_search_name'))) \
        .withColumn('block', only_get_last(F.col('transformed_search_name'))) \
        .select('work_author_id_2','name_match_list_2', 'orcid_2', 'transformed_search_name', 'block')\
        .withColumn('block_removed', F.expr("regexp_replace(transformed_search_name, block, '')")) \
        .withColumn('new_block_removed', F.trim(F.expr("regexp_replace(block_removed, '  ', ' ')"))) \
        .withColumn('letter', get_starting_letter(F.col('new_block_removed'))) \
        .select('work_author_id_2','orcid_2','name_match_list_2', 'block', 'letter') \
        .dropDuplicates() \
        .repartition(384) \
        .write.mode('overwrite') \
        .parquet(f"{prod_save_path}/current_author_names_match/")

    print(f"{datetime.datetime.now().strftime('%H:%M')}: Final author name match table written to S3")

# COMMAND ----------

for i,j in zip(name_of_stats_to_track,stats_to_track):
    print(f"{curr_date} -- {i} -- {j}")

# COMMAND ----------

end_datetime = datetime.datetime.now()
time_delta_minutes = round((end_datetime - start_datetime).seconds/60, 3)
