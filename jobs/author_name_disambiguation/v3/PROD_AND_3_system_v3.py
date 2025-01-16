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

# MAGIC %md #### Load secrets

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

start_datetime = datetime.datetime.now()
curr_date = start_datetime.strftime("%Y_%m_%d_%H_%M")
# curr_date = '2024_07_14_23_18'
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

total_count = spark.read.parquet(f"{prod_save_path}/input_data_for_AND").count()
total_count

# COMMAND ----------

sample_size_for_each_stage = 600000 # this is the sample size for each round of AND
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

            # check to see if first name in last and last name in first
            # if that is the case, need to do matching between first/last of both pairs
            first_last_check = check_first_and_last(block_1_names_list[0], block_2_names_list[0], 
                                                    block_1_names_list[-2], block_2_names_list[-2])
            if first_last_check:
                pass
            else:
                return 0

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
        
def check_first_and_last(first_names_1, first_names_2, last_names_1, last_names_2):
    """This function tries to catch times when the first and last name have been swapped
    at some point and so an authors first name and last name would show up in both the list
    of first names and the list of last names. This was causing authors to match with one name
    matching even though the other name did not match"""
    
    if first_names_1 and first_names_2 and last_names_1 and last_names_2:
        # check if both names in both lists
        
        if ((any(names in first_names_1 for names in last_names_1) and 
            ((len(first_names_1) > 1) or (len(first_names_1) > 1))) or 
            (any(names in first_names_2 for names in last_names_2) and 
            ((len(first_names_2) > 1) or (len(first_names_1) > 2)))):
            # if both names in both lists for both sets, each unique names need to match
            if (len(first_names_1) == 2 and 
                len(first_names_2) == 2 and 
                len(last_names_1) == 2 and 
                len(last_names_2) == 2):
                name_set_1 = set(first_names_1 + last_names_1)
                name_set_2 = set(first_names_2 + last_names_2)
                if all(names in name_set_1 for names in name_set_2):
                    return True
                else:
                    return False
            else:
                names_to_remove = []
                for first_name in first_names_1:
                    if ((first_name in first_names_2) and 
                        (first_name in last_names_1) and 
                        (first_name in last_names_2)):
                        names_to_remove.append(first_name)
                        
                new_first_names_1 = [x for x in first_names_1 if x not in names_to_remove]
                new_last_names_1 = [x for x in last_names_1 if x not in names_to_remove]
                new_first_names_2 = [x for x in first_names_2 if x not in names_to_remove]
                new_last_names_2 = [x for x in first_names_2 if x not in names_to_remove]
                
                names_are_good = 1
                if new_first_names_1 and new_last_names_1 and new_first_names_2 and new_last_names_2:
                    if (any(names in new_first_names_1 for names in new_first_names_2) or 
                        any(names in new_last_names_1 for names in new_last_names_2)):
                        return True
                    else:
                        return False
                elif (not new_last_names_1) or (not new_last_names_2):
                    if any(names in new_first_names_1 for names in new_first_names_2):
                        return True
                    else:
                        return False
                elif (not new_first_names_1) or (not new_first_names_2):
                    if any(names in new_last_names_1 for names in new_last_names_2):
                        return True
                    else:
                        return False
                else:
                    return True
        else:
            return True
            
    else:
        return True

def check_block_vs_block_reg(block_1_names_list, block_2_names_list):
    
    # check first names
    first_check, _ = match_block_names(block_1_names_list[0], block_1_names_list[1], block_2_names_list[0], 
                                    block_2_names_list[1])
    # print(f"FIRST {first_check}")
    
    if first_check:
        last_check, _ = match_block_names(block_1_names_list[-2], block_1_names_list[-1], block_2_names_list[-2], 
                                           block_2_names_list[-1])
        # print(f"LAST {last_check}")
        if last_check:

            # check to see if first name in last and last name in first
            # if that is the case, need to do matching between first/last of both pairs
            first_last_check = check_first_and_last(block_1_names_list[0], block_2_names_list[0], 
                                                    block_1_names_list[-2], block_2_names_list[-2])
            if first_last_check:
                pass
            else:
                return 0

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

def get_name_match_from_search_names(trans_names):
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
        .withColumn('paper_id', F.col('paper_id').cast(LongType())) \
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

@udf(returnType=IntegerType())
def check_display_name_change(display_name_1, display_name_2):
    if len(display_name_1) == len(display_name_2):
        if transform_name_for_search_reg(display_name_1) == transform_name_for_search_reg(display_name_2):
            return 0
        else:
            return 1
    else:
        return 1
    
@udf(returnType=ArrayType(ArrayType(ArrayType(StringType()))))
def go_through_names_to_take_out_mismatches(data_to_cluster):
    """
    Goes through each row of data to make sure the names match up
    """
    df_to_cluster = pd.DataFrame(data_to_cluster, columns=['work_author_id_2','orcid_2','author_2'])
    df_to_cluster['author_2'] = df_to_cluster['author_2'].apply(transform_name_for_search_reg)

    orcid_names = df_to_cluster[df_to_cluster['orcid_2']!='']['author_2'].drop_duplicates().tolist()
    df_non_orcid = df_to_cluster[(df_to_cluster['orcid_2']=='') & (~df_to_cluster['author_2'].isin(orcid_names))]['author_2'].value_counts().reset_index()

    group_cluster_dict = {}

    if df_non_orcid.shape[0] > 0:
        df_non_orcid.columns = ['author_2','count']
        df_non_orcid['name_len'] = df_non_orcid['author_2'].apply(len)

        name_list_to_check_against = get_name_match_from_search_names(orcid_names)

        for index, row in df_non_orcid.sort_values(['count','name_len'], ascending=False).iterrows():
            if check_block_vs_block_reg(get_name_match_from_search_names([row.author_2]), name_list_to_check_against)==1:
                orcid_names += [row.author_2]
                name_list_to_check_against = get_name_match_from_search_names(orcid_names)
            else:
                added_to_group = 0
                if not group_cluster_dict:
                    work_author_ids_right = df_to_cluster[df_to_cluster['author_2']==row.author_2]['work_author_id_2'].tolist()
                    group_cluster_dict[work_author_ids_right[0]] = {'trans_name_right': [row.author_2], 
                                                                    'name_match_list': get_name_match_from_search_names([row.author_2]), 
                                                                    'work_author_ids': work_author_ids_right}
                else:
                    name_list_right = get_name_match_list_reg(row.author_2)
                    for key in group_cluster_dict.keys():
                        if check_block_vs_block_reg(group_cluster_dict[key]['name_match_list'], name_list_right)==1:
                            group_cluster_dict[key]['trans_name_right'].append(row.author_2)
                            group_cluster_dict[key]['name_match_list'] = get_name_match_from_search_names(group_cluster_dict[key]['trans_name_right'])
                            group_cluster_dict[key]['work_author_ids'] += df_to_cluster[df_to_cluster['author_2']==row.author_2]['work_author_id_2'].tolist()
                            added_to_group = 1
                            break
                        else:
                            pass
                    if added_to_group==0:
                        work_author_ids_right = df_to_cluster[df_to_cluster['author_2']==row.author_2]['work_author_id_2'].tolist()
                        group_cluster_dict[work_author_ids_right[0]] = {'trans_name_right': [row.author_2],
                                                                    'name_match_list': get_name_match_from_search_names([row.author_2]),
                                                                    'work_author_ids': work_author_ids_right}
        return [[[key], group_cluster_dict[key]['work_author_ids']] for key in group_cluster_dict.keys()]
    else:
        return [[[],[]]]

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

@udf(returnType=LongType())
def check_new_clusters_for_old_id(old_id, new_id, old_works, new_works):
    old_works_set = set(old_works)
    new_works_set = set(new_works)
    if old_works_set == new_works_set:
        return old_id
    else:
        return new_id

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
                    'Occdownload Gbif.Org','GBIF.Org User','et al. et al.',',']

# COMMAND ----------

works = spark.read.parquet(f"{database_copy_save_path}/mid/work").filter(F.col('merge_into_id').isNull()).dropDuplicates() \
    .select(F.col('paper_id').alias('paper_id_2'))

# COMMAND ----------

# Looking at the affiliations table in order to determine which works have been merged/deleted but also replacing features
# that may have changed from the source such as author name
current_affs = spark.read.parquet(f"{database_copy_save_path}/mid/affiliation")\
    .dropDuplicates(subset=['paper_id', 'author_sequence_number']) \
    .select(F.col('paper_id').alias('paper_id_2'), F.concat_ws("_", F.col('paper_id'), F.col('author_sequence_number')).alias('work_author_id_2'), 
            'original_author') \
    .join(works.select('paper_id_2'), how='inner', on='paper_id_2') \
    .select('work_author_id_2', 'original_author') \
    .filter(~F.col('original_author').isin(bad_author_names)) \
    .withColumn('author_2', transform_author_name(F.col('original_author')))
current_affs.cache().count()

# COMMAND ----------

combined_source_orcid = spark.read.parquet(f"{database_copy_save_path}/orcid/combined_source_orcid") \
    .select(F.concat_ws("_", F.col('paper_id'), 
                              F.col('author_sequence_number')).alias('work_author_id_2'), F.col('final_orcid').alias('orcid_2'), 'orcid_source')
    
combined_source_orcid.cache().count()

# COMMAND ----------

# save prod features table
spark.read.parquet(f"{prod_save_path}/current_features_table/") \
    .dropDuplicates()\
    .select('work_author_id_2', 'citations_2', 'institutions_2','paper_id_2','concepts_shorter_2', 'coauthors_shorter_2')\
    .join(combined_source_orcid.select('work_author_id_2', 'orcid_2'), on=['work_author_id_2'], how='left') \
    .fillna("", subset=['orcid_2']) \
    .join(current_affs, on=['work_author_id_2'], how='inner') \
    .select('work_author_id_2', 'orcid_2', 'citations_2', 'institutions_2', 'author_2', 'paper_id_2', 'original_author', 
            'concepts_shorter_2', 'coauthors_shorter_2') \
    .withColumn('author_2', F.trim(F.regexp_replace(F.col('author_2'), '&NA;', ''))) \
    .withColumn('author_2', F.trim(F.regexp_replace(F.regexp_replace(F.col('author_2'), '[0-9]', ' '), ' +', ' '))) \
    .withColumn('author_2', F.trim(F.regexp_replace(F.regexp_replace(F.col('author_2'), '[\*$!<>/?~@|+=$#&^%«»:;\_•]', ' '), 
                                                            ' +', ' '))) \
    .withColumn('author_2', F.trim(F.regexp_replace(F.regexp_replace(F.col('author_2'), '[\[\]\{\}\(\)\\\]', ''), ' +', ' '))) \
    .withColumn('author_2', F.regexp_replace(F.col('author_2'), """^[,."`\-\‐\– ']+""", '')) \
    .filter(~(F.col('original_author').isin(bad_author_names) | 
              F.col('author_2').isin(bad_author_names) |
              (F.lower(F.col('author_2')).contains("download")) | 
              (F.lower(F.col('author_2')).contains("d.o.w.n.l.o.a.d")) |
              (F.length(F.col('author_2'))>120) |
              (F.trim(F.col('author_2'))==""))) \
    .select('work_author_id_2', 'orcid_2', 'citations_2', 'institutions_2', 'author_2', 'paper_id_2', 'original_author', 
            'concepts_shorter_2', 'coauthors_shorter_2') \
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
    .dropDuplicates()\
    .withColumn('author_2', F.trim(F.regexp_replace(F.col('author_2'), '&NA;', ''))) \
    .withColumn('author_2', F.trim(F.regexp_replace(F.regexp_replace(F.col('author_2'), '[0-9]', ' '), ' +', ' '))) \
    .withColumn('author_2', F.trim(F.regexp_replace(F.regexp_replace(F.col('author_2'), '[\*$!<>/?~@|+=$#&^%«»:;\_•]', ' '), 
                                                            ' +', ' '))) \
    .withColumn('author_2', F.trim(F.regexp_replace(F.regexp_replace(F.col('author_2'), '[\[\]\{\}\(\)\\\]', ''), ' +', ' '))) \
    .withColumn('author_2', F.regexp_replace(F.col('author_2'), """^[,."`\-\‐\– ']+""", '')) \
    .filter((F.col('original_author').isin(bad_author_names) | 
             F.col('author_2').isin(bad_author_names) |
              (F.lower(F.col('author_2')).contains("download")) | 
              (F.lower(F.col('author_2')).contains("d.o.w.n.l.o.a.d")) |
              (F.length(F.col('author_2'))>120) |
              (F.trim(F.col('author_2'))==""))) \
    .select('work_author_id_2', 'orcid_2', 'citations_2', 'institutions_2', 'author_2', 'paper_id_2', 'original_author', 
            'concepts_shorter_2', 'coauthors_shorter_2') \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}/null_author_rows_to_filter_out/")

spark.read.parquet(f"{temp_save_path}/current_features_table/").count()

# COMMAND ----------

curr_features = spark.read.parquet(f"{temp_save_path}/current_features_table/")

# COMMAND ----------

current_affs.join(curr_features, how='leftanti', on='work_author_id_2') \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}/testing/affs_not_in_feature_table")

# COMMAND ----------

curr_author_table = spark.read.parquet(f"{prod_save_path}/current_authors_table/")

# save prod authors table
curr_author_table\
    .join(curr_features.select(F.col('work_author_id_2').alias('work_author_id')), on='work_author_id', how='inner') \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}/current_authors_table/")

spark.read.parquet(f"{temp_save_path}/current_authors_table/").count()

# COMMAND ----------

mid_author_table = spark.read.parquet(f"{database_copy_save_path}/mid/author").dropDuplicates()\
    .filter(F.col('author_id')>=5000000000) \
    .filter(F.col('merge_into_id').isNull()).select('author_id')
mid_author_table.cache().count()

# COMMAND ----------

# Getting author IDs that no longer have works associated with them
old_author_id_counts = spark.read.parquet(f"{prod_save_path}/current_authors_table/") \
    .groupBy('author_id').count()
old_author_id_counts.cache().count()

# COMMAND ----------

mid_author_table.join(old_author_id_counts, how='leftanti', on='author_id').count()

# COMMAND ----------

new_author_id_counts = spark.read.parquet(f"{temp_save_path}/current_authors_table/") \
    .groupBy('author_id').count()
new_author_id_counts.cache().count()

# COMMAND ----------

old_author_id_counts.join(new_author_id_counts, on='author_id', how='leftanti')\
    .select('author_id')\
    .union(mid_author_table.join(new_author_id_counts, how='leftanti', on='author_id').select('author_id'))\
    .dropDuplicates().count()

# COMMAND ----------

(old_author_id_counts.join(new_author_id_counts, on='author_id', how='leftanti')
    .select('author_id')
    .union(mid_author_table.join(new_author_id_counts, how='leftanti', on='author_id').select('author_id'))
    .filter(~F.col('author_id').isin([9999999999, 5317838346]))
    .dropDuplicates()
    .repartition(6)
    .write.format("jdbc") 
    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
    .option("dbtable", 'authorships.authors_to_delete') 
    .option("user", secret['username']) 
    .option("password", secret['password']) 
    .option("driver", "org.postgresql.Driver") 
    .mode("overwrite") 
    .save())

# COMMAND ----------

authors_table_last_date = curr_author_table.select(F.max('modified_date')).collect()[0][0]

authors_table_last_date

# COMMAND ----------

spark.read\
    .parquet(f"{temp_save_path}/raw_data_to_disambiguate/").select(F.min('created_date'), F.max('created_date')) \
    .show(truncate=False)

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
    .withColumn('author', F.trim(F.regexp_replace(F.col('author'), '&NA;', ''))) \
    .withColumn('author', F.trim(F.regexp_replace(F.regexp_replace(F.col('author'), '[0-9]', ' '), ' +', ' '))) \
    .withColumn('author', F.trim(F.regexp_replace(F.regexp_replace(F.col('author'), '[\*$!<>/?~@|+=$#&^%«»:;\_•]', ' '), 
                                                            ' +', ' '))) \
    .withColumn('author', F.trim(F.regexp_replace(F.regexp_replace(F.col('author'), '[\[\]\{\}\(\)\\\]', ''), ' +', ' '))) \
    .withColumn('author', F.regexp_replace(F.col('author'), """^[,."`\-\‐\– ']+""", '')) \
    .filter(~(F.col('author').isin(bad_author_names) |
              (F.lower(F.col('author')).contains("download")) | 
              (F.lower(F.col('author')).contains("d.o.w.n.l.o.a.d")) |
              (F.length(F.col('author'))>120) |
              (F.trim(F.col('author'))==""))) \
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

spark.read.parquet(f"{temp_save_path}/final_data_to_disambiguate/").count()


# COMMAND ----------

final_new_data.sample(0.1).show(20)

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

# ids_to_change = ['2495086661_1', '4242421337_1']

# features = spark.read.parquet(f"{temp_save_path}/current_features_table/") \
#     .filter(F.col('work_author_id_2').isin(ids_to_change)) \
#     .select(F.col('work_author_id_2').alias('work_author_id'), 'author_2') \
#     .withColumn('new_author_id', F.lit(5099111335)) \
#     .withColumn('request_type', F.lit('user')) \
#     .withColumn('request_date', F.current_timestamp())

# print(features.cache().count())

# if features.count() == len(ids_to_change):
#     (features.select('work_author_id','new_author_id','request_type','request_date') \
#        .withColumn("partition_col", (F.rand()*40+1).cast(IntegerType()))
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

# ids_to_change = ['2031278299_5', '4387151906_3', '1976519379_1', '4300191113_3', '2605522527_3', '2797023940_1', '2098556928_2', '2807679418_2', '2792772187_3', '2054484325_3', '2014746815_3', '2028459115_4', '2053156463_7', '1970378065_3', '2094547765_6', '2042307558_3', '2321342090_9', '4384525591_6', '2953338327_4', '2057314539_3', '1984817035_6', '2023349773_6', '2027572529_3', '4387450863_2', '2006958206_2', '2043912917_2', '2564608042_3', '3103684480_3', '1056368457_4', '3041935151_2', '2062505842_2', '4387738205_3', '1975135073_5', '2025913063_7', '2955589198_5', '2002342675_5', '1544258620_3', '2073401403_1', '4238270647_7', '2071095312_3', '2320026348_3', '4297796787_7', '2312654982_3', '1970290378_1', '2036860328_2', '3012414416_8', '1996176542_8', '4242299694_6', '4386504076_3', '2061343856_3', '2078303575_3', '2326414568_3', '2057779250_5', '4220836250_2', '2055235703_9', '2127358644_2', '2014897716_6', '1963840591_6', '2417903834_6', '4310362883_1', '2046902792_4', '1985578967_4', '2045924774_1', '948149728_6', '1974691413_4', '2064631413_2', '2486048711_2', '2781956704_5', '2029402002_6', '4206981297_3', '2062022959_2', '2971637030_10', '2038494101_8', '1555848143_7', '2145071120_6', '1764181257_5', '2095231047_6', '2034588057_3', '1991844428_3', '1971611093_4', '2006774002_3', '4320930981_2', '2779172875_1', '3127631705_5', '2042996519_1', '4382343761_9', '4310363084_6', '3118610170_5', '2023089263_3', '2018898654_1', '1560888424_7', '2036052041_7', '2035318795_3', '2052348303_3', '1548129990_1', '2032746885_7', '3093557211_8', '12141033_5', '2085626713_2', '2144782188_5', '2004700150_2', '1996668500_3', '1977622699_3', '3180969892_1', '2016535359_1', '4327726455_3', '2015560851_4', '1966276616_5', '1975748819_2', '2083555138_2', '2104352698_7', '1988234522_2', '2481880151_3', '2794518685_8', '2043001825_3', '2951716789_2', '2417877924_2', '2064630663_1', '2326636037_1', '3176137101_2', '1917322496_9', '2013302092_10', '1995284717_2', '2142193843_2', '2328525092_3', '2510784946_3', '2020533006_1', '4302773392_6', '2044254998_2', '1973668956_1', '3161505316_5', '2161649556_8', '4256326086_1', '2323250119_1', '2074914143_6', '2045126735_1', '4243475135_5', '2573771477_1', '2082973985_3', '2072442362_3', '2366311632_5', '2135678766_3', '2563456577_1', '2014846150_3']


# features = spark.read.parquet(f"{temp_save_path}/current_features_table/") \
#    .filter(F.col('work_author_id_2').isin(ids_to_change)) \
#    .withColumn('groupby_num', F.lit(101)) \
#    .groupby('groupby_num') \
#    .agg(F.max(F.col('work_author_id_2')).alias('work_author_id_for_cluster'), 
#         F.collect_set(F.col('work_author_id_2')).alias('all_works_to_cluster')) \
#    .select('work_author_id_for_cluster', 
#            F.explode('all_works_to_cluster').alias('all_works_to_cluster')) \
#    .withColumn('request_type', F.lit('user')) \
#    .withColumn('request_date', F.current_timestamp())

# print(features.cache().count())

# (features.select('work_author_id_for_cluster','all_works_to_cluster','request_type','request_date') \
#    .withColumn("partition_col", (F.rand()*40+1).cast(IntegerType()))
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

# if temp_new_data.filter(F.col('created_date')>authors_table_last_date).count() <= 0:
if spark.read.parquet(f"{temp_save_path}/final_data_to_disambiguate/").count() == 0:
    print(f"{datetime.datetime.now().strftime('%H:%M')}: NO NEW DATA")
    pass
else:
    ########################################### INITIAL DATA PREP #####################################################
    # Read change_author_display_name table
    df = (spark.read
        .format("postgresql")
        .option("dbtable", "authorships.change_author_display_name")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("fetchSize", "15")
        .load()
    )

    df.write.mode('overwrite') \
        .parquet(f"{temp_save_path}/current_change_author_display_name_table/")

    frozen_authors = (spark.read
        .format("postgresql")
        .option("dbtable", "authorships.author_freeze")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("partitionColumn", "partition_col")
        .option("lowerBound", 1)
        .option("upperBound", 40)
        .option("numPartitions", 20)
        .load())

    frozen_authors.select('author_id').dropDuplicates().cache().count()

    # Read add_works table
    df = (spark.read
        .format("postgresql")
        .option("dbtable", "authorships.add_works")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("partitionColumn", "partition_col")
        .option("lowerBound", 1)
        .option("upperBound", 40)
        .option("numPartitions", 20)
        .load()
    )

    df.write.mode('overwrite') \
        .parquet(f"{temp_save_path}/current_add_works_table/")


    # Read remove works table
    df = (spark.read
        .format("postgresql")
        .option("dbtable", "authorships.remove_works")
        .option("host", secret['host'])
        .option("port", secret['port'])
        .option("database", secret['dbname'])
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("fetchSize", "15")
        .load()
    )

    df.write.mode('overwrite') \
        .parquet(f"{temp_save_path}/current_remove_works_table/")

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

    # check for frozen author IDs in the merge_from_id column
    merge_authors_df_temp = merge_authors_df.alias('a').join(frozen_authors.select(F.col('author_id').alias('merge_from_id')), how='inner', on='merge_from_id')
    merge_authors_df_leftover = merge_authors_df.alias('b').join(frozen_authors.select(F.col('author_id').alias('merge_from_id')), how='leftanti', on='merge_from_id')

    if merge_authors_df_temp.count() > 0:
        print(f"{datetime.datetime.now().strftime('%H:%M')}: Found frozen author IDs in merge_from_id column. Updating merge_from_id column to be merge_to_id")
        merge_authors_df = merge_authors_df_temp.select(F.col('merge_from_id').alias('new_merge_to_id'), F.col('merge_to_id').alias('new_merge_from_id')) \
            .select(F.col('new_merge_to_id').alias('merge_to_id'), F.col('new_merge_from_id').alias('merge_from_id')) \
            .union(merge_authors_df_leftover.select('merge_from_id', 'merge_to_id'))

    # Filter by request_date greater than 11/5/2024 because old curations may be pointing work to the wrong author ID
    w5 = Window.partitionBy('work_author_id').orderBy(F.col('request_date').desc())
    add_works_df = spark.read.parquet(f"{temp_save_path}/current_add_works_table/") \
        .filter(F.col('request_date') > '2024-11-05') \
        .withColumn('request_rank', F.row_number().over(w5)) \
        .filter(F.col('request_rank') == 1) \
        .select('work_author_id', F.col('new_author_id').alias('author_id')) \
        .join(merge_authors_df.select(F.col('merge_from_id').alias('author_id'), 'merge_to_id'), 
              how='left', on='author_id') \
        .withColumn('final_author_id', F.when(F.col('merge_to_id').isNull(), 
                                              F.col('author_id')).otherwise(F.col('merge_to_id'))) \
        .select('work_author_id', F.col('final_author_id').alias('author_id')) \
        .dropDuplicates(subset=['work_author_id'])

    w6 = Window.partitionBy('author_id').orderBy(F.col('request_date').desc())
    change_author_display_name_df = spark.read.parquet(f"{temp_save_path}/current_change_author_display_name_table/") \
        .withColumn('request_rank', F.row_number().over(w6)) \
        .filter(F.col('request_rank') == 1) \
        .select('author_id', F.col('new_display_name'))
    
    print(f"{datetime.datetime.now().strftime('%H:%M')}: Rows in add_works table: {add_works_df.cache().count()}")

    
    work_to_clusters_removed = spark.read.parquet(f"{temp_save_path}/current_remove_works_table/") \
        .select(F.col('paper_id').cast(LongType()), F.col('author_id').cast(LongType()))

    print(f"{datetime.datetime.now().strftime('%H:%M')}: Rows in remove clusters table: {work_to_clusters_removed.cache().count()}")

    # getting frozen author IDs and the associated work_author_ids (so that they cannot go through AND again)
    locked_work_authors_df = spark.read.parquet(f"{temp_save_path}/current_authors_table/")\
        .select('work_author_id','author_id') \
        .join(frozen_authors.select('author_id'), how='inner', on='author_id') \
        .select('work_author_id').dropDuplicates()

    all_new_data_removed_only = spark.read.parquet(f"{temp_save_path}/final_data_to_disambiguate/") \
        .dropDuplicates(subset=['work_author_id']) \
        .join(work_to_clusters_removed.select('paper_id').dropDuplicates(), how='inner', on='paper_id')

    all_new_data = spark.read.parquet(f"{temp_save_path}/final_data_to_disambiguate/") \
        .dropDuplicates(subset=['work_author_id']) \
        .join(add_works_df, how='leftanti', on='work_author_id') \
        .join(locked_work_authors_df, how='leftanti', on='work_author_id')

    if all_new_data_removed_only.count() > 0:
        all_new_data = all_new_data.alias('and').union(all_new_data_removed_only.select(*all_new_data.columns))
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
    .filter(~F.col('author_id').isin([9999999999, 5317838346]))

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
                F.col('paper_id_2').cast(LongType()),'original_author','concepts_shorter_2','coauthors_shorter_2') \
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

    ##################################### OVERMERGED AUTHORS ###########################################

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
        .option("partitionColumn", "partition_col")
        .option("lowerBound", 1)
        .option("upperBound", 40)
        .option("numPartitions", 20)
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
            .option("partitionColumn", "partition_col")
            .option("lowerBound", 1)
            .option("upperBound", 40)
            .option("numPartitions", 20)
            .load())
    except:
        overmerged_clusters_fixed = spark.sparkContext.emptyRDD()\
            .toDF(schema=StructType([StructField("work_author_id_for_cluster", StringType()),
                                     StructField("all_works_to_cluster", StringType())]))
    
    w_om = Window.partitionBy(['work_author_id_for_cluster','all_works_to_cluster']).orderBy(F.col('request_date').desc())
    overmerged_clusters = overmerged_clusters_raw\
        .withColumn('request_rank', F.row_number().over(w_om)) \
        .filter(F.col('request_rank') == 1) \
        .select('work_author_id_for_cluster','all_works_to_cluster') \
        .join(overmerged_clusters_fixed.select('work_author_id_for_cluster','all_works_to_cluster'), 
              how='leftanti', on=['work_author_id_for_cluster','all_works_to_cluster']) \
        .select(F.col('work_author_id_for_cluster').alias('work_author_id'),'all_works_to_cluster') \
        .groupBy('work_author_id') \
        .agg(F.collect_set(F.col('all_works_to_cluster')).alias('work_author_ids'))

    if overmerged_clusters.count() == 0:
        print(f"{datetime.datetime.now().strftime('%H:%M')}: No overmerges to fix!")
    elif overmerged_clusters.count() > 0:
        print(f"{datetime.datetime.now().strftime('%H:%M')}: Overmerge clusters to fix: {overmerged_clusters.count()}")

        new_max_id = int(temp_authors_table.filter(F.col("author_id")<9999999999).select(F.max(F.col('author_id'))).collect()[0][0])
        
        w2 = Window.orderBy(F.col('work_author_id'))

        # Getting work_author_ids that will be in new cluster
        ids_to_skip = overmerged_clusters.select(F.explode('work_author_ids').alias('work_author_id_2'))

        # Getting the new cluster number
        overmerged_clusters.select('work_author_id','work_author_ids')\
            .withColumn('temp_cluster_num', F.row_number().over(w2)) \
            .withColumn('author_id', F.lit(new_max_id) + F.col('temp_cluster_num')) \
            .select(F.explode('work_author_ids').alias('work_author_id'), 'author_id') \
            .write.mode('overwrite') \
            .parquet(f"{temp_save_path}/new_rows_for_author_table/overmerged_author_clusters/")

        # Filtering out changed work_author_ids from temp_authors_table
        temp_authors_table = temp_authors_table.alias('new_temp_authors_overmerge_clusters').join(ids_to_skip, how='leftanti', on='work_author_id_2')

        # Making all tables current
        new_loc = 'overmerged_author_clusters'

        _ = create_new_author_table(new_loc)
        print(f"{datetime.datetime.now().strftime('%H:%M')}: New authors table created")
        temp_authors_table = spark.read.parquet(f"{temp_save_path}/temp_authors_table/{new_loc}/")

    ########################################## MERGE ORCIDS ##################################################

    # get group of ORCIDs with multiple author_ids
    orcid_multiple = temp_authors_table.alias('orcid_multiple').filter(F.col('orcid_2')!='').dropDuplicates(subset=['author_id','orcid_2']) \
        .groupBy('orcid_2').count().filter(F.col('count')>1).select('orcid_2')

    orcid_multiple.cache().count()

    if orcid_multiple.count() > 0:
        print(f"{datetime.datetime.now().strftime('%H:%M')}: {orcid_multiple.count()} ORCIDs found with multiple profiles")

        # sort by works count (desc) and then by author ID to get final author ID for that ORCID
        w_mul_orc = Window.partitionBy('orcid_2').orderBy(F.col('id_to_use').desc(), F.col('count').desc(), F.col('author_id'))
        
        author_ids_to_use = temp_authors_table.alias('orcid_to_use').join(orcid_multiple.select('orcid_2'), on='orcid_2', how='inner')\
            .groupBy(['orcid_2','author_id']).count() \
            .join(frozen_authors.alias('frozen_orcid').withColumn('id_to_use', F.lit(1)), how='left', on='author_id') \
            .fillna(0, subset=['id_to_use']) \
            .withColumn('rank', F.row_number().over(w_mul_orc)) \
            .filter(F.col('rank') == 1)

        ############ OLD CODE: used for smartly joining together clusters with conflicting names ###############

        # for each ORCID, get all work_author_ids, groupby ones that have ORCID attached and for the remainder, run a modified leftovers function 
        # where it tries to match on name only
        # temp_authors_table_orcid = temp_authors_table.alias('new_orcid_clusters').join(author_ids_to_use.select('orcid_2',F.col('author_id').alias('new_author_id')), 
        #                                                                                 on='orcid_2', how='inner') \
        #     .withColumn('final_author_id', F.when(F.col('new_author_id').isNotNull(), F.col('new_author_id')).otherwise(F.col('author_id'))) \
        #     .select('work_author_id_2', 'final_author_id')

        # rows_to_remove = temp_authors_table_orcid.join(temp_features_table, how='inner', on='work_author_id_2') \
        #     .select(F.col('final_author_id').alias('author_id'), F.array([F.col('work_author_id_2'), F.col('orcid_2'), F.col('author_2')]).alias('works')) \
        #     .groupBy('author_id').agg(F.collect_list(F.col('works')).alias('works_to_check')) \
        #     .withColumn('work_author_to_group_mapping', go_through_names_to_take_out_mismatches(F.col('works_to_check'))) \
        #     .select('author_id',F.explode('work_author_to_group_mapping').alias('work_author_to_group_mapping')) \
        #     .select('author_id', 
        #             F.col('work_author_to_group_mapping').getItem(0).getItem(0).alias('group_cluster_id'), 
        #             F.col('work_author_to_group_mapping').getItem(1).alias('work_author_id_left')) \
        #     .select('author_id', 'group_cluster_id', F.col('work_author_id_left').alias('work_author_ids')) \
        #     .filter(F.col('group_cluster_id').isNotNull())

        # rows_to_remove.cache().count()

        # writing out rows for testing
        # rows_to_remove \
        #     .write.mode('overwrite') \
        #     .parquet(f"{temp_save_path}/testing_new_code/rows_to_remove_from_cluster_for_name_mismatch/")

        # write mid.author changes
        temp_authors_table.join(author_ids_to_use.select('orcid_2',F.col('author_id').alias('new_author_id')), how='inner', on='orcid_2') \
            .dropDuplicates(subset=['author_id','new_author_id']) \
            .filter(F.col('author_id') != F.col('new_author_id')) \
            .select('author_id', 'new_author_id') \
            .write.mode('overwrite')\
            .parquet(f"{prod_save_path}/author_merges_to_combine_orcid/")

        
        ############ OLD CODE: used for smartly joining together clusters with conflicting names ###############

        # new_max_id = int(temp_authors_table.select(F.max(F.col('author_id'))).collect()[0][0])
            
        # w2 = Window.orderBy(F.col('group_cluster_id'))

        # # Getting work_author_ids that will be in new cluster
        # ids_to_skip = rows_to_remove.select(F.explode('work_author_ids').alias('work_author_id_2'))

        # # Getting the new cluster number
        # rows_to_remove.select('group_cluster_id','work_author_ids')\
        #     .withColumn('temp_cluster_num', F.row_number().over(w2)) \
        #     .withColumn('author_id', F.lit(new_max_id) + F.col('temp_cluster_num')) \
        #     .select(F.explode('work_author_ids').alias('work_author_id'), 'author_id') \
        #     .write.mode('overwrite') \
        #     .parquet(f"{temp_save_path}/new_rows_for_author_table/leftovers_from_merging_orcid_author_clusters/")

        # # Filtering out changed work_author_ids from temp_authors_table
        # temp_authors_table = temp_authors_table.alias('new_temp_authors_overmerge_clusters').join(ids_to_skip, how='leftanti', on='work_author_id_2') \
        #     .join(author_ids_to_use.select('orcid_2',F.col('author_id').alias('new_author_id')), on='orcid_2', how='left') \
        #     .withColumn('final_author_id', F.when(F.col('new_author_id').isNotNull(), F.col('new_author_id')).otherwise(F.col('author_id'))) \
        #     .select('work_author_id_2', F.col('final_author_id').alias('author_id'))

        # Filtering out changed work_author_ids from temp_authors_table
        temp_authors_table = temp_authors_table.alias('new_temp_authors_overmerge_clusters') \
            .join(author_ids_to_use.select('orcid_2',F.col('author_id').alias('new_author_id')), on='orcid_2', how='left') \
            .withColumn('final_author_id', F.when(F.col('new_author_id').isNotNull(), F.col('new_author_id')).otherwise(F.col('author_id'))) \
            .select('work_author_id_2', 
                F.col('final_author_id').alias('author_id'),
                'orcid_2', 
                'display_name',
                'alternate_names',
                'author_alternate_names',
                'name_match_list')

        # # Making all tables current
        # new_loc = 'leftovers_from_merging_orcid_author_clusters'

        # _ = create_new_author_table(new_loc)
        # print(f"{datetime.datetime.now().strftime('%H:%M')}: New authors table created")
        # temp_authors_table = spark.read.parquet(f"{temp_save_path}/temp_authors_table/{new_loc}/")

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
            
    ####################################### NAME MATCH ROUND 1 ################################################
            
    round_2_new_data = spark.read.parquet(f"{temp_save_path}/round_2_of_clustering/")

    names_match = round_2_new_data \
        .withColumn('paper_id', F.split(F.col('work_author_id'), "_").getItem(0).cast(LongType())) \
        .join(temp_authors_table.select('work_author_id_2', 
                                        'orcid_2', 
                                        'author_id',
                                        F.explode(F.col('alternate_names')).alias('author')),
            how='inner', on='author') \
        .join(work_to_clusters_removed, how='leftanti', on=['paper_id','author_id']) \
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
        .join(work_to_clusters_removed, how='leftanti', on=['paper_id','author_id']) \
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
        .join(work_to_clusters_removed, how='leftanti', on=['paper_id','author_id']) \
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
    max_id = int(temp_authors_table.filter(F.col("author_id")<9999999999).select(F.max(F.col('author_id'))).collect()[0][0])

    # Create new clusters
    w1 = Window.orderBy(F.col('work_author_id'))

    spark.read.parquet(f"{temp_save_path}/end_of_clustering_leftovers/") \
        .select('work_author_id').distinct() \
        .withColumn('temp_cluster_num', F.row_number().over(w1)) \
        .withColumn('author_id', F.lit(max_id) + F.col('temp_cluster_num')) \
        .select('work_author_id','author_id') \
        .write.mode('overwrite') \
        .parquet(f"{temp_save_path}/new_rows_for_author_table/new_author_clusters/")


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

    # Comparing new cluster IDs vs old in order to see if there are clusters that have the same work_ids but only changed cluster ID
    # (for these cases, we want to use the old cluster ID)
    spark.read.parquet(f"{temp_save_path}/new_rows_for_author_table/new_author_clusters/") \
        .select('work_author_id', 'author_id') \
        .groupBy('author_id').agg(F.count(F.col('work_author_id')).alias('count'), 
                                    F.collect_set(F.col('work_author_id')).alias('new_work_author_ids'))\
        .filter(F.col('count')<=5).select(F.col('author_id').alias('new_author_id'),'new_work_author_ids', 
                                            F.explode('new_work_author_ids').alias('work_author_id_2')) \
        .write.mode('overwrite')\
        .parquet(f"{temp_save_path}/testing_new_clusters_for_old_id/")

    new_clusters = spark.read.parquet(f"{temp_save_path}/testing_new_clusters_for_old_id/")
    new_clusters.cache().count()

    spark.read.parquet(f"{temp_save_path}/new_rows_for_author_table/new_author_clusters/") \
        .select(F.col('work_author_id').alias('work_author_id_2'), 'author_id') \
        .join(new_clusters.select(F.col('new_author_id').alias('author_id')), how='inner', on='author_id') \
        .select('work_author_id_2') \
        .write.mode('overwrite')\
        .parquet(f"{temp_save_path}/testing_new_clusters_work_authors_to_check/")

    new_clusters_to_check = spark.read.parquet(f"{temp_save_path}/testing_new_clusters_work_authors_to_check/")
    new_clusters_to_check.cache().count()

    old_cluster_to_check = spark.read.parquet(f"{temp_save_path}/current_authors_table/") \
        .select('work_author_id', 'author_id') \
        .join(new_clusters_to_check.select(F.col('work_author_id_2').alias('work_author_id')), how='inner', on='work_author_id') \
        .select(F.col('work_author_id').alias('work_author_id_to_check'), 'author_id')
    old_cluster_to_check.cache().count()

    spark.read.parquet(f"{temp_save_path}/current_authors_table/") \
        .select('work_author_id', 'author_id') \
        .join(old_cluster_to_check, how='inner', on='author_id') \
        .groupby(['author_id','work_author_id_to_check']).agg(F.count(F.col('work_author_id')).alias('count'), 
                                    F.collect_set(F.col('work_author_id')).alias('old_work_author_ids'))\
        .filter(F.col('count')<=5)\
        .select(F.col('author_id').alias('old_author_id'),'old_work_author_ids', 
                F.col('work_author_id_to_check').alias('work_author_id_2')) \
        .write.mode('overwrite')\
        .parquet(f"{temp_save_path}/testing_old_clusters_for_new_id/")

    old_clusters = spark.read.parquet(f"{temp_save_path}/testing_old_clusters_for_new_id/")
    old_clusters.cache().count()

    new_clusters_to_check.select('work_author_id_2') \
        .join(old_clusters, how='inner', on='work_author_id_2') \
        .join(new_clusters, how='inner', on='work_author_id_2') \
        .withColumn('final_author_id', check_new_clusters_for_old_id(F.col('old_author_id'), F.col('new_author_id'), 
                                                                        F.col('old_work_author_ids'), F.col('new_work_author_ids'))) \
        .select('work_author_id_2','new_author_id', 'final_author_id') \
        .write.mode('overwrite')\
        .parquet(f"{temp_save_path}/final_author_ids_for_new_clusters/")

    # Loading initial and final null author tables to compare against
    new_null_author_data = spark.read.parquet(f"{temp_save_path}/new_data_to_be_given_null_value/")
    old_null_author_data = spark.read.parquet(f"{temp_save_path}/current_null_authors_table/")
    final_author_ids_new_clusters = spark.read.parquet(f"{temp_save_path}/final_author_ids_for_new_clusters/")

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
                'modified_date') \
        .join(final_new_data.select(F.col('work_author_id')), how='leftanti', on='work_author_id')

    # getting all null author data (except data that was just disambiguated)
    null_authors_not_disambiguated = spark.read.parquet(f"{temp_save_path}/current_authors_table/") \
        .filter(F.col('author_id').isin([9999999999, 5317838346])) \
        .join(all_new_data.select('work_author_id'), how='leftanti', on='work_author_id') \
        .select('work_author_id', F.col('author_id').alias('author_id_2'), F.col('display_name').alias('display_name_2'), 
                F.col('alternate_names').alias('alternate_names_2'), F.col('orcid').alias('orcid_2'))

    final_author_table = temp_authors_table \
            .select(F.col('work_author_id_2').alias('work_author_id'), 
                F.col('author_id').alias('author_id_2'),
                F.col('display_name').alias('display_name_2'),
                F.col('author_alternate_names').alias('alternate_names_2'), 
                'orcid_2') \
            .union(null_authors_not_disambiguated) \
            .join(final_author_ids_new_clusters.select(F.col('work_author_id_2').alias('work_author_id'), 'final_author_id'), 
                  how='left', on='work_author_id') \
            .withColumn('final_author_id', F.when(F.col('final_author_id').isNotNull(), F.col('final_author_id')).otherwise(F.col('author_id_2'))) \
            .join(add_orcid_df.select('work_author_id', 'new_orcid'), 
                  how='left', on='work_author_id') \
            .withColumn('final_orcid_id', F.when(F.col('new_orcid').isNull(), 
                                                 F.col('orcid_2')).otherwise(F.col('new_orcid'))) \
            .select('work_author_id', 
                    F.col('final_author_id').alias('author_id_2'),
                    'display_name_2',
                    'alternate_names_2', 
                    F.col('final_orcid_id').alias('orcid_2')) \
            .join(change_author_display_name_df.select(F.col('author_id').alias('author_id_2'), 'new_display_name'), 
                how='left', on='author_id_2') \
            .withColumn('final_display_name', F.when(F.col('new_display_name').isNull(), F.col('display_name_2')).otherwise(F.col('new_display_name'))) \
            .select('work_author_id', 
                    F.col('author_id_2'),
                    F.col('final_display_name').alias('display_name_2'),
                    'alternate_names_2', 
                    F.col('orcid_2'))
            
    print(f"{datetime.datetime.now().strftime('%H:%M')}: INITIAL TABLE: {init_author_table.count()}")
    print(f"{datetime.datetime.now().strftime('%H:%M')}: FINAL TABLE: {final_author_table.count()}")

    name_of_stats_to_track.append('init_author_table_count')
    stats_to_track.append(init_author_table.count())

    name_of_stats_to_track.append('final_author_table_count')
    stats_to_track.append(final_author_table.count())

    # take final author table, compare to init table (all columns) to see if anything has been changed
    compare_tables = final_author_table.join(init_author_table, how='inner', on='work_author_id') \
        .withColumn('orcid_compare', F.when(F.col('orcid_1')==F.col('orcid_2'), 0).otherwise(1)) \
        .withColumn('display_name_compare', check_display_name_change('display_name_1', 'display_name_2')) \
        .withColumn('author_id_compare', F.when(F.col('author_id_1')==F.col('author_id_2'), 0).otherwise(1)) \
        .withColumn('alternate_names_compare', check_list_vs_list(F.col('alternate_names_1'), 
                                                                    F.col('alternate_names_2'))) \
        .withColumn('total_changes', F.col('orcid_compare') + F.col('display_name_compare') + 
                                F.col('author_id_compare') + F.col('alternate_names_compare'))
        
    compare_tables.cache().count()

    compare_tables \
        .write.mode('overwrite')\
        .parquet(f"{temp_save_path}/final_author_table_for_testing/")
    
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

        print(f"{datetime.datetime.now().strftime('%H:%M')}: Author cluster changes: ", spark.read.parquet(f"{temp_save_path}/work_authors_changed_clusters/").count())
    
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
    
    (final_author_table.join(init_author_table.select('work_author_id'), how='leftanti', on='work_author_id') 
        .select('work_author_id', F.col('author_id_2').alias('author_id'), 
                F.col('display_name_2').alias('display_name'), 
                F.col('alternate_names_2').alias('alternate_names'), 
                F.col('orcid_2').alias('orcid'))
        .withColumn("created_date", F.current_timestamp()) 
        .withColumn("modified_date", F.current_timestamp())
        .withColumn('author_id_changed', F.lit(True))
        .write.mode('overwrite').parquet(f"{prod_save_path}/current_authors_modified_table"))
    
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
    
    (compare_tables.filter(F.col('author_id_compare')==1).filter(F.col('total_changes')>0)
            .select('work_author_id', F.col('author_id_2').alias('author_id'), 
                    F.col('display_name_2').alias('display_name'), 
                    F.col('alternate_names_2').alias('alternate_names'), 
                    F.col('orcid_2').alias('orcid'), 'created_date') 
        .withColumn("modified_date", F.current_timestamp())
        .withColumn('author_id_changed', F.lit(True))
        .write.mode('append').parquet(f"{prod_save_path}/current_authors_modified_table"))
    
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
        
        (null_authors_diff
            .select(F.col('work_author_id_2').alias('work_author_id'), 'author_id', 
                        'display_name', 'alternate_names', F.col('orcid_2').alias('orcid'), 
                        'created_date','modified_date','author_id_changed')
            .write.mode('append').parquet(f"{prod_save_path}/current_authors_modified_table"))
        
    print(f"{datetime.datetime.now().strftime('%H:%M')}: authorships.authors_modified write 3 done")

    # Getting any row changes to author ID metadata (but not author ID) and writing to authorships.authors_modified
    (compare_tables.filter(F.col('author_id_compare')==0).filter(F.col('total_changes')>0).filter((F.col('orcid_compare')==1) | 
                                                                                                  (F.col('display_name_compare')==1))
            .select('work_author_id', F.col('author_id_2').alias('author_id'), 
                    F.col('display_name_2').alias('display_name'), 
                    F.col('alternate_names_2').alias('alternate_names'), 
                    F.col('orcid_2').alias('orcid'), 'created_date') 
        .withColumn("modified_date", F.current_timestamp()) 
        .withColumn('author_id_changed', F.lit(True))
        .repartition(36)
        .write.format("jdbc")
        .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
        .option("dbtable", 'authorships.authors_modified')
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())
    
    (compare_tables.filter(F.col('author_id_compare')==0).filter(F.col('total_changes')>0).filter((F.col('orcid_compare')==1) | 
                                                                                                 (F.col('display_name_compare')==1))
            .select('work_author_id', F.col('author_id_2').alias('author_id'), 
                    F.col('display_name_2').alias('display_name'), 
                    F.col('alternate_names_2').alias('alternate_names'), 
                    F.col('orcid_2').alias('orcid'), 'created_date') 
        .withColumn("modified_date", F.current_timestamp()) 
        .withColumn('author_id_changed', F.lit(True))
        .write.mode('append').parquet(f"{prod_save_path}/current_authors_modified_table"))
    
    (compare_tables.filter(F.col('author_id_compare')==0).filter(F.col('total_changes')>0).filter((F.col('orcid_compare')==0) & 
                                                                                                  (F.col('display_name_compare')==0))
            .select('work_author_id', F.col('author_id_2').alias('author_id'), 
                    F.col('display_name_2').alias('display_name'), 
                    F.col('alternate_names_2').alias('alternate_names'), 
                    F.col('orcid_2').alias('orcid'), 'created_date') 
        .withColumn("modified_date", F.current_timestamp()) 
        .withColumn('author_id_changed', F.lit(False))
        .repartition(36)
        .write.format("jdbc")
        .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
        .option("dbtable", 'authorships.authors_modified')
        .option("user", secret['username'])
        .option("password", secret['password'])
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())
    
    (compare_tables.filter(F.col('author_id_compare')==0).filter(F.col('total_changes')>0).filter((F.col('orcid_compare')==0) & 
                                                                                                  (F.col('display_name_compare')==0))
            .select('work_author_id', F.col('author_id_2').alias('author_id'), 
                    F.col('display_name_2').alias('display_name'), 
                    F.col('alternate_names_2').alias('alternate_names'), 
                    F.col('orcid_2').alias('orcid'), 'created_date') 
        .withColumn("modified_date", F.current_timestamp()) 
        .withColumn('author_id_changed', F.lit(False))
        .write.mode('append').parquet(f"{prod_save_path}/current_authors_modified_table"))
    
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

        (spark.read.parquet(f"{temp_save_path}/final_author_table_part/no_changes/")\
                .join(prev_disam_authors, how='inner', on='work_author_id').dropDuplicates(subset=['work_author_id'])
                .select('work_author_id', F.col('author_id'), 
                        F.col('display_name'), 
                        F.col('alternate_names'), 
                        F.col('orcid'), 'created_date') 
            .withColumn("modified_date", F.current_timestamp()) 
            .withColumn('author_id_changed', F.lit(True))
            .write.mode('append').parquet(f"{prod_save_path}/current_authors_modified_table"))

    if overmerged_clusters.count() == 0:
        print(f"{datetime.datetime.now().strftime('%H:%M')}: Don't need to write to overmerge_fixed table!")
    elif overmerged_clusters.count() > 0:
        # Writing out changed data to authorships.overmerged_authors_fixed
        print(f"{datetime.datetime.now().strftime('%H:%M')}: Writing fixed overmerges to table")
        (overmerged_clusters.select(F.col('work_author_id').alias('work_author_id_for_cluster'),'work_author_ids')
            .select('work_author_id_for_cluster', F.explode('work_author_ids').alias('all_works_to_cluster'))
            .withColumn("modified_date", F.current_timestamp())
            .withColumn("partition_col", (F.rand()*40+1).cast(IntegerType()))
            .repartition(6)
            .write.format("jdbc") 
            .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
            .option("dbtable", 'authorships.overmerged_authors_fixed') 
            .option("user", secret['username']) 
            .option("password", secret['password']) 
            .option("driver", "org.postgresql.Driver") 
            .mode("append") 
            .save())

    # Writing out final table to show where ORCID came from for each work author
    (spark.read.parquet(f"{temp_save_path}/final_author_table_part/*").select('work_author_id', 'orcid').filter(F.col('orcid')!='')
        .join(combined_source_orcid.select(F.col('work_author_id_2').alias('work_author_id'), 'orcid_source'), 
              on='work_author_id', how='left')
        .select(F.split(F.col('work_author_id'), '_').alias('work_author_id'), 'orcid','orcid_source')
        .select(F.col('work_author_id').getItem(0).cast(LongType()).alias('paper_id'), 
                F.col('work_author_id').getItem(1).cast(IntegerType()).alias('author_sequence_number'), 'orcid', 'orcid_source')
        .withColumn('evidence', F.when(F.col('orcid_source').isNotNull(), F.col('orcid_source')).otherwise(F.lit('author_disambiguation')))
        .select('paper_id','author_sequence_number', F.col('orcid'), 'evidence')
        .repartition(20)
        .write.format("jdbc") 
        .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
        .option("dbtable", 'orcid.final_orcid') 
        .option("user", secret['username']) 
        .option("password", secret['password']) 
        .option("driver", "org.postgresql.Driver") 
        .option("truncate", True)
        .mode("overwrite") 
        .save())
    
    (spark.read.parquet(f"{temp_save_path}/final_author_table_part/*").select('work_author_id', 'orcid').filter(F.col('orcid')!='')
        .join(combined_source_orcid.select(F.col('work_author_id_2').alias('work_author_id'), 'orcid_source'), 
              on='work_author_id', how='left')
        .select(F.split(F.col('work_author_id'), '_').alias('work_author_id'), 'orcid','orcid_source')
        .select(F.col('work_author_id').getItem(0).cast(LongType()).alias('paper_id'), 
                F.col('work_author_id').getItem(1).cast(IntegerType()).alias('author_sequence_number'), 'orcid', 'orcid_source')
        .withColumn('evidence', F.when(F.col('orcid_source').isNotNull(), F.col('orcid_source')).otherwise(F.lit('author_disambiguation')))
        .select('paper_id','author_sequence_number', F.col('orcid'), 'evidence')
        .write.mode('overwrite')
        .parquet(f"{database_copy_save_path}/orcid/final_orcid"))
    
    # Future code to write out author IDs and associated ORCID
    # (spark.read.parquet(f"{temp_save_path}/final_author_table_part/*")
    # .select('author_id', 'orcid').filter(F.col('orcid')!='').dropDuplicates()
    # .groupBy(['orcid']).agg(F.count(F.col('author_id')).alias('count'), 
    #                                 F.collect_list(F.col('author_id')).alias('author_ids')).filter(F.col('count')==1)
    # .select(F.col('author_ids').getItem(0).alias('author_id'), 'orcid')
    # .withColumn('evidence', F.lit('author_disambiguation'))
    # .withColumn('updated', F.current_timestamp())
    # .select('author_id', 'orcid', 'updated', 'evidence')
    # .repartition(20)
    # .write.format("jdbc") 
    # .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
    # .option("dbtable", 'mid.author_orcid') 
    # .option("user", secret['username']) 
    # .option("password", secret['password']) 
    # .option("driver", "org.postgresql.Driver") 
    # .option("truncate", True)
    # .mode("overwrite") 
    # .save())

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

end_datetime = datetime.datetime.now()
time_delta_minutes = round((end_datetime - start_datetime).seconds/60, 3)
name_of_stats_to_track.append('run_time_minutes')
stats_to_track.append(time_delta_minutes)

# COMMAND ----------

for i,j in zip(name_of_stats_to_track,stats_to_track):
    print(f"{curr_date} -- {i} -- {j}")

# COMMAND ----------


