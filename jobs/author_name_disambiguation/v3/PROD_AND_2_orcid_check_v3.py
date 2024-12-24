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

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, FloatType, ArrayType, DoubleType, StructType, StructField, LongType

# COMMAND ----------

# MAGIC %md #### Load secrets

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
# curr_date = '2024_07_11_17_02'
prod_save_path = f"{buckets['and_save_path']}/V3/PROD"
temp_save_path = f"{buckets['temp_save_path']}/{curr_date}"
orcid_save_path = f"{buckets['orcid_save_path']}"
database_copy_save_path = f"{buckets['database_copy_save_path']}"
name_of_stats_to_track = []
stats_to_track = []
print(curr_date)

# COMMAND ----------

# MAGIC %md #### Functions

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

@udf(returnType=ArrayType(StringType()))
def transform_list_col_for_nulls_string(col_with_nulls):
    if isinstance(col_with_nulls, list):
        return col_with_nulls[:250]
    else:
        return []
    
@udf(returnType=ArrayType(StringType()))
def remove_current_author(author, coauthors):
    return [x for x in coauthors if x!=author][:250]

@udf(returnType=ArrayType(StringType()))
def get_non_null_orcid(orcids):
    return [x for x in orcids if (isinstance(x, str) & (x != ''))]

@udf(returnType=ArrayType(ArrayType(StringType())))
def go_through_rows_for_author(data_to_cluster):
    """
    Goes through each row of data to determine which ORCID it matches to
    """
    df_to_cluster = pd.DataFrame(data_to_cluster, columns=['orcid_left','trans_name_left','work_author_id_right','trans_name_right','total_score'])
    df_to_cluster['orcid_left'] = df_to_cluster['orcid_left'].apply(lambda x: x[0])
    df_to_cluster['work_author_id_right'] = df_to_cluster['work_author_id_right'].apply(lambda x: x[0])
    df_to_cluster['trans_name_right'] = df_to_cluster['trans_name_right'].apply(lambda x: x[0])
    df_to_cluster['total_score'] = df_to_cluster['total_score'].apply(lambda x: x[0]).astype('float')

    orcid_clusters = df_to_cluster[['orcid_left','trans_name_left']].drop_duplicates(subset=['orcid_left']).copy()

    orcid_cluster_dict = {}
    for index, row in orcid_clusters.iterrows():
        orcid_cluster_dict[row['orcid_left']] = {'trans_name_left': row['trans_name_left'], 
                                                 'name_match_list_left': get_name_match_from_search_names(row['trans_name_left'])}

    orcid_list = []
    work_authors_mapped = []

    for index, row in df_to_cluster.sort_values('total_score', ascending=False).iterrows():
        if row.work_author_id_right not in work_authors_mapped:
            if row.trans_name_right in orcid_cluster_dict[row.orcid_left]['trans_name_left']:
                orcid_list.append([row.orcid_left, row.work_author_id_right])
                work_authors_mapped.append(row.work_author_id_right)
            elif check_block_vs_block_reg(orcid_cluster_dict[row.orcid_left]['name_match_list_left'], get_name_match_list_reg(row.trans_name_right))==1:
                orcid_list.append([row.orcid_left, row.work_author_id_right])
                work_authors_mapped.append(row.work_author_id_right)
                new_names = orcid_cluster_dict[row.orcid_left]['trans_name_left'] + [row.trans_name_right]
                orcid_cluster_dict[row['orcid_left']] = {'trans_name_left': new_names, 
                                                 'name_match_list_left': get_name_match_from_search_names(new_names)}
            else:
                pass

    return orcid_list

@udf(returnType=ArrayType(ArrayType(ArrayType(StringType()))))
def go_through_leftovers_for_author(data_to_cluster):
    """
    Goes through each row of data to determine the final groups from leftover data
    """
    df_to_cluster = pd.DataFrame(data_to_cluster, columns=['trans_name_right','work_author_id_right'])
    df_to_cluster['trans_name_right'] = df_to_cluster['trans_name_right'].apply(lambda x: x[0])

    group_cluster_dict = {}

    for index, row in df_to_cluster.iterrows():
        added_to_group = 0
        if not group_cluster_dict:
            group_cluster_dict[row.work_author_id_right[0]] = {'trans_name_right': [row.trans_name_right], 
                                                               'name_match_list': get_name_match_from_search_names(row.trans_name_right), 
                                                               'work_author_ids': row.work_author_id_right}
        else:
            name_list_right = get_name_match_list_reg(row.trans_name_right)
            for key in group_cluster_dict.keys():
                if check_block_vs_block_reg(group_cluster_dict[key]['name_match_list'], name_list_right)==1:
                    group_cluster_dict[key]['trans_name_right'].append(row.trans_name_right)
                    group_cluster_dict[key]['name_match_list'] = get_name_match_from_search_names(group_cluster_dict[key]['trans_name_right'])
                    group_cluster_dict[key]['work_author_ids'] += row.work_author_id_right
                    added_to_group = 1
                    break
                else:
                    pass
            # if it didn't match, add to dict
            if added_to_group==0:
                group_cluster_dict[row.work_author_id_right[0]] = {'trans_name_right': [row.trans_name_right],
                                                                   'name_match_list': get_name_match_from_search_names([row.trans_name_right]),
                                                                   'work_author_ids': row.work_author_id_right}
                
    return [[[key], group_cluster_dict[key]['work_author_ids']] for key in group_cluster_dict.keys()]

# COMMAND ----------

@udf(returnType=StringType())
def process_orcid(orcid, orcid_len):
    if orcid == "":
        return ""
    else:
        if orcid_len == 19:
            return orcid
        elif orcid_len == 37:
            return orcid[-19:]
        else:
            return ""

# COMMAND ----------

def replace_symbols(name):
    name = name.replace(" ", " ").replace(".", " ").replace(",", " ").replace("|", " ").replace(")", "").replace("(", "")\
            .replace("-", " ").replace("&", "").replace("$", "").replace("#", "").replace("@", "").replace("%", "").replace("0", "") \
            .replace("1", "").replace("2", "").replace("3", "").replace("4", "").replace("5", "").replace("6", "").replace("7", "") \
            .replace("8", "").replace("9", "").replace("*", "").replace("^", "").replace("{", "").replace("}", "").replace("+", "") \
            .replace("=", "").replace("_", "").replace("~", "").replace("`", "").replace("[", "").replace("]", "").replace("\\", "") \
            .replace("<", "").replace(">", "").replace("?", "").replace("/", "").replace(";", "").replace(":", "").replace("\'", "") \
            .replace("\"", "").replace("‐", " ")
    return name


@udf(returnType=ArrayType(StringType()))
def transform_name_for_orcid_match_list(raw_string):
    if isinstance(raw_string, str):
        name = raw_string.title()
        if isinstance(name, str):
            name = unidecode(unicodedata.normalize('NFKC', name))
            name = replace_symbols(name)
            name = name.split()
        else:
            name = []
    else:
        name = []
    return name

@udf(returnType=StringType())
def transform_name_for_orcid_match_string(raw_string):
    if isinstance(raw_string, str):
        name = raw_string.title()
        if isinstance(name, str):
            name = unidecode(unicodedata.normalize('NFKC', name))
            name = replace_symbols(name)
            name = " ".join(name.split())
        else:
            name = ""
    else:
        name = ""
    return name

# COMMAND ----------

@udf(returnType=ArrayType(FloatType()))
def score_orcid_names_to_original_author(raw_string, given_names, family_name):

    # Making sure that if a name has 2 of the same initials, it can still match to each one (instead of 
    # just looking at unique names/initials)
    dict_key_num = 0
    family_name_dict = {}
    given_names_dict = {}
    full_names_dict = {}
    for one_name in given_names:
        given_names_dict[dict_key_num] = one_name
        full_names_dict[dict_key_num] = one_name
        dict_key_num += 1

    for one_name in family_name:
        family_name_dict[dict_key_num] = one_name
        full_names_dict[dict_key_num] = one_name
        dict_key_num += 1

    family_points = 0.0
    given_points = 0.0
    initial_points = 0.0

    # Finding family name matches (full name)
    if len(family_name) > 0:
        fam_names_found = sum([1 for single_name in family_name if single_name in raw_string])
        if fam_names_found == 0:
            return [0.0, 0.0, 0.0]
        else:
            for one_name_item in family_name_dict.items():
                if len(one_name_item[1]) > 1:
                    res = re.search(rf'\b{one_name_item[1]}\b', raw_string)
                    if res:
                        family_points += 1.0
                        beg = res.span()[0]
                        end = res.span()[1]
                        full_names_dict.pop(one_name_item[0])
                        raw_string = (raw_string[0:beg] + raw_string[end:]).strip()
                    else:
                        if one_name_item[1] in raw_string:
                            family_points += 0.75
                            beg = raw_string.find(one_name_item[1])
                            end = beg + len(one_name_item[1])
                            full_names_dict.pop(one_name_item[0])
                            raw_string = (raw_string[0:beg] + raw_string[end:]).strip()
                        else:
                            pass

    # Finding given name matches (full name)
    if len(given_names) > 0:
        for one_name_item in given_names_dict.items():
            if len(one_name_item[1]) > 1:
                res = re.search(rf'\b{one_name_item[1]}\b', raw_string)
                if res:
                    given_points += 1.0
                    beg = res.span()[0]
                    end = res.span()[1]
                    full_names_dict.pop(one_name_item[0])
                    raw_string = (raw_string[0:beg] + raw_string[end:]).strip()
                else:
                    if one_name_item[1] in raw_string:
                        given_points += 0.75
                        beg = raw_string.find(one_name_item[1])
                        end = beg + len(one_name_item[1])
                        full_names_dict.pop(one_name_item[0])
                        raw_string = (raw_string[0:beg] + raw_string[end:]).strip()
                    else:
                        pass

    # Finding leftover initial matches
    final_initial_dict = {i:j[0] for i,j in full_names_dict.items()}
    for one_name_item in final_initial_dict.items():
        res = re.search(rf'\b{one_name_item[1]}\b', raw_string)
        if res:
            initial_points += 1.0
            beg = res.span()[0]
            end = res.span()[1]
            raw_string = (raw_string[0:beg] + raw_string[end:]).strip()
                    
    return [family_points, given_points, initial_points]

# COMMAND ----------

# MAGIC %md ### Get final ORCID for each work

# COMMAND ----------

source_orcid = spark.read.parquet(f"{database_copy_save_path}/orcid/openalex_authorships") \
    .select(F.col('paper_id').alias('paper_id_2'), F.concat_ws("_", F.col('paper_id'), F.col('author_sequence_number')).alias('work_author_id_2'), 
            F.col('orcid').alias('source_orcid'))
source_orcid.cache().count()

# COMMAND ----------

aff_orcid = spark.read.parquet(f"{database_copy_save_path}/mid/affiliation")\
    .select(F.col('paper_id').alias('paper_id_2'), F.concat_ws("_", F.col('paper_id'), F.col('author_sequence_number')).alias('work_author_id_2'), 
            'original_orcid','original_author') \
    .dropDuplicates(subset=['work_author_id_2']) \
    .select('work_author_id_2', 'original_orcid','original_author')
aff_orcid.cache().count()

# COMMAND ----------

w1 = Window.partitionBy('work_author_id').orderBy(F.col('request_date').desc())
add_orcid = spark.read.parquet(f"{database_copy_save_path}/orcid/add_orcid") \
    .withColumn('rank', F.row_number().over(w1)) \
    .filter(F.col('rank')==1) \
    .select(F.col('work_author_id').alias('work_author_id_2'), F.col('new_orcid'))

# COMMAND ----------

orcid_names = spark.read.parquet(f"{orcid_save_path}/temp_working_files/raw_orcid_base")\
    .filter(F.col('given_names').isNotNull() | F.col('family_name').isNotNull()) \
    .filter((F.col('given_names')!="") | (F.col('family_name')!=""))
orcid_names.cache().count()

# COMMAND ----------

w_orcid_new = Window.partitionBy(['paper_id_2','final_orcid']).orderBy(F.col('family_name_match_score').desc(), F.col('given_names_match_score').desc(), F.col('initials_match_score').desc())

# COMMAND ----------

aff_orcid.filter(F.col('author_id').isNotNull())\
    .fillna("", subset=['original_orcid']) \
    .withColumn('original_orcid', F.trim(F.col('original_orcid'))) \
    .withColumn("orcid_len", F.length('original_orcid')) \
    .withColumn('original_orcid', process_orcid(F.col('original_orcid'), F.col('orcid_len'))) \
    .join(source_orcid, how='left', on=['work_author_id_2']) \
    .withColumn('best_orcid', F.when(F.col('source_orcid').isNotNull(), F.col('source_orcid')).otherwise(F.col('original_orcid'))) \
    .withColumn('orcid_source_init', F.when(F.col('source_orcid').isNotNull(), F.lit('orcid')).otherwise(F.lit('source'))) \
    .join(add_orcid.select('work_author_id_2', 'new_orcid'), how='left', on=['work_author_id_2']) \
    .withColumn('final_orcid', F.when(F.col('new_orcid').isNotNull(), F.col('new_orcid')).otherwise(F.col('best_orcid'))) \
    .withColumn('orcid_source', F.when(F.col('new_orcid').isNotNull(), F.lit('user')).otherwise(F.col('orcid_source_init'))) \
    .filter(F.col('final_orcid').isNotNull()) \
    .filter(F.col('final_orcid')!="") \
    .join(orcid_names.select(F.col('orcid').alias('final_orcid'), 'given_names','family_name'), how='left', on='final_orcid') \
    .withColumn('original_author_proc',transform_name_for_orcid_match_string('original_author')) \
    .withColumn('given_names_proc',transform_name_for_orcid_match_list('given_names')) \
    .withColumn('family_name_proc',transform_name_for_orcid_match_list('family_name')) \
    .withColumn('match_scores', score_orcid_names_to_original_author(F.col('original_author_proc'), F.col('given_names_proc'), F.col('family_name_proc'))) \
    .withColumn('family_name_match_score', F.col('match_scores').getItem(0)) \
    .withColumn('given_names_match_score', F.col('match_scores').getItem(1)) \
    .withColumn('initials_match_score', F.col('match_scores').getItem(2)) \
    .filter(F.col('family_name_match_score')>0.0) \
    .filter((F.col('given_names_match_score')>0.0) | (F.col('initials_match_score')>0.0)) \
    .withColumn('paper_orcid_rank', F.dense_rank().over(w_orcid_new)) \
    .filter(F.col('paper_orcid_rank')==1) \
    .select(F.split(F.col('work_author_id_2'), '_').alias('work_author_id'), 'final_orcid','orcid_source', 'original_author') \
    .select(F.col('work_author_id').getItem(0).cast(LongType()).alias('paper_id'), 
            F.col('work_author_id').getItem(1).cast(IntegerType()).alias('author_sequence_number'), 'final_orcid','orcid_source','original_author') \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/orcid/combined_source_orcid_temp")

# COMMAND ----------

dups = spark.read.parquet(f"{database_copy_save_path}/orcid/combined_source_orcid_temp").groupBy(['paper_id','final_orcid']).count().filter(F.col('count')>1).select('paper_id','final_orcid').alias('dup_orcids')

# COMMAND ----------

spark.read.parquet(f"{database_copy_save_path}/orcid/combined_source_orcid_temp") \
    .join(dups, how='leftanti', on=['paper_id','final_orcid']) \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/orcid/combined_source_orcid")

# COMMAND ----------

# 87567854
spark.read.parquet(f"{database_copy_save_path}/orcid/combined_source_orcid").count()

# COMMAND ----------

spark.read.parquet(f"{database_copy_save_path}/orcid/combined_source_orcid").groupBy(['paper_id','final_orcid']).count().filter(F.col('count')>1).count()

# COMMAND ----------

# spark.read.parquet(f"{temp_save_path}/source_orcid_final_table/")\
#     .select(F.split(F.col('work_author_id_2'), '_').alias('work_author_id'), 'final_orcid','orcid_source') \
#     .select(F.col('work_author_id').getItem(0).cast(LongType()).alias('paper_id'), 
#             F.col('work_author_id').getItem(1).cast(IntegerType()).alias('author_sequence_number'), 'final_orcid','orcid_source') \
#     .write.mode('overwrite') \
#     .parquet(f"{database_copy_save_path}/orcid/combined_source_orcid")

# COMMAND ----------

(spark.read.parquet(f"{database_copy_save_path}/orcid/combined_source_orcid") \
    .repartition(20)
    .write.format("jdbc")
    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
    .option("dbtable", 'orcid.combined_source_orcid')
    .option("user", secret['username'])
    .option("password", secret['password'])
    .option("driver", "org.postgresql.Driver")
    .option("truncate", True) \
    .mode("overwrite") \
    .save())

# COMMAND ----------

source_orcid_final_table = spark.read.parquet(f"{database_copy_save_path}/orcid/combined_source_orcid")
source_orcid_final_table.count()

# COMMAND ----------

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

# COMMAND ----------

curr_authors_grouped_temp = spark.read.parquet(f"{prod_save_path}/current_authors_table/") \
    .select('author_id', F.col('work_author_id').alias('work_author_id_2')) \
    .filter(~F.col('author_id').isin([9999999999, 5317838346])) \
    .join(source_orcid_final_table
          .select(F.concat_ws("_", F.col('paper_id'), 
                              F.col('author_sequence_number')).alias('work_author_id_2'), 'final_orcid'), how='left', on='work_author_id_2') \
    .fillna("", subset=['final_orcid']) \
    .groupBy('author_id').agg(F.collect_set('final_orcid').alias('final_orcid'), 
                              F.collect_set('work_author_id_2').alias('work_author_ids')) \
    .withColumn('final_orcid', get_non_null_orcid(F.col('final_orcid'))) \
    .withColumn('orcid_len', F.size(F.col('final_orcid'))) \
    .filter(F.col('orcid_len')>1) \
    .join(frozen_authors.select('author_id'), how='leftanti', on='author_id')

curr_authors_grouped_temp.cache().count()

# COMMAND ----------

print("2 orcid: ", curr_authors_grouped_temp.filter(F.col('orcid_len')==2).count())
print("3 orcid: ", curr_authors_grouped_temp.filter(F.col('orcid_len')==3).count())
print("4+ orcid: ", curr_authors_grouped_temp.filter(F.col('orcid_len')>=4).count())

# After overmerge fix round 1 (>=15): Added ratio of 44 new authors for every fixed author ID
# 2 orcid:  579977
# 3 orcid:  142373
# 4+ orcid:  118928

# After overmerge fix round 2 (>=7): Added ratio of 13 new authors for every fixed author ID
# 2 orcid:  580923
# 3 orcid:  142587
# 4+ orcid:  97480

# After overmerge fix round 3 (>=3): Added ratio of 6 new authors for every fixed author ID
# 2 orcid:  581588
# 3 orcid:  227
# 4+ orcid:  16

# After overmerge fix round 4 (>=2): Added ratio of 3 new authors for every fixed author ID
# 2 orcid:  1105
# 3 orcid:  34
# 4+ orcid:  7

# COMMAND ----------

curr_authors_grouped_temp.filter(F.col("author_id")==5100394072).display()

# COMMAND ----------

# MAGIC %md #### Code to run if there are multiple ORCIDs in a single author ID

# COMMAND ----------

if curr_authors_grouped_temp.filter(F.col('orcid_len')>=2).count() > 10000:
    number_of_partitions = int(curr_authors_grouped_temp.filter(F.col('orcid_len')>=2).count()/10000)
    # create random int to split up data
    curr_author_grouped_temp = curr_authors_grouped_temp\
        .withColumn('random_int', (F.rand()*number_of_partitions).cast(IntegerType()))
else:
    curr_author_grouped_temp = curr_authors_grouped_temp\
        .withColumn('random_int', F.lit(0))

curr_author_grouped_temp \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}/temp_grouped_orcid_table_with_rand_ints/")

curr_author_grouped_rand_int = spark.read.parquet(f"{temp_save_path}/temp_grouped_orcid_table_with_rand_ints/")

# get unique list of values in random int column
unique_random_ints = curr_author_grouped_rand_int.select(F.collect_set('random_int').alias('random_int')).first()['random_int']

# COMMAND ----------

print(unique_random_ints)

# COMMAND ----------

for random_int in unique_random_ints:
    print(random_int)
    curr_authors_grouped = curr_author_grouped_rand_int.filter(F.col('random_int')==random_int).alias('rand_group')

    if curr_authors_grouped.filter(F.col('orcid_len')>=2).count() > 0:
        print("Overmerge fix needed")

        # Get coauthor orcids
        spark.read.parquet(f"{prod_save_path}/current_features_table/") \
            .select('work_author_id_2','paper_id_2', 'original_author') \
            .join(source_orcid_final_table.select(F.concat_ws("_", F.col('paper_id'), F.col('author_sequence_number')).alias('work_author_id_2'), 
                                                F.col('final_orcid').alias('orcid_2')), how='left', on='work_author_id_2') \
            .fillna("", subset=['orcid_2']) \
            .groupBy('paper_id_2').agg(F.collect_set(F.col('orcid_2')).alias('coauthor_orcids'), 
                                    F.collect_set(F.col('work_author_id_2')).alias('work_author_ids')) \
            .withColumn('coauthor_orcids', get_non_null_orcid(F.col('coauthor_orcids'))) \
            .select('paper_id_2', 'coauthor_orcids', F.explode('work_author_ids').alias('work_author_id_2')) \
            .write.mode('overwrite') \
            .parquet(f"{temp_save_path}/coauthor_orcids/")
        
        coauthor_orcids = spark.read.parquet(f"{temp_save_path}/coauthor_orcids/")

        # Get topics
        topics = spark.read.parquet(f"{database_copy_save_path}/mid/topic") \
            .select('topic_id', F.col('display_name').alias('topic_name'), 'subfield_id', 'field_id')

        work_topics = spark.read.parquet(f"{database_copy_save_path}/mid/work_topic").dropDuplicates() \
            .filter(F.col('topic_rank')==1) \
            .select(F.col('paper_id').alias('paper_id_2'), 'topic_id') \
            .join(topics, on='topic_id', how='inner')

        # Creating a feature table of all work_authors that need to be reclustered
        spark.read.parquet(f"{prod_save_path}/current_features_table/") \
            .join(curr_authors_grouped.select('author_id',F.explode('work_author_ids').alias('work_author_id_2'), 'orcid_len'), how='inner', on='work_author_id_2') \
            .select('work_author_id_2', 'citations_2', 'institutions_2', 'author_2', 'paper_id_2', 'original_author', 
                    'concepts_shorter_2', 'coauthors_shorter_2','author_id','orcid_len') \
            .join(source_orcid_final_table.select(F.concat_ws("_", F.col('paper_id'), F.col('author_sequence_number')).alias('work_author_id_2'), 
                                                F.col('final_orcid').alias('orcid_2')), how='left', on='work_author_id_2') \
            .fillna("", subset=['orcid_2']) \
            .join(work_topics, how='left', on='paper_id_2') \
            .join(coauthor_orcids, how='left', on=['paper_id_2','work_author_id_2']) \
            .withColumn('coauthor_orcids', transform_list_col_for_nulls_string(F.col('coauthor_orcids'))) \
            .withColumn('coauthor_orcids', remove_current_author(F.col('orcid_2'), F.col('coauthor_orcids'))) \
            .write.mode('overwrite') \
            .parquet(f"{temp_save_path}/features_table_to_recluster_orcid/")

        all_features = spark.read.parquet(f"{temp_save_path}/features_table_to_recluster_orcid/")

        print("Size of reclustering DF: ", spark.read.parquet(f"{temp_save_path}/features_table_to_recluster_orcid/").dropDuplicates(subset=['work_author_id_2']).count())

        # Getting the ORCID that will retain the author ID for each multi-orcid author ID
        w_orc = Window.partitionBy('author_id').orderBy([F.col('count').desc(), F.col('orcid_2')])
        author_ids_keep_orcid = spark.read.parquet(f"{temp_save_path}/features_table_to_recluster_orcid/") \
            .filter(F.col('orcid_2')!='')\
            .groupBy(['author_id','orcid_2']).count() \
            .withColumn('orcid_rank', F.row_number().over(w_orc)) \
            .filter(F.col('orcid_rank')==1)

        print("Number of author IDs to keep: ", author_ids_keep_orcid.cache().count())

        #################################### Round 1 of reclustering ####################################
        left_side = spark.read.parquet(f"{temp_save_path}/features_table_to_recluster_orcid/").filter(F.col('orcid_len')>=2) \
            .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2'))) \
            .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups')) \
            .filter(F.col('name_to_keep_ind')==1) \
            .withColumn('trans_name', transform_name_for_search(F.col('author_2'))) \
            .withColumn('name_len', F.length(F.col('trans_name'))) \
            .filter(F.col('name_len')>1) \
            .fillna(-1, subset=['topic_id','subfield_id','field_id']) \
            .select(F.col('paper_id_2').alias('paper_id_left'), F.col('work_author_id_2').alias('work_author_id_left'), F.col('orcid_2').alias('orcid_left'), 
                            F.col('citations_2').alias('citations_left'), F.col('institutions_2').alias('institutions_left'), F.col('trans_name').alias('trans_name_left'), 
                            F.col('author_id'), F.col('topic_id').alias('topic_id_left'), F.col('topic_name').alias('topic_name_left'), 
                            F.col('concepts_shorter_2').alias('concepts_shorter_left'), F.col('coauthors_shorter_2').alias('coauthors_shorter_left'),
                            F.col('subfield_id').alias('subfield_id_left'), F.col('field_id').alias('field_id_left'), F.col('coauthor_orcids').alias('coauthor_orcids_left')) \
            .filter(F.col('orcid_left')!='') \
            .groupBy(['author_id','orcid_left']) \
            .agg(F.collect_set(F.col('work_author_id_left')).alias('work_author_id_left'), 
                F.collect_list(F.col('citations_left')).alias('citations_left'), 
                F.collect_list(F.col('institutions_left')).alias('institutions_left'), 
                F.collect_list(F.col('trans_name_left')).alias('trans_name_left'), 
                F.collect_list(F.col('coauthor_orcids_left')).alias('coauthor_orcids_left'), 
                F.collect_list(F.col('concepts_shorter_left')).alias('concepts_shorter_left'), 
                F.collect_list(F.col('coauthors_shorter_left')).alias('coauthors_shorter_left'), 
                F.collect_set(F.col('topic_id_left')).alias('topic_id_left'), 
                F.collect_set(F.col('subfield_id_left')).alias('subfield_id_left'), 
                F.collect_set(F.col('field_id_left')).alias('field_id_left')) \
            .select('author_id','orcid_left', 'work_author_id_left', 
                    F.array_distinct(F.col('trans_name_left')).alias('trans_name_left'),
                    F.array_remove(F.array_distinct(F.col('topic_id_left')), -1).alias('topic_id_left'), 
                    F.array_remove(F.array_distinct(F.col('subfield_id_left')), -1).alias('subfield_id_left'), 
                    F.array_remove(F.array_distinct(F.col('field_id_left')), -1).alias('field_id_left'),
                    F.array_distinct(F.flatten(F.col('citations_left'))).alias('citations_left'), 
                    F.array_distinct(F.flatten(F.col('institutions_left'))).alias('institutions_left'), 
                    F.array_remove(F.array_distinct(F.flatten(F.col('coauthor_orcids_left'))), "").alias('coauthor_orcids_left'), 
                    F.array_distinct(F.flatten(F.col('concepts_shorter_left'))).alias('concepts_shorter_left'), 
                    F.array_distinct(F.flatten(F.col('coauthors_shorter_left'))).alias('coauthors_shorter_left')) \
            .withColumn('name_match_list_left', get_name_match_from_alternate_names('trans_name_left'))


        right_side = spark.read.parquet(f"{temp_save_path}/features_table_to_recluster_orcid/").filter(F.col('orcid_len')>=2) \
            .filter(F.col('orcid_2')=='') \
            .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2'))) \
            .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups')) \
            .filter(F.col('name_to_keep_ind')==1) \
            .withColumn('trans_name', transform_name_for_search(F.col('author_2'))) \
            .withColumn('name_len', F.length(F.col('trans_name'))) \
            .filter(F.col('name_len')>1) \
            .withColumn('name_match_list', get_name_match_list(F.col('trans_name'))) \
            .fillna(-1, subset=['topic_id','subfield_id','field_id']) \
            .select(F.col('paper_id_2').alias('paper_id_right'), F.col('work_author_id_2').alias('work_author_id_right'), F.col('orcid_2').alias('orcid_right'), 
                            F.col('citations_2').alias('citations_right'), F.col('institutions_2').alias('institutions_right'), F.col('trans_name').alias('trans_name_right'), 
                            F.col('author_id'), F.col('topic_id').alias('topic_id_right'), F.col('topic_name').alias('topic_name_right'), 
                            F.col('concepts_shorter_2').alias('concepts_shorter_right'), F.col('coauthors_shorter_2').alias('coauthors_shorter_right'),
                            F.col('subfield_id').alias('subfield_id_right'), F.col('field_id').alias('field_id_right'), F.col('coauthor_orcids').alias('coauthor_orcids_right')) \
            .filter((F.size(F.col('citations_right'))>0) | 
                    (F.size(F.col('institutions_right'))>0) | 
                    (F.size(F.col('coauthor_orcids_right'))>0) | 
                    (F.size(F.col('concepts_shorter_right'))>0) | 
                    (F.size(F.col('coauthors_shorter_right'))>0) | 
                    (F.col('topic_id_right') != -1)) \
            .groupBy(['author_id','work_author_id_right']) \
            .agg(F.collect_list(F.col('citations_right')).alias('citations_right'), 
                F.collect_list(F.col('institutions_right')).alias('institutions_right'), 
                F.collect_list(F.col('trans_name_right')).alias('trans_name_right'), 
                F.collect_list(F.col('coauthor_orcids_right')).alias('coauthor_orcids_right'), 
                F.collect_list(F.col('concepts_shorter_right')).alias('concepts_shorter_right'), 
                F.collect_list(F.col('coauthors_shorter_right')).alias('coauthors_shorter_right'), 
                F.collect_set(F.col('topic_id_right')).alias('topic_id_right'), 
                F.collect_set(F.col('subfield_id_right')).alias('subfield_id_right'), 
                F.collect_set(F.col('field_id_right')).alias('field_id_right')) \
            .select('author_id','work_author_id_right', 
                    F.array_distinct(F.col('trans_name_right')).alias('trans_name_right'),
                    F.array_remove(F.array_distinct(F.col('topic_id_right')), -1).alias('topic_id_right'), 
                    F.array_remove(F.array_distinct(F.col('subfield_id_right')), -1).alias('subfield_id_right'), 
                    F.array_remove(F.array_distinct(F.col('field_id_right')), -1).alias('field_id_right'),
                    F.array_distinct(F.flatten(F.col('citations_right'))).alias('citations_right'), 
                    F.array_distinct(F.flatten(F.col('institutions_right'))).alias('institutions_right'), 
                    F.array_remove(F.array_distinct(F.flatten(F.col('coauthor_orcids_right'))), "").alias('coauthor_orcids_right'), 
                    F.array_distinct(F.flatten(F.col('concepts_shorter_right'))).alias('concepts_shorter_right'), 
                    F.array_distinct(F.flatten(F.col('coauthors_shorter_right'))).alias('coauthors_shorter_right')) \
            .withColumn('name_match_list_right', get_name_match_from_alternate_names('trans_name_right'))
        
        print("Round 1 Left side: ", left_side.cache().count())
        print("Round 1 Right side: ", right_side.cache().count())

        joined_df = left_side.join(right_side, how='inner', on='author_id')\
            .withColumn('insts_inter', F.size(F.array_intersect(F.col('institutions_left'), F.col('institutions_right')))*2) \
            .withColumn('coauths_inter', F.size(F.array_intersect(F.col('coauthors_shorter_left'), F.col('coauthors_shorter_right')))*0.05) \
            .withColumn('concps_inter', F.size(F.array_intersect(F.col('concepts_shorter_left'), F.col('concepts_shorter_right')))*0.01) \
            .withColumn('cites_inter', F.size(F.array_intersect(F.col('citations_left'), F.col('citations_right')))*0.2) \
            .withColumn('coauth_orcids_inter', F.size(F.array_intersect(F.col('coauthor_orcids_left'), F.col('coauthor_orcids_right')))*5) \
            .withColumn('topics_inter', F.size(F.array_intersect(F.col('topic_id_left'), F.col('topic_id_right')))*5) \
            .withColumn('subfields_inter', F.size(F.array_intersect(F.col('subfield_id_left'), F.col('subfield_id_right')))*1) \
            .withColumn('fields_inter', F.size(F.array_intersect(F.col('field_id_left'), F.col('field_id_right')))*0.01) \
            .select('author_id','orcid_left','trans_name_left','work_author_id_right','trans_name_right','insts_inter','coauths_inter','concps_inter','cites_inter',
                    'coauth_orcids_inter','topics_inter','subfields_inter','fields_inter','name_match_list_left','name_match_list_right') \
            .withColumn('total_score', F.col('insts_inter') + F.col('coauths_inter') + F.col('concps_inter') + F.col('cites_inter') + F.col('coauth_orcids_inter') + 
                        F.col('topics_inter') + F.col('subfields_inter') + F.col('fields_inter')) \
            .filter(F.col('total_score')>0) \
            .withColumn('matched_names', check_block_vs_block(F.col('name_match_list_left'), F.col('name_match_list_right'))) \
            .filter(F.col('matched_names')==1)

        work_authors_scored = joined_df\
            .repartition(320) \
            .select('author_id',F.array([F.array(F.col('orcid_left')), 'trans_name_left' ,F.array(F.col('work_author_id_right')), 
                                        'trans_name_right',F.array(F.col('total_score').cast(StringType()))]).alias('data_to_cluster')) \
            .groupBy('author_id').agg(F.collect_list(F.col('data_to_cluster')).alias('data_to_cluster')) \
            .withColumn('work_author_to_orcid_mapping', go_through_rows_for_author('data_to_cluster')) \
            .select('author_id',F.explode('work_author_to_orcid_mapping').alias('work_author_to_orcid_mapping')) \
            .select('author_id', 
                    F.col('work_author_to_orcid_mapping').getItem(0).alias('orcid_left'), 
                    F.col('work_author_to_orcid_mapping').getItem(1).alias('work_author_id_left'))

        work_authors_scored.cache().count()

        new_to_cluster = all_features.alias('left_side_new').filter(F.col('orcid_len')>=2)\
            .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2'))) \
            .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups')) \
            .filter(F.col('name_to_keep_ind')==1) \
            .withColumn('trans_name', transform_name_for_search(F.col('author_2'))) \
            .withColumn('name_len', F.length(F.col('trans_name'))) \
            .filter(F.col('name_len')>1) \
            .fillna(-1, subset=['topic_id','subfield_id','field_id']) \
            .select(F.col('paper_id_2').alias('paper_id_left'), F.col('work_author_id_2').alias('work_author_id_left'), 
                            F.col('citations_2').alias('citations_left'), F.col('institutions_2').alias('institutions_left'), F.col('trans_name').alias('trans_name_left'), 
                            F.col('author_id'), F.col('topic_id').alias('topic_id_left'), F.col('topic_name').alias('topic_name_left'), 
                            F.col('concepts_shorter_2').alias('concepts_shorter_left'), F.col('coauthors_shorter_2').alias('coauthors_shorter_left'),
                            F.col('subfield_id').alias('subfield_id_left'), F.col('field_id').alias('field_id_left'), F.col('coauthor_orcids').alias('coauthor_orcids_left')) \
            .join(work_authors_scored, how='inner', on=['author_id', 'work_author_id_left'])

        if new_to_cluster.count() == right_side.count():
            (all_features.alias('left_side_original').filter(F.col('orcid_len')>=2)
                .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2')))
                .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups'))
                .filter(F.col('name_to_keep_ind')==1)
                .withColumn('trans_name', transform_name_for_search(F.col('author_2')))
                .withColumn('name_len', F.length(F.col('trans_name')))
                .filter(F.col('name_len')>1)
                .fillna(-1, subset=['topic_id','subfield_id','field_id'])
                .select(F.col('paper_id_2').alias('paper_id_left'), F.col('work_author_id_2').alias('work_author_id_left'), F.col('orcid_2').alias('orcid_left'), 
                                F.col('citations_2').alias('citations_left'), F.col('institutions_2').alias('institutions_left'), F.col('trans_name').alias('trans_name_left'), 
                                F.col('author_id'), F.col('topic_id').alias('topic_id_left'), F.col('topic_name').alias('topic_name_left'), 
                                F.col('concepts_shorter_2').alias('concepts_shorter_left'), F.col('coauthors_shorter_2').alias('coauthors_shorter_left'),
                                F.col('subfield_id').alias('subfield_id_left'), F.col('field_id').alias('field_id_left'), F.col('coauthor_orcids').alias('coauthor_orcids_left'))
                .filter(F.col('orcid_left')!='')
                .union(new_to_cluster.select('paper_id_left', 'work_author_id_left', 'orcid_left', 'citations_left', 'institutions_left', 'trans_name_left', 
                                            'author_id', 'topic_id_left', 'topic_name_left', 'concepts_shorter_left', 'coauthors_shorter_left', 
                                            'subfield_id_left', 'field_id_left', 'coauthor_orcids_left'))
                .groupBy(['author_id','orcid_left']) \
                .agg(F.collect_set(F.col('work_author_id_left')).alias('work_author_id_left')) \
                .join(author_ids_keep_orcid.select('author_id', F.col('orcid_2').alias('orcid_left')), how='leftanti', on=['author_id','orcid_left'])
                .withColumn('current_date_str', F.current_date().cast(StringType()))
                .select(F.concat_ws("-", F.col('author_id'), F.lit('orc'), F.col('orcid_left'), 
                                    F.col('current_date_str')).alias('work_author_id_for_cluster'), 
                        F.explode('work_author_id_left').alias('all_works_to_cluster'))
                .withColumn("request_type", F.lit("orcid"))
                .withColumn("request_date", F.current_timestamp())
                .withColumn("partition_col", (F.rand()*40+1).cast(IntegerType()))
                .repartition(6)
                .write.format("jdbc") 
                .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
                .option("dbtable", 'authorships.overmerged_authors') 
                .option("user", secret['username']) 
                .option("password", secret['password']) 
                .option("driver", "org.postgresql.Driver") 
                .mode("append") 
                .save())
        else:
            #################################### Round 2 of reclustering ####################################
            left_side_2 = all_features.alias('left_side_original').filter(F.col('orcid_len')>=2) \
                .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2'))) \
                .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups')) \
                .filter(F.col('name_to_keep_ind')==1) \
                .withColumn('trans_name', transform_name_for_search(F.col('author_2'))) \
                .withColumn('name_len', F.length(F.col('trans_name'))) \
                .filter(F.col('name_len')>1) \
                .fillna(-1, subset=['topic_id','subfield_id','field_id']) \
                .select(F.col('paper_id_2').alias('paper_id_left'), F.col('work_author_id_2').alias('work_author_id_left'), F.col('orcid_2').alias('orcid_left'), 
                                F.col('citations_2').alias('citations_left'), F.col('institutions_2').alias('institutions_left'), F.col('trans_name').alias('trans_name_left'), 
                                F.col('author_id'), F.col('topic_id').alias('topic_id_left'), F.col('topic_name').alias('topic_name_left'), 
                                F.col('concepts_shorter_2').alias('concepts_shorter_left'), F.col('coauthors_shorter_2').alias('coauthors_shorter_left'),
                                F.col('subfield_id').alias('subfield_id_left'), F.col('field_id').alias('field_id_left'), F.col('coauthor_orcids').alias('coauthor_orcids_left')) \
                .filter(F.col('orcid_left')!='') \
                .union(new_to_cluster.select('paper_id_left', 'work_author_id_left', 'orcid_left', 'citations_left', 'institutions_left', 'trans_name_left', 
                                            'author_id', 'topic_id_left', 'topic_name_left', 'concepts_shorter_left', 'coauthors_shorter_left', 
                                            'subfield_id_left', 'field_id_left', 'coauthor_orcids_left')) \
                .groupBy(['author_id','orcid_left']) \
                .agg(F.collect_set(F.col('work_author_id_left')).alias('work_author_id_left'), 
                    F.collect_list(F.col('citations_left')).alias('citations_left'), 
                    F.collect_list(F.col('institutions_left')).alias('institutions_left'), 
                    F.collect_list(F.col('trans_name_left')).alias('trans_name_left'), 
                    F.collect_list(F.col('coauthor_orcids_left')).alias('coauthor_orcids_left'), 
                    F.collect_list(F.col('concepts_shorter_left')).alias('concepts_shorter_left'), 
                    F.collect_list(F.col('coauthors_shorter_left')).alias('coauthors_shorter_left'), 
                    F.collect_set(F.col('topic_id_left')).alias('topic_id_left'), 
                    F.collect_set(F.col('subfield_id_left')).alias('subfield_id_left'), 
                    F.collect_set(F.col('field_id_left')).alias('field_id_left')) \
                .select('author_id','orcid_left', 'work_author_id_left', 
                        F.array_distinct(F.col('trans_name_left')).alias('trans_name_left'),
                        F.array_remove(F.array_distinct(F.col('topic_id_left')), -1).alias('topic_id_left'), 
                        F.array_remove(F.array_distinct(F.col('subfield_id_left')), -1).alias('subfield_id_left'), 
                        F.array_remove(F.array_distinct(F.col('field_id_left')), -1).alias('field_id_left'),
                        F.array_distinct(F.flatten(F.col('citations_left'))).alias('citations_left'), 
                        F.array_distinct(F.flatten(F.col('institutions_left'))).alias('institutions_left'), 
                        F.array_remove(F.array_distinct(F.flatten(F.col('coauthor_orcids_left'))), "").alias('coauthor_orcids_left'), 
                        F.array_distinct(F.flatten(F.col('concepts_shorter_left'))).alias('concepts_shorter_left'), 
                        F.array_distinct(F.flatten(F.col('coauthors_shorter_left'))).alias('coauthors_shorter_left')) \
                .withColumn('name_match_list_left', get_name_match_from_alternate_names('trans_name_left'))


            right_side_2 = all_features.alias('right_side').filter(F.col('orcid_len')>=2) \
                .filter(F.col('orcid_2')=='') \
                .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2'))) \
                .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups')) \
                .filter(F.col('name_to_keep_ind')==1) \
                .withColumn('trans_name', transform_name_for_search(F.col('author_2'))) \
                .withColumn('name_len', F.length(F.col('trans_name'))) \
                .filter(F.col('name_len')>1) \
                .withColumn('name_match_list', get_name_match_list(F.col('trans_name'))) \
                .fillna(-1, subset=['topic_id','subfield_id','field_id']) \
                .select(F.col('paper_id_2').alias('paper_id_right'), F.col('work_author_id_2').alias('work_author_id_right'), F.col('orcid_2').alias('orcid_right'), 
                                F.col('citations_2').alias('citations_right'), F.col('institutions_2').alias('institutions_right'), F.col('trans_name').alias('trans_name_right'), 
                                F.col('author_id'), F.col('topic_id').alias('topic_id_right'), F.col('topic_name').alias('topic_name_right'), 
                                F.col('concepts_shorter_2').alias('concepts_shorter_right'), F.col('coauthors_shorter_2').alias('coauthors_shorter_right'),
                                F.col('subfield_id').alias('subfield_id_right'), F.col('field_id').alias('field_id_right'), F.col('coauthor_orcids').alias('coauthor_orcids_right')) \
                .join(work_authors_scored.select('author_id',F.col('work_author_id_left').alias('work_author_id_right')), how='leftanti', on=['author_id', 'work_author_id_right']) \
                .filter((F.size(F.col('citations_right'))>0) | 
                        (F.size(F.col('institutions_right'))>0) | 
                        (F.size(F.col('coauthor_orcids_right'))>0) | 
                        (F.size(F.col('concepts_shorter_right'))>0) | 
                        (F.size(F.col('coauthors_shorter_right'))>0) | 
                        (F.col('topic_id_right') != -1)) \
                .groupBy(['author_id','work_author_id_right']) \
                .agg(F.collect_list(F.col('citations_right')).alias('citations_right'), 
                    F.collect_list(F.col('institutions_right')).alias('institutions_right'), 
                    F.collect_list(F.col('trans_name_right')).alias('trans_name_right'), 
                    F.collect_list(F.col('coauthor_orcids_right')).alias('coauthor_orcids_right'), 
                    F.collect_list(F.col('concepts_shorter_right')).alias('concepts_shorter_right'), 
                    F.collect_list(F.col('coauthors_shorter_right')).alias('coauthors_shorter_right'), 
                    F.collect_set(F.col('topic_id_right')).alias('topic_id_right'), 
                    F.collect_set(F.col('subfield_id_right')).alias('subfield_id_right'), 
                    F.collect_set(F.col('field_id_right')).alias('field_id_right')) \
                .select('author_id','work_author_id_right', 
                        F.array_distinct(F.col('trans_name_right')).alias('trans_name_right'),
                        F.array_remove(F.array_distinct(F.col('topic_id_right')), -1).alias('topic_id_right'), 
                        F.array_remove(F.array_distinct(F.col('subfield_id_right')), -1).alias('subfield_id_right'), 
                        F.array_remove(F.array_distinct(F.col('field_id_right')), -1).alias('field_id_right'),
                        F.array_distinct(F.flatten(F.col('citations_right'))).alias('citations_right'), 
                        F.array_distinct(F.flatten(F.col('institutions_right'))).alias('institutions_right'), 
                        F.array_remove(F.array_distinct(F.flatten(F.col('coauthor_orcids_right'))), "").alias('coauthor_orcids_right'), 
                        F.array_distinct(F.flatten(F.col('concepts_shorter_right'))).alias('concepts_shorter_right'), 
                        F.array_distinct(F.flatten(F.col('coauthors_shorter_right'))).alias('coauthors_shorter_right')) \
                .withColumn('name_match_list_right', get_name_match_from_alternate_names('trans_name_right'))

            print("Round 2 Left side: ", left_side_2.cache().count())
            print("Round 2 Right side: ", right_side_2.cache().count())

            joined_df_2 = left_side_2.join(right_side_2, how='inner', on='author_id')\
                .withColumn('insts_inter', F.size(F.array_intersect(F.col('institutions_left'), F.col('institutions_right')))*2) \
                .withColumn('coauths_inter', F.size(F.array_intersect(F.col('coauthors_shorter_left'), F.col('coauthors_shorter_right')))*0.05) \
                .withColumn('concps_inter', F.size(F.array_intersect(F.col('concepts_shorter_left'), F.col('concepts_shorter_right')))*0.01) \
                .withColumn('cites_inter', F.size(F.array_intersect(F.col('citations_left'), F.col('citations_right')))*0.2) \
                .withColumn('coauth_orcids_inter', F.size(F.array_intersect(F.col('coauthor_orcids_left'), F.col('coauthor_orcids_right')))*5) \
                .withColumn('topics_inter', F.size(F.array_intersect(F.col('topic_id_left'), F.col('topic_id_right')))*5) \
                .withColumn('subfields_inter', F.size(F.array_intersect(F.col('subfield_id_left'), F.col('subfield_id_right')))*1) \
                .withColumn('fields_inter', F.size(F.array_intersect(F.col('field_id_left'), F.col('field_id_right')))*0.01) \
                .select('author_id','orcid_left','trans_name_left','work_author_id_right','trans_name_right','insts_inter','coauths_inter','concps_inter','cites_inter',
                        'coauth_orcids_inter','topics_inter','subfields_inter','fields_inter','name_match_list_left','name_match_list_right') \
                .withColumn('total_score', F.col('insts_inter') + F.col('coauths_inter') + F.col('concps_inter') + F.col('cites_inter') + F.col('coauth_orcids_inter') + 
                            F.col('topics_inter') + F.col('subfields_inter') + F.col('fields_inter')) \
                .filter(F.col('total_score')>0) \
                .withColumn('matched_names', check_block_vs_block(F.col('name_match_list_left'), F.col('name_match_list_right'))) \
                .filter(F.col('matched_names')==1)
            
            work_authors_scored_2 = joined_df_2\
                .select('author_id',F.array([F.array(F.col('orcid_left')), 'trans_name_left' ,F.array(F.col('work_author_id_right')), 
                                            'trans_name_right',F.array(F.col('total_score').cast(StringType()))]).alias('data_to_cluster')) \
                .groupBy('author_id').agg(F.collect_list(F.col('data_to_cluster')).alias('data_to_cluster')) \
                .withColumn('work_author_to_orcid_mapping', go_through_rows_for_author('data_to_cluster')) \
                .select('author_id',F.explode('work_author_to_orcid_mapping').alias('work_author_to_orcid_mapping')) \
                .select('author_id', 
                        F.col('work_author_to_orcid_mapping').getItem(0).alias('orcid_left'), 
                        F.col('work_author_to_orcid_mapping').getItem(1).alias('work_author_id_left'))

            work_authors_scored_2.cache().count()

            new_to_cluster_2 = all_features.alias('left_side_2_new').filter(F.col('orcid_len')>=2)\
                .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2'))) \
                .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups')) \
                .filter(F.col('name_to_keep_ind')==1) \
                .withColumn('trans_name', transform_name_for_search(F.col('author_2'))) \
                .withColumn('name_len', F.length(F.col('trans_name'))) \
                .filter(F.col('name_len')>1) \
                .fillna(-1, subset=['topic_id','subfield_id','field_id']) \
                .select(F.col('paper_id_2').alias('paper_id_left'), F.col('work_author_id_2').alias('work_author_id_left'), 
                                F.col('citations_2').alias('citations_left'), F.col('institutions_2').alias('institutions_left'), F.col('trans_name').alias('trans_name_left'), 
                                F.col('author_id'), F.col('topic_id').alias('topic_id_left'), F.col('topic_name').alias('topic_name_left'), 
                                F.col('concepts_shorter_2').alias('concepts_shorter_left'), F.col('coauthors_shorter_2').alias('coauthors_shorter_left'),
                                F.col('subfield_id').alias('subfield_id_left'), F.col('field_id').alias('field_id_left'), F.col('coauthor_orcids').alias('coauthor_orcids_left')) \
                .join(work_authors_scored_2.union(work_authors_scored.select(*work_authors_scored_2.columns)), how='inner', on=['author_id', 'work_author_id_left'])

            if new_to_cluster_2.count() == right_side.count():
                (all_features.alias('left_side_original').filter(F.col('orcid_len')>=2)
                    .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2')))
                    .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups'))
                    .filter(F.col('name_to_keep_ind')==1)
                    .withColumn('trans_name', transform_name_for_search(F.col('author_2')))
                    .withColumn('name_len', F.length(F.col('trans_name')))
                    .filter(F.col('name_len')>1)
                    .fillna(-1, subset=['topic_id','subfield_id','field_id'])
                    .select(F.col('paper_id_2').alias('paper_id_left'), F.col('work_author_id_2').alias('work_author_id_left'), F.col('orcid_2').alias('orcid_left'), 
                                    F.col('citations_2').alias('citations_left'), F.col('institutions_2').alias('institutions_left'), F.col('trans_name').alias('trans_name_left'), 
                                    F.col('author_id'), F.col('topic_id').alias('topic_id_left'), F.col('topic_name').alias('topic_name_left'), 
                                    F.col('concepts_shorter_2').alias('concepts_shorter_left'), F.col('coauthors_shorter_2').alias('coauthors_shorter_left'),
                                    F.col('subfield_id').alias('subfield_id_left'), F.col('field_id').alias('field_id_left'), F.col('coauthor_orcids').alias('coauthor_orcids_left'))
                    .filter(F.col('orcid_left')!='')
                    .union(new_to_cluster_2.select('paper_id_left', 'work_author_id_left', 'orcid_left', 'citations_left', 'institutions_left', 'trans_name_left', 
                                                'author_id', 'topic_id_left', 'topic_name_left', 'concepts_shorter_left', 'coauthors_shorter_left', 
                                                'subfield_id_left', 'field_id_left', 'coauthor_orcids_left'))
                    .groupBy(['author_id','orcid_left']) \
                    .agg(F.collect_set(F.col('work_author_id_left')).alias('work_author_id_left')) \
                    .join(author_ids_keep_orcid.select('author_id', F.col('orcid_2').alias('orcid_left')), how='leftanti', on=['author_id','orcid_left'])
                    .withColumn('current_date_str', F.current_date().cast(StringType()))
                    .select(F.concat_ws("-", F.col('author_id'), F.lit('orc'), F.col('orcid_left'), 
                                    F.col('current_date_str')).alias('work_author_id_for_cluster'), 
                            F.explode('work_author_id_left').alias('all_works_to_cluster'))
                    .withColumn("request_type", F.lit("orcid"))
                    .withColumn("request_date", F.current_timestamp())
                    .withColumn("partition_col", (F.rand()*40+1).cast(IntegerType()))
                    .repartition(6)
                    .write.format("jdbc") 
                    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
                    .option("dbtable", 'authorships.overmerged_authors') 
                    .option("user", secret['username']) 
                    .option("password", secret['password']) 
                    .option("driver", "org.postgresql.Driver") 
                    .mode("append") 
                    .save())
            else:
                #################################### Round 3 of reclustering ####################################
                left_side_3 = all_features.alias('left_side_2').filter(F.col('orcid_len')>=2) \
                    .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2'))) \
                    .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups')) \
                    .filter(F.col('name_to_keep_ind')==1) \
                    .withColumn('trans_name', transform_name_for_search(F.col('author_2'))) \
                    .withColumn('name_len', F.length(F.col('trans_name'))) \
                    .filter(F.col('name_len')>1) \
                    .fillna(-1, subset=['topic_id','subfield_id','field_id']) \
                    .select(F.col('paper_id_2').alias('paper_id_left'), F.col('work_author_id_2').alias('work_author_id_left'), F.col('orcid_2').alias('orcid_left'), 
                                    F.col('citations_2').alias('citations_left'), F.col('institutions_2').alias('institutions_left'), F.col('trans_name').alias('trans_name_left'), 
                                    F.col('author_id'), F.col('topic_id').alias('topic_id_left'), F.col('topic_name').alias('topic_name_left'), 
                                    F.col('concepts_shorter_2').alias('concepts_shorter_left'), F.col('coauthors_shorter_2').alias('coauthors_shorter_left'),
                                    F.col('subfield_id').alias('subfield_id_left'), F.col('field_id').alias('field_id_left'), F.col('coauthor_orcids').alias('coauthor_orcids_left')) \
                    .filter(F.col('orcid_left')!='') \
                    .union(new_to_cluster_2.select('paper_id_left', 'work_author_id_left', 'orcid_left', 'citations_left', 'institutions_left', 
                                                'trans_name_left', 'author_id', 
                                                'topic_id_left', 'topic_name_left', 'concepts_shorter_left', 'coauthors_shorter_left', 'subfield_id_left',
                                                'field_id_left', 'coauthor_orcids_left')) \
                    .groupBy(['author_id','orcid_left']) \
                    .agg(F.collect_set(F.col('work_author_id_left')).alias('work_author_id_left'), 
                        F.collect_list(F.col('citations_left')).alias('citations_left'), 
                        F.collect_list(F.col('institutions_left')).alias('institutions_left'), 
                        F.collect_list(F.col('trans_name_left')).alias('trans_name_left'), 
                        F.collect_list(F.col('coauthor_orcids_left')).alias('coauthor_orcids_left'), 
                        F.collect_list(F.col('concepts_shorter_left')).alias('concepts_shorter_left'), 
                        F.collect_list(F.col('coauthors_shorter_left')).alias('coauthors_shorter_left'), 
                        F.collect_set(F.col('topic_id_left')).alias('topic_id_left'), 
                        F.collect_set(F.col('subfield_id_left')).alias('subfield_id_left'), 
                        F.collect_set(F.col('field_id_left')).alias('field_id_left')) \
                    .select('author_id','orcid_left', 'work_author_id_left', 
                            F.array_distinct(F.col('trans_name_left')).alias('trans_name_left'),
                            F.array_remove(F.array_distinct(F.col('topic_id_left')), -1).alias('topic_id_left'), 
                            F.array_remove(F.array_distinct(F.col('subfield_id_left')), -1).alias('subfield_id_left'), 
                            F.array_remove(F.array_distinct(F.col('field_id_left')), -1).alias('field_id_left'),
                            F.array_distinct(F.flatten(F.col('citations_left'))).alias('citations_left'), 
                            F.array_distinct(F.flatten(F.col('institutions_left'))).alias('institutions_left'), 
                            F.array_remove(F.array_distinct(F.flatten(F.col('coauthor_orcids_left'))), "").alias('coauthor_orcids_left'), 
                            F.array_distinct(F.flatten(F.col('concepts_shorter_left'))).alias('concepts_shorter_left'), 
                            F.array_distinct(F.flatten(F.col('coauthors_shorter_left'))).alias('coauthors_shorter_left')) \
                    .withColumn('name_match_list_left', get_name_match_from_alternate_names('trans_name_left'))


                right_side_3 = all_features.alias('right_side_2').filter(F.col('orcid_len')>=2) \
                    .filter(F.col('orcid_2')=='') \
                    .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2'))) \
                    .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups')) \
                    .filter(F.col('name_to_keep_ind')==1) \
                    .withColumn('trans_name', transform_name_for_search(F.col('author_2'))) \
                    .withColumn('name_len', F.length(F.col('trans_name'))) \
                    .filter(F.col('name_len')>1) \
                    .withColumn('name_match_list', get_name_match_list(F.col('trans_name'))) \
                    .fillna(-1, subset=['topic_id','subfield_id','field_id']) \
                    .select(F.col('paper_id_2').alias('paper_id_right'), F.col('work_author_id_2').alias('work_author_id_right'), F.col('orcid_2').alias('orcid_right'), 
                                    F.col('citations_2').alias('citations_right'), F.col('institutions_2').alias('institutions_right'), F.col('trans_name').alias('trans_name_right'), 
                                    F.col('author_id'), F.col('topic_id').alias('topic_id_right'), F.col('topic_name').alias('topic_name_right'), 
                                    F.col('concepts_shorter_2').alias('concepts_shorter_right'), F.col('coauthors_shorter_2').alias('coauthors_shorter_right'),
                                    F.col('subfield_id').alias('subfield_id_right'), F.col('field_id').alias('field_id_right'), F.col('coauthor_orcids').alias('coauthor_orcids_right')) \
                    .join(work_authors_scored.select('author_id',F.col('work_author_id_left').alias('work_author_id_right')), 
                        how='leftanti', on=['author_id', 'work_author_id_right']) \
                    .join(work_authors_scored_2.select('author_id',F.col('work_author_id_left').alias('work_author_id_right')), 
                        how='leftanti', on=['author_id', 'work_author_id_right']) \
                    .filter((F.size(F.col('citations_right'))>0) | 
                            (F.size(F.col('institutions_right'))>0) | 
                            (F.size(F.col('coauthor_orcids_right'))>0) | 
                            (F.size(F.col('concepts_shorter_right'))>0) | 
                            (F.size(F.col('coauthors_shorter_right'))>0) | 
                            (F.col('topic_id_right') != -1)) \
                    .groupBy(['author_id','work_author_id_right']) \
                    .agg(F.collect_list(F.col('citations_right')).alias('citations_right'), 
                        F.collect_list(F.col('institutions_right')).alias('institutions_right'), 
                        F.collect_list(F.col('trans_name_right')).alias('trans_name_right'), 
                        F.collect_list(F.col('coauthor_orcids_right')).alias('coauthor_orcids_right'), 
                        F.collect_list(F.col('concepts_shorter_right')).alias('concepts_shorter_right'), 
                        F.collect_list(F.col('coauthors_shorter_right')).alias('coauthors_shorter_right'), 
                        F.collect_set(F.col('topic_id_right')).alias('topic_id_right'), 
                        F.collect_set(F.col('subfield_id_right')).alias('subfield_id_right'), 
                        F.collect_set(F.col('field_id_right')).alias('field_id_right')) \
                    .select('author_id','work_author_id_right', 
                            F.array_distinct(F.col('trans_name_right')).alias('trans_name_right'),
                            F.array_remove(F.array_distinct(F.col('topic_id_right')), -1).alias('topic_id_right'), 
                            F.array_remove(F.array_distinct(F.col('subfield_id_right')), -1).alias('subfield_id_right'), 
                            F.array_remove(F.array_distinct(F.col('field_id_right')), -1).alias('field_id_right'),
                            F.array_distinct(F.flatten(F.col('citations_right'))).alias('citations_right'), 
                            F.array_distinct(F.flatten(F.col('institutions_right'))).alias('institutions_right'), 
                            F.array_remove(F.array_distinct(F.flatten(F.col('coauthor_orcids_right'))), "").alias('coauthor_orcids_right'), 
                            F.array_distinct(F.flatten(F.col('concepts_shorter_right'))).alias('concepts_shorter_right'), 
                            F.array_distinct(F.flatten(F.col('coauthors_shorter_right'))).alias('coauthors_shorter_right')) \
                    .withColumn('name_match_list_right', get_name_match_from_alternate_names('trans_name_right'))
                
                print("Round 3 Left side: ", left_side_3.cache().count())
                print("Round 3 Right side: ", right_side_3.cache().count())

                joined_df_3 = left_side_3.join(right_side_3, how='inner', on='author_id')\
                    .withColumn('insts_inter', F.size(F.array_intersect(F.col('institutions_left'), F.col('institutions_right')))*2) \
                    .withColumn('coauths_inter', F.size(F.array_intersect(F.col('coauthors_shorter_left'), F.col('coauthors_shorter_right')))*0.05) \
                    .withColumn('concps_inter', F.size(F.array_intersect(F.col('concepts_shorter_left'), F.col('concepts_shorter_right')))*0.01) \
                    .withColumn('cites_inter', F.size(F.array_intersect(F.col('citations_left'), F.col('citations_right')))*0.2) \
                    .withColumn('coauth_orcids_inter', F.size(F.array_intersect(F.col('coauthor_orcids_left'), F.col('coauthor_orcids_right')))*5) \
                    .withColumn('topics_inter', F.size(F.array_intersect(F.col('topic_id_left'), F.col('topic_id_right')))*5) \
                    .withColumn('subfields_inter', F.size(F.array_intersect(F.col('subfield_id_left'), F.col('subfield_id_right')))*1) \
                    .withColumn('fields_inter', F.size(F.array_intersect(F.col('field_id_left'), F.col('field_id_right')))*0.01) \
                    .select('author_id','orcid_left','trans_name_left','work_author_id_right','trans_name_right','insts_inter','coauths_inter','concps_inter','cites_inter',
                            'coauth_orcids_inter','topics_inter','subfields_inter','fields_inter','name_match_list_left','name_match_list_right') \
                    .withColumn('total_score', F.col('insts_inter') + F.col('coauths_inter') + F.col('concps_inter') + F.col('cites_inter') + F.col('coauth_orcids_inter') + 
                                F.col('topics_inter') + F.col('subfields_inter') + F.col('fields_inter')) \
                    .filter(F.col('total_score')>0) \
                    .withColumn('matched_names', check_block_vs_block(F.col('name_match_list_left'), F.col('name_match_list_right'))) \
                    .filter(F.col('matched_names')==1)
                
                work_authors_scored_3 = joined_df_3\
                    .select('author_id',F.array([F.array(F.col('orcid_left')), 'trans_name_left' ,F.array(F.col('work_author_id_right')), 
                                                'trans_name_right',F.array(F.col('total_score').cast(StringType()))]).alias('data_to_cluster')) \
                    .groupBy('author_id').agg(F.collect_list(F.col('data_to_cluster')).alias('data_to_cluster')) \
                    .withColumn('work_author_to_orcid_mapping', go_through_rows_for_author('data_to_cluster')) \
                    .select('author_id',F.explode('work_author_to_orcid_mapping').alias('work_author_to_orcid_mapping')) \
                    .select('author_id', 
                            F.col('work_author_to_orcid_mapping').getItem(0).alias('orcid_left'), 
                            F.col('work_author_to_orcid_mapping').getItem(1).alias('work_author_id_left'))

                work_authors_scored_3.cache().count()

                new_to_cluster_3 = all_features.alias('left_side_3_new').filter(F.col('orcid_len')>=2)\
                    .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2'))) \
                    .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups')) \
                    .filter(F.col('name_to_keep_ind')==1) \
                    .withColumn('trans_name', transform_name_for_search(F.col('author_2'))) \
                    .withColumn('name_len', F.length(F.col('trans_name'))) \
                    .filter(F.col('name_len')>1) \
                    .fillna(-1, subset=['topic_id','subfield_id','field_id']) \
                    .select(F.col('paper_id_2').alias('paper_id_left'), F.col('work_author_id_2').alias('work_author_id_left'), 
                                    F.col('citations_2').alias('citations_left'), F.col('institutions_2').alias('institutions_left'), 
                                    F.col('trans_name').alias('trans_name_left'), 
                                    F.col('author_id'), F.col('topic_id').alias('topic_id_left'), F.col('topic_name').alias('topic_name_left'), 
                                    F.col('concepts_shorter_2').alias('concepts_shorter_left'), F.col('coauthors_shorter_2').alias('coauthors_shorter_left'),
                                    F.col('subfield_id').alias('subfield_id_left'), F.col('field_id').alias('field_id_left'), 
                                    F.col('coauthor_orcids').alias('coauthor_orcids_left')) \
                    .join(work_authors_scored_3
                        .union(work_authors_scored_2.select(*work_authors_scored_3.columns))
                        .union(work_authors_scored.select(*work_authors_scored_3.columns)), 
                        how='inner', on=['author_id', 'work_author_id_left'])
                
                #################################### Writing final results to overmerge tables ####################################    
                (all_features.alias('left_side_3').filter(F.col('orcid_len')>=2)
                    .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2')))
                    .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups')) \
                    .filter(F.col('name_to_keep_ind')==1) \
                    .withColumn('trans_name', transform_name_for_search(F.col('author_2')))
                    .withColumn('name_len', F.length(F.col('trans_name')))
                    .filter(F.col('name_len')>1)
                    .fillna(-1, subset=['topic_id','subfield_id','field_id'])
                    .select(F.col('paper_id_2').alias('paper_id_left'), F.col('work_author_id_2').alias('work_author_id_left'), F.col('orcid_2').alias('orcid_left'), 
                                    F.col('citations_2').alias('citations_left'), F.col('institutions_2').alias('institutions_left'), F.col('trans_name').alias('trans_name_left'), 
                                    F.col('author_id'), F.col('topic_id').alias('topic_id_left'), F.col('topic_name').alias('topic_name_left'), 
                                    F.col('concepts_shorter_2').alias('concepts_shorter_left'), F.col('coauthors_shorter_2').alias('coauthors_shorter_left'),
                                    F.col('subfield_id').alias('subfield_id_left'), F.col('field_id').alias('field_id_left'), F.col('coauthor_orcids').alias('coauthor_orcids_left'))
                    .filter(F.col('orcid_left')!='')
                    .union(new_to_cluster_3.select('paper_id_left', 'work_author_id_left', 'orcid_left', 'citations_left', 'institutions_left', 
                                                'trans_name_left', 'author_id', 
                                                'topic_id_left', 'topic_name_left', 'concepts_shorter_left', 'coauthors_shorter_left', 'subfield_id_left',
                                                'field_id_left', 'coauthor_orcids_left'))
                    .groupBy(['author_id','orcid_left'])
                    .agg(F.collect_set(F.col('work_author_id_left')).alias('work_author_id_left'))
                    .join(author_ids_keep_orcid.select('author_id', F.col('orcid_2').alias('orcid_left')), how='leftanti', on=['author_id','orcid_left'])
                    .withColumn('current_date_str', F.current_date().cast(StringType()))
                    .select(F.concat_ws("-", F.col('author_id'), F.lit('orc'), F.col('orcid_left'), 
                                    F.col('current_date_str')).alias('work_author_id_for_cluster'), 
                            F.explode('work_author_id_left').alias('all_works_to_cluster'))
                    .withColumn("request_type", F.lit("orcid"))
                    .withColumn("request_date", F.current_timestamp())
                    .withColumn("partition_col", (F.rand()*40+1).cast(IntegerType()))
                    .repartition(6)
                    .write.format("jdbc") 
                    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
                    .option("dbtable", 'authorships.overmerged_authors') 
                    .option("user", secret['username']) 
                    .option("password", secret['password']) 
                    .option("driver", "org.postgresql.Driver") 
                    .mode("append") 
                    .save())

                right_side_4 = all_features.alias('right_side_3').filter(F.col('orcid_len')>=2) \
                    .filter(F.col('orcid_2')=='') \
                    .withColumn('non_latin_groups', group_non_latin_characters(F.col('author_2'))) \
                    .withColumn('name_to_keep_ind', name_to_keep_ind('non_latin_groups')) \
                    .filter(F.col('name_to_keep_ind')==1) \
                    .withColumn('trans_name', transform_name_for_search(F.col('author_2'))) \
                    .withColumn('name_len', F.length(F.col('trans_name'))) \
                    .filter(F.col('name_len')>1) \
                    .withColumn('name_match_list', get_name_match_list(F.col('trans_name'))) \
                    .fillna(-1, subset=['topic_id','subfield_id','field_id']) \
                    .select(F.col('paper_id_2').alias('paper_id_right'), F.col('work_author_id_2').alias('work_author_id_right'), F.col('orcid_2').alias('orcid_right'), 
                                    F.col('citations_2').alias('citations_right'), F.col('institutions_2').alias('institutions_right'), F.col('trans_name').alias('trans_name_right'), 
                                    F.col('author_id'), F.col('topic_id').alias('topic_id_right'), F.col('topic_name').alias('topic_name_right'), 
                                    F.col('concepts_shorter_2').alias('concepts_shorter_right'), F.col('coauthors_shorter_2').alias('coauthors_shorter_right'),
                                    F.col('subfield_id').alias('subfield_id_right'), F.col('field_id').alias('field_id_right'), F.col('coauthor_orcids').alias('coauthor_orcids_right')) \
                    .join(work_authors_scored.select('author_id',F.col('work_author_id_left').alias('work_author_id_right')), 
                        how='leftanti', on=['author_id', 'work_author_id_right']) \
                    .join(work_authors_scored_2.select('author_id',F.col('work_author_id_left').alias('work_author_id_right')), 
                        how='leftanti', on=['author_id', 'work_author_id_right']) \
                    .join(work_authors_scored_3.select('author_id',F.col('work_author_id_left').alias('work_author_id_right')), 
                        how='leftanti', on=['author_id', 'work_author_id_right'])
                    
                if right_side_4.count() > 0:
                    print("Getting leftovers grouped")
                    leftover_groups = right_side_4.select('author_id', 'trans_name_right', 'work_author_id_right') \
                        .groupBy(['author_id','trans_name_right']).agg(F.collect_list(F.col('work_author_id_right')).alias('work_author_id_right')) \
                        .select('author_id', 
                                    F.array([F.array(F.col('trans_name_right')).alias('trans_name_right'),
                                    F.col('work_author_id_right')]).alias('data_to_cluster')) \
                        .groupBy('author_id').agg(F.collect_list('data_to_cluster').alias('data_to_cluster')) \
                        .withColumn('work_author_to_group_mapping', go_through_leftovers_for_author('data_to_cluster')) \
                        .select('author_id',F.explode('work_author_to_group_mapping').alias('work_author_to_group_mapping')) \
                        .select('author_id', 
                                F.col('work_author_to_group_mapping').getItem(0).getItem(0).alias('group_cluster_id'), 
                                F.col('work_author_to_group_mapping').getItem(1).alias('work_author_id_left'))
                        
                    (leftover_groups
                        .withColumn('current_date_str', F.current_date().cast(StringType()))
                        .select(F.concat_ws("-", F.col('group_cluster_id'), F.lit('orc'), 
                                    F.col('current_date_str')).alias('work_author_id_for_cluster'),
                                F.explode('work_author_id_left').alias('all_works_to_cluster'))
                        .withColumn("request_type", F.lit("orcid"))
                        .withColumn("request_date", F.current_timestamp())
                        .withColumn("partition_col", (F.rand()*40+1).cast(IntegerType()))
                        .repartition(6)
                        .write.format("jdbc") 
                        .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") 
                        .option("dbtable", 'authorships.overmerged_authors') 
                        .option("user", secret['username']) 
                        .option("password", secret['password']) 
                        .option("driver", "org.postgresql.Driver") 
                        .mode("append")
                        .save())
                
                right_side_3.unpersist()
                left_side_3.unpersist()
                right_side_4.unpersist()
                
        # something here to depersist
        author_ids_keep_orcid.unpersist()
        left_side.unpersist()
        right_side.unpersist()
        work_authors_scored.unpersist()
        work_authors_scored_2.unpersist()
        work_authors_scored_3.unpersist()
        left_side_2.unpersist()
        right_side_2.unpersist()
        all_features.unpersist()

# COMMAND ----------


