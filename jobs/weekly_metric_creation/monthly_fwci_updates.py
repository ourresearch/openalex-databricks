# Databricks notebook source
import pickle
import boto3
import re
import json
import pandas as pd
pd.set_option('display.max_rows', 100)
import numpy as np
import heroku3
from datetime import datetime

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, FloatType, ArrayType, DoubleType, StructType, StructField,LongType, BooleanType

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

start_datetime = datetime.now()
curr_date = start_datetime.strftime("%Y_%m_%d_%H_%M")
database_copy_save_path = f"{buckets['database_copy_save_path']}"
temp_save_path = f"{buckets['temp_save_path']}/{curr_date}_fwci/"

# COMMAND ----------

# MAGIC %md ## FWCI

# COMMAND ----------

@udf(returnType=StringType())
def type_crossref(looks_like_paratext, genre, journal, journal_type, doc_type):
    # legacy type used < 2023-08
    # (but don't get rid of it, it's used to derive the new type (display_genre))
    if looks_like_paratext:
        return "other"
    if genre:
        return genre
    if doc_type:
        lookup_mag_to_crossref_type = {
            "Journal": "journal-article",
            "Thesis": "dissertation",
            "Conference": "proceedings-article",
            "Repository": "posted-content",
            "Book": "book",
            "BookChapter": "book-chapter",
            "Dataset": "dataset",
        }
        if mag_type := lookup_mag_to_crossref_type.get(doc_type):
            return mag_type
    if journal and journal_type and 'book' in journal_type:
        return 'book-chapter'
    return 'journal-article'

REVIEW_JOURNAL_IDS = {206848516, 120397320, 111502347, 163102221, 316438,
                      101036573, 70620193, 160464432, 12874817, 4210178125,
                      169446993, 62674001, 7783507, 26843219, 157499488,
                      112662651, 186543748, 67216020, 2898133657, 129217179,
                      172508320, 201273009, 81141427, 96726197, 157921468,
                      183292605, 19775678, 92651206, 28635856, 116269268,
                      41981144, 18618073, 8553189, 37062379, 156364528,
                      78310130, 51877618, 2491932416, 88857351, 10734860,
                      2764917516, 93373720, 4210174246, 114927911, 4210234670,
                      83493678, 22232893, 90458436, 182717252, 181418318,
                      56802129, 150570842, 10577759, 129765729, 33396077,
                      76837745, 144797041, 124600697, 4632444, 41143188,
                      4210168218, 196724125, 4210201506, 24135080, 55210418,
                      192051125, 78119372, 116952013, 161680845, 2755384784,
                      4210198484, 62841813, 38078933, 118162903, 43764696,
                      4210219483, 173474783, 105347553}

@udf(returnType=StringType())
def display_genre(looks_like_paratext, original_title, is_review, is_preprint, guess_type_from_title, is_libguide, type_crossref):
    # this is what goes into the `Work.type` attribute
    if looks_like_paratext:
        return "paratext"
    if original_title and 'supplementary table' in original_title.lower():
        return 'supplementary-materials'
    if is_review:
        return 'review'
    if is_preprint:
        return 'preprint'
    if is_libguide:
        return 'libguides'

    # infer "erratum", "editorial", "letter" types:
    try:
        if guess_type_from_title:
            # todo: do another pass at this. improve precision and recall.
            return guess_type_from_title
    except AttributeError:
        pass
    lookup_crossref_to_openalex_type = {
        "journal-article": "article",
        "proceedings-article": "article",
        "posted-content": "article",
        "book-part": "book-chapter",
        "journal-issue": "paratext",
        "journal": "paratext",
        "journal-volume": "paratext",
        "report-series": "paratext",
        "proceedings": "paratext",
        "proceedings-series": "paratext",
        "book-series": "paratext",
        "component": "paratext",
        "monograph": "book",
        "reference-book": "book",
        "book-set": "book",
        "edited-book": "book",
    }
    # return mapping from lookup if it's in there, otherwise pass-through
    return lookup_crossref_to_openalex_type.get(type_crossref, type_crossref)

@udf(returnType=BooleanType())
def is_review_old(journal_id, original_title):
    return journal_id in REVIEW_JOURNAL_IDS or (original_title and 'a review' in original_title.lower())

@udf(returnType=BooleanType())
def is_review(journal_id, original_title):
    review_phrases = ["systematic review","systematic analysis", "pooled analysis", "meta-analysis of", "meta analysis of", "review of particle physics", 
                      "scoping review", "meta-analytic review", "a review","literature review"]
    return journal_id in REVIEW_JOURNAL_IDS or (original_title and any(review_phrase in original_title.lower() for review_phrase in review_phrases))

@udf(returnType=StringType())
def guess_type_from_title(work_title):
    erratum_exprs = [
        r'^erratum',
    ]
    for expr in erratum_exprs:
        if work_title and re.search(expr, work_title, re.IGNORECASE):
            return "erratum"

    letter_exprs = [
        r'^letter:',
        r'^letter to',
        r'^letter$',
        r'^\[letter to',
    ]
    for expr in letter_exprs:
        if work_title and re.search(expr, work_title, re.IGNORECASE):
            return "letter"

    editorial_exprs = [
        r'^editorial:',
        r'^editorial$',
        r'^editorial comment',
        r'^guest editorial',
        r'^editorial note',
        r'^editorial -'
    ]
    for expr in editorial_exprs:
        if work_title and re.search(expr, work_title, re.IGNORECASE):
            return "editorial"

    return None


@udf(returnType=BooleanType())
def looks_like_paratext(is_paratext, work_title):
        if is_paratext:
            return True

        paratext_exprs = [
            r'^Author Guidelines$',
            r'^Author Index$'
            r'^Back Cover',
            r'^Back Matter',
            r'^Contents$',
            r'^Contents:',
            r'^Cover Image',
            r'^Cover Picture',
            r'^Editorial Board',
            r'Editor Report$',
            r'^Front Cover',
            r'^Frontispiece',
            r'^Graphical Contents List$',
            r'^Index$',
            r'^Inside Back Cover',
            r'^Inside Cover',
            r'^Inside Front Cover',
            r'^Issue Information',
            r'^List of contents',
            r'^List of Tables$',
            r'^List of Figures$',
            r'^List of Plates$',
            r'^Masthead',
            r'^Pages de début$',
            r'^Title page',
            r"^Editor's Preface",
        ]

        for expr in paratext_exprs:
            if work_title and re.search(expr, work_title, re.IGNORECASE):
                return True

        return False

# COMMAND ----------

@udf(returnType=FloatType())
def get_final_fwci_for_work_type(fwci, work_type):
    types_to_keep = ['article', 'conference_article', 'book', 'review', 'book-chapter']
    if work_type in types_to_keep:
        return fwci
    else:
        return None

# COMMAND ----------

# MAGIC %md #### Getting merged works and updating mid.citation with merged data

# COMMAND ----------

work_merges = spark.read.parquet(f"{database_copy_save_path}/mid/work")\
    .select('paper_id', F.col('merge_into_id').alias('merge_into_paper_id'))\
    .dropDuplicates()
work_merges.cache().count()

work_merges_2 = work_merges.alias('work_merges_2')\
    .select(F.col('paper_id').alias('paper_reference_id'), F.col('merge_into_paper_id').alias('merge_into_paper_ref_id'))

work_merges_2.cache().count()

works_1 = spark.read.parquet(f"{database_copy_save_path}/mid/work")\
    .dropDuplicates() \
    .filter(F.col('merge_into_id').isNull()) \
    .select(F.col('paper_id').alias('paper_reference_id'), F.col('publication_date').alias('pub_date'), 'doc_type','journal_id')
works_1.cache().count()

works_2 = spark.read.parquet(f"{database_copy_save_path}/mid/work")\
    .dropDuplicates() \
    .filter(F.col('merge_into_id').isNull()) \
    .select('paper_id', F.col('publication_date').alias('citation_date'))
works_2.cache().count()

# COMMAND ----------

spark.read.parquet(f"{database_copy_save_path}/mid/citation") \
    .dropDuplicates() \
    .join(work_merges, how='inner', on='paper_id') \
    .join(work_merges_2, how='inner', on='paper_reference_id') \
    .withColumn('final_paper_id', F.when(F.col('merge_into_paper_id').isNotNull(), 
                                         F.col('merge_into_paper_id')).otherwise(F.col('paper_id'))) \
    .withColumn('final_paper_ref_id', F.when(F.col('merge_into_paper_ref_id').isNotNull(), 
                                             F.col('merge_into_paper_ref_id')).otherwise(F.col('paper_reference_id'))) \
    .select('final_paper_id', 'final_paper_ref_id').dropDuplicates() \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}fwci/citations_with_merges")

# COMMAND ----------

spark.read.parquet(f"{temp_save_path}fwci/citations_with_merges")\
    .select(F.col('final_paper_id').alias('paper_id'), F.col('final_paper_ref_id').alias('paper_reference_id')) \
    .join(works_1, on='paper_reference_id', how='left') \
    .join(works_2, on='paper_id', how='left') \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}fwci/citations_for_analysis")

# COMMAND ----------

# MAGIC %md #### Getting latest topics/subfields

# COMMAND ----------

topics = spark.read.parquet(f"{database_copy_save_path}/mid/topic")
subfields = spark.read.parquet(f"{database_copy_save_path}/mid/subfield")

primary_subfields = spark.read.parquet(f"{database_copy_save_path}/mid/work_topic").filter(F.col('topic_rank')==1)\
    .select(F.col('paper_id').alias('pub_paper'), 'topic_id').dropDuplicates(subset=['pub_paper']) \
    .join(topics.select('topic_id', 'subfield_id'), how='inner', on='topic_id') \
    .join(subfields, how='inner', on='subfield_id') \
    .select('pub_paper', 'topic_id', 'subfield_id','display_name')
primary_subfields.cache().count()

# COMMAND ----------

# MAGIC %md #### Attaching subfields to paper

# COMMAND ----------

citations_for_analysis = spark.read.parquet(f"{temp_save_path}fwci/citations_for_analysis")\
    .filter(F.col('pub_date').isNotNull() & F.col('citation_date').isNotNull()) \
    .select(F.col('paper_reference_id').alias('pub_paper'), F.to_date(F.col('pub_date'), 'yyyy-MM-dd').alias('publication_date'),
            'doc_type', F.col('paper_id').alias('citation_paper'), F.to_date(F.col('citation_date'), 'yyyy-MM-dd').alias('citation_date')) \
    .withColumn('pub_year', F.year(F.col('publication_date'))) \
    .withColumn('citation_year', F.year(F.col('citation_date'))) \
    .filter(F.col('citation_year') >= F.col('pub_year'))

citations_for_analysis \
    .join(primary_subfields, on='pub_paper', how='left') \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}fwci/citations_to_analyze_with_topics_attached")

primary_subfields \
    .join(citations_for_analysis, on='pub_paper', how='leftanti') \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}fwci/papers_with_no_citations")

# COMMAND ----------

# MAGIC %md #### Window functions and dataframes needed for calculating yearly citation counts

# COMMAND ----------

windowval = (Window.partitionBy('pub_paper').orderBy(F.col('year_diff'))
             .rowsBetween(Window.unboundedPreceding, 0))

year_diffs = list(range(0,100))

all_dates_and_diffs_df = pd.DataFrame(zip(year_diffs), columns=['year_diff'])
all_dates_and_diffs = spark.createDataFrame(all_dates_and_diffs_df).select('year_diff')
all_dates_and_diffs.cache().count()

w2 = Window.partitionBy('pub_paper')

min_year_to_look_at = datetime.now().year - 100
max_year_to_look_at = datetime.now().year

# COMMAND ----------

# MAGIC %md #### Getting work types (this should change in near future to just read from a work_types table)

# COMMAND ----------

# public.libguides_paper_ids

df = (spark.read
.format("postgresql")
.option("dbtable", 
        f"(select * from public.libguides_paper_ids) as new_table")
.option("host", secret['host'])
.option("port", secret['port'])
.option("database", secret['dbname'])
.option("user", secret['username'])
.option("password", secret['password'])
.load())

df \
.write.mode('overwrite') \
.parquet(f"{database_copy_save_path}/public/libguides_paper_ids")

journal_types = spark.read.parquet(f"{database_copy_save_path}/mid/journal").filter(F.col('merge_into_id').isNull()) \
    .select('journal_id',F.col('display_name').alias('journal_name'), F.col('type').alias('journal_type'))
journal_types.cache().count()

libguides = spark.read.parquet(f"{database_copy_save_path}/public/libguides_paper_ids").select(F.col('paper_id').alias('paper_reference_id'))\
    .withColumn('is_libguide', F.lit(True)).dropDuplicates()
libguides.cache().count()

# COMMAND ----------

preprints = spark.read.csv(f"{database_copy_save_path}/temp_files_for_analysis/preprints.csv")\
    .select(F.col('_c0').alias('paper_reference_id').cast(IntegerType()))\
    .withColumn('is_preprint', F.lit(True)).dropDuplicates()
preprints.cache().count()

# COMMAND ----------

work_types = spark.read.parquet(f"{database_copy_save_path}/mid/work")\
    .filter(F.col('merge_into_id').isNull()) \
    .select(F.col('paper_id').alias('paper_reference_id'), 'doc_type', 'genre', 'journal_id','original_title','is_paratext')\
    .join(journal_types, on='journal_id', how='left') \
    .join(libguides, on='paper_reference_id', how='left') \
    .join(preprints, on='paper_reference_id', how='left') \
    .fillna(False, subset=['is_libguide', 'is_paratext']) \
    .withColumn('is_review', is_review(F.col('journal_id'), F.col('original_title'))) \
    .withColumn('guess_type_from_title', guess_type_from_title(F.lower(F.col('original_title')))) \
    .withColumn('looks_like_paratext', looks_like_paratext(F.col('is_paratext'), F.col('original_title'))) \
    .withColumn('type_crossref', type_crossref(F.col('looks_like_paratext'), F.col('genre'), F.col('journal_id'), F.col('journal_type'), F.col('doc_type'))) \
    .withColumn('display_genre', display_genre(F.col('looks_like_paratext'), F.col('original_title'), F.col('is_review'), F.col('is_preprint'), 
                                               F.col('guess_type_from_title'), F.col('is_libguide'), F.col('type_crossref'))) \
    .withColumn('work_type_final', F.when((F.col('display_genre')=='article') & (F.col('journal_type')=='conference'), 'conference_article').otherwise(F.col('display_genre'))) \
    .select('paper_reference_id', 'original_title', 'work_type_final')

work_types.cache().count()

# COMMAND ----------

# MAGIC %md #### Getting one row per publication (with subfield and work type attached)

# COMMAND ----------

spark.read.parquet(f"{temp_save_path}fwci/citations_to_analyze_with_topics_attached")\
    .withColumn('pub_year', F.when(F.col('pub_year')<min_year_to_look_at, min_year_to_look_at).otherwise(F.col('pub_year'))) \
    .withColumn('pub_year', F.when(F.col('pub_year')>max_year_to_look_at, max_year_to_look_at).otherwise(F.col('pub_year'))) \
    .withColumn('citation_year', F.when(F.col('citation_year')<min_year_to_look_at, min_year_to_look_at).otherwise(F.col('citation_year'))) \
    .withColumn('citation_year', F.when(F.col('citation_year')>max_year_to_look_at, max_year_to_look_at).otherwise(F.col('citation_year'))) \
    .select('pub_paper','publication_date','pub_year',F.col('subfield_id')).dropDuplicates() \
    .join(work_types.select(F.col('paper_reference_id').alias('pub_paper'), F.col('work_type_final').alias('work_type')), how='left', on='pub_paper') \
    .withColumn('year_diff_max', (F.date_format(F.current_timestamp(), 'y')  -  F.col('pub_year')).cast(IntegerType())) \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}fwci/single_row_for_each_paper")

# COMMAND ----------

# MAGIC %md #### Getting total citation counts

# COMMAND ----------

spark.read.parquet(f"{temp_save_path}fwci/citations_to_analyze_with_topics_attached") \
    .withColumn('pub_year', F.when(F.col('pub_year')<min_year_to_look_at, min_year_to_look_at).otherwise(F.col('pub_year'))) \
    .withColumn('pub_year', F.when(F.col('pub_year')>max_year_to_look_at, max_year_to_look_at).otherwise(F.col('pub_year'))) \
    .withColumn('citation_year', F.when(F.col('citation_year')<min_year_to_look_at, min_year_to_look_at).otherwise(F.col('citation_year'))) \
    .withColumn('citation_year', F.when(F.col('citation_year')>max_year_to_look_at, max_year_to_look_at).otherwise(F.col('citation_year'))) \
    .groupBy('pub_paper').agg(F.count(F.col('citation_year')).alias('count')) \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}fwci/paper_citation_counts")

# COMMAND ----------

# MAGIC %md #### Getting citation counts by "year_diff" (difference in years between pub date and citation_date)

# COMMAND ----------

spark.read.parquet(f"{temp_save_path}fwci/citations_to_analyze_with_topics_attached") \
    .withColumn('pub_year', F.when(F.col('pub_year')<min_year_to_look_at, min_year_to_look_at).otherwise(F.col('pub_year'))) \
    .withColumn('pub_year', F.when(F.col('pub_year')>max_year_to_look_at, max_year_to_look_at).otherwise(F.col('pub_year'))) \
    .withColumn('citation_year', F.when(F.col('citation_year')<min_year_to_look_at, min_year_to_look_at).otherwise(F.col('citation_year'))) \
    .withColumn('citation_year', F.when(F.col('citation_year')>max_year_to_look_at, max_year_to_look_at).otherwise(F.col('citation_year'))) \
    .withColumn('year_diff', F.col('citation_year')-F.col('pub_year')) \
    .groupBy(['pub_paper','year_diff']).agg(F.count(F.col('citation_year')).alias('count')).orderBy('year_diff') \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}fwci/paper_citation_counts_by_year_diff")

# COMMAND ----------

single_rows = spark.read.parquet(f"{temp_save_path}fwci/single_row_for_each_paper")
citation_counts_by_year_diff = spark.read.parquet(f"{temp_save_path}fwci/paper_citation_counts_by_year_diff")

# COMMAND ----------

all_dates_and_diffs.join(single_rows, how='left')\
    .join(citation_counts_by_year_diff, how='left', on=['pub_paper','year_diff']).orderBy('year_diff').fillna(0) \
    .filter(F.col('year_diff')<=3) \
    .filter(F.col('year_diff')<=F.col('year_diff_max')) \
    .filter(F.col('year_diff')>=0) \
    .withColumn('pub_plus_3_citations', F.sum('count').over(windowval))\
    .withColumn('year_diff_max', F.when(F.col('year_diff_max')>3, 3).otherwise(F.col('year_diff_max'))) \
    .filter(F.col('year_diff')==F.col('year_diff_max')) \
    .select(F.col('pub_paper').alias('paper_id'),F.col('pub_year').alias('publication_year'),
            F.col('subfield_id'),'work_type', 'pub_plus_3_citations') \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}fwci/paper_citation_counts_latest")

# COMMAND ----------

# MAGIC %md #### Pulling in papers with no citations

# COMMAND ----------

no_citations = spark.read.parquet(f"{temp_save_path}fwci/papers_with_no_citations") \
    .join(works_1.select(F.col('paper_reference_id').alias('pub_paper'), F.year(F.col('pub_date')).alias('pub_year')).dropDuplicates(subset=['pub_paper']), 
          how='left', on='pub_paper') \
    .select('pub_paper', 'pub_year', F.col('subfield_id')) \
    .withColumn('pub_year', F.when(F.col('pub_year')<min_year_to_look_at, min_year_to_look_at).otherwise(F.col('pub_year'))) \
    .withColumn('pub_year', F.when(F.col('pub_year')>max_year_to_look_at, max_year_to_look_at).otherwise(F.col('pub_year'))) \
    .withColumn('pub_plus_3_citations', F.lit(0)) \
    .select(F.col('pub_paper').alias('paper_id'), F.col('pub_year').alias('publication_year'), F.col('subfield_id'), 'pub_plus_3_citations') \
    .join(work_types.select(F.col('paper_reference_id').alias('paper_id'), F.col('work_type_final').alias('work_type')), how='inner', on='paper_id')

# COMMAND ----------

# MAGIC %md #### Getting expected citation counts

# COMMAND ----------

spark.read.parquet(f"{temp_save_path}fwci/paper_citation_counts_latest") \
    .select('paper_id','publication_year','subfield_id','work_type','pub_plus_3_citations') \
    .union(no_citations.select('paper_id','publication_year','subfield_id','work_type','pub_plus_3_citations')) \
    .groupBy(['publication_year','subfield_id','work_type']).agg(F.mean('pub_plus_3_citations').alias('expected_citations')) \
    .select(F.col('publication_year'), F.col('subfield_id'), F.col('work_type'), F.round('expected_citations',8).alias('expected_citations'))  \
    .filter(F.col('publication_year')<=max_year_to_look_at) \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}fwci/expected_paper_citation_counts")

# COMMAND ----------

# MAGIC %md #### Get final FWCI

# COMMAND ----------

expected_counts = spark.read.parquet(f"{temp_save_path}fwci/expected_paper_citation_counts")
expected_counts.cache().count()

# COMMAND ----------

citation_counts_latest = spark.read.parquet(f"{temp_save_path}fwci/paper_citation_counts_latest") \
    .select('paper_id','publication_year','subfield_id','pub_plus_3_citations') \
    .union(no_citations.select('paper_id','publication_year','subfield_id','pub_plus_3_citations'))

# COMMAND ----------

work_types.select(F.col('paper_reference_id').alias('paper_id'), F.col('work_type_final').alias('work_type'))\
    .join(citation_counts_latest.select('paper_id','publication_year','subfield_id','pub_plus_3_citations'), how='left', on='paper_id') \
    .join(expected_counts, how='left', on=['publication_year','subfield_id','work_type']) \
    .withColumn('expected_citations', F.when(F.col('expected_citations').isNull(), -1.0).otherwise(F.col('expected_citations'))) \
    .withColumn('fwci', F.col('pub_plus_3_citations')/F.col('expected_citations')) \
    .select('paper_id','publication_year','subfield_id','work_type','pub_plus_3_citations',
            F.round(F.col('fwci'), 5).alias('fwci')) \
    .withColumn('fwci', F.when(F.col('fwci').isNull(), F.col('fwci')).otherwise(get_final_fwci_for_work_type(F.col('fwci'), F.col('work_type')))) \
    .write.mode('overwrite') \
    .parquet(f"{temp_save_path}fwci/final_fwci_all_papers")

# COMMAND ----------

spark.read.parquet(f"{temp_save_path}fwci/final_fwci_all_papers").count()

# COMMAND ----------

# for first write only
spark.read.parquet(f"{temp_save_path}fwci/final_fwci_all_papers") \
    .select('paper_id','publication_year','subfield_id','work_type','pub_plus_3_citations','fwci') \
    .withColumn("update_date", F.current_timestamp()) \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/counts/work_fwci")

# COMMAND ----------

# for first write only
spark.read.parquet(f"{temp_save_path}fwci/expected_paper_citation_counts") \
    .select('publication_year','subfield_id','work_type','expected_citations') \
    .withColumn("update_date", F.current_timestamp()) \
    .write.mode('overwrite') \
    .parquet(f"{database_copy_save_path}/counts/expected_citations")

# COMMAND ----------

# MAGIC %md #### Check for changes to table (only write changes for works that have changed for under pub_year + 3 years old)

# COMMAND ----------

old_expected_counts = spark.read.parquet(f"{database_copy_save_path}/counts/expected_citations").select('publication_year','subfield_id','work_type','expected_citations')

# COMMAND ----------

old_fwci = spark.read.parquet(f"{database_copy_save_path}/counts/work_fwci").select('paper_id','publication_year','subfield_id','work_type','fwci')

# COMMAND ----------

new_expected_counts = spark.read.parquet(f"{temp_save_path}fwci/expected_paper_citation_counts")

# COMMAND ----------

new_fwci = spark.read.parquet(f"{temp_save_path}fwci/final_fwci_all_papers")

# COMMAND ----------

# TO BE COMPLETED

# check that for a paper ID, the number of citations at the end of pub_year + 3, work_type, year, and subfield are all the same (if not, re-write the fwci)

# need to differentiate between total number of citations vs number of citations at the end of pub_year + 3

# COMMAND ----------

# MAGIC %md #### Save to postgres

# COMMAND ----------

dynos_to_shutdown = ['fast_store_works']
curr_q = shutdown_dynos(heroku_secret['heroku_token'], dynos_to_shutdown)
curr_q

# COMMAND ----------

spark.read.parquet(f"{database_copy_save_path}/counts/work_fwci") \
    .repartition(20) \
    .write.format("jdbc") \
    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") \
    .option("dbtable", 'counts.work_fwci') \
    .option("user", secret['username']) \
    .option("password", secret['password']) \
    .option("driver", "org.postgresql.Driver") \
    .option("truncate", True) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

spark.read.parquet(f"{database_copy_save_path}/counts/expected_citations") \
    .repartition(20) \
    .write.format("jdbc") \
    .option("url", f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}") \
    .option("dbtable", 'counts.expected_citations') \
    .option("user", secret['username']) \
    .option("password", secret['password']) \
    .option("driver", "org.postgresql.Driver") \
    .option("truncate", True) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

_ = restart_dynos(heroku_secret['heroku_token'], dynos_to_shutdown, curr_q)
