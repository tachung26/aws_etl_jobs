import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from functools import reduce
from pyspark.sql import SparkSession
import pyspark.sql.functions as sqlf
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, split, concat, lit, trim, countDistinct, count, row_number, expr

from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session 

job = Job(glueContext)
job.init(args['JOB_NAME'], args)




def filter_column(df, column, allowed_values):
    return df.select(
        when(col(column).isin(allowed_values), col(column)).otherwise(None).alias(column)
    )


def drop_negative_values(df, columns):
    for column in columns:
        df = df.withColumn(column, when(col(column) >= 0, col(column)).otherwise(None))
    return df


def drop_null_columns(df):
    total_count = df.count()
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).first().asDict()
    to_drop = [col_name for col_name, null_count in null_counts.items() if null_count == total_count]
    return df.drop(*to_drop)
    
    
    
def replace_non_numeric_except(df, exclude_columns):
    column_exprs = [
        when(col(c).cast("double").isNotNull(), col(c)).otherwise(None).alias(c) if c not in exclude_columns else col(c)
        for c in df.columns
    ]    
    return df.select(*column_exprs)
    
    
    
def check_and_replace_non_categorical(df, exclude_prefix, distinct_threshold=20):
    columns_to_check = [column for column in df.columns if column.startswith(exclude_prefix)]
    distinct_counts = df.agg(*[countDistinct(col(c)).alias(c) for c in columns_to_check]).collect()[0].asDict()
    column_exprs = [
        when(col(c).isNotNull(), None).alias(c) if distinct_counts[c] > distinct_threshold else col(c)
        for c in columns_to_check
    ]    
    other_columns = [col(c) for c in df.columns if c not in columns_to_check]  
    return df.select(*other_columns, *column_exprs)
    
    
    
def cast_all_columns_to_string(df):   
    column_exprs = [col(c).cast("string").alias(c) for c in df.columns] 
    return df.select(*column_exprs)


def trim_all_string_columns(df):
    string_cols = [c for c, dtype in df.dtypes if dtype == 'string']

    column_exprs = [
        trim(col(c)).alias(c) if c in string_cols else col(c) 
        for c in df.columns
    ]    
    return df.select(*column_exprs)


def write_output_to_s3(spark_df, s3_output_path):
 spark_df.write.mode("append").csv(s3_output_path, header=True)



s3 = boto3.client('s3')
years = ['2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']
for year in years:
    
    s3_path = f"s3://ncif.ai.dev/FFIEC - HMDA/01_raw/hmda_{year}_nationwide_all-records.csv"
    s3_output_path = f"s3://ncif.ai.dev/FFIEC - HMDA/02_processed/proccessed_{year}/"
    
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(s3_path)
    
    
    df_str = df.select([col(c).cast("string").alias(c) for c in df.columns])
    
    
    df_str.createOrReplaceTempView("hmda")
    sql_query = """
        SELECT
        `as_of_year`,
 
        `respondent_id`,
    
        CASE 
            WHEN `agency_name` REGEXP '^[^0-9]+$' THEN `agency_name` 
            ELSE NULL 
        END AS `agency_name`,
        
        CASE 
            WHEN `agency_abbr` REGEXP '^[^0-9]+$' THEN `agency_abbr` 
            ELSE NULL 
        END AS `agency_abbr`,
    
        CASE 
            WHEN `agency_code` REGEXP '^[0-9]+$' THEN `agency_code` 
            ELSE NULL 
        END AS `agency_code`,
        
        CASE 
            WHEN `loan_type_name` REGEXP '^[^0-9]+$' THEN `loan_type_name` 
            ELSE NULL 
        END AS `loan_type_name`,
        
        CASE WHEN `loan_type` IN ('1', '2', '3', '4') THEN `loan_type` ELSE NULL END AS `loan_type`,

        CASE 
            WHEN `property_type_name` REGEXP '^[^0-9]+$' THEN `property_type_name` 
            ELSE NULL 
        END AS `property_type_name`,
        
        CASE WHEN `property_type` IN ('1', '2', '3') THEN `property_type` ELSE NULL END AS `property_type`,
        
        CASE 
            WHEN `loan_purpose_name` REGEXP '^[^0-9]+$' THEN `loan_purpose_name` 
            ELSE NULL 
        END AS `loan_purpose_name`,
        
        CASE WHEN `loan_purpose` IN ('1', '2', '3', '4') THEN `loan_purpose` ELSE NULL END AS `loan_purpose`,
        
        CASE 
            WHEN `owner_occupancy_name` REGEXP '^[^0-9]+$' THEN `owner_occupancy_name` 
            ELSE NULL 
        END AS `owner_occupancy_name`,
        
        CASE WHEN `owner_occupancy` IN ('1', '2', '3') THEN `owner_occupancy` ELSE NULL END AS `owner_occupancy`,
        
        CASE WHEN (CAST(`loan_amount_000s` AS double) IS NOT NULL AND CAST(`loan_amount_000s` AS double) >= 0) THEN `loan_amount_000s` ELSE NULL END AS `loan_amount_000s`,
        
        CASE 
            WHEN `preapproval_name` REGEXP '^[^0-9]+$' THEN `preapproval_name` 
            ELSE NULL 
        END AS `preapproval_name`,
        
        CASE WHEN `preapproval` IN ('1', '2', '3') THEN `preapproval` ELSE NULL END AS `preapproval`,
        
        CASE 
            WHEN `action_taken_name` REGEXP '^[^0-9]+$' THEN `action_taken_name` 
            ELSE NULL 
        END AS `action_taken_name`,
        
        CASE WHEN `action_taken` IN ('1', '2', '3', '4', '5', '6', '7', '8') THEN `action_taken` ELSE NULL END AS `action_taken`,
        
        CASE 
            WHEN `msamd_name` REGEXP '^[^0-9]+$' THEN `msamd_name` 
            ELSE NULL 
        END AS `msamd_name`,
        
        CASE 
            WHEN `msamd` REGEXP '^[0-9]+$' OR `msamd` IN ('NA') THEN  `msamd` 
            ELSE NULL 
        END AS `msamd`,
        
        CASE 
            WHEN `state_name` REGEXP '^[^0-9]+$' THEN `state_name` 
            ELSE NULL 
        END AS `state_name`,
        
        CASE WHEN (LENGTH(`state_abbr`) = 2 AND `state_abbr` LIKE '%[^A-Za-z]%') THEN `state_abbr` ELSE NULL END AS `state_abbr`,
        
        CASE WHEN (LENGTH(`state_code`) >= 1 AND `state_code` NOT LIKE '%[^A-Za-z]%') THEN `state_code` ELSE NULL END AS `state_code`,
        
        CASE 
            WHEN `county_name` REGEXP '^[^0-9]+$' THEN `county_name` 
            ELSE NULL 
        END AS `county_name`,
        
        CASE WHEN (LENGTH(`county_code`) >= 2 AND `county_code` NOT LIKE '%[^A-Za-z]%') THEN `county_code` ELSE NULL END AS `county_code`,
        
        CASE WHEN CAST(`census_tract_number` AS double) IS NOT NULL THEN `census_tract_number` ELSE NULL END AS `census_tract_number`,

        CASE
          WHEN `state_code` IS NULL OR `county_code` IS NULL OR `census_tract_number` IS NULL THEN NULL
          ELSE
            LPAD(`state_code`, 2, '0') ||   
            LPAD(`county_code`, 3, '0') ||  
            LPAD(
              CASE 
                WHEN `census_tract_number` LIKE '%.%' THEN CAST(`census_tract_number` * POWER(10, LENGTH(SPLIT(`census_tract_number`, '\\.')[1])) AS BIGINT)
                ELSE `census_tract_number`
              END, 6, '0'  
            )
        END AS `census_tract`,

        
        CASE
            WHEN `applicant_ethnicity_name` IN ('Not Hispanic or Latino', 'Hispanic or Latino') THEN `applicant_ethnicity_name`
            WHEN `applicant_ethnicity_name` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `applicant_ethnicity_name`,
        
        CASE WHEN `applicant_ethnicity` IN ('1', '2', '3', '4') THEN `applicant_ethnicity` ELSE NULL END AS `applicant_ethnicity`,
        
        CASE
            WHEN `co_applicant_ethnicity_name` IN ('Not Hispanic or Latino', 'Hispanic or Latino', 'No co-applicant') THEN `co_applicant_ethnicity_name`
            WHEN `co_applicant_ethnicity_name` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `co_applicant_ethnicity_name`,
        
        CASE WHEN `co_applicant_ethnicity` IN ('1', '2', '3', '4', '5') THEN `co_applicant_ethnicity` ELSE NULL END AS `co_applicant_ethnicity`,
    
        CASE
            WHEN `applicant_race_name_1` IN ('American Indian or Alaskan Native',  'Asian or Pacific Islander', 'Black', 'Hispanic', 'White', 'Other', 'Native Hawaiian or Other Pacific Islander') THEN `applicant_race_name_1`
            WHEN `applicant_race_name_1` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `applicant_race_name_1`,
        
        CASE WHEN `applicant_race_1` IN ('1', '2', '3', '4', '5', '6', '7') THEN `applicant_race_1` ELSE NULL END AS `applicant_race_1`,
        
        CASE
            WHEN `applicant_race_name_2` IN ('American Indian or Alaskan Native',  'Asian or Pacific Islander', 'Black', 'Hispanic', 'White', 'Other', 'Native Hawaiian or Other Pacific Islander') THEN `applicant_race_name_2`
            WHEN `applicant_race_name_2` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `applicant_race_name_2`,
        
        CASE WHEN `applicant_race_2` IN ('1', '2', '3', '4', '5', '6', '7') THEN `applicant_race_2` ELSE NULL END AS `applicant_race_2`,
        
        CASE
            WHEN `applicant_race_name_3` IN ('American Indian or Alaskan Native',  'Asian or Pacific Islander', 'Black', 'Hispanic', 'White', 'Other', 'Native Hawaiian or Other Pacific Islander') THEN `applicant_race_name_3`
            WHEN `applicant_race_name_3` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `applicant_race_name_3`,

        CASE WHEN `applicant_race_3` IN ('1', '2', '3', '4', '5', '6', '7') THEN `applicant_race_3` ELSE NULL END AS `applicant_race_3`,    

        CASE
            WHEN `applicant_race_name_4` IN ('American Indian or Alaskan Native',  'Asian or Pacific Islander', 'Black', 'Hispanic', 'White', 'Other', 'Native Hawaiian or Other Pacific Islander') THEN `applicant_race_name_4`
            WHEN `applicant_race_name_4` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `applicant_race_name_4`,

        CASE WHEN `applicant_race_4` IN ('1', '2', '3', '4', '5', '6', '7') THEN `applicant_race_4` ELSE NULL END AS `applicant_race_4`, 

        CASE
            WHEN `applicant_race_name_5` IN ('American Indian or Alaskan Native',  'Asian or Pacific Islander', 'Black', 'Hispanic', 'White', 'Other', 'Native Hawaiian or Other Pacific Islander') THEN `applicant_race_name_5`
            WHEN `applicant_race_name_5` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `applicant_race_name_5`,   
        
        CASE WHEN `applicant_race_5` IN ('1', '2', '3', '4', '5', '6', '7') THEN `applicant_race_5` ELSE NULL END AS `applicant_race_5`, 
    
        CASE
            WHEN `co_applicant_race_name_1` IN ('American Indian or Alaskan Native',  'Asian or Pacific Islander', 'Black', 'Hispanic', 'White', 'Other', 'Native Hawaiian or Other Pacific Islander', 'No co-applicant') THEN `co_applicant_race_name_1`
            WHEN `co_applicant_race_name_1` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `co_applicant_race_name_1`,
        
        CASE WHEN `co_applicant_race_1` IN ('1', '2', '3', '4', '5', '6', '7', '8') THEN `co_applicant_race_1` ELSE NULL END AS `co_applicant_race_1`, 
    
        CASE
            WHEN `co_applicant_race_name_2` IN ('American Indian or Alaskan Native',  'Asian or Pacific Islander', 'Black', 'Hispanic', 'White', 'Other', 'Native Hawaiian or Other Pacific Islander', 'No co-applicant') THEN `co_applicant_race_name_2`
            WHEN `co_applicant_race_name_2` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `co_applicant_race_name_2`,
    
        CASE WHEN `co_applicant_race_2` IN ('1', '2', '3', '4', '5', '6', '7', '8') THEN `co_applicant_race_2` ELSE NULL END AS `co_applicant_race_2`, 
        
        CASE
            WHEN `co_applicant_race_name_3` IN ('American Indian or Alaskan Native',  'Asian or Pacific Islander', 'Black', 'Hispanic', 'White', 'Other', 'Native Hawaiian or Other Pacific Islander', 'No co-applicant') THEN `co_applicant_race_name_3`
            WHEN `co_applicant_race_name_3` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `co_applicant_race_name_3`,
        
        CASE WHEN `co_applicant_race_3` IN ('1', '2', '3', '4', '5', '6', '7', '8') THEN `co_applicant_race_3` ELSE NULL END AS `co_applicant_race_3`, 
        
        CASE
            WHEN `co_applicant_race_name_4` IN ('American Indian or Alaskan Native',  'Asian or Pacific Islander', 'Black', 'Hispanic', 'White', 'Other', 'Native Hawaiian or Other Pacific Islander', 'No co-applicant') THEN `co_applicant_race_name_4`
            WHEN `co_applicant_race_name_4` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `co_applicant_race_name_4`,  
        
        CASE WHEN `co_applicant_race_4` IN ('1', '2', '3', '4', '5', '6', '7', '8') THEN `co_applicant_race_4` ELSE NULL END AS `co_applicant_race_4`,
        
        CASE
            WHEN `co_applicant_race_name_5` IN ('American Indian or Alaskan Native',  'Asian or Pacific Islander', 'Black', 'Hispanic', 'White', 'Other', 'Native Hawaiian or Other Pacific Islander', 'No co-applicant') THEN `co_applicant_race_name_5`
            WHEN `co_applicant_race_name_5` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `co_applicant_race_name_5`,  
        
        CASE WHEN `co_applicant_race_5` IN ('1', '2', '3', '4', '5', '6', '7', '8') THEN `co_applicant_race_5` ELSE NULL END AS `co_applicant_race_5`,
        
        CASE
            WHEN `applicant_sex_name` IN ('Male', 'Female') THEN `applicant_sex_name`
            WHEN `applicant_sex_name` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `applicant_sex_name`,
        
        CASE WHEN `applicant_sex` IN ('1', '2', '3', '4') THEN `applicant_sex` ELSE NULL END AS `applicant_sex`,
        
        CASE
            WHEN `co_applicant_sex_name` IN ('Male', 'Female', 'No co-applicant') THEN `co_applicant_sex_name`
            WHEN `co_applicant_sex_name` = 'Not applicable' THEN 'NA'
            ELSE NULL
        END AS `co_applicant_sex_name`,

        CASE WHEN `co_applicant_sex` IN ('1', '2', '3', '4', '5') THEN `co_applicant_sex` ELSE NULL END AS `co_applicant_sex`,
        
        CASE
            WHEN CAST(`applicant_income_000s` AS DOUBLE) >= 0 THEN `applicant_income_000s`
            ELSE NULL
        END AS `applicant_income_000s`,
        
        CASE 
            WHEN `purchaser_type_name` REGEXP '^[^0-9]+$' THEN `purchaser_type_name` 
            ELSE NULL 
        END AS `purchaser_type_name`,

        CASE WHEN `purchaser_type` IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') THEN `purchaser_type` ELSE NULL END AS `purchaser_type`,
        
        CASE
            WHEN `denial_reason_name_1` IN ('Debt-to-income ratio','Employment history','Credit history','Collateral','Insufficient cash (downpayment, closing cost)',
                'Unverifiable information', 'Credit application incomplete','Mortgage insurance denied', 'Other') THEN `denial_reason_name_1` ELSE NULL
        END AS `denial_reason_name_1`,  
        
        CASE WHEN `denial_reason_1` IN ('1', '2', '3', '4', '5', '6', '7', '8', '9') THEN `denial_reason_1` ELSE NULL END AS `denial_reason_1`,
        
        CASE
            WHEN `denial_reason_name_2` IN ('Debt-to-income ratio','Employment history','Credit history','Collateral','Insufficient cash (downpayment, closing cost)',
                'Unverifiable information', 'Credit application incomplete','Mortgage insurance denied', 'Other') THEN `denial_reason_name_2` ELSE NULL
        END AS `denial_reason_name_2`,  
        
        CASE WHEN `denial_reason_2` IN ('1', '2', '3', '4', '5', '6', '7', '8', '9') THEN `denial_reason_2` ELSE NULL END AS `denial_reason_2`,
      
        CASE
            WHEN `denial_reason_name_3` IN ('Debt-to-income ratio','Employment history','Credit history','Collateral','Insufficient cash (downpayment, closing cost)',
                'Unverifiable information', 'Credit application incomplete','Mortgage insurance denied', 'Other') THEN `denial_reason_name_3` ELSE NULL
        END AS `denial_reason_name_3`,  
        
        CASE WHEN `denial_reason_3` IN ('1', '2', '3', '4', '5', '6', '7', '8', '9') THEN `denial_reason_3` ELSE NULL END AS `denial_reason_3`,  

        CASE 
            WHEN CAST(`rate_spread` AS DOUBLE) IS NOT NULL THEN `rate_spread` 
            ELSE NULL 
        END AS `rate_spread`,
        
        CASE
            WHEN `hoepa_status_name` IN ('HOEPA loan', 'Not a HOEPA loan') THEN `hoepa_status_name` ELSE NULL
        END AS `hoepa_status_name`,  

        CASE WHEN `hoepa_status` IN ('1', '2') THEN `hoepa_status` ELSE NULL END AS `hoepa_status`, 
        
        CASE
            WHEN `lien_status_name` IN ('Secured by a first lien','Secured by a subordinate lien','Not secured by a lien') THEN `lien_status_name` 
            WHEN `lien_status_name` = 'Not applicable (purchased loans)' THEN 'NA'
            ELSE NULL
        END AS `lien_status_name`,  
        
        CASE WHEN `lien_status` IN ('1', '2', '3', '4') THEN `lien_status` ELSE NULL END AS `lien_status`,
        
        CASE
            WHEN `edit_status_name` IN ('No edit failures', 'Validity edit failure only', 'Quality edit failure only', 'Validity and quality edit failures') 
            THEN `edit_status_name` ELSE NULL
        END AS `edit_status_name`,  
        
        CASE WHEN `edit_status` IN ('5', '6', '7') THEN `edit_status` ELSE NULL END AS `edit_status`, 
        
        CASE 
            WHEN CAST(`sequence_number` AS DOUBLE) IS NOT NULL THEN `sequence_number` 
            ELSE NULL 
        END AS `sequence_number`,
        
        CASE 
            WHEN CAST(`population` AS DOUBLE) IS NOT NULL THEN `population` 
            ELSE NULL 
        END AS `population`,
        
        CASE
            WHEN CAST(`minority_population` AS DOUBLE) IS NOT NULL AND CAST(`minority_population` AS DOUBLE) <= 100 THEN `minority_population`
            ELSE NULL
        END AS `minority_population`,
        
        CASE 
            WHEN CAST(`hud_median_family_income` AS DOUBLE) IS NOT NULL THEN `hud_median_family_income` 
            ELSE NULL 
        END AS `hud_median_family_income`,
        
        CASE
            WHEN CAST(`tract_to_msamd_income` AS DOUBLE) >= 0 THEN `tract_to_msamd_income`
            ELSE NULL
        END AS `tract_to_msamd_income`,
        
        CASE
            WHEN CAST(`number_of_owner_occupied_units` AS DOUBLE) >= 0 THEN `number_of_owner_occupied_units`
            ELSE NULL
        END AS `number_of_owner_occupied_units`,
        
        CASE
            WHEN CAST(`number_of_1_to_4_family_units` AS DOUBLE) >= 0 THEN `number_of_1_to_4_family_units`
            ELSE NULL
        END AS `number_of_1_to_4_family_units`,
        
        CASE WHEN `application_date_indicator` IN ('0', '1', '2') THEN `application_date_indicator` ELSE NULL END AS `application_date_indicator`

        FROM hmda
    """
    
    
    
    df_sql = spark.sql(sql_query)
    df_sql.show(5, truncate=False)
    df_non_null = df_sql.dropna(how='all', subset=df_sql.columns)
    df_new = df_non_null.select([col(c).cast("string").alias(c) for c in df_non_null.columns])
    
    df_processed = df_new.distinct()
    df_deduped = df_processed.dropDuplicates(subset=["respondent_id", "agency_code", "loan_type", "property_type", "loan_purpose", "owner_occupancy", "loan_amount_000s", "preapproval", "action_taken", "msamd", "state_code", "county_code", "census_tract_number",
    "applicant_ethnicity", "co_applicant_ethnicity", "applicant_race_1", "applicant_race_2", "applicant_race_3", "applicant_race_4", "applicant_race_5", "co_applicant_race_1", "co_applicant_race_2", "co_applicant_race_3", "co_applicant_race_4", "co_applicant_race_5",
    "applicant_sex", "co_applicant_sex", "applicant_income_000s", "purchaser_type", "denial_reason_1", "denial_reason_2", "denial_reason_3", "rate_spread", "hoepa_status", "lien_status", "edit_status", "population", "minority_population", "hud_median_family_income", "tract_to_msamd_income", "number_of_owner_occupied_units", "number_of_1_to_4_family_units", "application_date_indicator"])
    
    #partition_column = 'census_tract'
    #window_spec = Window.partitionBy(when(col(partition_column).isNotNull(), col(partition_column)).otherwise(None)).orderBy(
    #    *[when(col(c).isNotNull(), col(c)).otherwise(999999) for c in df_processed.columns]
    #)
    #df_with_row_num = df_processed.withColumn("row_num", row_number().over(window_spec))
    #df_deduped = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
    
    

    '''
    df_processed.createOrReplaceTempView("new_hmda")
    
    available_columns = df_processed.columns
    
    expected_columns = [
        'respondent_id', 'agency_code', 'loan_type', 'property_type', 'loan_purpose', 
        'owner_occupancy', 'loan_amount_000s', 'preapproval', 'action_taken', 'msamd',
        'state_code', 'county_code', 'census_tract_number', 'applicant_ethnicity',
        'co_applicant_ethnicity', 'applicant_race_1', 'applicant_race_2', 'applicant_race_3',
        'applicant_race_4', 'applicant_race_5', 'co_applicant_race_1', 'co_applicant_race_2',
        'co_applicant_race_3', 'co_applicant_race_4', 'applicant_sex', 'co_applicant_sex', 
        'applicant_income_000s', 'purchaser_type', 'denial_reason_1', 'denial_reason_2', 
        'denial_reason_3', 'rate_spread', 'hoepa_status', 'lien_status', 'edit_status', 
        'sequence_number', 'population', 'minority_population', 'hud_median_family_income',
        'tract_to_msamd_income', 'number_of_owner_occupied_units', 'number_of_1_to_4_family_units',
        'application_date_indicator'
    ]
    existing_columns = [col for col in expected_columns if col in available_columns]
    
    case_statements = [
        f"CASE WHEN `{col}` IS NOT NULL THEN `{col}` ELSE 999999 END" for col in existing_columns
    ]
    
    sql_drop_subsets = f"""
        WITH ranked_data AS (
          SELECT *,
            ROW_NUMBER() OVER (
              PARTITION BY CASE WHEN `census_tract` IS NOT NULL THEN `census_tract` ELSE NULL END
              ORDER BY {', '.join(case_statements)}
            ) AS row_num
          FROM new_hmda
        )
        SELECT * 
        FROM ranked_data
        WHERE row_num = 1
        """

    df_deduped = spark.sql(sql_drop_subsets)
    '''
    
    
    write_output_to_s3(df_deduped, s3_output_path)
    spark.catalog.clearCache()
    spark.catalog.dropTempView("hmda")
    #spark.catalog.dropTempView("new_hmda")
    
    job.commit()