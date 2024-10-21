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
from pyspark.sql.functions import col

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
s3_path = "s3://ncif.ai.dev/FFIEC - HMDA/01_raw/hmda_2018_nationwide_all-records.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)


df_str = df.select([col(c).cast("string").alias(c) for c in df.columns])

#df.printSchema()

#df = df.select([F.trim(F.col(c)).alias(c.strip()) for c in df.columns])
#df.printSchema()

#filter_negative_columns = ["tract_median_age_of_housing_units", "tract_one_to_four_family_homes", "tract_owner_occupied_units", "tract_to_msa_income_percentage", 
#                            "ffiec_msa_md_median_family_income", "tract_minority_population_percent", "tract_population"]
#select_exprs = [
#    f"CASE WHEN {col} >= 0 THEN {col} ELSE NULL END AS {col}" if col in filter_negative_columns else col
#    for col in df.columns
#]

# filter out negative values
# filter out values not in 1~9
#spark = SparkSession.builder.appName("Denial Reasons View").getOrCreate()

df.createOrReplaceTempView("hmda")
sql_query = """
    SELECT
    `activity_year`,

    CASE 
        WHEN (`applicant_age` REGEXP '^[0-9]+-[0-9]+$' OR CAST(`applicant_age` AS DOUBLE) IS NOT NULL OR `applicant_age` IN ('NA', 'EXEMPT')) AND `applicant_age` NOT IN ('8888', '9999') 
        THEN `applicant_age` ELSE NULL
    END AS `applicant_age`,

    CASE WHEN (cast(`census_tract` AS bigint) IS NOT NULL AND (LENGTH(`census_tract`) = 10 OR LENGTH(`census_tract`) = 11)) THEN `census_tract` ELSE NULL
    END AS `census_tract`,

    CASE WHEN (cast(`county_code` AS bigint) IS NOT NULL AND (LENGTH(`county_code`) = 4 OR LENGTH(`county_code`) = 5)) THEN `county_code` ELSE NULL
    END AS `county_code`,

    CASE WHEN (cast(`derived_msa-md` AS bigint) IS NOT NULL AND (LENGTH(`derived_msa-md`) = 4 OR LENGTH(`derived_msa-md`) = 5)) THEN `derived_msa-md` ELSE NULL
    END AS `derived_msa-md`,

    CASE WHEN (CAST(`discount_points` AS double) IS NOT NULL OR `discount_points` IN ('NA', 'EXEMPT')) THEN `discount_points` ELSE NULL END AS `discount_points`,

    CASE WHEN (CAST(`income` AS double) IS NOT NULL OR `income` IN ('NA', 'EXEMPT')) THEN `income` ELSE NULL END AS `income`,

    CASE WHEN (CAST(`interest_rate` AS double) IS NOT NULL OR `interest_rate` IN ('NA', 'EXEMPT')) THEN `interest_rate` ELSE NULL END AS `interest_rate`,

    CASE WHEN (CAST(`intro_rate_period` AS double) IS NOT NULL AND CAST(`intro_rate_period` AS double) >= 0) OR `intro_rate_period` IN ('NA', 'EXEMPT') THEN `intro_rate_period`
        ELSE NULL END AS `intro_rate_period`,  

    `lei`,

    CASE WHEN (CAST(`lender_credits` AS double) IS NOT NULL OR `lender_credits` IN ('NA', 'EXEMPT')) THEN `lender_credits` ELSE NULL END AS `lender_credits`,

    CASE WHEN (CAST(`loan_amount` AS double) IS NOT NULL AND CAST(`loan_amount` AS double) >= 0) OR `loan_amount` IN ('NA', 'EXEMPT') THEN `loan_amount`
        ELSE NULL END AS `loan_amount`,  
    
    CASE WHEN (CAST(`loan_to_value_ratio` AS double) IS NOT NULL AND CAST(`loan_to_value_ratio` AS double) >= 0) OR `loan_to_value_ratio` IN ('NA', 'EXEMPT') THEN `loan_to_value_ratio`
        ELSE NULL END AS `loan_to_value_ratio`,  

    CASE WHEN (CAST(`multifamily_affordable_units` AS double) IS NOT NULL AND CAST(`multifamily_affordable_units` AS double) >= 0) OR `multifamily_affordable_units` IN ('NA', 'EXEMPT') THEN `multifamily_affordable_units`
        ELSE NULL END AS `multifamily_affordable_units`,  

    CASE WHEN (CAST(`origination_charges` AS double) IS NOT NULL AND CAST(`origination_charges` AS double) >= 0) OR `origination_charges` IN ('NA', 'EXEMPT') THEN `origination_charges`
        ELSE NULL END AS `origination_charges`,     

    CASE WHEN (CAST(`prepayment_penalty_term` AS double) IS NOT NULL AND CAST(`prepayment_penalty_term` AS double) >= 0) OR `prepayment_penalty_term` IN ('NA', 'EXEMPT') THEN `prepayment_penalty_term`
        ELSE NULL END AS `prepayment_penalty_term`, 

    CASE WHEN (CAST(`property_value` AS double) IS NOT NULL AND CAST(`property_value` AS double) >= 0) OR `property_value` IN ('NA', 'EXEMPT') THEN `property_value`
        ELSE NULL END AS `property_value`, 

    CASE WHEN (CAST(`rate_spread` AS double) IS NOT NULL OR `rate_spread` IN ('NA', 'EXEMPT')) THEN `rate_spread` ELSE NULL END AS `rate_spread`,

    CASE WHEN (LENGTH(`state_code`) = 2 AND `state_code` NOT LIKE '%[^A-Za-z]%') OR `state_code` IN ('NA', 'EXEMPT') THEN `state_code` ELSE NULL END AS `state_code`,

    CASE WHEN (CAST(`total_loan_costs` AS double) IS NOT NULL AND CAST(`total_loan_costs` AS double) >= 0) OR `total_loan_costs` IN ('NA', 'EXEMPT') THEN `total_loan_costs`
        ELSE NULL END AS `total_loan_costs`, 

    CASE WHEN (CAST(`total_points_and_fees` AS double) IS NOT NULL AND CAST(`total_points_and_fees` AS double) >= 0) OR `total_points_and_fees` IN ('NA', 'EXEMPT') THEN `total_points_and_fees`
        ELSE NULL END AS `total_points_and_fees`,     

    CASE WHEN `tract_median_age_of_housing_units` >= 0 THEN `tract_median_age_of_housing_units` ELSE NULL END AS `tract_median_age_of_housing_units`,
    CASE WHEN `tract_one_to_four_family_homes` >= 0 THEN `tract_one_to_four_family_homes` ELSE NULL END AS `tract_one_to_four_family_homes`,
    CASE WHEN `tract_owner_occupied_units` >= 0 THEN `tract_owner_occupied_units` ELSE NULL END AS `tract_owner_occupied_units`,
    CASE WHEN `tract_to_msa_income_percentage` >= 0 THEN `tract_to_msa_income_percentage` ELSE NULL END AS `tract_to_msa_income_percentage`,
    CASE WHEN `ffiec_msa_md_median_family_income` >= 0 THEN `ffiec_msa_md_median_family_income` ELSE NULL END AS `ffiec_msa_md_median_family_income`,
    CASE WHEN `tract_minority_population_percent` >= 0 THEN `tract_minority_population_percent` ELSE NULL END AS `tract_minority_population_percent`,
    CASE WHEN `tract_population` >= 0 THEN `tract_population` ELSE NULL END AS `tract_population`,
    
    -- Denial reasons
    CASE WHEN `denial_reason-1` IN ('1', '2', '3', '4', '5', '6', '7', '8', '9') THEN `denial_reason-1` ELSE NULL END AS `denial_reason-1`,
    CASE WHEN `denial_reason-2` IN ('1', '2', '3', '4', '5', '6', '7', '8', '9') THEN `denial_reason-2` ELSE NULL END AS `denial_reason-2`,
    CASE WHEN `denial_reason-3` IN ('1', '2', '3', '4', '5', '6', '7', '8', '9') THEN `denial_reason-3` ELSE NULL END AS `denial_reason-3`,
    CASE WHEN `denial_reason-4` IN ('1', '2', '3', '4', '5', '6', '7', '8', '9') THEN `denial_reason-4` ELSE NULL END AS `denial_reason-4`,
    
    -- AUS columns
    CASE WHEN `aus-1` IN ('1', '2', '3', '4', '5', '6', '7', '1111') THEN `aus-1` ELSE NULL END AS `aus-1`,
    CASE WHEN `aus-2` IN ('1', '2', '3', '4', '5', '7') THEN `aus-2` ELSE NULL END AS `aus-2`,
    CASE WHEN `aus-3` IN ('1', '2', '3', '4', '7') THEN `aus-3` ELSE NULL END AS `aus-3`,
    CASE WHEN `aus-4` IN ('1', '2', '3', '4', '7') THEN `aus-4` ELSE NULL END AS `aus-4`,
    CASE WHEN `aus-5` IN ('1', '2', '3', '4', '7') THEN `aus-5` ELSE NULL END AS `aus-5`,
    
    CASE WHEN `initially_payable_to_institution` IN ('1', '2', '3', '1111') THEN `initially_payable_to_institution` ELSE NULL END AS `initially_payable_to_institution`,
    CASE WHEN `submission_of_application` IN ('1', '2', '3', '1111') THEN `submission_of_application` ELSE NULL END AS `submission_of_application`,
    
    CASE WHEN `co-applicant_age_above_62` IN ('yes', 'no' , 'NA') THEN `co-applicant_age_above_62` ELSE NULL END AS `co-applicant_age_above_62`,
    CASE WHEN `co-applicant_age` IN ( '< 25', '25-34', '35-44', '45-54', '55-64', '65-74', '> 74') THEN `co-applicant_age` ELSE NULL END AS `co-applicant_age`,
    CASE WHEN `applicant_age_above_62` IN ('Yes', 'No', 'NA') THEN `applicant_age_above_62` ELSE NULL END AS `applicant_age_above_62`,
    
    CASE WHEN `co-applicant_sex_observed` IN ('1', '2', '3', '4') THEN `co-applicant_sex_observed` ELSE NULL END AS `co-applicant_sex_observed`,
    CASE WHEN `applicant_sex_observed` IN ('1', '2', '3') THEN `applicant_sex_observed` ELSE NULL END AS `applicant_sex_observed`,
    CASE WHEN `co-applicant_sex` IN ('1', '2', '3', '4', '5', '6') THEN `co-applicant_sex` ELSE NULL END AS `co-applicant_sex`,
    CASE WHEN `applicant_sex` IN ('1', '2', '3', '4', '6') THEN `applicant_sex` ELSE NULL END AS `applicant_sex`,
    
    CASE WHEN `co-applicant_race_observed` IN ('1', '2', '3', '4') THEN `co-applicant_race_observed` ELSE NULL END AS `co-applicant_race_observed`,
    
    CASE WHEN `applicant_race_observed` IN ('1', '2', '3') THEN `applicant_race_observed` ELSE NULL END AS `applicant_race_observed`,
    CASE WHEN `co-applicant_race-5` IN ('1', '2', '21', '22', '23', '24', '25', '26', '27', '3', '4', '41', '42', '43', '44', '5') THEN `co-applicant_race-5` ELSE NULL END AS `co-applicant_race-5`,
    CASE WHEN `co-applicant_race-4` IN ('1', '2', '21', '22', '23', '24', '25', '26', '27', '3', '4', '41', '42', '43', '44', '5') THEN `co-applicant_race-4` ELSE NULL END AS `co-applicant_race-4`,
    CASE WHEN `co-applicant_race-3` IN ('1', '2', '21', '22', '23', '24', '25', '26', '27', '3', '4', '41', '42', '43', '44', '5') THEN `co-applicant_race-3` ELSE NULL END AS `co-applicant_race-3`,
    CASE WHEN `co-applicant_race-2` IN ('1', '2', '21', '22', '23', '24', '25', '26', '27', '3', '4', '41', '42', '43', '44', '5') THEN `co-applicant_race-2` ELSE NULL END AS `co-applicant_race-2`,
    CASE WHEN `co-applicant_race-1` IN ('1', '2', '21', '22', '23', '24', '25', '26', '27', '3', '4', '41', '42', '43', '44', '5', '6', '7', '8') THEN `co-applicant_race-1` ELSE NULL END AS `co-applicant_race-1`,
    CASE WHEN `applicant_race-5` IN ('1', '2', '21', '22', '23', '24', '25', '26', '27', '3', '4', '41', '42', '43', '44', '5') THEN `applicant_race-5` ELSE NULL END AS `applicant_race-5`,
    CASE WHEN `applicant_race-4` IN ('1', '2', '21', '22', '23', '24', '25', '26', '27', '3', '4', '41', '42', '43', '44', '5') THEN `applicant_race-4` ELSE NULL END AS `applicant_race-4`,
    CASE WHEN `applicant_race-3` IN ('1', '2', '21', '22', '23', '24', '25', '26', '27', '3', '4', '41', '42', '43', '44', '5') THEN `applicant_race-3` ELSE NULL END AS `applicant_race-3`,
    CASE WHEN `applicant_race-2` IN ('1', '2', '21', '22', '23', '24', '25', '26', '27', '3', '4', '41', '42', '43', '44', '5') THEN `applicant_race-2` ELSE NULL END AS `applicant_race-2`,
    CASE WHEN `applicant_race-1` IN ('1', '2', '21', '22', '23', '24', '25', '26', '27', '3', '4', '41', '42', '43', '44', '5', '6', '7') THEN `applicant_race-1` ELSE NULL END AS `applicant_race-1`,
    
    CASE WHEN `co-applicant_ethnicity_observed` IN ('1', '2', '3', '4') THEN `co-applicant_ethnicity_observed` ELSE NULL END AS `co-applicant_ethnicity_observed`,
    CASE WHEN `applicant_ethnicity_observed` IN ('1', '2', '3') THEN `applicant_ethnicity_observed` ELSE NULL END AS `applicant_ethnicity_observed`,
    CASE WHEN `co-applicant_ethnicity-5` IN ('1', '11', '12', '13', '14', '2') THEN `co-applicant_ethnicity-5` ELSE NULL END AS `co-applicant_ethnicity-5`,
    CASE WHEN `co-applicant_ethnicity-4` IN ('1', '11', '12', '13', '14', '2') THEN `co-applicant_ethnicity-4` ELSE NULL END AS `co-applicant_ethnicity-4`,
    CASE WHEN `co-applicant_ethnicity-3` IN ('1', '11', '12', '13', '14', '2') THEN `co-applicant_ethnicity-3` ELSE NULL END AS `co-applicant_ethnicity-3`,
    CASE WHEN `co-applicant_ethnicity-2` IN ('1', '11', '12', '13', '14', '2') THEN `co-applicant_ethnicity-2` ELSE NULL END AS `co-applicant_ethnicity-2`,
    CASE WHEN `co-applicant_ethnicity-1` IN ('1', '11', '12', '13', '14', '2', '3', '4', '5') THEN `co-applicant_ethnicity-1` ELSE NULL END AS `co-applicant_ethnicity-1`,
    CASE WHEN `applicant_ethnicity-5` IN ('1', '11', '12', '13', '14', '2') THEN `applicant_ethnicity-5` ELSE NULL END AS `applicant_ethnicity-5`,
    CASE WHEN `applicant_ethnicity-4` IN ('1', '11', '12', '13', '14', '2') THEN `applicant_ethnicity-4` ELSE NULL END AS `applicant_ethnicity-4`,
    CASE WHEN `applicant_ethnicity-3` IN ('1', '11', '12', '13', '14', '2') THEN `applicant_ethnicity-3` ELSE NULL END AS `applicant_ethnicity-3`,
    CASE WHEN `applicant_ethnicity-2` IN ('1', '11', '12', '13', '14', '2') THEN `applicant_ethnicity-2` ELSE NULL END AS `applicant_ethnicity-2`,
    CASE WHEN `applicant_ethnicity-1` IN ('1', '11', '12', '13', '14', '2', '3', '4') THEN `applicant_ethnicity-1` ELSE NULL END AS `applicant_ethnicity-1`,
    
    CASE WHEN `co-applicant_credit_score_type` IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '1111') THEN `co-applicant_credit_score_type` ELSE NULL END AS `co-applicant_credit_score_type`,
    CASE WHEN `applicant_credit_score_type` IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '1111') THEN `applicant_credit_score_type` ELSE NULL END AS `applicant_credit_score_type`,
    CASE WHEN `debt_to_income_ratio` IN ('<20%', '20%-<30%', '30%-<36%', '36%', '37%', '38%', '39%', '40%', '41%', '42%', '43%', '44%', '45%', '46%', '47%', '48%', '49%', '50%-60%', '>60%', 'NA', 'Exempt') THEN `debt_to_income_ratio` ELSE NULL END AS `debt_to_income_ratio`,

    --CASE WHEN `ageapplicant` IN ('<25', '25-34','35-44', '45-54', '55-64', '65-74', '>74') THEN `ageapplicant` ELSE NULL END AS `ageapplicant`,
    CASE WHEN `total_units` IN ('1', '2', '3', '4', '5-24', '25-49', '50-99', '100-149', '>149') THEN `total_units` ELSE NULL END AS `total_units`,
    CASE WHEN `manufactured_home_land_property_interest` IN ('1', '2', '3', '4', '5', '1111') THEN `manufactured_home_land_property_interest` ELSE NULL END AS `manufactured_home_land_property_interest`,
    CASE WHEN `manufactured_home_secured_property_type` IN ('1', '2', '3', '1111') THEN `manufactured_home_secured_property_type` ELSE NULL END AS `manufactured_home_secured_property_type`,
    CASE WHEN `occupancy_type` IN ('1', '2', '3') THEN `occupancy_type` ELSE NULL END AS `occupancy_type`,
    CASE WHEN `construction_method` IN ('1', '2') THEN `construction_method` ELSE NULL END AS `construction_method`,
    
    CASE WHEN `other_nonamortizing_features` IN ('1', '2', '1111') THEN `other_nonamortizing_features` ELSE NULL END AS `other_nonamortizing_features`,
    CASE WHEN `balloon_payment` IN ('1', '2', '1111') THEN `balloon_payment` ELSE NULL END AS `balloon_payment`,
    CASE WHEN `interest_only_payment` IN ('1', '2', '1111') THEN `interest_only_payment` ELSE NULL END AS `interest_only_payment`,
    CASE WHEN `negative_amortization` IN ('1', '2', '1111') THEN `negative_amortization` ELSE NULL END AS `negative_amortization`,

    CASE WHEN `loan_term` >= 0 THEN `loan_term` ELSE NULL END AS `loan_term`,
    
    CASE WHEN `hoepa_status` IN ('1', '2', '1111') THEN `hoepa_status` ELSE NULL END AS `hoepa_status`,
    
    CASE WHEN `business_or_commercial_purpose` IN ('1', '2', '1111') THEN `business_or_commercial_purpose` ELSE NULL END AS `business_or_commercial_purpose`,
    CASE WHEN `open-end_line_of_credit` IN ('1', '2', '1111') THEN `open-end_line_of_credit` ELSE NULL END AS `open-end_line_of_credit`,
    CASE WHEN `reverse_mortgage` IN ('1', '2', '1111') THEN `reverse_mortgage` ELSE NULL END AS `reverse_mortgage`,
    CASE WHEN `lien_status` IN ('1', '2') THEN `lien_status` ELSE NULL END AS `lien_status`,
    CASE WHEN `loan_purpose` IN ('1', '2', '31', '32', '4', '5') THEN `loan_purpose` ELSE NULL END AS `loan_purpose`,
    CASE WHEN `loan_type` IN ('1', '2', '3', '4') THEN `loan_type` ELSE NULL END AS `loan_type`,
    CASE WHEN `preapproval` IN ('1', '2') THEN `preapproval` ELSE NULL END AS `preapproval`,
    CASE WHEN `purchaser_type` IN ('0', '1', '2', '3', '4', '5', '6', '71', '72', '8', '9') THEN `purchaser_type` ELSE NULL END AS `purchaser_type`,
    CASE WHEN `action_taken` IN ('1', '2', '3', '4', '5', '6', '7', '8') THEN `action_taken` ELSE NULL END AS `action_taken`,
    
    CASE WHEN `derived_sex` IN ('Male', 'Female', 'Joint', 'Sex Not Available') THEN `derived_sex` ELSE NULL END AS `derived_sex`,
    CASE WHEN `derived_race` IN ('American Indian or Alaska Native', 'Asian', 'Black or African American', 'Native Hawaiian or Other Pacific Islander', 'White', '2 or more minority races', 'Joint', 'Free Form Text Only', 'Race Not Available') THEN `derived_race` ELSE NULL END AS `derived_race`,
    CASE WHEN `derived_ethnicity` IN ('Hispanic or Latino', 'Not Hispanic or Latino', 'Joint', 'Ethnicity Not Available', 'Free Form Text Only') THEN `derived_ethnicity` ELSE NULL END AS `derived_ethnicity`,
    CASE WHEN `conforming_loan_limit` IN ('C', 'NC', 'U', 'NA') THEN `conforming_loan_limit` ELSE NULL END AS `conforming_loan_limit`,
    CASE WHEN `derived_dwelling_category` IN ('Single Family (1-4 Units):Site-Built', 'Multifamily:Site-Built (5+ Units)', 'Single Family (1-4 Units):Manufactured', 'Multifamily:Manufactured (5+ Units)') THEN `derived_dwelling_category` ELSE NULL END AS `derived_dwelling_category`,
    CASE WHEN `derived_loan_product_type` IN ('Conventional:First Lien', 'FHA:First Lien', 'VA:First Lien', 'FSA/RHS:First Lien', 'Conventional:Subordinate Lien', 'FHA:Subordinate Lien', 'VA:Subordinate Lien', 'FSA/RHS:Subordinate Lien') THEN `derived_loan_product_type` ELSE NULL END AS `derived_loan_product_type`
    
    FROM hmda
"""

s3_output_path = "s3://ncif.ai.dev/FFIEC - HMDA/02_processed/proccessed_2018/"


df_sql = spark.sql(sql_query)
df_sql.show(10, truncate=False)
df_non_null = df_sql.dropna(how='all', subset=df_sql.columns)
df_new = df_non_null.select([col(c).cast("string").alias(c) for c in df_non_null.columns])

df_new.createOrReplaceTempView("new_hmda")

sql_drop_duplicate = """
  SELECT DISTINCT *
  FROM new_hmda
"""

df_proccessed = spark.sql(sql_drop_duplicate)
#df_proccessed.show(5)

write_output_to_s3(df_proccessed, s3_output_path)
job.commit()