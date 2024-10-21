import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, regexp_extract
import re
import boto3

# Initialize the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.appName('age_data_processing').getOrCreate()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize the S3 client
s3 = boto3.client('s3')
bucket_name = 'ncif.ai.dev'  # Bucket name
prefix = 'acs/01_raw/acs1/age/'   # Prefix for the age data directory

# Retrieve all CSV files from the S3 bucket
file_list = []
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
for content in response.get('Contents', []):
    if content['Key'].endswith(".csv"):  # Filter CSV files
        file_list.append(content['Key'])

# Initialize an empty DataFrame to hold the merged data
final_df = None

# Function to process each file: extract year from the filename and clean data
def process_file(file_name, data_df):
    # Extract the year from the filename
    year_match = re.search(r'_(\d{4})', file_name)
    extracted_year = year_match.group(1) if year_match else "Unknown"
    
    # Add a new column for the year
    data_df = data_df.withColumn("Year", lit(extracted_year))
    
    # Replace special characters ("-888888888", "(X)", "") with None
    data_df = data_df.replace(["-888888888", "(X)", ""], None)
    
    # Drop columns that are entirely empty or contain only the specified invalid values
    non_empty_columns = [col for col in data_df.columns if data_df.filter(data_df[col].isNotNull()).count() > 0]
    data_df = data_df.select(non_empty_columns)
    
    return data_df

# Process each file in the file list
for file_path in file_list:
    # Read the current CSV file into a DataFrame
    current_df = spark.read.option("header", "true").csv(f"s3://{bucket_name}/{file_path}")
    
    # Process the file and merge it into the final DataFrame
    processed_df = process_file(file_path, current_df)
    final_df = processed_df if final_df is None else final_df.union(processed_df)

# Write the final merged DataFrame back to S3 as a single CSV file
if final_df:
    output_path = "s3://ncif.ai.dev/acs/02_processed/acs1/age_processed_data.csv"
    final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

# Commit the Glue job
job.commit()
