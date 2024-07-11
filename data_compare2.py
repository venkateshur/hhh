import sys
import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
import boto3

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define database and table names
source_database = "source_database"
source_table_name = "source_table"
target_database = "target_database"
target_table_name = "target_table"

# Read source table from Glue Data Catalog
source_df = glueContext.create_dynamic_frame.from_catalog(database=source_database, table_name=source_table_name).toDF()

# Read target Iceberg table
target_df = spark.read.format("iceberg").load(f"{target_database}.{target_table_name}")

# Comparison Report DataFrame
columns = source_df.columns
comparison_list = []

for column in columns:
    source_data = source_df.select(column).distinct().rdd.flatMap(lambda x: x).collect()
    target_data = target_df.select(column).distinct().rdd.flatMap(lambda x: x).collect()
    
    source_set = set(source_data)
    target_set = set(target_data)
    
    common_data = source_set.intersection(target_set)
    only_in_source = source_set - target_set
    only_in_target = target_set - source_set

    for data in common_data:
        comparison_list.append((source_database, source_table_name, target_database, target_table_name, column, data, data, "MATCH"))
    for data in only_in_source:
        comparison_list.append((source_database, source_table_name, target_database, target_table_name, column, data, None, "SOURCE_ONLY"))
    for data in only_in_target:
        comparison_list.append((source_database, source_table_name, target_database, target_table_name, column, None, data, "TARGET_ONLY"))

    # Add NOT MATCH flag for any discrepancies not captured above
    for data in source_set.union(target_set):
        if data not in common_data:
            comparison_list.append((source_database, source_table_name, target_database, target_table_name, column, data if data in source_set else None, data if data in target_set else None, "NOT_MATCH"))

comparison_df = spark.createDataFrame(comparison_list, ["source_database", "source_table", "target_database", "target_table", "column_name", "source_data", "target_data", "match_flag"])

# Define temporary output path
temp_output_path = "s3://your-bucket/temp/comparison_report"
# Write Comparison Report to a temporary location
comparison_df.coalesce(1).write.mode('overwrite').csv(temp_output_path, header=True)

# List the files in the temporary directory and find the part file
s3 = boto3.client('s3')
bucket = "your-bucket"
temp_output_prefix = "temp/comparison_report"
result = s3.list_objects_v2(Bucket=bucket, Prefix=temp_output_prefix)
for obj in result.get('Contents', []):
    if obj['Key'].endswith('.csv'):
        temp_file_key = obj['Key']
        break

# Generate timestamped filename
timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
final_output_path = f"comparison_report_{timestamp}.csv"

# Copy and rename the part file to the final destination with a timestamp
copy_source = {'Bucket': bucket, 'Key': temp_file_key}
s3.copy_object(CopySource=copy_source, Bucket=bucket, Key=final_output_path)

# Clean up temporary files
s3.delete_object(Bucket=bucket, Key=temp_file_key)

# Summary Report DataFrame
def summary_report(df, table_name, db_name):
    row_count = df.count()
    # This is a placeholder for duplicate count; actual logic may vary
    dup_row_count = df.count() - df.dropDuplicates().count()
    unique_row_count = df.dropDuplicates().count()
    # Replace "value_column" with actual column name for max and min value calculations
    max_value = df.agg(F.max("value_column")).collect()[0][0]  
    min_value = df.agg(F.min("value_column")).collect()[0][0]

    return (db_name, table_name, row_count, dup_row_count, unique_row_count, max_value, min_value)

source_summary = summary_report(source_df, source_table_name, source_database)
target_summary = summary_report(target_df, target_table_name, target_database)

summary_list = [source_summary, target_summary]
summary_df = spark.createDataFrame(summary_list, ["database_name", "table_name", "row_count", "dup_row_count", "unique_row_count", "max_value", "min_value"])

# Define temporary output paths for the summary report
temp_summary_output_path = "s3://your-bucket/temp/summary_report"
# Write Summary Report to a temporary location
summary_df.coalesce(1).write.mode('overwrite').csv(temp_summary_output_path, header=True)

# List the files in the temporary directory and find the part file for summary report
result = s3.list_objects_v2(Bucket=bucket, Prefix=temp_summary_output_path)
for obj in result.get('Contents', []):
    if obj['Key'].endswith('.csv'):
        temp_summary_file_key = obj['Key']
        break

# Generate timestamped filename for the summary report
final_summary_output_path = f"summary_report_{timestamp}.csv"

# Copy and rename the part file to the final destination with a timestamp for summary report
copy_source = {'Bucket': bucket, 'Key': temp_summary_file_key}
s3.copy_object(CopySource=copy_source, Bucket=bucket, Key=final_summary_output_path)

# Clean up temporary files for summary report
s3.delete_object(Bucket=bucket, Key=temp_summary_file_key)
