import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Load source and target data
source_df = glueContext.create_dynamic_frame.from_catalog(database="source_database", table_name="source_table").toDF()
target_df = glueContext.create_dynamic_frame.from_catalog(database="target_database", table_name="target_table").toDF()

# Ensure the columns are the same in both dataframes
source_columns = set(source_df.columns)
target_columns = set(target_df.columns)

if source_columns != target_columns:
    raise ValueError("Source and target datasets have different columns.")

# Create SQL views for easy querying
source_df.createOrReplaceTempView("source")
target_df.createOrReplaceTempView("target")

# Find rows in source that are not in target
diff_query_source_to_target = f"""
SELECT *
FROM source
EXCEPT
SELECT *
FROM target
"""

# Find rows in target that are not in source
diff_query_target_to_source = f"""
SELECT *
FROM target
EXCEPT
SELECT *
FROM source
"""

# Execute the queries and get the differences
differences_source_to_target = spark.sql(diff_query_source_to_target)
differences_target_to_source = spark.sql(diff_query_target_to_source)

# Show the differences
print("Differences from Source to Target:")
differences_source_to_target.show(truncate=False)

print("Differences from Target to Source:")
differences_target_to_source.show(truncate=False)

# Optionally write differences to S3
differences_source_to_target.write.mode("overwrite").csv("s3://your-bucket/path/to/source_to_target_differences/")
differences_target_to_source.write.mode("overwrite").csv("s3://your-bucket/path/to/target_to_source_differences/")
