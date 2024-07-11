from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("IcebergColumnLevelUpdateReport") \
    .getOrCreate()

# Configuration
iceberg_table_path = "s3://your-bucket/your-iceberg-table/"
report_path = "s3://your-bucket/column-level-updates-report/"

# Load Iceberg table history
df_history = spark.read.format("iceberg").option("snapshot-id", "history").load(iceberg_table_path)

# Get snapshot IDs
latest_snapshot_id = df_history.orderBy(col("snapshot_id").desc()).limit(1).select("snapshot_id").collect()[0][0]
previous_snapshot_id = df_history.orderBy(col("snapshot_id").desc()).offset(1).limit(1).select("snapshot_id").collect()[0][0]

# Load data from snapshots
df_latest = spark.read.format("iceberg").option("snapshot-id", latest_snapshot_id).load(iceberg_table_path)
df_previous = spark.read.format("iceberg").option("snapshot-id", previous_snapshot_id).load(iceberg_table_path)

# Get common columns
columns_latest = df_latest.columns
columns_previous = df_previous.columns
common_columns = list(set(columns_latest) & set(columns_previous))

# Define function to detect changes
def detect_changes(df1, df2, common_columns):
    df1 = df1.withColumnRenamed("id", "id_latest")
    df2 = df2.withColumnRenamed("id", "id_previous")
    
    join_expr = [df1["id_latest"] == df2["id_previous"]]
    
    df_changes = df1.join(df2, join_expr, "inner")
    
    for column in common_columns:
        df_changes = df_changes.filter(col(f"id_latest.{column}") != col(f"id_previous.{column}"))
    
    return df_changes.select("id_latest", *[col(f"id_latest.{col}").alias(f"{col}_latest") for col in common_columns],
                                 *[col(f"id_previous.{col}").alias(f"{col}_previous") for col in common_columns])

df_changes = detect_changes(df_latest, df_previous, common_columns)

# Write the report
df_changes.write.format("csv").mode("overwrite").save(report_path)

print("Report generated successfully.")

# Stop the Spark session
spark.stop()
