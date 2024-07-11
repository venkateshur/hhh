from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize Spark session
spark = SparkSession.builder \
    .appName("IcebergColumnLevelUpdateReport") \
    .getOrCreate()

# Configuration
iceberg_table_path = "s3://your-bucket/your-iceberg-table/"
report_path = "s3://your-bucket/column-level-updates-report/"

# Load Iceberg table history
df_history = spark.read.format("iceberg") \
    .option("snapshot-id", "history") \
    .load(iceberg_table_path)

# Extract current and previous snapshots
latest_snapshot = df_history.orderBy(col("snapshot_id").desc()).limit(1)
previous_snapshot = df_history.orderBy(col("snapshot_id").desc()).offset(1).limit(1)

# Load data from current and previous snapshots
df_latest = spark.read.format("iceberg") \
    .option("snapshot-id", latest_snapshot.select("snapshot_id").collect()[0][0]) \
    .load(iceberg_table_path)

df_previous = spark.read.format("iceberg") \
    .option("snapshot-id", previous_snapshot.select("snapshot_id").collect()[0][0]) \
    .load(iceberg_table_path)

# Detect column-level changes
df_changes = df_latest.alias("latest") \
    .join(df_previous.alias("previous"), "id") \
    .select("latest.id",
            "latest.column1", "previous.column1",
            "latest.column2", "previous.column2",
            # Add more columns as needed
            ) \
    .filter(
        (col("latest.column1") != col("previous.column1")) |
        (col("latest.column2") != col("previous.column2"))
        # Add more column comparisons as needed
    )

# Write the report to S3
df_changes.write.format("csv").mode("overwrite").save(report_path)

print("Report generated successfully.")

# Stop the Spark session
spark.stop()
