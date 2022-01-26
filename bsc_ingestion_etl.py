
import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *


def read_parquet(input_landing_path):
    return spark.read.parquet(input_landing_path)\


def write_parquet_file(processed_df, output_path):
    processed_df.write.mode("overwrite").partitionBy("file_date_time").parquet(output_path)


if __name__ == "__main__":
    print("System")


spark = SparkSession.builder.appName("").getOrCreate()

try:
    parser = argparse.ArgumentParser()
    parser.add_argument("--processing_date", help="Processing Date")
    parser.add_argument("--src_file_path", help="Source File Path")
    parser.add_argument("--tgt_file_path", help="Target File Path")

    args = parser.parse_args()
    process_date = parser.processing_date
    source_file_path = parser.src_file_path
    target_file_path = parser.tft_file_path

    in_parquet = read_parquet("{}/*.parquet".format(source_file_path))

except Exception as e:
    sys.exit(e)

