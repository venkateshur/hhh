
import argparse
import sys

from pyspark.sql import SparkSession
import argparse
import traceback
import sys, os, shutil, requests
from pyspark.sql.functions import *
from DataTransformation.util import Util
from pyspark.storagelevel import StorageLevel


def read_parquet(input_landing_path):
    return spark.read.parquet(input_landing_path)\



def write_parquet_file(processed_df, output_path):
    processed_df.write.mode("overwrite").partitionBy("file_date_time").parquet(output_path)
    
    
def write_csv_file(processed_df, output_path):
    processed_df.write.mode("overwrite").partitionBy("file_date_time").csv(output_path)


if __name__ == "__main__":
    print("System")


spark = SparkSession.builder.appName("").getOrCreate()


def perform_transformations(df):
    
    
def process_data(df, df2, df3, df4):
    return df.join(df2, [], "inner").join(df3, [], "inner").join(df4, [], "inner")

try:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file", help="Config File")
    parser.add_argument("--procd_dt", help="Processing Date")
    parser.add_argument("--src_file_path", help="Source File Path")
    parser.add_argument("--tgt_file_path", help="Target File Path")
    parser.add_argument("--consolidated_file_path", help="Consolidated File Path")


    args = parser.parse_args()

    args = parser.parse_args()
    process_date = parser.processing_date
    source_file_path = parser.src_file_path
    target_file_path = parser.tft_file_path

    config_file = args.config_file
    procd_dt = args.procd_dt
    src_file_path = args.src_file_path
    tgt_file_path = args.tgt_file_path
    consolidated_file_path = args.consolidated_file_path

    # creating util object
    obj_util = Util()
    conf_data = obj_util.open_json('/home/DIS_User/' + config_file)
    logger_level = "DEBUG"
    logger_path = conf_data['log_file_dir']
    logger = obj_util.event_logger(logger_path, logger_path, 'SalesForce_CUNA_Holdings_Delta_File_Load', logger_level)

    logger.info("Creating the Spark Session")
    
    logger.info("Spark Session created")

    obj_util = Util(logger)

    logger.info("Getting the RepIDs from API")

    in_parquet = read_parquet("{}/*.parquet".format(source_file_path))

except Exception as e:
    sys.exit(e)
