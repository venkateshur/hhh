import sys

import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import json


if __name__ == '__main__':
    print('File parser')

appName = "Pyspark"
start_time = datetime.now()

spark = SparkSession.builder \
    .appName(appName) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

currentDT = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

num_partitions = 200
input_file_path = sys.argv[1]
output_file_path = sys.argv[2]
schema_file_path = sys.argv[3]

if len(sys.argv) > 4:
    num_partitions = int(sys.argv[4])


def write_output(df, output_path):
    df.write.mode("overwrite").option("header", "true").csv(output_path)


def load_config(config_path):
    f = open(config_path)
    return json.loads(f)


def get_schema(schema_dict, req_seq):
    return schema_dict.get(req_seq)


def parse_df(seq_schema_dict, in_seq_df):
    in_seq_df_parse = in_seq_df
    for column in seq_schema_dict:
        split_positions = seq_schema_dict[column].split("-")
        start_pos = split_positions[0]
        end_pos = split_positions[1]
        in_seq_df_parse = in_seq_df_parse.withColumn(column, F.substring("value", start_pos, end_pos))
        in_seq_df_parse = in_seq_df_parse.drop("value")
        return in_seq_df_parse


try:
    input_file_df = spark.read.text(input_file_path).repartition(num_partitions)\
        .filter(F.substring(F.col("value"), 2, 4) == "APR").cache()

    seq001_df = input_file_df.filter(F.substring(F.col("value"), 3, 6) == "001")
    seq002_df = input_file_df.filter(F.substring(F.col("value"), 3, 6) == "002")
    seq003_df = input_file_df.filter(F.substring(F.col("value"), 3, 6) == "003")
    seq004_df = input_file_df.filter(F.substring(F.col("value"), 3, 6) == "004")
    seq005_df = input_file_df.filter(F.substring(F.col("value"), 3, 6) == "005")
    seq006_df = input_file_df.filter(F.substring(F.col("value"), 3, 6) == "006")
    seq007_df = input_file_df.filter(F.substring(F.col("value"), 3, 6) == "007")
    seq008_df = input_file_df.filter(F.substring(F.col("value"), 3, 6) == "008")
    seq009_df = input_file_df.filter(F.substring(F.col("value"), 3, 6) == "009")
    seq010_df = input_file_df.filter(F.substring(F.col("value"), 3, 6) == "0010")

    write_output(seq010_df, output_file_path + "/" + str(currentDT) + "/SEQ001")
    write_output(seq010_df, output_file_path + "/" + str(currentDT) + "/SEQ002")
    write_output(seq010_df, output_file_path + "/" + str(currentDT) + "/SEQ003")

except Exception as ex:
    print("Parsing failed with error: " + str(ex))
    raise ex
