import datetime
import json
import sys
from collections import OrderedDict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == '__main__':
    print('File parser')

appName = "Pyspark"
start_time = datetime.datetime.now()

spark = SparkSession.builder.master("local")\
    .appName(appName) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

currentDT = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

num_partitions = 200
input_file_path = sys.argv[1]
output_file_path = sys.argv[2]
schema_file_path = sys.argv[3]
detail_rec_identifier = sys.argv[4].upper()

if len(sys.argv) > 5:
    num_partitions = int(sys.argv[5])

schema_dict = {"17-36" : "SEQ1736"}


def write_output(df, output_path):
    df.coalesce(1).write.mode("overwrite").option(
        "header", "true").csv(output_path, emptyValue="")


def load_config(config_path):
    with open(config_path) as f:
        data = json.load(f, object_pairs_hook=OrderedDict)
    return data


def get_schema(schema_dict, req_seq):
    return schema_dict.get(req_seq)


def parse_df(seq_schema_dict, in_seq_df):
    in_seq_df_parse = in_seq_df
    for column in seq_schema_dict:
        split_positions = seq_schema_dict[column].split("-")
        start_pos = int(split_positions[0])
        length = int(split_positions[1])
        in_seq_df_parse = in_seq_df_parse.withColumn(
            column, F.substring("value", start_pos, length))

    in_seq_df_parse = in_seq_df_parse.drop(
        "value").select(*seq_schema_dict.keys())
    return in_seq_df_parse


def add_index(df, offset=1, colName="index"):
    new_schema = StructType(
        [StructField(colName, LongType(), True)]
        + df.schema.fields
    )
    zipped_rdd = df.rdd.zipWithIndex()

    new_rdd = zipped_rdd.map(lambda args: (
        [args[1] + offset] + list(args[0])))
    df = spark.createDataFrame(new_rdd, new_schema)
    return df


def build_data(in_df, seq_num, seq_value, start_pos, end_pos, schema):
    df = add_index(parse_df(schema.get(seq_num), in_df.filter(F.substring("value", start_pos, end_pos) == seq_value)))
    return df


config_dict = load_config(schema_file_path)

input_file_df = spark.read.text(input_file_path).repartition(num_partitions)\
    .filter(F.substring("value", 1, 3) == detail_rec_identifier).cache()

sequences = ["SEQ01", "SEQ02"]

data_dict = {}
for seq in sequences:
    data_dict[seq] = build_data(input_file_df, seq, seq[2:], 2, 4, schema_dict)


final_df = data_dict["SEQ01"]\
    .join(data_dict["SEQ02"], ["index"], "outer")\
    .join(data_dict["SEQ03"], ["index"], "outer")\
    .join(data_dict["SEQ04"], ["index"], "outer")\
    .drop("index")

write_output(final_df, output_file_path + "/" + str(currentDT) + "/MERGED")


