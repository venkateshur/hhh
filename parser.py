import datetime
import json
import sys
from collections import OrderedDict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType
from functools import reduce

if __name__ == '__main__':
    print('File parser')

appName = "Pyspark"
start_time = datetime.datetime.now()

spark = SparkSession.builder.master("local") \
    .appName(appName) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

currentDT = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

num_partitions = 200
input_file_path = sys.argv[1]
output_file_path = sys.argv[2]
schema_file_path = sys.argv[3]
detail_rec_identifier = str(sys.argv[4]).split(",")

if len(sys.argv) > 5:
    num_partitions = int(sys.argv[5])

record_types = {"F6162": [1, 2], "F63": [1, 2]}


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


def build_data(in_df, seq_num, seq_value, start_pos, length, schema):
    seq_schema = schema.get(str(seq_num))
    df = add_index(parse_df(seq_schema, in_df.filter(
        F.substring("value", start_pos, length) == seq_value)))
    return df


def determine_record_type(file_name):
    if file_name in "02102026.D211126":
        return ["F63"]


def build_all_columns(schema_dict):
    columns = []
    for record_type in schema_dict:
        for seq in schema_dict[record_type]:
            col_names_for_seq = seq.keys()
            for actual_name in col_names_for_seq:
                columns.append(f"{record_type}_${seq}_${actual_name}")
    return columns


def build_missing_columns(df, all_columns):
    diff = list(set(df.columns) - set(all_columns))
    for missing_col in diff:
        df = df.withColumn(missing_col, F.lit(""))

    return df


def get_data(schema_dict, sequences):
    seq_dict = {}
    input_file_df = input_file_df_raw.filter(
        F.substring("value", 2, 2).isin(*sequences))

    for seq in sequences:
        seq_dict[seq] = build_data(
            input_file_df, seq, str(seq), 4, 2, schema_dict)
    return seq_dict


def final_data(data_dict, sequences, catageory):
    dfs = []
    for sequence in sequences:
        if sequence in data_dict:
            df = data_dict[sequence].cache()

            if len(df.take(1)) > 0:
                no_index_columns = list(
                    filter(lambda x: (x != "index"), df.columns))
                columns_with_seq = list(
                    map(lambda x: str(sequence + "_" + x).strip(), no_index_columns))
                columns_with_seq_rectype = columns_with_seq[1]
                columns_with_seq_systemcode = columns_with_seq[0]
                columns_with_seq = ['RecordType' if i == columns_with_seq_rectype else 'SystemCode' if i ==
                                                                                                       columns_with_seq_systemcode else i
                                    for i in columns_with_seq]
                columns_with_seq.insert(0, "index")
                df = df.toDF(*columns_with_seq)
                # df_dict[sequence] = df
                dfs.append(df)
    final_df = reduce(lambda x, y: x.union(y), dfs).sort("RecordType", "index").withColumn("load_id", F.col("index")).drop("index")

    return final_df


schema_dict = load_config(schema_file_path)
all_columns = build_all_columns(schema_dict)

input_file_df_raw = spark.read.text(input_file_path).repartition(num_partitions) \
    .filter(F.substring("value", 2, 2).isin(*detail_rec_identifier)).withColumn("file_name",
                                                                                F.input_file_name()).cache()
file_name = [str(row.file_name) for row in input_file_df_raw.collect()][0].split("/").pop()

sequences = determine_record_type(file_name)

data_dict = get_data(schema_dict, sequences)


