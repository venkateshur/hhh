import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

raw_data_location_hdfs = sys.argv[1]  # /edx/us/lowes/data/raw/files/marketmediadatapipeline/salesforce/events
file_receipt_date = sys.argv[2]  # 2021-08-24
delimiter = sys.argv[3]  # ,
environment = sys.argv[4]  # dev

salesforce_events_table = 'marketing_media.event_marketing'
salesforce_events_stage_table = 'marketing_media.event_marketing_stage'
salesforce_events_raw_table = "marketing_media.event_marketing_raw"

feed_name = "salesforce_events_incremental_ingestion"

if environment == "prod":
    customer_table = "cust_vm.i0018h_cus_dat_hub"
    customer_secure_vm_table = "cust_sec_vm.i0033a_src_sys_cfr"

else:
    customer_table = "cust_vm.i0018e_lws_cus"
    customer_secure_vm_table = "cust_sec_vm.i0033a_src_sys_cfr"

spark = SparkSession.builder \
    .appName(feed_name) \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel('WARN')

log4jLogger = spark._jvm.org.apache.log4j
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)

logger.info("pyspark script logger initialized")

logger.info("raw_data_location_hdfs: " + raw_data_location_hdfs)
logger.info("file_receipt_date:      " + file_receipt_date)
logger.info("delimiter:              " + delimiter)

raw_data_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", delimiter) \
    .option("multiline", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .load(raw_data_location_hdfs + "/" + file_receipt_date)

renamed_columns_df_raw = raw_data_df \
    .withColumnRenamed("PhoneNumber", "phone_number") \
    .withColumnRenamed("EventName", "event_name") \
    .withColumnRenamed("EmailVersion", "email_version") \
    .withColumnRenamed("EventStartDateTime", "event_start_date_time") \
    .withColumnRenamed("BookingCode", "booking_code") \
    .withColumnRenamed("Identity_ID", "identity_id") \
    .withColumn("load_timestamp", f.current_timestamp()) \
    .withColumn("identity_id", f.trim(f.col("identity_id"))) \
    .withColumn("partition_date", f.lit(file_receipt_date)) \
    .select("*").persist(StorageLevel.MEMORY_AND_DISK)


def write_output(output_df, table_name):
    output_df.createOrReplaceTempView("salesforce_events_stage_temp")
    spark.sql("INSERT OVERWRITE TABLE {0} PARTITION(partition_date) SELECT * FROM salesforce_events_stage_temp".format(
        table_name))


if len(renamed_columns_df_raw.take(1)) > 0:
    write_output(renamed_columns_df_raw, salesforce_events_raw_table)
else:
    empty_data = []
    for col in renamed_columns_df_raw.columns:
        empty_data.append("")
    dataframe = spark.createDataFrame([empty_data], renamed_columns_df_raw.columns)\
        .withColumn("partition_date", f.lit(file_receipt_date)) \
        .withColumn("load_timestamp", f.current_timestamp())

    write_output(dataframe, salesforce_events_raw_table)

renamed_columns_df = renamed_columns_df_raw.filter(f.col("identity_id").isNotNull()).persist(
    StorageLevel.MEMORY_AND_DISK)
renamed_columns_df_raw.unpersist()

salesforce_events_hive_table = spark.read.table(salesforce_events_table).repartition("identity_id").persist(
    StorageLevel.MEMORY_AND_DISK)

cust_table_df = spark.read.table(customer_table).select("cus_id").dropDuplicates()
cust_secure_vm_table_df = spark.read.table(customer_secure_vm_table).select("cus_id", "src_id").dropDuplicates()
cust_join_df = cust_table_df.join(cust_secure_vm_table_df, cust_table_df.cus_id == cust_secure_vm_table_df.cus_id) \
    .select(f.trim(f.col("src_id")).alias("identity_id")).dropDuplicates()

if len(salesforce_events_hive_table.head(1)) > 0:
    existing_sales_force_events_df = renamed_columns_df.join(salesforce_events_hive_table,
                                                             renamed_columns_df.identity_id == salesforce_events_hive_table.identity_id,
                                                             "left_semi")
    new_salesforce_events_df = renamed_columns_df.join(salesforce_events_hive_table,
                                                       renamed_columns_df.identity_id == salesforce_events_hive_table.identity_id,
                                                       "left_anti")
    not_matched_existing_salesforce_events_df = salesforce_events_hive_table.join(existing_sales_force_events_df,
                                                                                  salesforce_events_hive_table.identity_id == existing_sales_force_events_df.identity_id,
                                                                                  "left_anti")
    final_events_df = not_matched_existing_salesforce_events_df \
        .union(existing_sales_force_events_df) \
        .union(new_salesforce_events_df)
else:
    final_events_df = renamed_columns_df

final_events_df = final_events_df.join(cust_join_df, final_events_df.identity_id == cust_join_df.identity_id,
                                       "left_semi") \
    .select(salesforce_events_hive_table.columns)

logger.info("writing final output into staging table " + salesforce_events_stage_table)

if len(final_events_df.take(1)) > 0:
    write_output(final_events_df, salesforce_events_stage_table)
else:
    empty_data = []
    for col in final_events_df.columns:
        empty_data.append("")
    dataframe = spark.createDataFrame([empty_data], final_events_df.columns)\
        .withColumn("partition_date", f.lit(file_receipt_date))\
        .withColumn("load_timestamp", f.current_timestamp())
    write_output(dataframe, salesforce_events_stage_table)
spark.stop()
