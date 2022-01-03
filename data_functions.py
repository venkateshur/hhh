
'''
This function will get current time stamp
'''


def get_current_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")


'''
This function will format time stamp value and create data time object
'''


def get_time_value(timestamp):
    time = datetime.datetime.fromtimestamp(timestamp)
    return time.strftime("%Y%m%d%H%M%S%f")


'''
This function will build data frame with stats
'''


def build_stats(total_number_of_records, ingestion_start_time, ingestion_end_time):
    run_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    time_took_for_completion = ingestion_end_time - ingestion_start_time
    df = spark.createDataFrame(spark.sparkContext.parallelize(
        [[run_id, total_number_of_records, ingestion_start_time, ingestion_end_time, time_took_for_completion]]),
        ["run_id", "total_number_of_records", "ingestion_start_time", "ingestion_end_time",
         "execution_time"])
    return df
