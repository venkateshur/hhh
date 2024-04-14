#!/bin/bash


DB=""
TARGET_TABLE=""

TABLES=("table1" "table2" "table3")

# Hive query to append table to target table
append_table_query() {
    local source_table=$1
    echo "INSERT INTO TABLE $TARGET_TABLE SELECT * FROM $DB.$source_table;"
}

# Hive query to get record count from a table
record_count_query() {
    local table_name=$1
    echo "SELECT COUNT(*) FROM $DB.$table_name;"
}

# Hive query to get record count from target table
target_count_query="SELECT COUNT(*) FROM $DB.$TARGET_TABLE;"

# Append tables to target table
for table in "${TABLES[@]}"
do
    hive -e "$(append_table_query $DB.$table)"
done

# Get count of records in target table
target_count=$(hive -e "$target_count_query" | tail -n1)

# Get sum of record counts from all tables
total_count=0
for table in "${TABLES[@]}"
do
    count=$(hive -e "$(record_count_query $DB.$table)" | tail -n1)
    total_count=$((total_count + count))
done

# Compare total count with target table count
if [ $total_count -eq $target_count ]; then
    echo "Successfully appended all tables to $TARGET_TABLE. Total count matches target table count."
else
    echo "Error: Total count of records from all tables does not match target table count."
fi
