SELECT 'source' AS table_name, COUNT(*) AS row_count FROM source_table
UNION ALL
SELECT 'target' AS table_name, COUNT(*) AS row_count FROM target_table;


SELECT source.*
FROM source_table source
LEFT JOIN target_table target ON source.id = target.id
WHERE target.id IS NULL
UNION ALL
SELECT target.*
FROM target_table target
LEFT JOIN source_table source ON target.id = source.id
WHERE source.id IS NULL;


SELECT id, COUNT(*) AS row_count
FROM source_table
GROUP BY id
HAVING COUNT(*) > 1;


SELECT id, COUNT(*) AS row_count
FROM target_table
GROUP BY id
HAVING COUNT(*) > 1;

# Build dynamic SQL query for comparing specific columns
compare_clauses = ' AND '.join([f'source.{col} <> target.{col}' for col in compare_columns])
select_columns = ', '.join([f'source.{col} AS source_{col}, target.{col} AS target_{col}' for col in compare_columns])

query = f"""
SELECT source.id, {select_columns}
FROM {source_table} source
JOIN {target_table} target ON source.id = target.id
WHERE {compare_clauses};
"""

# Execute the SQL query
result_df = spark.sql(query)
