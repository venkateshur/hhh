import argparse
import os
import sys
import traceback

import boto3
import psycopg2
import qppar_configuration as qppar


class Part:
    def __init__(self, schema, table, partitions, path):
        self.schema = schema
        self.table = table
        self.partitions = partitions
        self.path = path

    def __str__(self):
        return f"Schema: {self.schema}, Table: {self.table}, Partitions: {self.partitions}, Path: {self.path}"


def get_connection():
    """Establishes a connection to the Redshift database."""
    print("Connecting to Redshift database...")
    connection_props = qppar.redshift
    conn = psycopg2.connect(**connection_props)
    conn.autocommit = True
    print("Connection to Redshift successful.")
    return conn


def execute_query(sql, connection):
    """Executes a SQL query and returns the results."""
    with connection.cursor() as cursor:
        print(f"Executing query: {sql}")
        cursor.execute(sql)
        results = cursor.fetchall()
        print(f"Query executed successfully. Retrieved {len(results)} rows.")
        return results


def write_sql_to_file(filename, sql_statements, output_dir, write_to_s3=False, s3_bucket=None):
    """Writes SQL statements to a file in the specified directory or uploads them to S3."""
    file_path = os.path.join(output_dir, filename)

    if not write_to_s3:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created output directory: {output_dir}")

        with open(file_path, 'w') as f:
            for statement in sql_statements:
                f.write(statement + '\n')
        print(f"SQL statements written to {file_path}")
    else:
        if not s3_bucket:
            raise ValueError("S3 bucket must be specified if write_to_s3 is True.")

        s3_client = boto3.client('s3')
        s3_key = os.path.join(output_dir, filename)

        with open(file_path, 'w') as f:
            for statement in sql_statements:
                f.write(statement + '\n')

        s3_client.upload_file(file_path, s3_bucket, s3_key)
        print(f"SQL statements uploaded to s3://{s3_bucket}/{s3_key}")


def get_all_partitions(connection, schemas):
    """Retrieves all partitions for the specified schemas."""
    partitions = []
    for schema in schemas:
        sql = f"""SELECT schemaname, tablename, values, location
                  FROM svv_external_partitions
                  WHERE schemaname = '{schema}'"""
        results = execute_query(sql, connection)
        for row in results:
            partitions.append(Part(row[0], row[1], row[2], row[3]))
            print(f"Partition found: {partitions[-1]}")
    statements = [f"ALTER TABLE {part.schema}.{part.table} ADD PARTITION ({part.partitions}) LOCATION '{part.path}'" for
                  part in partitions]
    return statements


def write_add_partitions_sql(sql_statements, output_dir, write_to_s3, s3_bucket):
    """SQL statements to add partitions."""
    write_sql_to_file("add_partitions.sql", sql_statements, output_dir, write_to_s3, s3_bucket)


def unload_statements(schemas, unload_to_location, iam_role):
    """Generates SQL unload statements."""
    statements = [
        f"UNLOAD ('SELECT * FROM {schema}.{table}') TO '{unload_to_location}' IAM_ROLE '{iam_role}';"
        for schema, table in schemas.items()
    ]
    return statements


def get_all_schemas(connection, schema_list, table_name="qppardb"):
    """Retrieves all schemas and tables from the database."""
    sql = f"""SELECT schema_name, table_name
              FROM svv_all_tables
              WHERE database_name = '{table_name}'
              AND table_type = 'TABLE'"""

    if schema_list:
        schema_filter = ','.join(f"'{s}'" for s in schema_list)
        sql += f" AND schema_name IN ({schema_filter})"
    results = execute_query(sql, connection)
    return {row[0]: row[1] for row in results}


def get_external_schemas(connection):
    """Retrieves all external schemas."""
    sql = "SELECT schemaname FROM SVV_EXTERNAL_SCHEMAS"
    return [row[0] for row in execute_query(sql, connection)]


def create_statements(schemas, iam_role, database):
    """Creates Glue schema creation statements."""
    statements = [
        f"CREATE EXTERNAL SCHEMA {schema} FROM DATABASE {database} IAM_ROLE {iam_role}"
        for schema in schemas
    ]
    return statements


def write_glue_statements(statements, output_dir, write_to_s3, s3_bucket):
    """Writes Glue schema creation statements to a file."""
    write_sql_to_file("glue_statements.sql", statements, output_dir, write_to_s3, s3_bucket)


def generate_ddl(connection, schemas=None):
    """Generates DDL statements for tables in the specified schemas."""
    schema_filter = "NOT IN ('pg_catalog', 'public', 'information_schema')" if schemas is None else \
        "IN (" + ','.join(f"'{s}'" for s in schemas) + ")"
    sql = f"""SELECT REGEXP_REPLACE(schemaname, '^zzzzzzzz', '') AS schemaname,
                     REGEXP_REPLACE(tablename, '^zzzzzzzz', '') AS tablename,
                     ddl
              FROM (
                SELECT table_id, schemaname, tablename, seq, ddl
                FROM (
                  -- DROP TABLE
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 0 AS seq,
                         '--DROP TABLE ' || QUOTE_IDENT(n.nspname) || '.' || QUOTE_IDENT(c.relname) || ';' AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  WHERE c.relkind = 'r'
                  -- CREATE TABLE
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 2 AS seq,
                         'CREATE TABLE IF NOT EXISTS ' || QUOTE_IDENT(n.nspname) || '.' || QUOTE_IDENT(c.relname) AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  WHERE c.relkind = 'r'
                  -- OPEN PAREN COLUMN LIST
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 5 AS seq, '(' AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  WHERE c.relkind = 'r'
                  -- COLUMN LIST
                  UNION
                  SELECT table_id, schemaname, tablename, seq,
                         '\t' || col_delim || col_name || ' ' || col_datatype || ' ' || col_nullable || ' ' || col_default || ' ' || col_encoding AS ddl
                  FROM (
                    SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 100000000 + a.attnum AS seq,
                           CASE WHEN a.attnum > 1 THEN ',' ELSE '' END AS col_delim,
                           QUOTE_IDENT(a.attname) AS col_name,
                           CASE WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING') > 0
                                THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING', 'VARCHAR')
                                WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER') > 0
                                THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER', 'CHAR')
                                ELSE UPPER(format_type(a.atttypid, a.atttypmod))
                           END AS col_datatype,
                           CASE WHEN format_encoding((a.attencodingtype)::integer) = 'none'
                                THEN 'ENCODE RAW'
                                ELSE 'ENCODE ' || format_encoding((a.attencodingtype)::integer)
                           END AS col_encoding,
                           CASE WHEN a.atthasdef IS TRUE THEN 'DEFAULT ' || adef.adsrc ELSE '' END AS col_default,
                           CASE WHEN a.attnotnull IS TRUE THEN 'NOT NULL' ELSE '' END AS col_nullable
                    FROM pg_namespace AS n
                    INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                    INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
                    LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum
                    WHERE c.relkind = 'r' AND a.attnum > 0
                    ORDER BY a.attnum
                  )
                  -- CONSTRAINT LIST
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 200000000 + MOD(CAST(con.oid AS INT), 100000000) AS seq,
                         '\t,' || pg_get_constraintdef(con.oid) AS ddl
                  FROM pg_constraint AS con
                  INNER JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid
                  INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
                  WHERE c.relkind = 'r' AND pg_get_constraintdef(con.oid) NOT LIKE 'FOREIGN KEY%'
                  ORDER BY seq
                  -- CLOSE PAREN COLUMN LIST
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 299999999 AS seq, ')' AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  WHERE c.relkind = 'r'
                  -- BACKUP
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 300000000 AS seq, 'BACKUP NO' AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  INNER JOIN (
                    SELECT SPLIT_PART(key, '_', 5) AS id
                    FROM pg_conf
                    WHERE key LIKE 'pg_class_backup_%'
                    AND SPLIT_PART(key, '_', 4) = (
                      SELECT oid FROM pg_database WHERE datname = current_database()
                    )
                  ) t ON t.id = c.oid
                  WHERE c.relkind = 'r'
                  -- BACKUP WARNING
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 1 AS seq,
                         '--WARNING: This DDL inherited the BACKUP NO property from the source table' AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  INNER JOIN (
                    SELECT SPLIT_PART(key, '_', 5) AS id
                    FROM pg_conf
                    WHERE key LIKE 'pg_class_backup_%'
                    AND SPLIT_PART(key, '_', 4) = (
                      SELECT oid FROM pg_database WHERE datname = current_database()
                    )
                  ) t ON t.id = c.oid
                  WHERE c.relkind = 'r'
                  -- DISTSTYLE
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 300000001 AS seq,
                         CASE WHEN c.reldiststyle = 0 THEN 'DISTSTYLE EVEN'
                              WHEN c.reldiststyle = 1 THEN 'DISTSTYLE KEY'
                              WHEN c.reldiststyle = 8 THEN 'DISTSTYLE ALL'
                              WHEN c.reldiststyle = 9 THEN 'DISTSTYLE AUTO'
                              ELSE '<<Error - UNKNOWN DISTSTYLE>>'
                         END AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  WHERE c.relkind = 'r'
                  -- DISTKEY COLUMNS
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 400000000 + a.attnum AS seq,
                         ' DISTKEY (' || QUOTE_IDENT(a.attname) || ')' AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
                  WHERE c.relkind = 'r' AND a.attisdistkey IS TRUE AND a.attnum > 0
                  -- SORTKEY COLUMNS
                  UNION
                  SELECT table_id, schemaname, tablename, seq,
                         CASE WHEN min_sort < 0 THEN 'INTERLEAVED SORTKEY (' ELSE ' SORTKEY (' END AS ddl
                  FROM (
                    SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 499999999 AS seq,
                           MIN(attsortkeyord) AS min_sort
                    FROM pg_namespace AS n
                    INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                    INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
                    WHERE c.relkind = 'r' AND ABS(a.attsortkeyord) > 0 AND a.attnum > 0
                    GROUP BY c.oid, n.nspname, c.relname
                  )
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 500000000 + ABS(a.attsortkeyord) AS seq,
                         CASE WHEN ABS(a.attsortkeyord) = 1 THEN '\t' || QUOTE_IDENT(a.attname)
                              ELSE '\t, ' || QUOTE_IDENT(a.attname)
                         END AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
                  WHERE c.relkind = 'r' AND ABS(a.attsortkeyord) > 0 AND a.attnum > 0
                  ORDER BY ABS(a.attsortkeyord)
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 599999999 AS seq, '\t)' AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
                  WHERE c.relkind = 'r' AND ABS(a.attsortkeyord) > 0 AND a.attnum > 0
                  -- END SEMICOLON
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 600000000 AS seq, ';' AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  WHERE c.relkind = 'r'
                  -- COMMENT
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 600250000 AS seq,
                         'COMMENT ON ' || COALESCE(CASE WHEN cl.column_name IS NOT NULL THEN 'COLUMN ' ELSE 'TABLE ' END, '') ||
                         QUOTE_IDENT(n.nspname) || '.' || QUOTE_IDENT(c.relname) ||
                         COALESCE(CASE WHEN cl.column_name IS NOT NULL THEN '.' || cl.column_name ELSE '' END, '') ||
                         ' IS \'' || TRIM(des.description) || '\';' AS ddl
                  FROM pg_description AS des
                  JOIN pg_class AS c ON c.oid = des.objoid
                  JOIN pg_namespace AS n ON n.oid = c.relnamespace
                  LEFT JOIN information_schema.columns AS cl ON cl.ordinal_position::integer = des.objsubid AND cl.table_name::NAME = c.relname
                  WHERE c.relkind = 'r'
                  -- TABLE OWNERSHIP AS AN ALTER TABLE STATEMENT
                  UNION
                  SELECT c.oid::bigint AS table_id, n.nspname AS schemaname, c.relname AS tablename, 600500000 AS seq,
                         'ALTER TABLE ' || QUOTE_IDENT(n.nspname) || '.' || QUOTE_IDENT(c.relname) ||
                         ' OWNER TO ' || QUOTE_IDENT(u.usename) || ';' AS ddl
                  FROM pg_namespace AS n
                  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
                  INNER JOIN pg_user AS u ON c.relowner = u.usesysid
                  WHERE c.relkind = 'r'
                )
                UNION
                SELECT c.oid::bigint AS table_id, 'zzzzzzzz' || n.nspname AS schemaname, 'zzzzzzzz' || c.relname AS tablename,
                       700000000 + MOD(CAST(con.oid AS INT), 100000000) AS seq,
                       'ALTER TABLE ' || QUOTE_IDENT(n.nspname) || '.' || QUOTE_IDENT(c.relname) ||
                       ' ADD ' || pg_get_constraintdef(con.oid)::VARCHAR(10240) || ';' AS ddl
                FROM pg_constraint AS con
                INNER JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid
                INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE c.relkind = 'r' AND con.contype = 'f'
                ORDER BY seq
              ) ORDER BY table_id, schemaname, tablename, seq
              WHERE schemaname {schema_filter};"""
    data = execute_query(sql, connection)
    schema_ddl = {}
    for row in data:
        schema, table, ddl = row
        if schema not in schema_ddl:
            schema_ddl[schema] = {}
        if table not in schema_ddl[schema]:
            schema_ddl[schema][table] = ""
        schema_ddl[schema][table] += ddl + "\n"
        print(f"Generated DDL for schema: {schema}, table: {table}")
    return schema_ddl


def write_ddl_statements_to_files(ddl_statements, output_dir, write_to_s3, s3_bucket):
    """Writes each DDL statement to a separate file."""
    statements = []
    for schema, tables in ddl_statements.items():
        for table, ddl in tables.items():
            statements.append(ddl)
            print(f"Writing DDL for {schema}.{table}")
    write_sql_to_file("ddl_statements.sql", statements, output_dir, write_to_s3, s3_bucket)


def generate_view_statements(connection, schemas=None):
    """Generates view statements for the specified schemas."""
    schema_filter = "NOT IN ('pg_catalog', 'public', 'information_schema')" if schemas is None else \
        "IN (" + ','.join(f"'{s}'" for s in schemas) + ")"
    sql = f"""SELECT schemaname, viewname, definition
              FROM pg_views
              WHERE schemaname {schema_filter};"""
    return execute_query(sql, connection)


def write_view_statements_to_files(view_statements, output_dir, write_to_s3, s3_bucket):
    """Writes view statements to a file."""
    statements = [view[2] for view in view_statements]
    write_sql_to_file("view_statements.sql", statements, output_dir, write_to_s3, s3_bucket)


def main():
    """Main function to parse arguments and execute relevant actions."""
    parser = argparse.ArgumentParser(description="Redshift Database Management")
    parser.add_argument("--action", choices=["add_part", "migrate", "glue", "gen_ddl", "gen_views"], required=True,
                        help="Action to perform")
    parser.add_argument("--schemas", nargs='*', default=[], help="Schemas to include (for migrate action)")
    parser.add_argument("--iam_role", help="IAM role for Glue schema creation")
    parser.add_argument("--database", help="Database for Glue schema creation", default="qppardb")
    parser.add_argument("--unload_path", help="s3 Path for unloading data")
    parser.add_argument("--output_dir", default="sql_output", help="Directory to store SQL statements", required=True)
    parser.add_argument("--s3_write", action='store_true', help="Flag to write output files to S3")
    parser.add_argument("--s3_bucket", help="S3 bucket name for storing SQL files when s3_write is enabled")
    args = parser.parse_args()

    connection = get_connection()

    try:

        if args.action == "add_part":
            if not args.schemas:
                print("Schema, table, partitions, and path are required for add_part action")
                sys.exit(1)
            alter_add_partitions_sqls = get_all_partitions(connection=connection, schemas=args.schemas)
            write_add_partitions_sql(alter_add_partitions_sqls, args.output_dir, args.s3_write, args.s3_bucket)
        elif args.action == "migrate":
            schemas = get_all_schemas(connection, args.schemas)
            unload_sqls = unload_statements(schemas, args.output_dir, args.iam_role)
            write_sql_to_file("migration_statements.sql", unload_sqls, args.output_dir, args.s3_write, args.s3_bucket)
        elif args.action == "glue":
            if not args.iam_role:
                print("IAM role is required for glue action")
                sys.exit(1)
            external_schemas = get_external_schemas(connection)
            statements = create_statements(external_schemas, args.iam_role, args.database)
            write_glue_statements(statements, args.output_dir, args.s3_write, args.s3_bucket)
        elif args.action == "gen_ddl":
            ddl_statements = generate_ddl(connection, args.schemas)
            write_ddl_statements_to_files(ddl_statements, args.output_dir, args.s3_write, args.s3_bucket)
            print(f"DDL statements written to directory: {args.output_dir}")
        elif args.action == "gen_views":
            view_statements = generate_view_statements(connection, args.schemas)
            write_view_statements_to_files(view_statements, args.output_dir, args.s3_write, args.s3_bucket)
            print(f"View statements written to directory: {args.output_dir}")

        else:
            print("Invalid action")
            sys.exit(1)
    except Exception as ex:
        traceback.print_exc()
        print("Failed with exception: " + str(ex))
        raise ex
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
