mport argparse
import sys
import os
import qppar_configuration as qppar
import utils_parse_and_save as utils
import boto3
import psycopg2


# python migration_helper.py --action add_part --schemas your_schema --path "s3://your-bucket/path" --output_dir partition_sql

class Part:
    schema: str
    table: str
    partitions: str
    path: str

    def __init__(self, schema, table, p, path):
        print(schema)
        self.schema = schema
        self.table = table
        self.partitions = p
        self.path = path

    def __str__(self):
        return f"{self.schema}, {self.table}, {self.partitions}, {self.path}"


# Database connection
def get_connection():
    connection_props = qppar.redshift
    conn = psycopg2.connect(**connection_props)
    conn.autocommit = True
    print("Connection to Redshift Successful")
    return conn


# Common function to execute SQL
def execute_query(sql, connection):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


# Common method to write SQL to file
def write_sql_to_file(filename, sql_statements, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_path = os.path.join(output_dir, filename)
    with open(file_path, 'w') as f:
        for statement in sql_statements:
            f.write(statement + '\n')
    print(f"SQL statements written to {file_path}")


def get_all_partitions(connection, schemas):
    partitions = []
    for sch in schemas:
        sql = f""" select schemaname, tablename, values, location
                from svv_external_partitions
                where schemaname = '{sch}'
                """
        print(f"get_all_partitions query: \n {sql}")
        q = execute_query(sql, connection)
        for s in q:
            partitions.append(Part(s[0], s[1], s[2], s[3]))
    return partitions


# Add Partition Functionality
def build_add_partition(partition_objects, output_dir):
    partitions = []
    for part in partition_objects:
        sql = f"ALTER TABLE {part.schema}.{part.table} ADD PARTITION ({part.partitions}) LOCATION '{part.path}'"
        partitions.append(sql)
    write_sql_to_file(f"add_partitions.sql", partitions, output_dir)


def unload_statements(schemas, unload_to_location, iam_role):
    statements = []
    for s, t in schemas.items():
        statements.append(f"""unload ('select * from {s}.{t}') 
        to '{unload_to_location}' iam_role '{iam_role}';""")
    return statements


# Get all schemas for Migration
def get_all_schemas(connection, schema_list, table_name="qppardb"):
    sql = f"""
    SELECT schema_name, table_name
    FROM svv_all_tables
    WHERE database_name = '{table_name}'
    AND table_type = 'TABLE'
    """

    if schema_list:
        schema_filter = ','.join(f"'{s}'" for s in schema_list)
        sql += f" AND schema_name IN ({schema_filter})"
        print(f"et_all_schemas query: \n {sql}")

    return {data[0]: data[1] for data in execute_query(sql, connection)}


def get_external_schemas(connection):
    sql = "SELECT schemaname FROM SVV_EXTERNAL_SCHEMAS"
    return [data[0] for data in execute_query(sql, connection)]


def create_statements(schema, iam_role, database):
    return [
        f"CREATE EXTERNAL SCHEMA {s} FROM DATABASE {database} IAM_ROLE {iam_role}"
        for s in schema
    ]


def write_glue_statements(statements, output_dir):
    write_sql_to_file("glue_statements.sql", statements, output_dir)


def generate_ddl(connection, schemas=None):
    if schemas is not None:
        schema_filter = "not in ('pg_catalog', 'public', 'information_schema')"
    else:
        schema_filter = "in " + ','.join(f"'{s}'" for s in schemas)
    sql = f"""SELECT
 REGEXP_REPLACE (schemaname, '^zzzzzzzz', '') AS schemaname
 ,REGEXP_REPLACE (tablename, '^zzzzzzzz', '') AS tablename
 ,ddl
FROM
 (
 SELECT
  table_id
  ,schemaname
  ,tablename
  ,seq
  ,ddl
 FROM
  (
  --DROP TABLE
  SELECT
   c.oid::bigint as table_id
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,0 AS seq
   ,'--DROP TABLE ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ';' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --CREATE TABLE
  UNION SELECT
   c.oid::bigint as table_id
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,2 AS seq
   ,'CREATE TABLE IF NOT EXISTS ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + '' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --OPEN PAREN COLUMN LIST
  UNION SELECT c.oid::bigint as table_id,n.nspname AS schemaname, c.relname AS tablename, 5 AS seq, '(' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --COLUMN LIST
  UNION SELECT
   table_id
   ,schemaname
   ,tablename
   ,seq
   ,'\t' + col_delim + col_name + ' ' + col_datatype + ' ' + col_nullable + ' ' + col_default + ' ' + col_encoding AS ddl
  FROM
   (
   SELECT
    c.oid::bigint as table_id
   ,n.nspname AS schemaname
    ,c.relname AS tablename
    ,100000000 + a.attnum AS seq
    ,CASE WHEN a.attnum > 1 THEN ',' ELSE '' END AS col_delim
    ,QUOTE_IDENT(a.attname) AS col_name
    ,CASE WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING') > 0
      THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING', 'VARCHAR')
     WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER') > 0
      THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER', 'CHAR')
     ELSE UPPER(format_type(a.atttypid, a.atttypmod))
     END AS col_datatype
    ,CASE WHEN format_encoding((a.attencodingtype)::integer) = 'none'
     THEN 'ENCODE RAW'
     ELSE 'ENCODE ' + format_encoding((a.attencodingtype)::integer)
     END AS col_encoding
    ,CASE WHEN a.atthasdef IS TRUE THEN 'DEFAULT ' + adef.adsrc ELSE '' END AS col_default
    ,CASE WHEN a.attnotnull IS TRUE THEN 'NOT NULL' ELSE '' END AS col_nullable
   FROM pg_namespace AS n
   INNER JOIN pg_class AS c ON n.oid = c.relnamespace
   INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
   LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum
   WHERE c.relkind = 'r'
     AND a.attnum > 0
   ORDER BY a.attnum
   )
  --CONSTRAINT LIST
  UNION (SELECT
   c.oid::bigint as table_id
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,200000000 + MOD(CAST(con.oid AS INT),100000000) AS seq
   ,'\t,' + pg_get_constraintdef(con.oid) AS ddl
  FROM pg_constraint AS con
  INNER JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid
  INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' AND pg_get_constraintdef(con.oid) NOT LIKE 'FOREIGN KEY%'
  ORDER BY seq)
  --CLOSE PAREN COLUMN LIST
  UNION SELECT c.oid::bigint as table_id,n.nspname AS schemaname, c.relname AS tablename, 299999999 AS seq, ')' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --BACKUP
  UNION SELECT
  c.oid::bigint as table_id
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,300000000 AS seq
   ,'BACKUP NO' as ddl
FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN (SELECT
    SPLIT_PART(key,'_',5) id
    FROM pg_conf
    WHERE key LIKE 'pg_class_backup_%'
    AND SPLIT_PART(key,'_',4) = (SELECT
      oid
      FROM pg_database
      WHERE datname = current_database())) t ON t.id=c.oid
  WHERE c.relkind = 'r'
  --BACKUP WARNING
  UNION SELECT
  c.oid::bigint as table_id
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,1 AS seq
   ,'--WARNING: This DDL inherited the BACKUP NO property from the source table' as ddl
FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN (SELECT
    SPLIT_PART(key,'_',5) id
    FROM pg_conf
    WHERE key LIKE 'pg_class_backup_%'
    AND SPLIT_PART(key,'_',4) = (SELECT
      oid
      FROM pg_database
      WHERE datname = current_database())) t ON t.id=c.oid
  WHERE c.relkind = 'r'
  --DISTSTYLE
  UNION SELECT
   c.oid::bigint as table_id
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,300000001 AS seq
   ,CASE WHEN c.reldiststyle = 0 THEN 'DISTSTYLE EVEN'
    WHEN c.reldiststyle = 1 THEN 'DISTSTYLE KEY'
    WHEN c.reldiststyle = 8 THEN 'DISTSTYLE ALL'
    WHEN c.reldiststyle = 9 THEN 'DISTSTYLE AUTO'
    ELSE '<<Error - UNKNOWN DISTSTYLE>>'
    END AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --DISTKEY COLUMNS
  UNION SELECT
   c.oid::bigint as table_id
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,400000000 + a.attnum AS seq
   ,' DISTKEY (' + QUOTE_IDENT(a.attname) + ')' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND a.attisdistkey IS TRUE
    AND a.attnum > 0
  --SORTKEY COLUMNS
  UNION select table_id,schemaname, tablename, seq,
       case when min_sort <0 then 'INTERLEAVED SORTKEY (' else ' SORTKEY (' end as ddl
from (SELECT
   c.oid::bigint as table_id
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,499999999 AS seq
   ,min(attsortkeyord) min_sort FROM pg_namespace AS n
  INNER JOIN  pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
  AND abs(a.attsortkeyord) > 0
  AND a.attnum > 0
  group by 1,2,3,4 )
  UNION (SELECT
   c.oid::bigint as table_id
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,500000000 + abs(a.attsortkeyord) AS seq
   ,CASE WHEN abs(a.attsortkeyord) = 1
    THEN '\t' + QUOTE_IDENT(a.attname)
    ELSE '\t, ' + QUOTE_IDENT(a.attname)
    END AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND abs(a.attsortkeyord) > 0
    AND a.attnum > 0
  ORDER BY abs(a.attsortkeyord))
  UNION SELECT
   c.oid::bigint as table_id
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,599999999 AS seq
   ,'\t)' AS ddl
  FROM pg_namespace AS n
  INNER JOIN  pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN  pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND abs(a.attsortkeyord) > 0
    AND a.attnum > 0
  --END SEMICOLON
  UNION SELECT c.oid::bigint as table_id ,n.nspname AS schemaname, c.relname AS tablename, 600000000 AS seq, ';' AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' 
  --COMMENT
  UNION
  SELECT c.oid::bigint AS table_id,
       n.nspname     AS schemaname,
       c.relname     AS tablename,
       600250000     AS seq,
       ('COMMENT ON '::text + nvl2(cl.column_name, 'column '::text, 'table '::text) + quote_ident(n.nspname::text) + '.'::text + quote_ident(c.relname::text) + nvl2(cl.column_name, '.'::text + cl.column_name::text, ''::text) + ' IS \''::text + trim(des.description) + '\'; '::text)::character VARYING AS ddl
  FROM pg_description des
  JOIN pg_class c ON c.oid = des.objoid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN information_schema."columns" cl
  ON cl.ordinal_position::integer = des.objsubid AND cl.table_name::NAME = c.relname
  WHERE c.relkind = 'r'

  UNION
  --TABLE OWNERSHIP AS AN ALTER TABLE STATMENT
  SELECT c.oid::bigint as table_id ,n.nspname AS schemaname, c.relname AS tablename, 600500000 AS seq, 
  'ALTER TABLE ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ' owner to '+  QUOTE_IDENT(u.usename) +';' AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_user AS u ON c.relowner = u.usesysid
  WHERE c.relkind = 'r'
  
  )
  UNION (
    SELECT c.oid::bigint as table_id,'zzzzzzzz' || n.nspname AS schemaname,
       'zzzzzzzz' || c.relname AS tablename,
       700000000 + MOD(CAST(con.oid AS INT),100000000) AS seq,
       'ALTER TABLE ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ' ADD ' + pg_get_constraintdef(con.oid)::VARCHAR(10240) + ';' AS ddl
    FROM pg_constraint AS con
      INNER JOIN pg_class AS c
             ON c.relnamespace = con.connamespace
             AND c.oid = con.conrelid
      INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
    AND con.contype = 'f'
    ORDER BY seq
  )
 ORDER BY table_id,schemaname, tablename, seq
 )
 where schemaname {schema_filter};
"""
    all_data = execute_query(sql, connection)
    m = {}
    for data in all_data:
        schema = data[0]
        table = data[1]
        ddl = data[2]
        if schema not in  m:
            m[schema] = {}

        if table not in m[schema]:
            m[schema][table] = ""
        m[schema][table] += (ddl + "\n")
    return m


# Write each DDL statement to a separate file
def write_ddl_statements_to_files(ddl_statements, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    statements = []
    for s in ddl_statements:
        for t in ddl_statements[s]:
            statements.append(ddl_statements[s][t])

    write_sql_to_file("ddl_statements.sql", statements, output_dir)


# Generate view statements
def generate_view_statements(connection, schemas=None):
    if schemas is not None:
        schema_filter = "not in ('pg_catalog', 'public', 'information_schema')"
    else:
        schema_filter = "in " + ','.join(f"'{s}'" for s in schemas)

    sql = f"""
    SELECT schemaname, viewname, definition
    FROM pg_views
    WHERE schemaname {schema_filter};
    """
    return execute_query(sql, connection)


def write_view_statements_to_files(view_statements, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    statements = []
    for view in view_statements:
        statements.append(view[2])

    write_sql_to_file("view_statements.sql", statements, output_dir)


# Main function to parse arguments and execute relevant actions
def main():
    parser = argparse.ArgumentParser(description="Redshift Database Management")
    parser.add_argument("--action", choices=["add_part", "migrate", "glue", "gen_ddl", "gen_views"], required=True,
                        help="Action to perform")
    parser.add_argument("--schemas", nargs='*', default=[], help="Schemas to include (for migrate action)")
    parser.add_argument("--iam_role", help="IAM role for Glue schema creation")
    parser.add_argument("--database", help="Database for Glue schema creation", default="qppardb")
    parser.add_argument("--path", help="Path for partition location")
    parser.add_argument("--unload_path", help="Path for un loading data")
    parser.add_argument("--output_dir", default="sql_output", help="Directory to store SQL statements")
    args = parser.parse_args()

    connection = get_connection()

    if args.action == "add_part":
        if not args.schemas or not args.path:
            print("Schema, table, partitions, and path are required for add_part action")
            sys.exit(1)
        partitions = get_all_partitions(connection=connection, schemas=args.schemas)
        build_add_partition(partitions, args.output_dir)
    elif args.action == "migrate":
        schemas = get_all_schemas(connection, args.schemas)
        unload_sqls = unload_statements(schemas, args.output_dir, args.iam_role)
        write_sql_to_file("migration_statements.sql", unload_sqls, args.output_dir)
    elif args.action == "glue":
        if not args.iam_role:
            print("IAM role and database are required for glue action")
            sys.exit(1)
        external_schemas = get_external_schemas(connection)
        statements = create_statements(external_schemas, args.iam_role, args.database)
        write_glue_statements(statements, args.output_dir)
    elif args.action == "gen_ddl":
        ddl_statements = generate_ddl(connection, args.schemas)
        write_ddl_statements_to_files(ddl_statements, args.output_dir)
        print(f"DDL statements written to directory: {args.output_dir}")
    elif args.action == "gen_views":
        view_statements = generate_view_statements(connection, args.schemas)
        write_view_statements_to_files(view_statements, args.output_dir)
        print(f"View statements written to directory: {args.output_dir}")


if __name__ == "__main__":
    main()
