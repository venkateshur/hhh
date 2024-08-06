import argparse
import sys
import redshift_connector
import os
from db_cfg import redshift


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
    return redshift_connector.connect(
        host=redshift['host'],
        database=redshift['database'],
        user=redshift['user'],
        password=redshift['password']
    )


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


def get_all_partitions(schemas):
    partitions = []
    for sch in schemas:
        sql = f""" select schemaname, tablename, values, location
                from svv_external_partitions
                where schemaname = '{sch}'
                """
        print(sql)
        q = execute_query(sql).fetchall()
        for s in q:
            partitions.append(Part(s[0], s[1], s[2], s[3]))
    return partitions


# Add Partition Functionality
def add_partition(part, connection, output_dir):
    sql = f"ALTER TABLE {part.schema}.{part.table} ADD PARTITION ({part.partitions}) LOCATION '{part.path}'"
    execute_query(sql, connection)
    write_sql_to_file(f"add_partition_{part.schema}_{part.table}.sql", [sql], output_dir)


# Get all schemas for Migration
def get_all_schemas(connection, schema_list=[], table_name="qppardb"):
    sql = """
    SELECT schema_name, table_name
    FROM svv_all_tables
    WHERE database_name = 'qppardb'
    AND table_type = 'TABLE'
    """
    if schema_list:
        schema_filter = ','.join(f"'{s}'" for s in schema_list)
        sql += f" AND schema_name IN ({schema_filter})"

    return {data[0]: data[1] for data in execute_query(sql, connection)}


# Write migration SQL statements to file
def write_migration_statements(schemas, output_dir):
    migration_statements = [
        f"ALTER TABLE {schema}.{table} OWNER TO new_owner;" for schema, table in schemas.items()  # Example statement
    ]
    write_sql_to_file("migration_statements.sql", migration_statements, output_dir)


# Get all external schemas for Glue
def get_external_schemas(connection):
    sql = "SELECT schemaname FROM SVV_EXTERNAL_SCHEMAS"
    return [data[0] for data in execute_query(sql, connection)]


# Create external schema statements
def create_statements(schema, iam_role, database):
    return [
        f"CREATE EXTERNAL SCHEMA {s} FROM DATABASE {database} IAM_ROLE {iam_role}"
        for s in schema
    ]


# Write glue SQL statements to file
def write_glue_statements(statements, output_dir):
    write_sql_to_file("glue_statements.sql", statements, output_dir)


# Generate DDL statements
def generate_ddl(connection):
    sql = """SELECT
    REGEXP_REPLACE(schemaname, '^zzzzzzzz', '') AS schemaname,
    REGEXP_REPLACE(tablename, '^zzzzzzzz', '') AS tablename,
    ddl
    FROM (
        SELECT
            c.oid::bigint as table_id,
            n.nspname AS schemaname,
            c.relname AS tablename,
            0 AS seq,
            '--DROP TABLE ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ';' AS ddl
        FROM
            pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE
            n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND c.relkind = 'r'
        UNION ALL
        SELECT
            c.oid::bigint as table_id,
            n.nspname AS schemaname,
            c.relname AS tablename,
            1 AS seq,
            'CREATE TABLE ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + E'\n(\n' + LISTAGG(
                E'\t' + QUOTE_IDENT(a.attname) + ' ' + pg_catalog.format_type(a.atttypid, a.atttypmod) + ' ' +
                CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE 'NULL' END, E',\n') WITHIN GROUP (ORDER BY a.attnum) + E'\n)' AS ddl
        FROM
            pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid
            JOIN pg_type t ON a.atttypid = t.oid
        WHERE
            n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND c.relkind = 'r'
            AND a.attnum > 0
            AND NOT a.attisdropped
        GROUP BY
            c.oid, n.nspname, c.relname
    ) b
    ORDER BY table_id, seq;
    """
    return execute_query(sql, connection)


# Write each DDL statement to a separate file
def write_ddl_statements_to_files(ddl_statements, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for ddl in ddl_statements:
        schema, table, statement = ddl
        file_name = f"{schema}_{table}.sql"
        write_sql_to_file(file_name, [statement], output_dir)


# Generate view statements
def generate_view_statements(connection):
    sql = """
    SELECT schemaname, viewname, definition
    FROM pg_views
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
    """
    return execute_query(sql, connection)


# Write view statements to separate files
def write_view_statements_to_files(view_statements, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for view in view_statements:
        schema, viewname, definition = view
        statement = f"CREATE OR REPLACE VIEW {schema}.{viewname} AS\n{definition};"
        file_name = f"{schema}_{viewname}.sql"
        write_sql_to_file(file_name, [statement], output_dir)


# Main function to parse arguments and execute relevant actions
def main():
    parser = argparse.ArgumentParser(description="Redshift Database Management")
    parser.add_argument("--action", choices=["add_part", "migrate", "glue", "gen_ddl", "gen_views"], required=True,
                        help="Action to perform")
    parser.add_argument("--schemas", nargs='*', default=[], help="Schemas to include (for migrate action)")
    parser.add_argument("--iam_role", help="IAM role for Glue schema creation")
    parser.add_argument("--database", help="Database for Glue schema creation")
    parser.add_argument("--path", help="Path for partition location")
    parser.add_argument("--output_dir", default="sql_output", help="Directory to store SQL statements")
    args = parser.parse_args()

    connection = get_connection()

    if args.action == "add_part":
        if not args.schemas or not args.path:
            print("Schema, table, partitions, and path are required for add_part action")
            sys.exit(1)
        partitions = get_all_partitions(schemas=args.schemas)
        add_partition(partitions, connection, args.output_dir)
    elif args.action == "migrate":
        schemas = get_all_schemas(connection, args.schemas)
        write_migration_statements(schemas, args.output_dir)
    elif args.action == "glue":
        if not args.iam_role or not args.database:
            print("IAM role and database are required for glue action")
            sys.exit(1)
        external_schemas = get_external_schemas(connection)
        statements = create_statements(external_schemas, args.iam_role, args.database)
        write_glue_statements(statements, args.output_dir)
    elif args.action == "gen_ddl":
        ddl_statements = generate_ddl(connection)
        write_ddl_statements_to_files(ddl_statements, args.output_dir)
        print(f"DDL statements written to directory: {args.output_dir}")
    elif args.action == "gen_views":
        view_statements = generate_view_statements(connection)
        write_view_statements_to_files(view_statements, args.output_dir)
        print(f"View statements written to directory: {args.output_dir}")


if __name__ == "__main__":
    main()
