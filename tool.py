import argparse
import sys
import redshift_connector
from db_cfg import redshift

db = redshift



def get_all_schemas():
    sql = """
    SELECT schema_name, table_name
    FROM svv_all_tables
    WHERE database_name = 'qppardb'
    AND table_type = 'TABLE'
    """
    if len(schema_list) > 0:
        a = ','.join("'{0}'".format(s) for s in schema_list)
        sql += f"AND schema_name IN ({a})"

    q = nullipotent_execute(sql)
    all_data = q.fetchall()
    m = {}
    for data in all_data:
        if data[0] not in m:
            m[data[0]] = []
        m[data[0]].append(data[1])
    return m


def get_all_views():
    sql = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'VIEW'
    """
    if len(schema_list) > 0:
        a = ','.join("'{0}'".format(s) for s in schema_list)
        sql += f"AND table_schema IN ({a})"

    q = nullipotent_execute(sql)
    all_data = q.fetchall()
    views = {}
    for data in all_data:
        if data[0] not in views:
            views[data[0]] = []
        views[data[0]].append(data[1])
    return views



def get_view_ddl(view_schema):
    sql = f"""
    SELECT 'CREATE VIEW ' || table_schema || '.' || table_name || ' AS ' || view_definition AS ddl
    FROM information_schema.views
    WHERE table_schema = '{view_schema}'
    """
    q = nullipotent_execute(sql)
    ddl_data = q.fetchone()
    if ddl_data:
        return ddl_data[0]
    return None

def format_views(views):
	ddls = []
    for schema in views:
        for view in views[schema]:
            ddl = get_view_ddl(schema, view)
            if ddl:
                ddls.append(ddl)
    return ddl

def write_ddls_to_disk(objects, output_path):
    if output_path:
        with open(output_path, 'w') as file:
            file.write('\n\n'.join(objects))
    else:
        for ddl in objects:
            print(ddl)


def upload_to_s3(file_name, bucket_name, object_name=None):
    if object_name is None:
        object_name = file_name
    s3_client = boto3.client('s3')

    try:
        s3_client.upload_file(file_name, bucket_name, object_name)
        print(f"File {file_name} uploaded to {bucket_name}/{object_name}")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")
    except PartialCredentialsError:
        print("Incomplete credentials provided")
		
def unload_statements():
    statements = []
    for s in schemas:
        for t in schemas[s]:
            statements.append(f"""UNLOAD ('SELECT * FROM {s}.{t}')   
                TO '{unload_to_location}' 
                IAM_ROLE '{iam_role}';
            """)
    return statements


# Only use for nullipotent queries, primarily select
def nullipotent_execute(sql):
    print(sql)
    return cursor.execute(sql)


def write_to_disk(str):
    # Out of scope for current ticket
    # Should support S3 and local filesystem
    return


# Use with insert, update, drop, etc. Will only run query when dry_run is false
def safe_execute(sql):
    print(sql)
    # if not dry_run:
    # cursor.execute(sql)


def get_db():
    if db_name == 'redshift':
        db = redshift
    else:
        print('Unable to proceed with db accounts creation. No config found for the DB Name specified!')
        return
    conn = redshift_connector.connect(
        host=db["DB_HOST"],
        port=db["DB_PORT"],
        user=db["DB_USER"],
        database=db["DB_NAME"],
        password=db["DB_PASSWORD"]
    )
    return conn.cursor()


def str2bool(v):
    print(v)
    return v.lower() != "false"


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--db_name", help="Name of RS database", default="redshift")
    parser.add_argument("--iam_role", help="Iam role used for unload statements", required=True)
    parser.add_argument("--unload_to_location", help="S3 path to unload table(s)", required=True)
    parser.add_argument("--schemas", help="Comma separated list of schemas", default=[])
    parser.add_argument("--dry_run", help="Run script in write mode. Must be set to false, "
                                          "All other values default to true", default="true")
    parser.add_argument("--output_path", help="File path to write DDL statements for views. stdout if not provided.", default="")
    parser.add_argument("--do_unload", help="Run unload statements if set to true", default=False)

    args = vars(parser.parse_args())
    db_name = args.get("db_name")
    iam_role = args.get("iam_role")
    sch = args.get("schemas")
    schema_list = sch if sch == [] else [s.strip() for s in sch.split(",")]
    dry_run = str2bool(args.get("dry_run"))
    output_path = args.get("output_path")
    unload_to_location = args.get("unload_to_location")
    do_unload = str2bool(args.get("do_unload"))

    print(f"Dry run is {dry_run}")

    cursor = get_db()
    schemas = get_all_schemas()
    stmts = unload_statements()
    write_ddls_to_disk(stmts, 'ddl_table.sql')
    
	views = get_all_views()
    _views = format_views(views)
    write_ddls_to_disk(_views, 'ddl_views.sql')
	
    '''if output_path != "":
        write_to_disk('\n'.join(stmts))
    if do_unload:
        for stmt in stmts:
            safe_execute(stmt)'''

    
    
