import psycopg2
from psycopg2 import sql
from p3_datawarehouse_aws_redshift.src.etl.sql_queries_etl import insert_table_queries, staging_events_copy, staging_songs_copy
from p3_datawarehouse_aws_redshift.src.utils import get_cluster_properties

def load_staging_table(cur, conn, query, filelocation, jsonpath):
    q = sql.SQL(query).format(filelocation=sql.Literal(filelocation), jsonpath=sql.Literal(jsonpath))
    print(q)
    cur.execute(q)
    conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def get_conn(config):
    x = get_cluster_properties(config)
    host = x['Endpoint_address']
    DWH_DB = config.get("DB", "DB_NAME")
    DWH_DB_USER = config.get("DB", "DB_USER")
    DWH_DB_PASSWORD = config.get("DB", "DB_PASSWORD")
    DWH_PORT = config.get("DB", "DB_PORT")
    jdbcstring = "host={} dbname={} user={} password={} port={}".format(
        host,
        DWH_DB,
        DWH_DB_USER,
        DWH_DB_PASSWORD,
        DWH_PORT
    )
    conn = psycopg2.connect(jdbcstring)
    print('*****\nChecking Connectionstatus:\n{}\n********'.format(conn.closed))
    return conn

def etl_main(config):
    x = get_cluster_properties(config)
    arn_role = x['Role_arn']
    conn = get_conn(config)
    cur = conn.cursor()
    print('load staging tables')
    load_staging_table(cur, conn, query=staging_events_copy, filelocation=)
    print('insert tables')
    insert_tables(cur, conn)
    print('closing connection')
    conn.close()
