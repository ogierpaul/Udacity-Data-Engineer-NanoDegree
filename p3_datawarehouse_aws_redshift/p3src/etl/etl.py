import psycopg2
from psycopg2 import sql
from p3src.etl.sql_queries_etl import insert_table_queries, staging_events_copy, staging_songs_copy
from p3src.utils import get_cluster_properties, get_conn


def load_staging_songs(cur, conn, arn, filelocation):
    """
    Load the song data from s3 into the staging tables for songs
    Args:
        cur (psycopg2.Cursor):
        conn (psycopg2.Connector):
        arn (str): ARN role to user
        filelocation (str): s3 bucket location
    Returns:
        None
    """
    q = sql.SQL(staging_songs_copy).format(
        filelocation=sql.Literal(filelocation),
        arn=sql.Literal(arn)
    )
    print(q.as_string(conn))
    cur.execute(q)
    conn.commit()
    return None

def load_staging_events(cur, conn, arn, filelocation, jsonpath):
    """
    Load the log (events) data from s3 into the staging table for events
    Args:
        cur (psycopg2.Cursor):
        conn (psycopg2.Connector):
        arn (str): ARN role to user
        filelocation (str): s3 bucket location
        jsonpath (str): jsonpath

    Returns:
        None
    """
    q = sql.SQL(staging_events_copy).format(
        filelocation=sql.Literal(filelocation),
        jsonpath=sql.Literal(jsonpath),
        arn=sql.Literal(arn)
    )
    print(q.as_string(conn))
    cur.execute(q)
    conn.commit()
    return None




def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def etl_main(config):
    x = get_cluster_properties(config)
    arn_role = x['Role_arn']
    conn = get_conn(config)
    cur = conn.cursor()
    print('load staging tables')
    loglocation = config.get("S3", "LOGPATH")
    logjsonpath = config.get("S3", "LOGJSONPATH")
    songlocation = config.get("S3", "SONGPATH")
    print('\nStart loading data into staging tables\n')
    load_staging_events(cur, conn, filelocation=loglocation, jsonpath=logjsonpath, arn=arn_role)
    load_staging_songs(cur, conn, filelocation=songlocation, arn=arn_role)
    print('\nStart inserting data into star schema\n')
    insert_tables(cur, conn)
    print('\nclosing connection\n')
    conn.close()
