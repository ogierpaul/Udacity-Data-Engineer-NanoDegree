import psycopg2
from p3_datawarehouse_aws_redshift.p3src.createenv.sql_queries_create import create_table_queries, drop_table_queries
from p3_datawarehouse_aws_redshift.p3src.utils import get_cluster_properties

def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables_main(config):
    host = get_cluster_properties(config)['Endpoint_address']
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
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()
    print('tables created')


