import psycopg2
from p3_datawarehouse_aws_redshift.src.etl.sql_queries_etl import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main(config):
    jdbcstring = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    conn = psycopg2.connect(jdbcstring)
    print('*****\nChecking Connectionstatus:\n{}\n********'.format(conn.closed))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


