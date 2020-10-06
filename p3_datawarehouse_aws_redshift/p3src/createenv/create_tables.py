import psycopg2
from p3src.createenv.sql_queries_create import create_table_queries, drop_table_queries
from p3src.utils import get_conn

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
    conn = get_conn(config)
    cur = conn.cursor()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()
    print('tables created')


