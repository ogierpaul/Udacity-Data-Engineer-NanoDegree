import psycopg2.sql as S
from psycopg2 import (OperationalError, ProgrammingError, DatabaseError, DataError, NotSupportedError)


class RedshiftCreateTables():
    def __init__(self,  query, schema, table, drop=False, aws_credentials="", conn_id="",):
        self.queries_raw = query
        self.schema = S.Identifier(schema)
        self.table = S.Identifier(table)
        self.drop_query = S.SQL("""DROP TABLE IF EXISTS {schema}.{table};""").format(schema=self.schema, table=self.table)
        self.create_query = S.SQL(query).format(schema=self.schema, table=self.table)
        self.drop = drop

    def execute(self, conn):
        cur = conn.cursor()
        if self.drop:
            cur.execute(self.drop_query)
        cur.execute(self.create_query)

