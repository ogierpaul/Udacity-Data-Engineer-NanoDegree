from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2.sql as S


class DataQualityOperator(BaseOperator):
    """
    - Verify that a table has a positive number of rows
    - Verify that a table has no duplicate rows using the primary key
    """
    q_rowcount = "SELECT COUNT(*) FROM {table};"
    q_noduplicates = """
    SELECT MAX(n)
    FROM
        (SELECT {pkey}, COUNT(*) as n FROM {table} GROUPBY {pkey}) b ;
    """
    ui_color = '#76DD48'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 conn_id="",
                 table="",
                 pkey="",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id,
        self.conn_id = conn_id,
        self.table = table
        self.pkey = pkey

    def execute(self, context):
        hook = PostgresHook(self.conn_id)
        # Test for presence of any records
        records = hook.get_records(S.SQL(DataQualityOperator.q_rowcount).format(S.Identifier(self.table)))
        if any([len(records) < 1, len(records[0]) < 1, records[0][0] < 1]):
            self.log.error("{table} returned no lines".format(self.table))
            raise ValueError("{table} returned no lines".format(self.table))
        del records
        # Test for no duplicates
        records = hook.get_records(
            S.SQL(DataQualityOperator.q_noduplicates).format(
                table=S.Identifier(self.table),
                pkey=S.Identifier(self.pkey)
            )
        )
        if records[0][0]>1:
            self.log.error("{table} returned  duplicates".format(self.table))
            raise ValueError("{table} returned duplicates".format(self.table))
        self.log.info("Data Quality checked passed on {}".format(self.table))
