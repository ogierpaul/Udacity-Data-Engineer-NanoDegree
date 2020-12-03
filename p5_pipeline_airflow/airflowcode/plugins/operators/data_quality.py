from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2.sql as S


class DataQualityOperator(BaseOperator):
    """
    Data Quality Checks:
    1. Check the target table has a positive number of rows
    2. Check the target table has no duplicate primary key
    Properties:
    - q_rowcount: query used to count the number of rows
    - q_pkeycount: query used to count the number of rows per primary key
    - qf_ stands for a template query q_ formatted with arguments
    """
    ui_color = '#ffc6ff'
    # Queries used for Data Quality checks
    q_rowcount = """SELECT COUNT(*) as n FROM {table}"""
    q_pkeycount = """SELECT MAX(n)  as n_pkey FROM (
    SELECT {pkey}, COUNT(*) as n FROM {table} GROUP BY {pkey} 
    ) b ;"""

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 pkey = "",
                 table = "",
                 *args, **kwargs):
        """

        Args:
            conn_id (str): in Airflow Connection Database, name of Redshift connection
            pkey (str): Name of primary key of table
            table (str): Name of table
            *args:
            **kwargs:
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.pkey = pkey
        self.params = {
            'pkey': S.Identifier(self.pkey),
            'table': S.Identifier(self.table)
        }
        self.qf_rowcount = S.SQL(DataQualityOperator.q_rowcount).format(**self.params)
        self.qf_pkeycount = S.SQL(DataQualityOperator.q_pkeycount).format(**self.params)


    def execute(self, context):
        """
        Data Quality Checks:
        1. Check the target table has a positive number of rows
        2. Check the target table has no duplicate primary key
        Args:
            context:

        Returns:
            None
        """
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info('Starting Data Quality Checks')
        # Test for presence of any records
        records = hook.get_records(self.qf_rowcount)
        if any([len(records) < 1, len(records[0]) < 1, records[0][0] < 1]):
            self.log.error("{} returned no lines".format(self.table))
            raise ValueError("{} returned no lines".format(self.table))
        del records
        # Test for no duplicates
        records = hook.get_records(self.qf_pkeycount)
        if records[0][0] > 1:
            self.log.error("{} returned  duplicates".format(self.table))
            raise ValueError("{} returned duplicates".format(self.table))
        self.log.info("Data Quality checked passed on {}".format(self.table))