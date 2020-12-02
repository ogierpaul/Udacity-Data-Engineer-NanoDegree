from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2.sql as S

class LoadDimensionOperator(BaseOperator):
    """
    Demonstrator of an Operator to UPSERT data in Redshift, and then perform data quality checks
    See pseudocode here: https://docs.aws.amazon.com/redshift/latest/dg/merge-replacing-existing-rows.html
    Upsert:
    1. Create  an empty Temporary Staging Table and fill it with the values to be inserted in the Target Table
    2. Delete rows from Target table that are present in Staging tables
    3. Insert into Target Tables from Staging Table
    4. Delete Staging Table

    Props:
    - q_all: list of all queries that will be executed for upsert
    - qf_ stands for a template query q_ formatted with arguments
    - params: dictionnary of parameters: table name, staging table name, primary key, query used to select

    Notes:
        - Select Distinct On is not supported by Redshift
        - See here a workaround : https://gist.github.com/jmindek/62c50dd766556b7b16d6
        - Make sure that there are no duplicates in the select query
    """
    ui_color = '#9bf6ff'
    # Queries used for Upsert
    q_temp_drop = """DROP TABLE IF EXISTS {stage};"""
    q_temp_create = """CREATE TABLE IF NOT EXISTS {stage}  (LIKE {table});"""
    q_temp_load = """
    INSERT INTO {stage}
    {query};
    """
    q_begin_transaction = """BEGIN TRANSACTION;"""
    q_delete_target = """DELETE FROM {table} USING {stage} WHERE {table}.{pkey} = {stage}.{pkey};"""
    q_insert_target = """INSERT INTO {table} SELECT DISTINCT * FROM {stage};"""
    q_end_transaction = """END TRANSACTION;"""
    q_all = [q_temp_drop, q_temp_create, q_temp_load, q_begin_transaction, q_delete_target, q_insert_target,
             q_end_transaction, q_temp_drop]

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 pkey="",
                 query="",
                 stageprefix="stageupsert_",
                 table="",
                 *args, **kwargs):
        """

        Args:
            conn_id (str): in Airflow Connection Database, name of Redshift connection
            pkey (str): primary key of the table (should be one column
            query (str): query to execute which returns values to be upserted (SELECT FROM without ;)
            stageprefix (str): prefix to be added for a temporary staging table to allow upsert
            table (str): Target table to Upsert
            *args:
            **kwargs:
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.query = query
        self.table = table
        self.pkey = pkey
        self.stage = "".join([stageprefix, self.table])
        self.params = {
            'pkey': S.Identifier(self.pkey),
            'table': S.Identifier(self.table),
            'stage': S.Identifier(self.stage),
            'query': S.SQL(self.query)
        }
        self.qf_all = [S.SQL(q).format(**self.params) for q in LoadDimensionOperator.q_all]


    def execute(self, context):
        """
        Uses a PostgresHook to connect to a Redshift and execute an UPSERT
        1. Create  an empty Temporary Staging Table and fill it with the values to be inserted in the Target Table
        2. Delete rows from Target table that are present in Staging tables
        3. Insert into Target Tables from Staging Table
        4. Delete Staging Table

        Args:
            context: can be passed on via the provide_context=True parameter when using the operator. See BaseOperator for more info on context

        Returns:
            None
        """
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info('Running list of upsert queries for table:{}'.format(self.table))
        hook.run(self.qf_all)
        self.log.info('Data upserted')