from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2.sql as S


class PgUpsertOperator(BaseOperator):
    """
    Demonstrator of an Operator to UPSERT data in Redshift or PG
    """
    ui_color = '#F98866'
    temp_drop = """
    DROP TABLE IF EXISTS {stage};
    """
    temp_create = """
    CREATE TEMP TABLE IF NOT EXISTS {stage}  (LIKE {target});
    """
    temp_load = """
    INSERT INTO {stage}
    {q_select};
    """
    begin_transaction = """
    BEGIN TRANSACTION;
    """
    delete_target = """
    DELETE FROM {target}
    USING {stage}
    WHERE {target}.{pkey} = {stage}.{pkey};
    """
    insert_target = """
    INSERT INTO {target}
    SELECT DISTINCT ON({pkey}) * FROM {stage};
    """
    end_transaction = """
    END TRANSACTION;
    """
    upsert_queries = [temp_drop, temp_create, temp_load, begin_transaction, delete_target, insert_target, end_transaction]

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 conn_id="",
                 query="",
                 table="",
                 pkey="",
                 stageprefix="stageupsert_",
                 q_select="",
                 truncate=False,
                 *args, **kwargs):
        """

        Args:
            aws_credentials_id (str): in Airflow Connection Database, name of AWS credentials
            conn_id (str): in Airflow Connection Database, name of Redshift connection
            query (str): query to execute
            table (str): table to truncate
            stageprefix (str): prefix to be added for a temporary staging table to allow upsert
            q_select (str): Select query which returns values to be upserted
            truncate (bool): Run Truncate before executing query
            pkey (str): primary key of the table (should be one column)
            *args:
            **kwargs:
        """
        super(PgUpsertOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.conn_id = conn_id
        self.query = query
        self.table = table
        self.truncate = truncate
        self.pkey = pkey
        self.stage_name = "".join([stageprefix, self.table])
        self.q_select = q_select

    def execute(self, context):
        """
        Uses a PostgresHook to connect to a postgre database (like Redshift) and execute an UPSERT

        Args:
            context: can be passed on via the provide_context=True parameter when using the operator. See BaseOperator for more info on context

        Returns:
            None
        """
        self.log.info('Creating hook')
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        if self.truncate:
            hook.run(sql=(S.SQL('Truncate {};').format(S.Identifier(self.table)),))
            hook.run(sql=(S.SQL(self.query),))
        else:
            for q in PgUpsertOperator.upsert_queries:
                qf = S.SQL(q).format(**{
                    'pkey': S.Identifier(self.pkey),
                    'target': S.Identifier(self.table),
                    'stage': S.Identifier(self.stage_name),
                    'q_select': S.Composable(self.q_select)})
                self.log.info('for table {}: SQL:{}'.format(self.table, q))

