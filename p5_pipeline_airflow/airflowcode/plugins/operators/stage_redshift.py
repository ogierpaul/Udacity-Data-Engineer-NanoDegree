from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2.sql as S

class StageToRedshiftOperator(BaseOperator):
    """
    Copy Data from S3 onto Redshift
    Props:
    - arn, path, conn_id, region, table: see __init__ docstring
    - qf_truncate: SQL to TRUNCATE the table. Qf means Query Formatted.
    - qf_copy: SQL to COPY FROM.
    """
    ui_color = '#fdffb6'
    q_copy = """
    COPY {table}
    FROM {path}
    IAM_ROLE AS {arn}
    REGION {region}
    COMPUPDATE OFF
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {jsonformat};
    """

    @apply_defaults
    def __init__(self,
                 arn="",
                 conn_id="",
                 region="",
                 path="",
                 table="",
                 jsonformat="auto",
                 *args, **kwargs):
        """

        Args:
            arn (str): name of ARN role assumed by the Redshift cluster
            path (str): path to file(s)
            conn_id (str): Redshift connection ID in Airflow
            region (str): AWS region
            table (str): Redshift table name
            **kwargs:
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.arn = arn
        self.conn_id = conn_id
        self.jsonformat= jsonformat
        self.path = path
        self.region = region
        self.table = table
        self.qf_truncate = S.SQL("TRUNCATE {};").format(S.Identifier(self.table))
        self.qf_copy = S.SQL(StageToRedshiftOperator.q_copy).format(
            table=S.Identifier(self.table),
            path=S.Literal(self.path),
            arn=S.Literal(self.arn),
            region=S.Literal(self.region),
            jsonformat=S.Literal(self.jsonformat)

        )

    def execute(self, context):
        """
        COPY table from an S3 bucket
        - truncate table
        - COPY table FROM sourcepath
        Args:
            context: See Airflow context

        Returns:
            None
        """
        self.log.info('Executing StagingOperator')
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        hook.run(sql=(
            self.qf_truncate,
            self.qf_copy)
        )
        self.log.info('Ran COPY query for table {}'.format(self.table))
        pass






