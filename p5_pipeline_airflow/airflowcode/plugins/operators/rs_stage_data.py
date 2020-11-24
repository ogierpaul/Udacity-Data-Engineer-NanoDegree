from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2.sql as S

class RedshiftStagingOperator(BaseOperator):
    """
    """
    ui_color = '#3482ab'
    query = """
    COPY {table}
    FROM {sourcepath}
    IAM_ROLE {arn}
    COMPUPDATE OFF
    REGION {region}
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto' {options};
    """

    @apply_defaults
    def __init__(self,
                 aws_id="",
                 arn="",
                 conn_id="",
                 region="",
                 sourcepath="",
                 table="",
                 *args, **kwargs):
        """

        Args:
            aws_id (str): name of AWS credentials connection in Ariflow
            arn (str): name of ARN role used by the Redshift cluster
            sourcepath (str): path to file
            conn_id (str): Airflow connection ID
            region (str): AWS region
            table (str): Redshift table name
            *args:
            **kwargs:
        """

        super(RedshiftStagingOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sourcepath = sourcepath
        self.table = table
        self.aws_id = aws_id
        self.arn = arn
        self.region= region

    def execute(self, context):
        """
        COPY table from an S3 bucket
        - truncate table
        - COPY table FROM sourcepath
        Args:
            context:

        Returns:
            None
        """
        self.log.info('Executing StagingOperator')
        self.log.info('Creating hook')
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        hook.run(sql=(S.SQL("truncate {}").format(S.Identifier(self.table)), ))
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        hook.run(
             sql=(
                 S.SQL(RedshiftStagingOperator.query).format(
                     table=S.Identifier(self.table),
                     sourcepath=S.Literal(self.sourcepath),
                     arn=S.Literal(self.arn),
                     region=S.Literal(self.region)
                 ),
            )
        )
        self.log.info('Ran SQL query for table {}'.format(self.table))
        pass
