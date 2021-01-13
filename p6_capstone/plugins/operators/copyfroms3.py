import psycopg2.sql as S
from psycopg2 import (OperationalError, ProgrammingError, DatabaseError, DataError, NotSupportedError)
from dendutils.config import get_project_config
from dendutils.redshift import get_conn
from p6_capstone.plugins.operators import AwsTestHook
import  logging


class CopyFromS3():
    truncate_template = """TRUNCATE {schema}.{table};"""
    copy_template = """
    COPY {schema}.{table}
    FROM {inputpath}
    IAM_ROLE AS {arn}
    REGION {region}
    COMPUPDATE OFF
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    {optionals};
    """

    def __init__(self, conn,  table, inputpath, arn, region, format='csv', jsonpath='auto', delimiter=',',
                 ignoreheader=1, truncate=True, schema="staging"):
        self.conn = conn
        self.schema = schema
        self.table = table
        self.inputpath = inputpath
        self.arn = arn
        self.region = region
        assert format in ['csv', 'json']
        self.format = format
        self.jsonpath = jsonpath
        self.delimiter = delimiter
        self.ignoreheader = ignoreheader
        self.truncate = truncate

        if format == 'json':
            optionals = S.SQL("""FORMAT AS JSON {jsonpath}""").format(jsonpath=S.Literal(jsonpath))
        else:
            optionals = S.SQL("""FORMAT AS CSV IGNOREHEADER AS {ignoreheader} DELIMITER AS {delimiter}""") \
                .format(ignoreheader=S.Literal(self.ignoreheader), delimiter=S.Literal(self.delimiter))

        self.truncate_query = S.SQL(CopyFromS3.truncate_template).format(**{
            'schema': S.Identifier(self.schema),
            'table': S.Identifier(self.table)
        })

        self.copy_query = S.SQL(CopyFromS3.copy_template).format(**{
            'schema': S.Identifier(self.schema),
            'table': S.Identifier(self.table),
            'inputpath': S.Literal(self.inputpath),
            'arn': S.Literal(self.arn),
            'region': S.Literal(self.region),
            'optionals': optionals
        })

    def execute(self, context=None):
        cur = self.conn.cursor()
        logger = logging.getLogger()
        try:
            if self.truncate:
                logger.info(self.truncate_query.as_string(self.conn))
                cur.execute(self.truncate_query)
            logger.info(self.copy_query.as_string(self.conn))
            cur.execute(self.copy_query)
        except (OperationalError, ProgrammingError, DatabaseError, DataError, NotSupportedError) as e:
            print(e)


if __name__ == '__main__':
    config_path = '/config/config_path.cfg'
    config = get_project_config(config_path)
    conn = get_conn(config)
    aws_credentials = AwsTestHook(region_name=config.get("REGION", "REGION"),
                                  aws_access_key_id=config.get("AWS", "KEY"),
                                  aws_secret_access_key=config.get("AWS", "SECRET")
                                  )
    s3_bucket = config.get("S3", "BUCKET")
    s3_outputfolder = config.get("S3", "CPV_OUTPUTFOLDER")
    s3_bucket = s3_bucket.rstrip('/')
    s3_outputfolder = s3_outputfolder.rstrip('/')
    output_cpv_s3 = 's3://' + s3_bucket + '/' + s3_outputfolder + '/' + 'cpv_2008_ver_2013.csv'
    params = {
        'schema': S.Identifier(config.get("DB", "SCHEMA_INT")),
        'arn': S.Literal(config.get("IAM", "CLUSTER_IAM_ARN")),
        'region': S.Literal(config.get("REGION", "REGION")),
        'table': S.Identifier('cpv'),
        'inputpath': S.Literal(output_cpv_s3)
    }
    cp = CopyFromS3(
        schema='myint',
        table='cpv',
        inputpath=output_cpv_s3,
        arn=config.get("IAM", "CLUSTER_IAM_ARN"),
        region=aws_credentials.region_name,
        format='csv',
        delimiter='|',
        ignoreheader=1,
        truncate=True
    )
    cp.execute(conn)
