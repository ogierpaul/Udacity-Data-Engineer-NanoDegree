
import os
from pyspark.sql import SparkSession

def get_spark(config):
    """
    Creates a SparkSession
    Args:
        config (cfg): Config File *.cfg parsed via config parser

    Returns:
        pyspark.sql.SparkSession
    """
    os.environ['AWS_KEY_ID'] = config.get("AWS", "KEY")
    os.environ['AWS_SECRET'] = config.get("AWS", 'SECRET')
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:3.2.0" pyspark-shell'
    spark = SparkSession.builder.\
        master("local[*]").\
        config("spark.hadoop.fs.s3a.access.key", os.environ.get('AWS_KEY_ID')).\
        config("spark.hadoop.fs.s3a.secret.key", os.environ.get('AWS_SECRET')).\
        getOrCreate()
    return spark