
import os
from pyspark.sql import SparkSession

import dendutils.ec2.interact
import ec2.getorcreate


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
    spark = ec2.getorcreate.getOrCreate_config()
    return spark