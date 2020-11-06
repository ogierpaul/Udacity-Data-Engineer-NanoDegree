from pyspark.sql import SparkSession
import os


def create_local_session(config):
    """
    Launch a spark session configured with AWS credentials
    Args:
        config (cfg): Config file

    Returns:
        SparkSession
    """
    # Launch the spark session
    os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/3.0.1/libexec"
    os.environ['JAVA_HOME'] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home"
    os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'KEY')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'SECRET')
    os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/envs/dend/bin/python3"
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
    spark = SparkSession.builder.appName('P4DEND').getOrCreate()
    # Set the AWS credentials
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", config.get("AWS", "KEY"))
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", config.get("AWS", "SECRET"))
    # spark._jsc.hadoopConfiguration().set("fs.s3a.", config.get("AWS", "SECRET"))
    spark._jsc.hadoopConfiguration().set("hadoop.fs.s3a.path.style.access", "true")
    spark._jsc.hadoopConfiguration().set("hadoop.fs.s3a.fast.upload", "true")
    spark._jsc.hadoopConfiguration().set("hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.multipart.size", "128M")
    spark._jsc.hadoopConfiguration().set("fs.s3a.fast.upload.active.blocks", "4")
    spark._jsc.hadoopConfiguration().set("fs.s3a.committer.name", "magic")
    spark._jsc.hadoopConfiguration().set("fs.s3a.committer.staging.conflict-mode", "append")
    return spark

from pyspark import SparkContext
from pyspark.sql import SparkSession
import os

def create_spark_session(config):
    """
    Generate a spark session
    Args:
        config (config): config file [AWS]:(KEY, SECRET)
    Returns:
        SparkSession
    """
    #os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/envs/dend/bin/python3"
    # os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
    # Set the AWS credentials
    os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','KEY')
    os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','SECRET')
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", config.get("AWS", "KEY"))
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", config.get("AWS", "SECRET"))
    return spark