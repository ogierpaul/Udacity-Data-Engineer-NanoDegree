from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

def staging_song_schema():
    """
    Schema for the raw songs data
    Returns:
        T.StructType: Schema
    """
    s = T.StructType([
        T.StructField("artist_id", T.StringType()),
        T.StructField("artist_latitude", T.DoubleType()),
        T.StructField("artist_location", T.StringType()),
        T.StructField("artist_longitude", T.DoubleType()),
        T.StructField("artist_name", T.StringType()),
        T.StructField("duration", T.DoubleType()),
        T.StructField("num_songs", T.IntegerType()),
        T.StructField("title", T.StringType()),
        T.StructField("year", T.IntegerType()),
    ])
    return s


def staging_event_schema():
    """
    Schema for the raw events data
    Returns:
        T.StructType: Schema
    """
    s = T.StructType([
        T.StructField("artist", T.StringType()),
        T.StructField("auth", T.StringType()),
        T.StructField("firstName", T.StringType()),
        T.StructField("gender", T.StringType()),
        T.StructField("itemInSession", T.IntegerType()),
        T.StructField("lastName", T.StringType()),
        T.StructField("length", T.DoubleType()),
        T.StructField("level", T.StringType()),
        T.StructField("location", T.StringType()),
        T.StructField("method", T.StringType()),
        T.StructField("page", T.StringType()),
        T.StructField("registration", T.StringType()),
        T.StructField("sessionId", T.IntegerType()),
        T.StructField("song", T.StringType()),
        T.StructField("status", T.IntegerType()),
        T.StructField("ts", T.StringType()),
        T.StructField("userAgent", T.StringType()),
        T.StructField("userId", T.StringType())
    ])
    return s


def read_logs(spark, logpath):
    """

    Args:
        spark (SparkSession):
        songpath (str): s3 path

    Returns:
        pyspark.sql.DataFrame
    """
    staging_events = spark.read.json(logpath, schema=staging_event_schema())
    staging_events = staging_events.filter(F.col("page") == F.lit("NextSong"))
    get_timestamp = F.udf(lambda x: datetime.utcfromtimestamp(int(x) / 1000), T.TimestampType())
    staging_events = staging_events.withColumn("start_time", get_timestamp("ts"))
    return staging_events

def read_songs(spark, songpath):
    """

    Args:
        spark (SparkSession):
        songpath (str): s3 path

    Returns:
        pyspark.sql.DataFrame
    """
    staging_songs = spark.read.json(songpath, schema=staging_song_schema())
    return staging_songs