from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
from pyspark.sql import SparkSession
import os
import configparser


def create_spark_session(config):
    """
    Launch a spark session configured with AWS credentials
    Args:
        config (cfg): Config file

    Returns:
        SparkSession
    """
    # Launch the spark session
    os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'KEY')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'SECRET')
    os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/envs/dend/bin/python3"
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.0"
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").appName(
        'P4DEND').getOrCreate()
    # Set the AWS credentials
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", config.get("AWS", "KEY"))
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", config.get("AWS", "SECRET"))
    return spark


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
        T.StructField("registration", T.IntegerType()),
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


def select_songs_data(df):
    """
    Extract the song information from the raw songs table
    Columns: ["song_id", "title", "artist_id", "year", "duration"]
    Args:
        df (pyspark.sql.DataFrame):

    Returns:
        pyspark.sql.DataFrame
    """
    # extract columns to create songs table
    song_cols = ["title", "artist_id", "year", "duration"]
    songs_table = df.select(song_cols). \
        dropDuplicates(). \
        withColumn("song_id", F.monotonically_increasing_id())
    return songs_table


def select_artists_data(df):
    """
    Extract the artist information from the raw songs table
    Columns: ["artist_id", "name", "location", "latitude", "longitude"]
    Args:
        df (pyspark.sql.DataFrame):

    Returns:
        pyspark.sql.DataFrame
    """
    # extract columns to create artists table
    artist_cols = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = df.select(artist_cols). \
        withColumnRenamed("artist_name", "name"). \
        withColumnRenamed("artist_location", "location"). \
        withColumnRenamed("artist_latitude", "latitude"). \
        withColumnRenamed("artist_longitude", "longitude")
    artists_table = artists_table.dropDuplicates()
    return artists_table


def select_users_data(df):
    """
    Extract the user information from the raw events table
    Columns: ["user_id", "first_name", "last_name", "gender", "level"]
    Args:
        df (pyspark.sql.DataFrame):

    Returns:
        pyspark.sql.DataFrame
    """
    user_cols = ['userId', 'firstName', 'lastName', 'gender', 'level', ]
    user_table = df.select(user_cols). \
        withColumnRenamed("firstName", "first_name"). \
        withColumnRenamed("lastName", "last_name"). \
        withColumnRenamed("userId", "user_id")
    user_table = user_table.dropDuplicates()
    return user_table


def select_time_data(df):
    """
    Extract the time information from the raw events table
    Columns: ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    Args:
        df (pyspark.sql.DataFrame):

    Returns:
        pyspark.sql.DataFrame
    """
    df2 = df.select("start_time").dropDuplicates() \
        .withColumn("hour", F.hour(F.col("start_time"))) \
        .withColumn("day", F.dayofmonth(F.col("start_time"))) \
        .withColumn("week", F.weekofyear(F.col("start_time"))) \
        .withColumn("month", F.month(F.col("start_time"))) \
        .withColumn("year", F.year(F.col("start_time"))) \
        .withColumn("weekday", F.date_format(F.col("start_time"), 'E'))
    time_table = df2.dropDuplicates().select("start_time", "hour", "day", "week", "month", "year", "weekday")
    return time_table


def create_songplay(df_event, songs, artists):
    """
    Create the fact table
    Columns: ["start_time", "user_id", "artist_id", "song_id", "session_id", 'user_agent', "iteminsession", 'year', "month"]
    Args:
        df_event (pyspark.sql.DataFrame): raw event table
        songs: song dimension table (with song_id)
        artists: artist dimension table (with artist_id)

    Returns:
        pyspark.sql.DataFrame
    """
    df_song = songs.select(
        'song_id', 'title', 'artist_id'
    ).join(
        artists.select('artist_id', 'name'), on='artist_id'
    ).select(
        F.col('title').alias('song'),
        F.col('name').alias('artist'),
        F.col('artist_id'),
        F.col('song_id')
    )
    songplay = df_event.select(
        'start_time', 'userId', 'artist', 'song', 'sessionId', 'userAgent', 'itemInSession'
    ).join(
        df_song,
        on=['song', 'artist']
    )
    songplay = songplay.select(
        F.col('start_time'),
        F.col('userId').alias('user_id'),
        F.col('artist_id'),
        F.col('song_id'),
        F.col('sessionId').alias('session_id'),
        F.col('userAgent').alias('user_agent'),
        F.col('itemInSession').alias('iteminsession')
    ).dropDuplicates()
    songplay = songplay.\
        withColumn("month", F.month(F.col("start_time"))).\
        withColumn("year", F.year(F.col("start_time")))
    return songplay


if __name__ == '__main__':
    log_path = "s3a://udacity-dend/log_data/2018/11/2018-11-0*.json"
    song_path = "s3a://udacity-dend/song_data/A/A/*/*.json"
    output_path = "s3a://dendpaulogieruswest2/p4emr/"
    # Load config file
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    # Create spark session
    ss = create_spark_session(config)
    # Read log data from s3
    staging_events = read_logs(ss, log_path)
    # Read song data from S3
    staging_songs = read_songs(ss, song_path)
    # Transform the dimension tables
    songs = select_songs_data(staging_songs)
    artists = select_artists_data(staging_songs)
    users = select_users_data(staging_events)
    time = select_time_data(staging_events)
    # Create the songplay fact table
    ## Cache the songs and artists table in order not to recalculate it
    songs = songs.cache()
    artists = artists.cache()
    songplay = create_songplay(staging_events, songs, artists)
    # Write the data
    songs.write.partitionBy("year", "artist_id").parquet(output_path + 'songs/')
    artists.write.parquet(output_path + 'artists/')
    users.write.parquet(output_path + 'users/')
    time.write.partitionBy("year", "month").parquet(output_path + 'time/')
    songplay.write.partitionBy("year", "month").parquet(output_path + 'songplay/')

