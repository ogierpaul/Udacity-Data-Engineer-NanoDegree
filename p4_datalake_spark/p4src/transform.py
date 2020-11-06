from pyspark.sql import functions as F
from pyspark.sql import types as T

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
    songs_table = df.select(song_cols).\
        dropDuplicates().\
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
    user_table = df.select(user_cols).\
        withColumnRenamed("firstName", "first_name").\
        withColumnRenamed("lastName", "last_name").\
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
    Columns: ["start_time", "user_id", "artist_id", "song_id", "session_id", 'user_agent', "iteminsession"]
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
    return songplay
