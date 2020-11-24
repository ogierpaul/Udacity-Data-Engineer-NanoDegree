class SqlQueries:
    songplays_table_insert = """
    INSERT INTO songplays
        SELECT
                md5(cast(events.sessionid as varchar) || cast(events.ts as varchar)) songplay_id,
                events.start_time, 
                events.userid as user_id, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid as session_id, 
                events.location, 
                events.useragent as user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    ;
    """

    users_table_insert = """
    INSERT INTO users
    SELECT distinct ON (userid) userid as user_id, firstname as first_name, lastname as last_name, gender, "level"
    FROM staging_events
    WHERE page='NextSong'
    """

    songs_table_insert = """
    INSERT INTO songs
    SELECT distinct ON (song_id) song_id,  title, artist_id, year, duration
    FROM staging_songs
    """

    artists_table_insert = """
    INSERT INTO artists
    SELECT distinct ON (artist_id) artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
    FROM staging_songs
    """

    time_table_insert = """
    INSERT INTO time
    SELECT DISTINCT ON (start_time) start_time, extract(hour from start_time) as "hour", extract(day from start_time) as "day", extract(week from start_time) as "week", 
           extract(month from start_time) as "month", extract(year from start_time) as "year", extract(dow from start_time) as "weekday" 
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time
        FROM staging_events) b
    """

    staging_events_copy = """
    COPY staging_events
    FROM {filelocation}
    IAM_ROLE {arn}
    COMPUPDATE OFF
    REGION 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {jsonpath} ;
    """
    staging_events_delete = """
    TRUNCATE staging_events
    """

    staging_songs_copy = """
    COPY staging_songs
    FROM {filelocation}
    IAM_ROLE {arn}
    COMPUPDATE OFF
    REGION 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto' ;
    """

    staging_songs_truncate = """
    TRUNCATE staging_songs;
    """

    staging_events_truncate = """
    TRUNCATE staging_events;
    """