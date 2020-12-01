class SqlQueries:
    songplays_table_select = """
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
            WHERE page='NextSong' AND (NOT sessionid IS NULL) AND (NOT ts IS NULL)) events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """

    users_table_select = """
    SELECT distinct  userid as user_id, firstname as first_name, lastname as last_name, gender, "level"
    FROM 
    (SELECT userid, 
            firstname,
            lastname,
            gender,
            "level", 
            rank() OVER (PARTITION BY userid ORDER BY "level" DESC) AS pkey_ranked
     FROM staging_events
    WHERE page='NextSong' AND (NOT userid IS NULL)
    ) AS ranked
    WHERE ranked.pkey_ranked = 1;
    """



    songs_table_select = """
        SELECT distinct  song_id,  title, artist_id, year, duration
    FROM staging_songs
    """


    artists_table_select = """
    SELECT distinct  artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
    FROM 
    (SELECT DISTINCT artist_id, 
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude,
            rank() OVER (PARTITION BY artist_id ORDER BY "artist_name", "artist_location", "artist_latitude", "artist_longitude" DESC) AS pkey_ranked
     FROM staging_songs
    ) AS ranked
    WHERE ranked.pkey_ranked = 1;
    """

    time_table_select = """
        SELECT DISTINCT start_time, extract(hour from start_time) as "hour", extract(day from start_time) as "day", extract(week from start_time) as "week", 
           extract(month from start_time) as "month", extract(year from start_time) as "year", extract(dow from start_time) as "weekday" 
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time
        FROM staging_events) b
    """

    staging_data_copy = """
    COPY {table}
    FROM {sourcepath}
    IAM_ROLE {arn}
    COMPUPDATE OFF
    REGION {region}
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON;
    """

    staging_songs_truncate = """
    TRUNCATE staging_songs;
    """

    staging_events_truncate = """
    TRUNCATE staging_events;
    """