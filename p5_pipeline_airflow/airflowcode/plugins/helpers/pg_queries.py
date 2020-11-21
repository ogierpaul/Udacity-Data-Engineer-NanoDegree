class PgQueries:

    user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level
FROM (SELECT DISTINCT ON(userId) userId,  firstName, lastName, gender, level FROM staging_events WHERE userId is NOT NULL) b
ON CONFLICT (user_id)
    DO UPDATE
    SET level = excluded.level;
    """)

    time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time,
                EXTRACT(HOUR from start_time) as hour,
                EXTRACT(DAY from start_time) as day,
                EXTRACT(WEEK from start_time) as week,
                EXTRACT(MONTH from start_time) as month,
                EXTRACT(YEAR from start_time) as year,
                EXTRACT(DOW from start_time) as weekday
FROM (SELECT to_timestamp( TRUNC( CAST( ts AS bigint ) / 1000 ) ) as start_time FROM staging_events
WHERE ts IS NOT NULL) b
ON CONFLICT (start_time)
    DO NOTHING;
    """)

    songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp( TRUNC( CAST( ts AS bigint ) / 1000 ) ) as start_time,
                    a.userId as user_id,
                    a.level as level,
                    b.song_id as song_id,
                    b.artist_id as artist_id,
                    a.sessionId as session_id,
                    a.location as location,
                    a.userAgent as user_agent
    FROM 
    (SELECT 
      ts,
      userId,
      sessionId,
      location,
      userAgent,
      song,
      artist,
      level
    FROM staging_events WHERE userId IS NOT NULL
    ) a
    LEFT JOIN (SELECT song_id, title, artist_id, name FROM songs c
                LEFT JOIN artists USING(artist_id)
    ) b
    ON
        a.song = b.title
    AND
        a.artist = b.name;
    """)

    staging_events_delete = """
    DELETE FROM staging_events WHERE 1=1;
    """

    staging_events_copy = """
    COPY staging_events (
    artist,
    auth,
    firstName ,
    gender,
    itemInSession ,
    lastName,
    length,
    level ,
    location,
    method,
    page,
    registration,
    sessionId,
    song,
    status,
    ts,
    userAgent,
    userId)
    FROM '/data/stagingarea/staging_events.csv'
    WITH CSV HEADER DELIMITER '|';
    """

    staging_songs_copy = """
    COPY staging_songs (
        artist_id,
     artist_latitude,
     artist_location,
     artist_longitude,
     artist_name,
     duration,
     song_id,
     title, 
     year)
    FROM '/data/stagingarea/staging_songs.csv'
    WITH CSV HEADER DELIMITER '|';
    """

    songs_table_insert = """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs
    ON CONFLICT (song_id)
        DO NOTHING;
    """

    artists_table_insert = """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
    FROM staging_songs
    ON CONFLICT (artist_id)
        DO NOTHING;
    """

    staging_songs_delete = """
    DELETE FROM staging_songs WHERE 1=1;
    """
