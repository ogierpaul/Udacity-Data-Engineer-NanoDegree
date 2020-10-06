import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('admin_config.cfg')

# STAGING TABLES INSERT
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
#
# .format(
#     config.get("S3", "LOG_DATA"),
#     config.get('IAM_ROLE', 'ARN'),
#     config.get('S3', 'LOG_JSONPATH'),
#

staging_songs_copy = """
COPY staging_songs
FROM {filelocation}
IAM_ROLE {arn}
COMPUPDATE OFF
REGION 'us-west-2'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON 'auto' ;
"""
#
#     .format(
#     config.get("S3", "SONG_DATA"),
#     config.get('IAM_ROLE', 'ARN')
# )

# FINAL TABLES INSERT

songplay_table_insert = ("""
INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                a.userId as user_id,
                a.level as level,
                b.song_id as song_id,
                b.artist_id as artist_id,
                a.sessionId as session_id,
                a.location as location,
                a.userAgent as user_agent
FROM staging_events a
JOIN staging_songs b
ON
    a.song = b.title
AND
    a.artist = b.artist_name;
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE
userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_songs
where artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
                EXTRACT(HOUR from ts),
                EXTRACT(DAY from ts),
                EXTRACT(WEEK from ts),
                EXTRACT(MONTH from ts),
                EXTRACT(YEAR from ts),
                EXTRACT(WEEKDAY from ts)
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
    ]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
