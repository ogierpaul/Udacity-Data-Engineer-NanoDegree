CREATE TABLE IF NOT EXISTS staging_events
(
artist          VARCHAR,
auth            VARCHAR,
firstName       VARCHAR,
gender          VARCHAR,
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR,
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
)
DISTSTYLE EVEN;


COPY staging_events FROM 's3://udacity-dend/log_data'
CREDENTIALS 'aws_iam_role=arn:aws:iam::075227836161:role/dwhRole'
COMPUPDATE OFF
region 'us-west-2'
timeformat as 'epochmillisecs'
FORMAT AS JSON 's3://udacity-dend/log_json_path.json';

CREATE TABLE staging_songs(
    staging_song_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER
)
DISTSTYLE EVEN;

COPY staging_songs FROM 's3://udacity-dend/log_data/'
CREDENTIALS 'aws_iam_role=arn:aws:iam::075227836161:role/dwhRole'
COMPUPDATE OFF
region 'us-west-2'
FORMAT AS JSON 's3://dendpaulogieruswest2/song_jsonpath.json';
