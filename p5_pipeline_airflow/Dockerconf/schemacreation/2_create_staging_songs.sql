CREATE TABLE IF NOT EXISTS staging_songs
(
staging_song_id SERIAL,
artist_id          VARCHAR,
artist_latitude    FLOAT,
artist_location    VARCHAR,
artist_longitude   FLOAT,
artist_name        VARCHAR,
duration           FLOAT,
num_songs          INTEGER,
song_id            VARCHAR,
title              VARCHAR,
year               INTEGER,
PRIMARY KEY (staging_song_id)
);
