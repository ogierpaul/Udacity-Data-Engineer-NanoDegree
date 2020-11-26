CREATE TABLE  IF NOT EXISTS public.artists
(
    artist_id varchar(256) NOT NULL,
    name      varchar(256),
    location  varchar(256),
    latitude  numeric(18, 0),
    longitude numeric(18, 0),
    CONSTRAINT artists_pkey PRIMARY KEY (artist_id)
);

CREATE TABLE  IF NOT EXISTS public.songplays
(
    songplay_id varchar(32) NOT NULL,
    start_time  timestamp   NOT NULL,
    user_id     int4        NOT NULL,
    "level"     varchar(256),
    song_id     varchar(256),
    artist_id   varchar(256),
    session_id  int4,
    location    varchar(256),
    user_agent  varchar(256),
    CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
);

CREATE TABLE  IF NOT EXISTS public.songs
(
    song_id   varchar(256) NOT NULL,
    title     varchar(256),
    artist_id varchar(256),
    "year"    int4,
    duration  numeric(18, 0),
    CONSTRAINT songs_pkey PRIMARY KEY (song_id)
);

CREATE TABLE  IF NOT EXISTS public.staging_events
(
    artist        varchar(256),
    auth          varchar(256),
    firstname     varchar(256),
    gender        varchar(256),
    iteminsession int4,
    lastname      varchar(256),
    length        numeric(18, 0),
    "level"       varchar(256),
    location      varchar(256),
    "method"      varchar(256),
    page          varchar(256),
    registration  numeric(18, 0),
    sessionid     int4,
    song          varchar(256),
    status        int4,
    ts            int8,
    useragent     varchar(256),
    userid        int4
);

CREATE TABLE  IF NOT EXISTS public.staging_songs
(
    artist_id        varchar(256),
    artist_latitude  numeric(18, 0),
    artist_location  varchar(256),
    artist_longitude numeric(18, 0),
    artist_name      varchar(256),
    duration         numeric(18, 0),
    num_songs        int4,
    song_id          varchar(256),
    title            varchar(256),
    "year"           int4
);

CREATE TABLE IF NOT EXISTS public."time"
(
    start_time timestamp NOT NULL,
    "hour"     int4,
    "day"      int4,
    week       int4,
    "month"    varchar(256),
    "year"     int4,
    weekday    varchar(256),
    CONSTRAINT time_pkey PRIMARY KEY (start_time)
);

CREATE TABLE IF NOT EXISTS public.users
(
    user_id    int4 NOT NULL,
    first_name varchar(256),
    last_name  varchar(256),
    gender     varchar(256),
    "level"    varchar(256),
    CONSTRAINT users_pkey PRIMARY KEY (user_id)
);
