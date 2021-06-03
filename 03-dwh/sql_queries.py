import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist TEXT,
        auth TEXT,
        first_name TEXT,
        gender TEXT,
        item_in_session INT,
        last_name TEXT,
        length DECIMAL(7,2),
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration BIGINT,
        session_id INT,
        song TEXT,
        status INT,
        ts BIGINT,
        user_agent TEXT,
        user_id INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs TEXT,
        artist_id TEXT,
        artist_latitude DECIMAL(10,6),
        artist_longitude DECIMAL(10,6),
        artist_location VARCHAR(512),
        artist_name TEXT,
        song_id TEXT,
        title TEXT,
        duration DECIMAL(7,2),
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        id INT IDENTITY(0,1),
        start_time BIGINT NOT NULL sortkey,
        user_id INT NOT NULL distkey,
        level TEXT,
        song_id TEXT,
        artist_id TEXT,
        session_id INT,
        location VARCHAR(512),
        user_agent TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        id INT sortkey distkey,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        gender TEXT,
        level TEXT NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        id TEXT sortkey,
        title TEXT NOT NULL,
        artist_id TEXT NOT NULL distkey,
        year INT,
        duration DECIMAL(7,2)
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        id TEXT sortkey distkey,
        name TEXT NOT NULL,
        location TEXT,
        latitude DECIMAL(10,6),
        longitude DECIMAL(10,6)
    );
""")

time_table_create = ("""
    CREATE TABLE TIME (
        start_time BIGINT sortkey,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
    COMPUPDATE OFF
    region 'us-west-2'
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    COMPUPDATE OFF
    region 'us-west-2'
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN')
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) (
        SELECT se.ts AS start_time, se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent 
        FROM staging_events se
        LEFT JOIN staging_songs ss ON (ss.artist_name = se.artist AND ss.title = se.song) 
        WHERE page = 'NextSong'
    )
""")

user_table_insert = ("""
    INSERT INTO users (
        SELECT id, first_name, last_name, gender, level FROM (
            SELECT DISTINCT user_id AS id, first_name, last_name, gender, level, row_number() over (partition by id order by ts desc) as row
            FROM staging_events
            WHERE page = 'NextSong'
        )
        WHERE row = 1
    )
""")

song_table_insert = ("""
    INSERT INTO songs (
        SELECT song_id AS id, title, artist_id, duration, year
        FROM staging_songs
    )
""")

artist_table_insert = ("""
    INSERT INTO artists (
        SELECT DISTINCT artist_id AS id, artist_name AS name, artist_location AS location, artist_latitude as latitude, artist_longitude AS longitude
        FROM staging_songs
    )
""")

time_table_insert = ("""
    INSERT INTO time(
        SELECT  start_time,
            extract(hour FROM timestamp) AS hour,
            extract(day FROM timestamp) AS day,
            extract(week FROM timestamp) AS week,
            extract(month FROM timestamp) AS month,
            extract(year FROM timestamp) AS year,
            extract(weekday FROM timestamp) AS weekday 
        FROM (
            SELECT DISTINCT ts AS start_time, timestamp 'epoch' + ts/1000 * interval '1 second' AS timestamp 
            FROM staging_events
            WHERE page = 'NextSong'
        )
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
