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
        auth TEXT NOT NULL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        gender TEXT,
        item_in_session INT NOT NULL,
        length DECIMAL,
        level TEXT NOT NULL,
        location TEXT,
        method TEXT NOT NULL,
        page TEXT NOT NULL,
        registration TIMESTAMP NOT NULL,
        session_id INT NOT NULL,
        song TEXT,
        status INT NOT NULL,
        ts TIMESTAMP NOT NULL,
        user_agent TEXT NOT NULL,
        user_id INT NOT NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs TEXT,
        artist_id TEXT NOT NULL,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location TEXT,
        artist_name TEXT NOT NULL,
        song_id TEXT NOT NULL,
        title TEXT NOT NULL,
        duration DECIMAL,
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level TEXT,
        song_id TEXT,
        artist_id TEXT,
        session_id INT,
        location TEXT,
        user_agent TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        id INT PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        gender TEXT,
        level TEXT NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        year INT,
        duration DECIMAL
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        location TEXT,
        latitude DECIMAL,
        longitude DECIMAL
    );
""")

time_table_create = ("""
    CREATE TABLE TIME (
        start_time TIMESTAMP PRIMARY KEY,
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
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
