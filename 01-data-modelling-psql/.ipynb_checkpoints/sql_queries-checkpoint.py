# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (
    id SERIAL PRIMARY KEY,
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
CREATE TABLE time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs (id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
INSERT INTO artists (id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT songs.id, artist_id
FROM songs 
JOIN artists ON songs.artist_id = artists.id
WHERE songs.title=%s AND artists.name=%s AND duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]