# Purpose of the Data Warehouse and the ETL pipeline

In this assignment we have a series of log files and song data files in Amazon S3 with the same format as the ones we used for the Data Modelling assignment.

For reasons already specified in this previous assignment, it is pretty complex for the analysts at Sparkify to run analytics on the songs their users are listening to. Setting up a data warehouse and a series of analytics tables they could use on it is one of the ways to go to make their work easier, especially with larger datasets, as it happens in this assignment where we have a much bigger amount of song files to process.

# Design

### Database schema

The database schema consists on a star schema which is at first sight identical to the one used in the Data Modelling assignment 

To refresh that, the schema will consist on a fact table, `songplays` that will contain:

- A reference to the **user**
- A reference to the **song**
- A reference to the **artist**
- A **timestamp** indicating when the song play was **started**
- Information on the **subscription level** under which the song was played
- The **session id** under which the song was played
- The **user agent** header containing info about the user agent (app, browser, etc.) requesting to play the song.
- The **location** (city) where the song play was triggered from

Together with the fact table the schema will contain four dimension tables. Three of them will offer more information about the user, the song and the artist respectively and the fourth one, `time`, will extend the different timestamps in the events allowing to classify the different events within different time frames.

![dbmodel](dbmodel.png)

### Sort keys and distribution keys

##### STAGING TABLES:

- staging_events: `ts` is the timestamp of the event and a good <u>sort key</u>. A good <u>distribution key</u> would be `artist`. The reason is that to build the `songplays` table we need to join `staging_events` with `staging_songs` using the `artist` and the `song` fields for it. As artist will be the first criteria for the JOIN, it will be better to have all the entries about the same artist stored together.
- staging_songs: `artist_name` and `song_title` will be the fields used to join this table to `staging_events` in order to build the fact table `songplays`. As `artist_name` is the first criteria for this JOIN, it will make sense to have all the songs by the same artist stored together and use `artist_name` as <u>distribution key</u>

##### ANALYTICS TABLES:

- **songplays:** `start_time` is a good <u>sort key</u> as it determines when the songplay event was created and `user_id` was chosen as <u>distribution key</u>, in order to keep all the songplays by the same user together in the same place.
- **users:** Since users have no associated timestamp but an already existing user id, this `id` could act as the <u>sort key</u> for this table. Since this is a small table and will be joined quite often to `songplays` it makes sense that this table uses the ALL distribution style.
- **songs:** songs does not have an associated timestamp either, so their pre-existing `id` could be also a good sorting criteria and a good <u>sort key</u>. Since several songs can belong to the same artist and it is very possible that this table will be joined with artists, it makes sense to use `artist_id` as <u>distribution key</u>
- **artists:** Same as with songs and users, artists have no associated timestamp, so `id` could be a good sort criteria and <u>sort key</u>.
- **time:** `start_time` contains all the information needed about the timestamp and should be used as <u>sort key</u>.

### Other decisions

Some additional changes were needed from the schema in the Data Modelling assignment, which were:

- Primary keys constraints do not get applied in Redshift, therefore they are not needed
- `id` in `songplays` cannot be of `SERIAL` type in Redshift. Its equivalent type is `IDENTITY(0,1)`
- `DECIMAL` fields specified just like that did not respect the decimal part of the original data. `DECIMAL (10, 6)` was used for longitudes and latitudes and `DECIMAL (7,2)` for song duration.
- In this assignment some location fields with more than 256 characters allocated for the type `TEXT` were found in the event files causing the ETL to break. For this reason, `VARCHAR(512)` was chosen as the type for the `location` field in `songplays`.

Additionally, `ON CONFLICT` sentences within `INSERT` statements are not valid in Redshift. In order to guarantee insertions with unique ids for `users`, `artists` and `time`, `SELECT DISTINCT` statements have been used in combination with the `row_number()` function when needed (for `users`, so the last user level is kept, and for `artists`, so the row we try to store for an artist is the one that includes a location, if this exists)

### ETL Pipeline script

The ETL Pipeline script is an easy script that first connects to Redshift using the credentials available in `dwh.cfg`, then copies the information from S3 to the two staging tables `staging_events` and `staging_songs`, and finally executes five different `INSERT` queries available in `sql_queries.py` that extract and transform the data from the staging tables into the five analytics tables.

# Project structure

The project contains the following files. and directories:

- `sql_queries.py`: Python file that contains all the SQL queries needed at the `create_tables.py` script and the ETL pipeline script. 
- `create_tables.py`: Python script that creates the Redshift tables, wiping out their data if they happen to contain some information.
- `etl.py`: Python script that executes the ETL pipeline that will copy the data available in the cloud into two staging tables, and extract and transform the data from these two staging tables in order to create the analytics tables.
- `test.ipynb:` Jupyter notebook with different SQL queries used to verify the correct behaviour of the ETL.
- `dwh.cfg:` Configuration file with the different variables and credentials needed for this assignment.

# Execution

To run this project it is precise to have:

- A Python installation or a Python virtual environment with all the imported libraries installed
- A Redshift cluster running
- The correct credentials in `dwh.cfg`

With the prerequisites described above all set, it is only necessary to run first the `create_tables.py` script

```
python create_tables.py
```

Followed by the ETL pipeline script

```
python etl.py
```

# Example queries

List of users with their membership levels sorted by number of song plays 

```
SELECT user_id, level, count(1) FROM songplays GROUP BY user_id, level ORDER BY count DESC
```

Amount of song plays done under the different membership levels

```
SELECT level, count(1) FROM songplays GROUP BY level ORDER BY level DESC
```

Find the 5 users with more songplays with their first name, last_name and number of songplays

```
SELECT users.first_name, users.last_name, count(1) FROM songplays JOIN users ON songplays.user_id = users.id GROUP BY users.first_name, users.last_name ORDER BY count DESC LIMIT 5
```

Find the name of the 5 most listened artists

```
SELECT artists.name AS artist_name, count(1) FROM songplays JOIN artists ON songplays.artist_id = artists.id GROUP BY artist_name ORDER BY count DESC LIMIT 5
```

Find the title of the 10 most listened songs

```
SELECT artists.name AS artist_name, songs.title, count(1) FROM songplays JOIN artists ON songplays.artist_id = artists.id JOIN songs ON songplays.song_id = songs.id GROUP BY artists.name, songs.title ORDER BY count DESC LIMIT 10
```

Find the 5 hour time frames with more song plays:

```
SELECT time.hour, count(1) FROM songplays JOIN time ON songplays.start_time = time.start_time GROUP BY time.hour ORDER BY count DESC LIMIT 5
```

