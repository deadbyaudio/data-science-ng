# Purpose of the Database and the ETL pipeline

The analysts of the startup Sparkify are trying to run analysis on the data they have been collecting on songs and user activity on their app. However, this data is currently available only in different event log files containing user activity in a pretty raw state and different song JSON files spreaded across a quite complex directory structure, making the analysts' work pretty challenging.

The analysts are looking forward to know better which songs their users are listening to, and this information can be found currently within the available event logs but not in the most simple way because, for example:

- The event log currently contains all kind of events, and not just song plays, that would need to be manually filtered out
- To determine for example which song a user is listening to the most, all the entries involving song play events for a user would have to be manually filtered and then manually grouped by song name to know which is the one with most occurrences.
- Same thing as mentioned in the previous point will have to be done to any kind of query, e.g. most listened artist by a user, most listened song in a certain location, most listened song by paid users, etc.
- Trying to match some event information available in the event log with some of the artist or song metadata available within the song files will require going through the song files sequentially until the desired information is found.
- The analysts will need to carry on all these complex operations to query their data pretty often, as new events are coming every second. A much simpler solution to query this data is needed.

Luckily, relational databases offer us an easy way to store and organise information like the one available in the event logs and song files, and will make possible to make queries like the ones mentioned in the example list above.

Finally, the ETL pipeline will be the tool used to transfer the data that Sparkify is currently storing at their event logs and song files to the new database. 

# Design

### Database schema

The suggested database schema for this project is a star schema optimised for queries on song play analysis. This design is justified by the fact that the analysts at Sparkify have mentioned song plays as their main area of interest. 

The schema will consist on a fact table, songplays that will contain:

- A reference to the **user**
- A reference to the **song**
- A reference to the **artist**
- A **timestamp** indicating when the song play was **started**
- Information on the **subscription level** under which the song was played
- The **session id** under which the song was played
- The **user agent** header containing info about the user agent (app, browser, etc.) requesting to play the song.
- The **location** (city) where the song play was triggered from

Together with the fact table the schema will contain four dimension tables. Three of them will offer more information about the user, the song and the artist respectively and the fourth one, `time`, will extend the different timestamps in the events allowing to classify the different events within different time frames.

![dbmodel](/home/albvt/work/data-science-ng/01-data-modelling-psql/dbmodel.png)

### ETL Pipeline script

The pipeline script will carry out the following tasks in the given order:

1. Connect to the database
2. Extract all the information available within the song files into the `artists` and `songs` tables.
3. Extract and transform the information available within the event logs as follows:
   1. Extend the existing timestamp in the logs to create the `time` table, so events can be easily classified within a time frame.
   2. Collect the users information and populate the `users` table with it.
   3. Collect the data to be stored at the `songplays` table trying to match the song title, artist name and song length information in each row with the data already populated before to the `songs` and `artists` tables, in order to retrieve the `song_id` and the `artist_id`
4. Close the database connection

# Project structure

The project contains the following files. and directories:

- `data`: Directory where the event logs and song files can be found.
- `etl.ipynb`: Jupyter notebook where the different steps of the ETL pipeline script were tried out initially.
- `test.ipynb`: Jupyter notebook that runs different SQL queries to test that the ETL script migrated the data successfully.
- `sql_queries.py`: Python file that contains all the SQL queries needed at the `create_tables.py` script and the ETL pipeline script. 
- `create_tables.py`: Python script that creates the database tables, wiping out their data if they happen to contain some information.
- `etl.py`: Python script that executes the ETL pipeline that will migrate the data from the event logs and song files to our database.

# Execution

To run this project it is precise to have:

- A Python installation or a Python virtual environment with all the imported libraries installed

- JupyterLab installed to be able to explore and run the code in the Jupyter notebooks

- A Postgres database running. With a Docker installation, this is possible to do just with the following command

  ```
  docker run --name udacity-postgres -e POSTGRES_USER=student -e POSTGRES_PASSWORD=student -e POSTGRES_DB=studentdb -p 5432:5432 -d postgres
  ```

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

Find the favourite artist for user id 15

```
SELECT artist_id, artists.name, count(1) FROM songplays JOIN artists ON songplays.artist_id=artists.id WHERE songplays.user_id = 15 GROUP BY artist_id, artists.name ORDER BY count LIMIT 1
```

Find the 5 hour time frames with more song plays:

```
SELECT time.hour, count(1) FROM songplays JOIN time ON songplays.start_time = time.start_time GROUP BY time.hour ORDER BY count DESC LIMIT 5
```

