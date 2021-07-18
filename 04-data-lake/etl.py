import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (dayofmonth, dayofweek, hour, month, udf,
                                   weekofyear, year)
from pyspark.sql.types import IntegerType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: Creates and returns a spark session object for the project

    Returns:
        spark: The spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description:
        Reads preprocessed song data from a specified location, generates the
        star schema tables 'songs' and 'artists' and saves them to parquet
        files at another specified location

    Arguments:
        spark: the spark session object.
        input_data: URL or directory where to find the data to import
        output_data: URL or directory where to export the parquet files
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # Exporting the table to a view so SQL queries could be ran against it
    df.createOrReplaceTempView("song_data")

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT  song_id AS id,
                title,
                artist_id,
                duration,
                year
        FROM song_data
    """)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet(os.path.join(output_data, 'songs.parquet'))

    # extract columns to create artists table
    artists_table = spark.sql("""
          SELECT id, name, location, latitude, longitude FROM (
              SELECT DISTINCT
                  artist_id AS id,
                  artist_name AS name,
                  NULLIF(artist_location, '') AS location,
                  artist_latitude AS latitude,
                  artist_longitude AS longitude,
                  row_number() over (
                      partition by artist_id ORDER BY artist_location
                  ) AS row
              FROM song_data
          ) WHERE row = 1
      """)

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'))


def process_log_data(spark, input_data, output_data):
    """
    Description:
        Reads logs from a specified location, generates the star schema
        tables 'users', 'time' and 'songplays' and saves them to parquet files
        at another specified location

    Arguments:
        spark: the spark session object.
        input_data: URL or directory where to find the data to import
        output_data: URL or directory where to export the parquet files
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")

    # Creating a view from the DataFrame to use SQL
    df.createOrReplaceTempView("log_data")

    # extract columns for users table
    users_table = spark.sql("""
        SELECT  id,
                first_name,
                last_name,
                gender,
                level
        FROM (
            SELECT DISTINCT
                userId AS id,
                firstName AS first_name,
                lastName AS last_name,
                gender,
                level,
                row_number() over (
                    partition by userId order by ts desc
                ) as row
            FROM log_data
        ) WHERE row = 1
      """)

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x/1000.0), IntegerType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.timestamp))

    # extract columns to create time table
    time_table = df.select(
        df.datetime.alias('start_time'),
        year(df.datetime).alias('year'),
        month(df.datetime).alias('month'),
        weekofyear(df.datetime).alias('week'),
        dayofmonth(df.datetime).alias('day'),
        hour(df.datetime).alias('hour'),
        dayofweek(df.datetime).alias('weekday')
    ).drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
        .parquet(os.path.join(output_data, 'time.parquet'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs.parquet'))
    artist_df = spark.read.parquet(
        os.path.join(output_data, 'artists.parquet')
    )

    # create views for the DFS we want to join
    song_df.createOrReplaceTempView("song_data")
    artist_df.createOrReplaceTempView("artist_data")
    time_table.createOrReplaceTempView("time")

    # generate this view again to make use of the added columns in queries
    df.createOrReplaceTempView("log_data")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
        SELECT  log_data.datetime AS start_time,
                log_data.userId AS user_id,
                log_data.level,
                songs.song_id,
                songs.artist_id,
                log_data.sessionId AS session_id,
                log_data.location,
                log_data.userAgent AS user_agent,
                time.year,
                time.month
        FROM log_data
        LEFT JOIN (
            SELECT  song_data.id AS song_id,
                    artist_data.id AS artist_id,
                    artist_data.name AS artist_name,
                    song_data.title AS song_title
            FROM song_data
            JOIN artist_data ON song_data.artist_id = artist_data.id
        ) songs ON (
            log_data.artist = songs.artist_name
            AND log_data.song = songs.song_title
        )
        LEFT JOIN time ON log_data.datetime = time.start_time
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
        .parquet(os.path.join(output_data, 'songplays.parquet'))


def main():
    """
    Description: Main function that starts the program
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-datalake-albvt/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
