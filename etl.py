import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    - Establishes Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Reads in song data files
    
    - Creates songs table by transforming log files and writing to s3 as parquet
    
    - Creates artists table by transforming log files and writing to s3 as parquet
    """

    # get filepath to song data file
    song_data =input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView('df')

    

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT DISTINCT song_id,
                            artist_id,
                            title,
                            year,
                            duration
                            FROM df
                            WHERE song_id IS NOT NULL
                            and artist_id is not null 
                            and year is not null
                        """)

    songs_table.head()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')
    df.createOrReplaceTempView('df')

    # extract columns to create artists table
    artists_table = spark.sql("""
                            SELECT DISTINCT artist_id,
                            artist_name,
                            artist_location,
                            artist_latitude,
                            artist_longitude
                            FROM df
                            WHERE artist_id IS NOT NULL
                        """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    - Reads in log data files and song data files
    
    - Creates users table by transforming log files and writing to s3 as parquet
    
    - Creates time table by transforming log files and writing to s3 as parquet
    
    - Creates songplays table by transforming log and song files and writing to s3 as parquet
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')
    df.select(col("ts").cast("timestamp"));
    df.createOrReplaceTempView('df')

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT userId as user_id,
                            firstName as first_name,
                            lastName as last_name,
                            gender,
                            level
                            FROM df
                            WHERE userId IS NOT NULL
                            GROUP BY userId, level, gender, firstName, lastName
                        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT to_timestamp(ts/1000) as start_time,
                            hour(to_timestamp(ts/1000)) as hour,
                            day(to_timestamp(ts/1000)) as day,
                            weekofyear(to_timestamp(ts/1000)) as week,
                            month(to_timestamp(ts/1000)) as month,
                            year(to_timestamp(ts/1000)) as year,
                            dayofweek(to_timestamp(ts/1000)) as weekday
                            FROM df
                            WHERE ts IS NOT NULL
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(output_data + 'time_table/')

    # read in song data to use for songplays table
    song_data =input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView('song_df')
    print(song_df.schema)
    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                            SELECT monotonically_increasing_id() as songplay_id,
                            to_timestamp(log.ts/1000) as start_time,
                            month(to_timestamp(log.ts/1000)) as month,
                            year(to_timestamp(log.ts/1000)) as year,
                            log.userId as user_id,
                            log.level as level,
                            song.song_id,
                            song.artist_id,
                            log.sessionId as session_id,
                            log.location,
                            log.userAgent as user_agent
                            FROM df log
                            JOIN song_df song on log.song = song.title  and log.artist = song.artist_name
                        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(output_data + 'songplays_table/')


def main():
    """
    - calls function to establish spark session
    
    - Calls function to process song files
    
    - Calls function to process log files
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-datalake-steph-m/"
    
    process_log_data(spark, input_data, output_data)   
    process_song_data(spark, input_data, output_data) 

if __name__ == "__main__":
    main()
