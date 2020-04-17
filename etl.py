import configparser
import os
import boto
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# reading config file
config = configparser.ConfigParser()
config.read('dl.cfg')

# setting access keys
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# creating spark session
def create_spark_session():
    """ 
    creates and returns a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """ 
    creates songs and artists table by reading song data from s3 and 
    writes extracted tables to s3
    Args:
        spark: spark session
        input_data: s3 location of song-data
        output_data: s3 location where final tables are written to
    """
    
    # filepath to song data file
    song_data = os.path.join(input_data, "song-data/*/*/*/*.json")
    # song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    
    # reading song data file
    df = spark.read.json(song_data)

    # extracting columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # writing songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')

    # extracting columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    # writing artists table to parquet files
    artists_table = artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')

def process_log_data(spark, input_data, output_data):
    """
    creates users, time and songplays tables by reading log-data from s3 and 
    writes extracted tables to s3
    Args:
        spark: spark session
        input_data: s3 location of log-data
        output_data:  s3 location where final tables are written to
    """
    
    # filepath to log data file
    log_data = os.path.join(input_data, "log-data/*/*/*.json")

    # reading log data file
    df = spark.read.json(log_data)
    
    # filtering by actions for song plays
    df = df[df.page == 'NextSong']

    # extracting columns for users table    
    users_table = df.select(
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        'gender',
        'level'
    )
    
    # writing users table to parquet files
    users_table = users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # creating timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # creating datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extracting columns to create time table
    time_table = df.select(
        'timestamp',
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        date_format('datetime', 'F').alias('weekday')
    )
    
    # writing time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'), 'overwrite')

    # reading song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song_data/*/*/*/*.json"))

    # extracting columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration),'left_outer')\
        .select(
            df.timestamp,
            col("userId").alias('user_id'),
            df.level,
            song_df.song_id,
            song_df.artist_id,
            col("sessionId").alias("session_id"),
            df.location,
            col("useragent").alias("user_agent"),
            year('datetime').alias('year'),
            month('datetime').alias('month')
        )

    # writing songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'), 'overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://***********/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()