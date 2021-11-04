import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld,\
     DoubleType as Dbl, LongType as Long, StringType as Str, \
     IntegerType as Int, DecimalType as Dec, DateType as Date,TimestampType as Stamp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session and return spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    print("Spark session created successfully.")
    return spark

def song_data_schema():
    """
    Creates schema for song_data.
    """
    songs_schema = R([
        Fld("num_songs",Int()),
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dec()),
        Fld("artist_longitude",Dec()),
        Fld("artist_location",Str()),
        Fld("artist_name",Str()),
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("duration",Dbl()),
        Fld("year",Int())
    ])
    return songs_schema

def process_song_data(spark, input_data, output_data):
    """
    Process song_data by creating songs and artist table
    and writing the result to the given S3 bucket.
    
    Parameters :
    spark : spark session object,
    input_data : S3 bucket with input data
    output_data : S3 bucket for output data
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    print("-> song_data reading started.")
    df = spark.read.json(song_data,schema = song_data_schema())

    # extract columns to create songs table'
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    print("-> Writing in songs_table.")
    songs_table.write.parquet(output_data + 'songs_table.parquet', partitionBy = ["year","artist_id"], mode = "overwrite")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 
                              "artist_name",
                              "artist_location",
                              "artist_latitude",
                              "artist_longitude").dropDuplicates(['artist_id'])
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    print("-> Writing in artists_table")
    artists_table.write.parquet(output_data + 'artists.parquet',mode = 'overwrite')
    
def log_data_schema():
"""
Creates schema for log_data.
"""
    logs_schema = R([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Str()),
        Fld("lastName", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId", Str()),
        Fld("song", Str()),
        Fld("status", Str()),
        Fld("ts", Long()),
        Fld("userAgent", Str()),
        Fld("userId", Str())
    ])
    return logs_schema

def process_log_data(spark, input_data, output_data):
    """
    Process log_data by creating user table and time table
    and writing the results to the given S3 bucket.
    """
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    print("-> log_data file reading started.")
    df = spark.read.json(log_data, schema = log_data_schema())
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id",
                                "firstName as first_name",
                                "lastName as last_name",
                                "gender",
                                "level").dropDuplicates(["user_id"])
    
    # write users table to parquet files
    print("-> Writing in users_table.")
    users_table.write.parquet(output_data + "users_table.parquet", mode = "overwrite")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000)), Stamp())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    #print("Data-frame of timestamp column from original timestamp column : ",df.head())
    #print("first two rows : ",df.take(2))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x / 1000)), Stamp())
    df = df.withColumn("datetime", get_datetime(col("ts")))
    #print("Data-frame of datetime column from original timestamp column : ",df.head())
    #print("first two rows : ",df.take(2))
    
    # extract columns to create time table
    time_table = df.selectExpr("timestamp as start_time",
                               "hour(timestamp) as hour",
                               "dayofmonth(timestamp) as day",
                               "weekofyear(timestamp) as week",
                               "month(timestamp) as month",
                               "year(timestamp) as year",
                               "dayofweek(timestamp) as weekday"
                               ).dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    print("-> Writing in time_table.")
    time_table.write.parquet(output_data + "time_table.parquet", partitionBy = ["year", "month"],
                             mode = "overwrite")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data, schema = song_data_schema())
    
    # write time table to parquet files partitioned by year and month
    print("-> Writing in time_table.")
    time_table.write.parquet(output_data + "time_table.parquet",
                             partitionBy = ["year", "month"],
                             mode = "overwrite")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data, schema = get_song_schema())
    
    # extract columns from joined song and log datasets to create songplays table
    song_df.createOrReplaceTempView("song_data")
    df.createOrReplaceTempView("log_data")
    
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                ld.timestamp as start_time,
                                year(ld.timestamp) as year,
                                month(ld.timestamp) as month,
                                ld.userId as user_id,
                                ld.level as level,
                                sd.song_id as song_id,
                                sd.artist_id as artist_id,
                                ld.sessionId as session_id,
                                ld.location as location,
                                ld.userAgent as user_agent
                                FROM log_data ld
                                JOIN song_data sd
                                ON (ld.song = sd.title
                                AND ld.length = sd.duration
                                AND ld.artist = sd.artist_name)
                                """)

    # write songplays table to parquet files partitioned by year and month
    print("-> Writing in songplays_table")
    songplays_table.write.parquet(output_data + "songplays_table.parquet",
                                  partitionBy=["year", "month"], mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-output17/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
