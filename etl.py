import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    # song_data = "s3a://udacity-dend/song_data/A/A/A/*.json"     # Use for Testing. Keep this commented otherwise.
    
    # read song data file
    df = spark.read.json(song_data)

    # Create Temp View on song data dataframe
    df.createOrReplaceTempView("song_data_view")

    # ------------------------------------------------------------------------
    # SONGS DIMENSION LOAD
    #-------------------------------------------------------------------------
    # extract columns to create songs table
    songs_table = spark.sql("select distinct song_id, title, artist_id, year, duration from song_data_view")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'),'overwrite')
    
    print("Loading of Songs table is completed.")

    # ------------------------------------------------------------------------
    # ARTISTS DIMENSION LOAD
    #-------------------------------------------------------------------------
    # extract columns to create artists table
    artists_table = spark.sql('''
    select distinct artist_id
        , artist_name as name
        , artist_location as location
        , artist_latitude as lattitude
        , artist_longitude as longitude 
    from song_data_view
    ''') 
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'),'overwrite')
    
    print("Loading of Artists table is completed.")
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')
    # log_data = "s3a://udacity-dend/log_data/2018/11/*.json"         # Use for Testing. Keep this commented otherwise.

    # read log data file
    df = spark.read.json(log_data)
    print("Number of log data records read = " + str(df.count()))
    
    # filter by actions for song plays
    df = df.filter(col("page")=="NextSong")

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', from_unixtime(df.ts / 1000).cast('timestamp').alias('start_time'))
    df.createOrReplaceTempView("log_data_view")

    # ------------------------------------------------------------------------
    # USERS DIMENSION LOAD
    #-------------------------------------------------------------------------
    # extract columns for users table    
    users_table = spark.sql(" \
    select  \
          cast(userId as integer) as userId \
        , firstName \
        , lastName \
        , gender \
        , max(level) as level \
    from log_data_view \
    where nvl(trim(userid), '') <> '' \
    group by userId, firstName, lastName, gender \
    ")
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'),'overwrite')
    
    print("Loading of Users table is completed.")

    # ------------------------------------------------------------------------
    # TIME DIMENSION LOAD
    #-------------------------------------------------------------------------
    # extract columns to create time table
    time_table = spark.sql("\
    select distinct start_time \
    , hour(start_time) as hour \
    , day(start_time) as day \
    , weekofyear(start_time) as week \
    , month(start_time) as month \
    , year(start_time) as year \
    , weekday(start_time) as weekday \
    from log_data_view")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(os.path.join(output_data, 'time'),'overwrite')
    
    print("Loading of Time table is completed.")

    # ------------------------------------------------------------------------
    # SONGPLAYS FACT TABLE LOAD
    #------------------------------------------------------------------------

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
    select row_number() over(partition by null order by start_time) as songplay_id
        , e.start_time 
        , case when nvl(userId, '') = '' then NULL else cast(e.userId as integer) end as userId 
        , e.level 
        , sa.song_id 
        , sa.artist_id 
        , e.sessionId
        , e.location
        , e.userAgent
        , year(start_time) as year
        , month(start_time) as month
    from log_data_view e
    left join 
    (
        select distinct song_id, title as song_title, duration, artist_id, artist_name
        from song_data_view 
    ) sa
    on e.song = sa.song_title
    and e.length = sa.duration
    and e.artist = sa.artist_name
    where e.page = 'NextSong'
    ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, 'songplays'), 'overwrite')

    print("Loading of Songplays table is completed.")
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://rakesh-udacity-project4-parquet-uswest2/"
    # output_data = "./spark-warehouse"                         # Use for testing. Keep this commented otherwise.
    
    process_data(spark, input_data, output_data)    


if __name__ == "__main__":
    main()
