# Cleaning, Transforming and Ingesting Crop Data Right Now and Writing this Data to PostgreSQL on RDS

# Import the required libraries and set up the AWS Glue context:

import sys
import json
import os
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import logging
from datetime import datetime
from pytz import timezone
import time
import pymysql
import base64

# define the CloudWatch Log Group and Log Stream names
log_group_name = '/aws/lambda/ColaberryLambdaTriggersGlueandEMR'
log_stream_name = 'ColaberryLambdaTriggersGlueandEMRExecution' + datetime.now(timezone('US/Eastern')).strftime(
    '%m/%d/%Y/%H/%M/%S %Z/%z')


# define a custom log handler to send logs to CloudWatch Logs
class CloudWatchLogHandler(logging.Handler):
    def __init__(self, log_group_name=None, log_stream_name=None, region_name=None):
        # logging.Handler.__init__(self)
        super(CloudWatchLogHandler, self).__init__()
        self.logs_client = boto3.client('logs', region_name=region_name)
        self.log_group = log_group_name
        self.log_stream = log_stream_name or str(uuid.uuid4())
        if self.log_group:
            try:
                self.logs_client.create_log_group(logGroupName=self.log_group)
            except self.logs_client.exceptions.ResourceAlreadyExistsException:
                pass
        if self.log_stream:
            try:
                self.logs_client.create_log_stream(logGroupName=self.log_group, logStreamName=self.log_stream)
            except self.logs_client.exceptions.ResourceAlreadyExistsException:
                pass
        self.sequence_token = None

    def emit(self, record):
        try:
            # create the log event
            log_event = {
                'timestamp': int(record.created * 1000),
                'message': self.format(record)
            }
            if self.sequence_token:
                # log_event['sequenceToken'] = self.sequence_token
                sequenceToken = self.sequence_token
            if self.sequence_token is None:
                response = self.logs_client.put_log_events(
                    logGroupName=self.log_group,
                    logStreamName=self.log_stream,
                    logEvents=[log_event]
                )
            else:
                response = self.logs_client.put_log_events(
                    logGroupName=self.log_group,
                    logStreamName=self.log_stream,
                    logEvents=[log_event],
                    sequenceToken=sequenceToken  # sequenceToken=log_event['sequenceToken']
                )
            self.sequence_token = response['nextSequenceToken']
        except Exception as e:
            logger.debug("Error sending logs to CloudWatch Logs: {}".format(e))


# create CloudWatchLogHandler
cloudwatch_handler = CloudWatchLogHandler(log_group_name=log_group_name, log_stream_name=log_stream_name,
                                          region_name='us-east-2')
cloudwatch_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# create logger and add CloudWatchLogHandler
logger = logging.getLogger(__name__)

# set the log level and add the custom log handler to the logger
logger.setLevel(logging.INFO)
logger.addHandler(cloudwatch_handler)


def main():
    spark = SparkSession.builder \
        .appName("colaberrycodechallengegluejobforcropdata") \
        .getOrCreate()
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_path'])

    glueContext = GlueContext(SparkContext.getOrCreate())

    schema = StructType([
        StructField("year", IntegerType(), False),
        StructField("corn_yield", DoubleType(), False),
    ])

    yld_df = spark.read.format("csv") \
        .option("delimiter", "\t") \
        .option("header", False) \
        .schema(schema) \
        .load("s3://colaberrycodechallenges3/yld_data/US_corn_grain_yield.txt") \
        .toDF("year", "corn_yield")

    # check if a column exists
    if "year" in yld_df.columns:
        yld_df.select(col("year")).show()
    else:
        # handle the case where the column doesn't exist
        logger.info("Column 'year' does not exist")

    # cast data types
    yld_df = yld_df.withColumn("year", yld_df["year"].cast(IntegerType()))
    yld_df = yld_df.withColumn("corn_yield", yld_df["corn_yield"].cast(DoubleType()))

    # group by year and calculate mean corn yield
    df_agg = yld_df.groupBy("year").agg(avg("corn_yield").alias("mean_corn_yield"))

    df_agg.show()

    # select columns and order by year
    df_cleaned = df_agg.select("year", "mean_corn_yield").orderBy("year")

    df_cleaned.show()

    # write data to the MySQL database on AWS RDS using Data API
    table_name = 'crop_data'

    # create connection to the MySQL database on AWS RDS
    connection = pymysql.connect(
        host='colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com',
        user='admin',
        password='<password>',
        db='colaberryrdsdb',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    df_cleaned = df_cleaned.selectExpr(
        'year AS year',
        'mean_corn_yield AS corn_yield'
    )

    df_cleaned.show()

    # list all tables in the database
    with connection.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        table_list = [row['Tables_in_colaberryrdsdb'] for row in cursor.fetchall()]

    logger.info("table_list :: ")
    logger.info(table_list)

    # check if the table exists
    if table_name not in table_list:

        # create the table if it doesn't exist along with its schema
        try:
            # Define the SQL query to create the table
            create_table_query = """
            CREATE TABLE crop_data (
            year INT NOT NULL,
            corn_yield DOUBLE NOT NULL
            ) ENGINE=InnoDB;
            """
            # create a cursor object
            with connection.cursor() as cursor:
                # execute the SQL query to create the table
                cursor.execute(create_table_query)
                logger.info("Table created successfully")
            # commit the transaction
            connection.commit()
        except Exception as e:
            logger.info("Error creating table :: ")
            logger.info(str(e))

    # track the start time of the data ingestion process
    start_time = datetime.now()

    # get the number of records before ingestion using pymysql connection
    pre_count = None
    try:
        connection = pymysql.connect(
            host='colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com',
            user='admin',
            password='<password>',
            db='colaberryrdsdb',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with connection.cursor() as cursor:
            # execute the SQL query to get the count of rows in the table
            sql = "SELECT COUNT(*) AS count FROM crop_data"
            cursor.execute(sql)

            # get the count of rows from the result set
            result = cursor.fetchone()
            pre_count = result['count']

        # close the database connection
        connection.close()
    except Exception as e:
        logger.error(f"Error getting count of rows before ingestion: {str(e)}")
        if connection:
            connection.close()

    # insert data into the table using SQL
    try:
        # create a connection and cursor objects
        connection = pymysql.connect(
            host='colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com',
            user='admin',
            password='<password>',
            db='colaberryrdsdb',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = connection.cursor()

        # check if the year column already has a unique constraint
        sql = f"SHOW CREATE TABLE {table_name}"
        cursor.execute(sql)
        result = cursor.fetchone()
        create_table_stmt = result['Create Table']

        logger.info("create_table_stmt")
        logger.info(create_table_stmt)

        if 'UNIQUE KEY `year_unique` (`year`)' not in create_table_stmt:
            # add a unique constraint to the year column using SQL
            sql = f"ALTER TABLE {table_name} ADD CONSTRAINT year_unique UNIQUE (year)"
            logger.info("sql query :: ")
            logger.info(sql)
            cursor.execute(sql)
            logger.info("Unique constraint added to year column")
            # write data to the MySQL database on AWS RDS using JDBC
            # create a JDBC URL for the MySQL database on AWS RDS
            jdbc_url = "jdbc:mysql://colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com:3306/colaberryrdsdb"

            # set properties for the JDBC driver
            properties = {
                "user": "admin",
                "password": "<password>",
                "driver": "com.mysql.jdbc.Driver"
            }

            # write the data to MySQL using the JDBC method
            df_cleaned.write.jdbc(url=jdbc_url, table="crop_data", mode="append", properties=properties)
        else:
            logger.info("Year column already has a unique constraint")

        # commit the transaction
        connection.commit()
    except Exception as e:
        logger.error(f"Error inserting data into MySQL: {str(e)}")
        # rollback the changes if there is an error
        connection.rollback()
    finally:
        # close the database connection
        connection.close()

    # track the end time for the data ingestion process
    end_time = datetime.now()

    # get the number of records after ingestion
    post_count = None
    try:
        connection = pymysql.connect(
            host='colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com',
            user='admin',
            password='<password>',
            db='colaberryrdsdb',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with connection.cursor() as cursor:
            # execute the SQL query to get the count of rows in the table
            sql = "SELECT COUNT(*) AS count FROM crop_data"
            logger.info("sql query :: ")
            logger.info(sql)
            cursor.execute(sql)

            # get the count of rows from the result set
            result = cursor.fetchone()
            post_count = result['count']
    except Exception as e:
        logger.error(f"Error getting count of rows after ingestion: {str(e)}")
    finally:
        # close the database connection
        connection.close()

    # print the summary of the data ingestion process
    logger.info("Data ingested for crop data :: ")
    logger.info("Start time for ingestion process :: ")
    logger.info(start_time)
    logger.info("End time for ingestion process :: ")
    logger.info(end_time)
    logger.info("Number of records before ingestion process :: ")
    logger.info(pre_count)
    logger.info("Number of records after ingestion process :: ")
    logger.info(post_count)
    logger.info(
        f"Number of records after ingestion process - Number of records before ingestion process or total number of records ingested: {post_count - pre_count}")
    logger.info(f"Time taken to ingest the records : {end_time - start_time}")

    # Cleaning, Transforming and Ingesting Weather Data Right Now and Writing this Data to Aurora MySQL DB Table on RDS)

    # initialize SparkContext and GlueContext
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)

    # create S3 client
    s3 = boto3.client('s3')

    input_bucket = args['s3_input_path'].split('/')[2]
    logger.info("input_bucket :: ")
    logger.info(input_bucket)
    input_prefix = '/'.join(args['s3_input_path'].split('/')[3:])
    logger.info("input_prefix :: ")
    logger.info(input_prefix)
    s3_objects = s3.list_objects_v2(Bucket=input_bucket, Prefix=input_prefix)

    # Define your schema with "type" field for "station_id" column
    schema = StructType([
        StructField("date", IntegerType(), False),
        StructField("max_temp", DoubleType(), False),
        StructField("min_temp", DoubleType(), False),
        StructField("precipitation", DoubleType(), False),
        StructField("station_id", StringType(), False)
    ])

    # Convert the schema to a JSON string
    schema_json = json.dumps(schema.jsonValue())

    logger.info("schema_json :: ")
    logger.info(schema_json)

    # write data to the MySQL database on AWS RDS using Data API
    table_name = 'weather_data'

    weather_dynamic_frames = []
    for s3_object in s3_objects['Contents']:
        if s3_object['Key'].endswith(".txt"):
            file_location = "s3://{}/{}".format(input_bucket, s3_object['Key'])
            logger.info(f"Reading file {file_location}")
            # weather_dynamic_frame = glueContext.create_dynamic_frame_from_options(
            #     "s3", {"paths": [file_location]}, format="csv", format_options={"header": "false", "delimiter": "\t"})
            weather_dynamic_frame = glueContext.create_dynamic_frame_from_options(
                "s3",
                {"paths": [file_location]},
                format="csv",
                format_options={"header": "false", "delimiter": "\t"},
                schema=schema_json
            )
            weather_dynamic_frames.append(weather_dynamic_frame)

    weather_df = None
    for weather_dynamic_frame in weather_dynamic_frames:
        # Create a DataFrame from the DynamicFrame and attach the schema
        df = weather_dynamic_frame.toDF()
        logger.info("df :: ")
        df.show()
        logger.info("df.printSchema() :: ")
        df.printSchema()
        df = df.selectExpr("split(col0, '\t')[0] as date", "split(col0, '\t')[1] as max_temp",
                           "split(col0, '\t')[2] as min_temp", "split(col0, '\t')[3] as precipitation")
        # cast data types
        df = df.withColumn("date", df["date"].cast(IntegerType()))
        df = df.withColumn("max_temp", df["max_temp"].cast(DoubleType()))
        df = df.withColumn("min_temp", df["min_temp"].cast(DoubleType()))
        df = df.withColumn("precipitation", df["precipitation"].cast(DoubleType()))
        # extract station_id from file name
        # df = df.withColumn("station_id", split(input_file_name(), "/")[-1]) \
        #       .withColumn("station_id", regexp_extract("station_id", r"^(.*?)\.txt$", 1))
        df = df.withColumn("station_id", regexp_extract(input_file_name(), r".*/(.+)\.txt$", 1))
        if weather_df is None:
            weather_df = df
        else:
            weather_df = weather_df.union(df)

    # rename columns
    weather_df = weather_df.select(col("date"), col("max_temp"), col("min_temp"), col("precipitation"),
                                   col("station_id"))

    logger.info("weather_df :: ")
    weather_df.show()
    logger.info("weather_df.printSchema( :: ")
    weather_df.printSchema()

    # write data to Aurora MySQL DB table on RDS using JDBC
    jdbc_url = "jdbc:mysql://colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com:3306/colaberryrdsdb"
    table_name = "weather_data"
    properties = {
        "user": "admin",
        "password": "<password>",
        "driver": "com.mysql.jdbc.Driver"
    }

    # create connection to the MySQL database on AWS RDS
    connection = pymysql.connect(
        host='colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com',
        user='admin',
        password='<password>',
        db='colaberryrdsdb',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    # list all tables in the database
    with connection.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        table_list = [row['Tables_in_colaberryrdsdb'] for row in cursor.fetchall()]

    logger.info("table_list :: ")
    logger.info(table_list)

    # check if the table exists
    if table_name not in table_list:

        # create the table if it doesn't exist along with its schema
        try:
            # Define the SQL query to create the table
            create_table_query = """
            CREATE TABLE weather_data (
            date INT NOT NULL,
            max_temp DOUBLE NOT NULL,
            min_temp DOUBLE NOT NULL,
            precipitation DOUBLE NOT NULL,
            station_id VARCHAR(30) NOT NULL
            ) ENGINE=InnoDB;
            """
            # create a cursor object
            with connection.cursor() as cursor:
                # execute the SQL query to create the table
                cursor.execute(create_table_query)
                logger.info("Table created successfully")
            # commit the transaction
            connection.commit()
        except Exception as e:
            logger.info("Error creating table :: ")
            logger.info(str(e))

    # track the start time of the data ingestion process
    start_time = datetime.now()

    # get the number of records before ingestion using pymysql connection
    pre_count = None
    try:
        connection = pymysql.connect(
            host='colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com',
            user='admin',
            password='<password>',
            db='colaberryrdsdb',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with connection.cursor() as cursor:
            # execute the SQL query to get the count of rows in the table
            sql = "SELECT COUNT(*) AS count FROM weather_data"
            cursor.execute(sql)

            # get the count of rows from the result set
            result = cursor.fetchone()
            pre_count = result['count']

        # close the database connection
        connection.close()
    except Exception as e:
        logger.error(f"Error getting count of rows before ingestion: {str(e)}")
        if connection:
            connection.close()

    # insert data into the table using SQL
    try:
        # create a connection and cursor objects
        connection = pymysql.connect(
            host='colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com',
            user='admin',
            password='<password>',
            db='colaberryrdsdb',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = connection.cursor()

        # check if the year column already has a unique constraint
        sql = f"SHOW CREATE TABLE {table_name}"
        cursor.execute(sql)
        result = cursor.fetchone()
        create_table_stmt = result['Create Table']

        logger.info("create_table_stmt")
        logger.info(create_table_stmt)

        if 'UNIQUE KEY `unique_station_data_for_particular_day` (`station_id`,`date`)' not in create_table_stmt:
            # add a unique constraint to the year column using SQL
            sql = f"ALTER TABLE {table_name} ADD CONSTRAINT unique_station_data_for_particular_day UNIQUE (station_id,date)"
            logger.info("sql query :: ")
            logger.info(sql)
            cursor.execute(sql)
            logger.info("Unique constraint added to station_id and date column")
            # write data to the MySQL database on AWS RDS using JDBC
            # create a JDBC URL for the MySQL database on AWS RDS
            jdbc_url = "jdbc:mysql://colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com:3306/colaberryrdsdb"

            # set properties for the JDBC driver
            properties = {
                "user": "admin",
                "password": "<password>",
                "driver": "com.mysql.jdbc.Driver"
            }

            # write the data to MySQL using the JDBC method
            weather_df.write.jdbc(url=jdbc_url, table="weather_data", mode="append", properties=properties)
        else:
            logger.info("station_id and date columns already have unique constraints")

        # commit the transaction
        connection.commit()
    except Exception as e:
        logger.error(f"Error inserting data into MySQL: {str(e)}")
        # rollback the changes if there is an error
        connection.rollback()
    finally:
        # close the database connection
        connection.close()

    # track the end time for the data ingestion process
    end_time = datetime.now()

    # get the number of records after ingestion
    post_count = None
    try:
        connection = pymysql.connect(
            host='colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com',
            user='admin',
            password='<password>',
            db='colaberryrdsdb',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with connection.cursor() as cursor:
            # execute the SQL query to get the count of rows in the table
            sql = "SELECT COUNT(*) AS count FROM weather_data"
            logger.info("sql query :: ")
            logger.info(sql)
            cursor.execute(sql)

            # get the count of rows from the result set
            result = cursor.fetchone()
            post_count = result['count']
    except Exception as e:
        logger.error(f"Error getting count of rows after ingestion: {str(e)}")
    finally:
        # close the database connection
        connection.close()

    # print the summary of the data ingestion process
    logger.info("Data ingested for weather data :: ")
    logger.info("Start time for ingestion process :: ")
    logger.info(start_time)
    print("End time for ingestion process :: ")
    logger.info(end_time)
    logger.info("Number of records before ingestion process :: ")
    logger.info(pre_count)
    logger.info("Number of records after ingestion process :: ")
    logger.info(post_count)
    logger.info(
        f"Number of records after ingestion process - Number of records before ingestion process or total number of records ingested: {post_count - pre_count}")
    logger.info(f"Time taken to ingest the records : {end_time - start_time}")

    # cleanup resources
    sc.stop()


main()
