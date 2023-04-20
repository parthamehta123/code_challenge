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
log_group_name = '/aws/lambda/ColaberryLambdaTriggersGlueandEMRCropDataIngestion'
log_stream_name = 'ColaberryLambdaTriggersGlueandEMRExecutionForCropDataIngestion' + datetime.now(
    timezone('US/Eastern')).strftime('%m/%d/%Y/%H/%M/%S %Z/%z')


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
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_path', 's3_file_name', 'db_name_rds_instance',
                                         'host_rds_instance', 'jdbc_url', 'password_rds_instance', 'user_rds_instance'])

    glueContext = GlueContext(SparkContext.getOrCreate())

    schema = StructType([
        StructField("year", IntegerType(), False),
        StructField("corn_yield", DoubleType(), False),
        StructField("created_timestamp", TimestampType(), False),
        StructField("updated_timestamp", TimestampType(), False)
    ])

    yld_df = spark.read.format("csv") \
        .option("delimiter", "\t") \
        .option("header", False) \
        .schema(schema) \
        .load(args['s3_input_path'] + "US_corn_grain_yield.txt") \
        .toDF("year", "corn_yield", "created_timestamp", "updated_timestamp")

    # check if a column exists
    if "year" in yld_df.columns:
        yld_df.select(col("year")).show()
    else:
        # handle the case where the column doesn't exist
        logger.info("Column 'year' does not exist")

    # cast data types
    yld_df = yld_df.withColumn("year", yld_df["year"].cast(IntegerType()))
    yld_df = yld_df.withColumn("corn_yield", yld_df["corn_yield"].cast(DoubleType()))
    yld_df = yld_df.withColumn("created_timestamp", current_timestamp())
    yld_df = yld_df.withColumn("updated_timestamp", current_timestamp())

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
        host=args['host_rds_instance'],
        user=args['user_rds_instance'],
        password=args['password_rds_instance'],
        db=args['db_name_rds_instance'],
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
            corn_yield DOUBLE NOT NULL,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            updated_timestamp TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL
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
            host=args['host_rds_instance'],
            user=args['user_rds_instance'],
            password=args['password_rds_instance'],
            db=args['db_name_rds_instance'],
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
            host=args['host_rds_instance'],
            user=args['user_rds_instance'],
            password=args['password_rds_instance'],
            db=args['db_name_rds_instance'],
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
            jdbc_url = args['jdbc_url']

            # set properties for the JDBC driver
            properties = {
                "user": args['user_rds_instance'],
                "password": args['password_rds_instance'],
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
            host=args['host_rds_instance'],
            user=args['user_rds_instance'],
            password=args['password_rds_instance'],
            db=args['db_name_rds_instance'],
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

    # cleanup resources
    spark.stop()


main()