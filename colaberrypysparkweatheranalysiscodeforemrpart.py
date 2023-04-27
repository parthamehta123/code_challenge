import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrameWriter
from pyspark.sql.types import StructField, IntegerType, DoubleType, TimestampType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("WeatherAnalysis") \
    .config("spark.jars", "mysql-connector-java-8.0.27.jar") \
    .config("spark.driver.extraClassPath", "mysql-connector-java-8.0.27.jar") \
    .getOrCreate()

# spark = SparkSession.builder \
#     .appName("WeatherAnalysis") \
#     .getOrCreate()

# Read the data from Aurora using JDBC
aurora_url = "jdbc:mysql://colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com:3306/colaberryrdsdb"
properties = {
    "user": "admin",
    "password": "<Password>",
    "driver": "com.mysql.jdbc.Driver"
}

# weather_data = spark.read.jdbc(url=aurora_url, table="weather_data", properties=properties)
weather_data = spark.read.format("jdbc") \
    .option("driver", properties["driver"]) \
    .option("url", aurora_url) \
    .option("query", "select * from weather_data") \
    .option("user", properties["user"]) \
    .option("password", properties["password"]) \
    .load()

weather_data.show()

# Define the schema for the Statistics table
# schema = StructType([
#     StructField("station_id", StringType(), False),
#     StructField("year", IntegerType(), False),
#     StructField("avg_max_temp", DoubleType()),
#     StructField("avg_min_temp", DoubleType()),
#     StructField("total_precipitation", DoubleType()),
#     StructField("created_timestamp", TimestampType(), False),
#     StructField("updated_timestamp", TimestampType(), False),
# ])

# Create the empty table
spark.read.format("jdbc").options(url=aurora_url, driver=properties["driver"], user=properties["user"],
                                  password=properties["password"],
                                  dbtable="(SELECT 1) tmp").load().createOrReplaceTempView("tmp_table")

# create connection to the MySQL database on AWS RDS
connection = pymysql.connect(
    host='colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com',
    user='admin',
    password='<Password>',
    db='colaberryrdsdb',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

# list all tables in the database
with connection.cursor() as cursor:
    cursor.execute("SHOW TABLES")
    table_list = [row['Tables_in_colaberryrdsdb'] for row in cursor.fetchall()]

print("table_list :: ")
print(table_list)

table_name = 'weather_statistics'

# check if the table exists
if table_name not in table_list:

    # create the table if it doesn't exist along with its schema
    try:
        # Define the SQL query to create the table
        create_table_query = """
            CREATE TABLE weather_statistics (
                        station_id VARCHAR(30) NOT NULL,
                        year INT NOT NULL,
                        avg_max_temp DOUBLE,
                        avg_min_temp DOUBLE,
                        total_precipitation DOUBLE,
                        created_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (year, station_id)
                    ) ENGINE=InnoDB;
            """
        # create a cursor object
        with connection.cursor() as cursor:
            # execute the SQL query to create the table
            cursor.execute(create_table_query)
            print("Table created successfully")
        # commit the transaction
        connection.commit()
    except Exception as e:
        print("Error creating table :: ")
        print(str(e))

# spark.sql(table_schema)
# # Create a DataFrame with the specified schema
# empty_df = spark.createDataFrame([], schema)
#
# # Write the DataFrame to the Aurora database
# empty_df.write.jdbc(url=aurora_url, table="weather_statistics", mode="overwrite", properties=properties)

# Calculate the start and end years
start_year = weather_data.select(to_date("date").alias("date")).agg({"date": "min"}).collect()[0][0].year
end_year = weather_data.select(to_date("date").alias("date")).agg({"date": "max"}).collect()[0][0].year

# Calculate the statistics
statistics = weather_data.filter(
    (weather_data.max_temp != -9999) & (weather_data.min_temp != -9999) & (weather_data.precipitation != -9999)) \
    .groupBy("station_id", year("date").alias("year")) \
    .agg(avg("max_temp").alias("avg_max_temp"), avg("min_temp").alias("avg_min_temp"),
         sum("precipitation").alias("total_precipitation"))

statistics.write.jdbc(url=aurora_url, table="weather_statistics", mode="overwrite", properties=properties)

statistics \
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("s3a://colaberrycodechallenges3/emrsparkcodeoutputlocation/")

print("Weather Statistics Data Calculated and Stats are Written to the MySQL DB")
statistics.show()

# # Write the results to Redshift using JDBC
redshift_url = "jdbc:redshift://colaberryredshiftnewcluster.803471918786.us-east-2.redshift-serverless.amazonaws.com" \
               ":5439/dev"
redshift_properties = {
    "user": "admin",
    "password": "<Password>",
    "driver": "com.amazon.redshift.jdbc.Driver"
}
statistics_writer = DataFrameWriter(statistics)
statistics_writer.jdbc(url=redshift_url, table="colaberryoutputtablethroughemrjob", mode="overwrite",
                       properties=redshift_properties)

# Stop the SparkSession
spark.stop()
