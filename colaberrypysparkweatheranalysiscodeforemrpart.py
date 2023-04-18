from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrameWriter

# Create a SparkSession
spark = SparkSession.builder.appName("WeatherAnalysis").getOrCreate()

# # Read the data from the Hive table
# weather_data_hive_input_table = spark.table("weather_data")

# Read the data from Aurora using JDBC
aurora_url = "jdbc:mysql://colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com:3306" \
             "/colaberryrdsdb "
properties = {
    "user": "admin",
    "password": "<password>",
    "driver": "com.mysql.cj.jdbc.Driver"
}
weather_data = spark.read.jdbc(url=aurora_url, table="weather_data", properties=properties)

# Calculate the start and end years
start_year = weather_data.agg({"date": "min"}).collect()[0][0].year
end_year = weather_data.agg({"date": "max"}).collect()[0][0].year

# Calculate the statistics
statistics = weather_data.filter((weather_data.max_temp != -9999) & (weather_data.min_temp != -9999) & (weather_data.precipitation != -9999)) \
    .groupBy("station_id", year("date").alias("year")) \
    .agg(avg("max_temp").alias("avg_max_temp"), avg("min_temp").alias("avg_min_temp"), sum("precipitation").alias("total_precipitation"))

# Write the results to Redshift using JDBC
redshift_url = "jdbc:redshift://colaberryredshiftnewcluster.803471918786.us-east-2.redshift-serverless.amazonaws.com" \
               ":5439/dev"
redshift_properties = {
    "user": "admin",
    "password": "<password>",
    "driver": "com.amazon.redshift.jdbc.Driver"
}
statistics_writer = DataFrameWriter(statistics)
statistics_writer.jdbc(url=redshift_url, table="colaberryoutputtablethroughemrjob", mode="overwrite", properties=redshift_properties)

# Write the results to a new Hive table
# statistics.write.mode("overwrite").saveAsTable("weather_statistics")

# Stop the SparkSession
spark.stop()
