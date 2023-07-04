from pyspark.sql import SparkSession
from my_app import process_data

if __name__ == "__main__":
    # Create the spark session object
    # After the application is executed, the event logs are stored in the logs directory, which can then be used to view the jobs using the Spark History Server.
    # To view the Spark History Server, you can use the logs directory as your spark.history.fs.logDirectory property value 
    # or you can copy the logs directory to a location that you have configured as your spark.history.fs.logDirectory property value.
    spark = SparkSession.builder.master("local[*]").config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", "logs").appName("newDayHw").getOrCreate()
    # Call our main routine
    process_data(spark)
    # Stop the spark session object
    spark.stop()