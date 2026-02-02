from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql import DataFrame

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming")\
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk:1.11.469")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))\
    .config("spark.hadoop.fs.s3a.credential.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .getOrCreate()

    #Adjust the log level to minimise the console output on executors
    spark.sparkContext.setLogLevel("WARN")

    #Vehicle Schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    #GPS Schema
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    #Traffic Schema
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    #Weather Schema
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True)
    ])

    #Emergency Incident Schema
    emergencyIncidentSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),  
        StructField("type", StringType(), True),  
        StructField("timestamp", TimestampType(), True),    
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "broker:29092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withWatermark("timestamp", "2 minutes")
                )

    def streamWriter(DataFrame, checkpointFolder, output):
        return (DataFrame.writeStream
                .format("parquet")
                .option("checkpointLocation", checkpointFolder)
                .option("path", output)
                .outputMode("append")
                .start())

    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencyIncidentSchema).alias('emergency')

    #Join all the DFs 
    query1 = streamWriter(vehicleDF, 's3a://spark-streaming-data-dmengji1-2/checkpoints/vehicle_data', 
                            's3a://spark-streaming-data-dmengji1-2/data/vehicle_data')
    query2 = streamWriter(gpsDF, 's3a://spark-streaming-data-dmengji1-2/checkpoints/gps_data', 
                            's3a://spark-streaming-data-dmengji1-2/data/gps_data')
    query3 = streamWriter(trafficDF, 's3a://spark-streaming-data-dmengji1-2/checkpoints/traffic_data', 
                            's3a://spark-streaming-data-dmengji1-2/data/traffic_data')
    query4 = streamWriter(weatherDF, 's3a://spark-streaming-data-dmengji1-2/checkpoints/weather_data', 
                            's3a://spark-streaming-data-dmengji1-2/data/weather_data')
    query5 = streamWriter(emergencyDF, 's3a://spark-streaming-data-dmengji1-2/checkpoints/emergency_data', 
                            's3a://spark-streaming-data-dmengji1-2/data/emergency_data')    
    
    query5.awaitTermination()

if __name__ == "__main__":
    main()