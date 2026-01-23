from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import from_json, col, round, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

spark = SparkSession.builder \
    .appName("FlightRadarTracker") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True)
])

def write_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "live_flights") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

opensky_schema = StructType([
    StructField("time", LongType(), True),
    StructField("states", ArrayType(ArrayType(StringType())), True)
])


raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "opensky_flights") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

flights_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), opensky_schema).alias("json_data")) \
    .select(explode(col("json_data.states")).alias("flight_list"))

processed_df = flights_df.select(
    col("flight_list").getItem(0).alias("ID_Maszyny"),
    col("flight_list").getItem(1).alias("Nr_Lotu"),
    col("flight_list").getItem(6).cast(DoubleType()).alias("Szerokosc"),
    col("flight_list").getItem(5).cast(DoubleType()).alias("Dlugosc"),
    (col("flight_list").getItem(9).cast(DoubleType()) * 3.6).alias("Predkosc_KMH"),
    col("flight_list").getItem(7).cast(DoubleType()).alias("Wysokosc_m"),
    col("flight_list").getItem(10).cast(DoubleType()).alias("kierunek"),
    functions.current_timestamp().alias("created_at")
).filter(col("ID_Maszyny").isNotNull())

query = processed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/opt/spark/apps/checkpoints/postgres_sync") \
    .start()

query.awaitTermination()