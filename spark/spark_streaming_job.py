from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, from_unixtime
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
import psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

spark = SparkSession.builder.appName("Stocks").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def write_to_db(row):
    conn = psycopg2.connect(dbname = "stocks",user = "root", password = "root", host = "db")
    cur = conn.cursor()
    timestamp = row.timestamp
    datetime_object = datetime.fromtimestamp(timestamp, ZoneInfo("America/New_York"))
    print(datetime_object)
    cur.execute("Insert Into stock_ticks_stream (symbol,timestamp,price) VALUES (%s,%s,%s)", (row.symbol,datetime_object,row.price))
    conn.commit()
    cur.close()
    conn.close()


schema = StructType([
    StructField("c", DoubleType()),   
    StructField("h", DoubleType()),
    StructField("l", DoubleType()),
    StructField("o", DoubleType()),
    StructField("pc", DoubleType()),
    StructField("t", LongType()),
    StructField("ticker", StringType())     
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(col("data.ticker").alias("symbol"),col("data.t").alias("timestamp"),col("data.c").alias("price"))

query = df_parsed.writeStream \
    .foreach(lambda row: write_to_db(row)) \
    .start()


query.awaitTermination()
