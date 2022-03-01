import sys
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_temperature_summary(transformed_df):
    windowSpec = Window.partitionBy("day")
    temperatureSummary_per_day = (transformed_df
                                  .withColumn("avg_temperature", F.avg("temperature").over(windowSpec))
                                  .withColumn("min_temperature", F.min("temperature").over(windowSpec))
                                  .withColumn("max_temperature", F.max("temperature").over(windowSpec))
                                  .select("avg_temperature", "min_temperature", "max_temperature", "location", "day")
                                  ).distinct()

    return temperatureSummary_per_day


def generate_highest_temperature(transformed_df):
    agg_df = (transformed_df
              .groupBy("location", "month")
              .agg(F.max("temperature").alias("highest_temperature"))
              .select("location", "month", "highest_temperature")
              )
    highestTemperature_per_locationAndMonth = (transformed_df.alias("transformed")
                                               .join(agg_df.alias("agg"),
                                                     on=[F.col("transformed.temperature") == F.col("agg.highest_temperature")], how="inner")
                                               .select("agg.location",
                                                       "transformed.date",
                                                       "agg.highest_temperature"
                                                       )).distinct()

    return highestTemperature_per_locationAndMonth


def transform_dataframe(df):
    file_name_arr = F.split(F.col("filename"), "/")
    file_name = file_name_arr.getItem(F.size(file_name_arr) - 1)

    transformed_df = (
        df.withColumn("hours", F.explode("hourly"))
        .select(
            F.substring_index(file_name, ".", 1).alias("location"),
            F.from_unixtime("hours.dt").alias("hours"),
            F.date_format(F.from_unixtime("hours.dt"),
                          "yyyy-MM-dd").alias("date"),
            F.dayofmonth(F.from_unixtime("hours.dt")).alias("day"),
            F.month(F.from_unixtime("hours.dt")).alias("month"),
            F.year(F.from_unixtime("hours.dt")).alias("year"),
            F.col("hours.temp").alias("temperature")
        ))
    return transformed_df


def readRawData(spark, input_raw):
    try:
        return spark.read.format("json").load(input_raw).withColumn(
            "filename", F.input_file_name())
    except Exception:
        raise


if __name__ == "__main__":

    # Initiating Spark Session
    spark = SparkSession \
        .builder \
        .appName("WeatherJob") \
        .getOrCreate()

    RAW_DIR = sys.argv[1]
    TRUSTED_DIR = sys.argv[2]

    # Read raw data
    input_raw = RAW_DIR + "/" + "*/*.json"

    df = spark.createDataFrame([], StructType([]))
    try:
        df = readRawData(spark, input_raw)
    except:
        logger.error(f"Failed to read data from {input_raw}")
        exit(1)
    # Pre processing
    transformed_df = transform_dataframe(df)
    # A dataset containing the location, date and temperature of the highest temperatures reported by location and month.
    # I am assuming that date = "year, month and days" when the highest temperature happened at such location
    highestTemperature_per_locationAndMonth = generate_highest_temperature(
        transformed_df)
    # A dataset containing the average temperature, min temperature, location of min temperature, and location of max temperature per day.
    temperatureSummary_per_day = generate_temperature_summary(transformed_df)
    # Generate Outputs
    highestTemperature_per_locationAndMonth.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(TRUSTED_DIR + "/" + "maxTemperature_per_LocationAndMonth")

    temperatureSummary_per_day.write \
        .partitionBy("day") \
        .format("parquet") \
        .mode("overwrite") \
        .save(TRUSTED_DIR + "/" + "temperatureSummary_per_day")
