from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

# create spark session
spark = SparkSession.Builder().appName("min-temp").getOrCreate()

# create schema

schema = StructType([
    StructField("station_name", StringType()),
    StructField("date", IntegerType()),
    StructField("temp_value",StringType()),
    StructField("temp", FloatType())
])

# create dataframe for min
temp_df = spark.read.schema(schema).csv("file:///C:/SparkCourse/1800.csv").cache()
temp_df.createOrReplaceTempView("temp_table")

# count min temp
# temp_df = temp_df.filter(temp_df.temp_value == "TMIN").show()

temp_df = spark.sql("Select * from temp_table where temp_value = 'TMIN' order by temp limit 1").show()


# convert temp to cel


spark.stop()