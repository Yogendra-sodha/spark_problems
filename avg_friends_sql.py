from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

#  Create Session

spark = SparkSession.Builder().appName("avg_friends").getOrCreate()

#  Infer Schema

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("friends", IntegerType(), True)
])

# Create Dataframe
friends_df = spark.read.schema(schema).csv("file:///C:/SparkCourse/fakefriends.csv").cache()

friends_df.createOrReplaceTempView("friends_table")

spark.sql("Select age, round(avg(friends),2) as avgfr From friends_table group by age").show(30)


spark.stop()