from pyspark.sql import SparkSession, Row
from pyspark.sql.types import IntegerType, StructType, StringType, StructField

spark = SparkSession.Builder().appName("schema").getOrCreate()

# create dataframe directly
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("friends", IntegerType(), True)
])

friend = spark.read.schema(schema).csv("file:///C:/SparkCourse/fakefriends.csv")


# friend.filter(friend.age>25).show()


friend.groupBy(friend.age).count().show()

friend.select(friend.name, friend.age+2).show()

spark.stop()