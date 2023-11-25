from pyspark.sql import SparkSession 
from pyspark.sql import  Row

spark = SparkSession.Builder().appName("Spark-sql").master("local").getOrCreate()

def mapper(line):
    line = line.split(",")
    return Row(
        id = int(line[0]),
        name = str(line[1].encode("utf-8")),
        age = int(line[2]),
        friends = int(line[3])
    )

rdd = spark.sparkContext.textFile("file:///SparkCourse/fakefriends.csv")
rdd = rdd.map(mapper)

# Create dataframe and caching frame in memory for sql operations
friends_df = spark.createDataFrame(rdd).cache()
# creating temporary view 
friends_df.createOrReplaceTempView("People")

#perform sql commands

friends = spark.sql("SELECT id, name,age from People where age>25 and age <= 30")

# for i in friends.collect():
#     print(i)

friends_rdd = friends.groupBy("age").count().orderBy("age")
print(friends_rdd.show())

spark.stop()