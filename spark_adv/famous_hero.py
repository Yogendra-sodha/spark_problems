from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

spark = SparkSession.Builder().appName("superhero").getOrCreate()

# Schema
schema = StructType([
    StructField("id", IntegerType()),
    StructField("movie_name", StringType())
])


# read marvel names
names = spark.read.option("sep"," ").schema(schema).csv("file:///C:/SparkCourse/spark_adv/Marvel+Names").cache()
names.createOrReplaceTempView("names")

names.show()

# read hero and count friends size
connections = spark.read.text("file:///C:/SparkCourse/spark_adv/Marvel+Graph").cache()

frnds_conn = connections.withColumn("id",(func.split(func.col("value")," ")[0])).withColumn("friend_size", func.size(func.split(func.col("value"), " "))-1)
frnds_conn.createOrReplaceTempView("frnds_conn")

frnds_conn = spark.sql("SELECT cast(frnds_conn.id as INT), sum(friend_size) as total from frnds_conn \
                       group by frnds_conn.id \
                       order by total desc limit 1")
frnds_conn.createOrReplaceTempView("famous")

# selected_id = frnds_conn.select("id").first()["id"]

# famous_hero = names.filter(func.col("id") == selected_id).select("movie_name").first()
famous_hero = spark.sql("select movie_name from names join famous as f on f.id = names.id")

list_name = famous_hero.select("movie_name")
list_name.show()

spark.stop()
