from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, IntegerType

spark = SparkSession.Builder().appName("famous_movie").getOrCreate()


# write mapper
def parser(lines):
    lines = lines.split("\t")
    id = int(lines[0])
    movie = int(lines[1])
    rating = int(lines[2])
    return (id,movie,rating)

# create rdd
movie_rdd = spark.sparkContext.textFile("file:///C:/SparkCourse/ml-100k/u.data")
movie_rdd = movie_rdd.map(parser)

# create schema 
# create schema
schema = StructType([
    StructField("id",IntegerType()),
    StructField("movie",IntegerType()),
    StructField("rating",IntegerType())])

# create dataframe
movie_df = spark.createDataFrame(movie_rdd, schema=schema).cache()
movie_df.createOrReplaceTempView("movies")


# famous based on most rating

famous_movies = spark.sql("select movie, count(movie) as movie_count from movies group by movie order by movie_count desc")

famous_movies.show()


spark.stop()