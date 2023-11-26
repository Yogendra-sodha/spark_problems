from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, IntegerType
import codecs

# create spark session
spark = SparkSession.Builder().appName("broadcast").getOrCreate()

# create schema
schema = StructType([
    StructField("id",IntegerType()),
    StructField("movie",IntegerType()),
    StructField("rating",IntegerType())])

# create dataframe
movie_df = spark.read.option("sep","\t").schema(schema).csv("file:///C:/SparkCourse/ml-100k/u.data", schema=schema).cache()
movie_df.createOrReplaceTempView("movie_ratings")

def loadMovies():
    movieNames = {}
    with codecs.open("C:/SparkCourse/ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore") as file:
        for line in file:
            line = line.split("|")
            movieNames[int(line[0])] = line[1]
    return movieNames

# broadcast the object to spark nodes and executor

movie_name = spark.sparkContext.broadcast(loadMovies())

# run the agg
movie_count = spark.sql("select movie, count(movie) as mv_count \
                         from movie_ratings\
                         group by movie \
                         order by mv_count desc")


# create udf and register udf to join movie names from object to this df

def lookupMovie(movie_id):
    return movie_name.value[movie_id]


# Register UDF
lookupUdf = func.udf(lookupMovie)


# use the udf
movie_name_rdd = movie_count.withColumn("Movie_name", lookupUdf(func.col("movie")))

movie_name_rdd.show()

spark.stop()