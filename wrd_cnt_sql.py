from pyspark.sql import SparkSession, functions as func

# spark session

spark = SparkSession.Builder().appName("wrd-cnt").getOrCreate()

# read from text file in dataframe

text_df = spark.read.text("file:///C:/SparkCourse/book.txt")

# split the words
text_df = text_df.select(func.explode(func.split(text_df.value , "\\W+")).alias("words"))
text_df.filter(text_df.words != " ")

# lower the words
text_df = text_df.select(func.lower(text_df.words).alias("words"))

# group the words
text_df = text_df.groupBy(text_df.words).count().sort("count", ascending = [False]).show()
