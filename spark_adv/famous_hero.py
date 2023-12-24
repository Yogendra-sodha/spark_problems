from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = SparkSession.Builder().appName("famous-hero").getOrCreate()

schema = StructType(
    [StructField("id", IntegerType()), StructField("names", StringType())]
)

# read names 
names = spark.read.option("sep", " ").schema(schema).csv("file:///C:/SparkCourse/spark_adv/Marvel+Names")
names.createOrReplaceTempView("hero_names")
names.printSchema()

# read connections
conn = spark.read.csv("file:///C:/SparkCourse/spark_adv/Marvel+Graph")

conn = conn.withColumn("id", func.split(func.col("_c0"), " ")[0])\
        .withColumn("friends", func.size(func.split(func.col("_c0"), " "))-1).select("id","friends")
conn.createOrReplaceTempView("connection")

# find the famous heroes with most friends 
grouped_conn = spark.sql("select id, sum(friends) as total from connection group by id order by total desc")
grouped_conn.show()

# join both to find most famous superhero
print("Most famous super hero is: ")
famous_hero = names.filter(func.col("id") == grouped_conn.select("id").first()["id"])

f_hero = famous_hero.select("names").rdd.flatMap(lambda x: x).collect()
print(f_hero)

# most obscure hero
obscure_hero = spark.sql("select id, sum(friends) as total from connection group by id having total <= 1 order by total")
obscure_hero.show()
obscure_hero.createOrReplaceTempView("obscure")


# finding names of obsecure
obscure_hero = spark.sql("SELECT  n.names FROM obscure as o inner join hero_names as n on n.id = o.id ")
obscure_hero_names = obscure_hero.rdd.flatMap(lambda x: (x)).collect()
print(obscure_hero_names)


spark.stop()