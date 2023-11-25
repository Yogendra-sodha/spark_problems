from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType

spark = SparkSession.Builder().appName("total_amt").getOrCreate()

# schema
schema = StructType([
    StructField("id", IntegerType()),
    StructField("height", IntegerType()),
    StructField("amt", FloatType())
])

# create df
cust_df = spark.read.schema(schema).csv("file:///C:/SparkCourse/customer-orders.csv").cache()

cust_df.createOrReplaceTempView("cust_table")

# sql query

spark.sql(" select id, round(sum(amt),2) as total_amt from cust_table group by id order by total_amt asc").show()

spark.stop()