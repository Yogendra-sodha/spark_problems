from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Amount")

sc = SparkContext(conf=conf)

def parselines(line):
    line = line.split(",")
    cid = int(line[0])
    amt = float(line[2])
    return (cid,amt)

rdd = sc.textFile("file:///SparkCourse/customer-orders.csv")
# Count total amount speant by customer
rdd = rdd.map(parselines)
total = rdd.reduceByKey(lambda x ,y: x+y )
flippedTotal = total.map(lambda x : (x[1],x[0]))
sortTotal = flippedTotal.sortByKey()
print(sortTotal.collect())


