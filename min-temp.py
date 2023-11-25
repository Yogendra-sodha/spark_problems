from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Min-Temp")

sc = SparkContext(conf=conf)

def parselines(line):
    line = line.split(',')
    stationId = line[0]
    tempType = line[2]
    temp = float(line[3])*0.1*(9/5)+32
    return (stationId,tempType,temp)

readLines = sc.textFile("file:///SparkCourse/1800.csv")
rdd = readLines.map(parselines)
rdd = rdd.filter(lambda x : "TMAX" in x)
rdd = rdd.map(lambda x : (x[0],x[2]))
rdd = rdd.reduceByKey(lambda x,y: max(x,y))
print(rdd.collect())