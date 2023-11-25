from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Average-friends")
sc = SparkContext(conf=conf)

def parseLine(line):
    line = line.split(',')
    age = int(line[2])
    frnd = int(line[3])
    return (age,frnd)

readFile = sc.textFile("file:///C:/SparkCourse/fakefriends.csv")
rdd = readFile.map(parseLine)
total = rdd.mapValues(lambda x : (x,1))
print(total.take(10))
addTotal = total.reduceByKey(lambda x , y : (x[0]+y[0],x[1]+y[1]))
print(addTotal.take(10))
average = addTotal.mapValues(lambda x : round(x[0]/x[1],2))

result = average.collect()

for i in sorted(result, key =lambda x:x[0]):
    print(i)