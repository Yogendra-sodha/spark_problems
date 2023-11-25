from pyspark import SparkConf, SparkContext
import re
conf = SparkConf().setMaster("local").setAppName("Flat-map")
sc = SparkContext(conf=conf)



readLines = sc.textFile("file:///C:/SparkCourse/Book")
words = readLines.flatMap(lambda x: x.split())
wordcnt = words.countByValue()

for i,count in sorted(wordcnt.items(), key = lambda x : x[1]):
    cw = i.encode("ascii","ignore")
    if cw:
        print(i,count)