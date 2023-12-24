from pyspark import SparkConf, SparkContext
import re
conf = SparkConf().setMaster("local").setAppName("Flat-map")
sc = SparkContext(conf=conf)

a = ["Freedom to Live Where You Want \
Freedom to, Work When You Want \
Freedom to Work How You Want"]

readLines = sc.parallelize(a)
words = readLines.map(lambda x: x.split(',')).collect()
words1 = readLines.flatMap(lambda x : x.split(','))
print(words)
print(words1)
wordcnt = words1.countByValue()

# for i,count in sorted(wordcnt.items(), key = lambda x : x[1]):
#     cw = i.encode("ascii","ignore")
#     if cw:
#         print(i,count)