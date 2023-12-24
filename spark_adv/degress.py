from pyspark import SparkConf, SparkContext

# create spark session
conf = SparkConf().setMaster("local").setAppName("bfs")

sc = SparkContext(conf=conf)

# target character nad friend
targetId = 459
friendId = 4817

# hit accumalor
hit = sc.accumulator(0)

# process the data and prepare the structure of node
def preprocess(line):
    line = line.split()
    charId = int(line[0])
    color = "White"
    distance = 9999
    connection = []
    for i in line[1:]:
        connection.append(int(i))
    if charId == targetId:
        color = "Gray"
        distance = 0
    return (charId,(connection,distance,color))

# read the data and prepare the data
def prepareRdd():
    readLines = sc.textFile("file:///C:/SparkCourse/spark_adv/Marvel+Graph")
    return readLines.map(preprocess)

# how to procee the gray node and expand the node
def processNode(node):
    # (,([],,))
    charId = node[0]
    connection = node[1][0]
    color = node[1][2]
    distance = node[1][1]
    result = []
    
    if color == "Gray":
        for i in connection:
            newCharId = i
            distance = distance + 1
            color = "Gray"
            result.append((newCharId,([],distance,color)))
            if newCharId == friendId:
                hit.add(1)
        # BECAUSE WE have expanded and proceed the original node so this node is black and it append with all these node 
        color = "Black"

    result.append((charId,(connection,distance,color)))
    return result
        
# reduce the node and change the color and distance for processed node
def reducer(node1, node2):
    conn1 = node1[0]
    conn2 = node2[0]
    dist1 = node1[1]
    dist2 = node2[1]
    color1 = node1[2]
    color2 = node2[2]
    color = color1
    # preserve the connection with friends
    if len(conn1) > len(conn2):
        conn = conn1
    else:
        conn = conn2
    
    # preserve the minimum distance
    if dist1>dist2:
        dist = dist2
    else:
        dist = dist1
    
    # preserve the darkest color
    if color1 == "White" and (color2 == "Gray" or color2 == "Black"):
        color = color2
    if (color1 == "Gray" or color1 == "Black") and color2 == "White":
        color = color1
    if color1 == "Gray" and color2 == "Black":
        color = color2
    if color1 == "Black" and color2 == "Gray":
        color = color1
    
    return (conn,dist,color)



# write the program
intialRdd = prepareRdd()
for i in range(1,11):
    print("Iteration round",i)

    map_rdd = intialRdd.flatMap(processNode)
    print("Processed nodes:",map_rdd.count())

    if hit.value>0:
        print("Found in",hit.value,"directions")
        break
    intialRdd = map_rdd.reduceByKey(reducer)


    
