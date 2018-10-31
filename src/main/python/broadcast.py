from pyspark import SparkContext 
sc = SparkContext("local", "Broadcast app") 
words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"]) 
data = words_new.value 
print("Stored data -> {}".format(data))
elem = words_new.value[2] 
print("Printing a particular element in RDD -> {}".format(elem))
