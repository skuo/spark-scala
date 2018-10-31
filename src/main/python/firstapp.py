from pyspark import SparkContext

logFile = "file:///data/spark/README.md"  
sc = SparkContext("local", "first app")

logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: {}, lines with b: {}".format(numAs, numBs))
