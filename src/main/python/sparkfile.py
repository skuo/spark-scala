from pyspark import SparkContext
from pyspark import SparkFiles
dataframe_r = "/data/spark/examples/src/main/r/dataframe.R"
dataframe_name = "dataframe.R"
sc = SparkContext("local", "SparkFile App")
sc.addFile(dataframe_r)
print("Absolute Path -> {}".format(SparkFiles.get(dataframe_name)))
