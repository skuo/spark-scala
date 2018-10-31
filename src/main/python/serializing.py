from pyspark.context import SparkContext
from pyspark.serializers import MarshalSerializer
sc = SparkContext("local", "serialization app", serializer = MarshalSerializer())
logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org"). setLevel( logger.Level.WARN )
logger.LogManager.getLogger("akka").setLevel( logger.Level.WARN )
print(sc.parallelize(list(range(1000))).map(lambda x: 2 * x).take(10))
sc.stop()
