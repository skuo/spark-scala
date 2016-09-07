# spark-scala

# Build steps for SparkWordCount
$ git clone git@github.com:skuo/spark-scala.git
$ cd spark-scala
$ sbt package
$ find . -name "*.jar" -print
./target/scala-2.11/spark-scala-project_2.11-1.0.jar
$ spark-submit --class "SparkWordCount" --master local[2] target/scala-2.11/spark-scala-project_2.11-1.0.jar 

The output is in the outfile/ directory.

# Build and Run steps for SocketStreaming
# 
$ sbt package
$ cd ~/git/learning-spark
$ ./bin/fakelogs.sh
$ spark-submit --class "SocketStreaming" --master local[2] target/scala-2.11/spark-scala-project_2.11-1.0.jar