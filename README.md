# spark-scala

#===============================
# Build steps for SparkWordCount
$ git clone git@github.com:skuo/spark-scala.git
$ cd spark-scala
$ sbt package
$ find . -name "*.jar" -print
./target/scala-2.11/spark-scala-project_2.11-1.0.jar
$ spark-submit --class "SparkWordCount" --master local[2] target/scala-2.11/spark-scala-project_2.11-1.0.jar 

The output is in the outfile/ directory.

#===============================
# Build and Run steps for SocketStreaming
# 
$ sbt package
$ cd ~/git/learning-spark
$ ./bin/fakelogs.sh
$ spark-submit --class "SocketStreaming" --master local[2] target/scala-2.11/spark-scala-project_2.11-1.0.jar

#===============================
# Build and Run steps for TextStreaming, batch=15 seconds
# output to directories ~/data/spark/output-timestamp 
#
$ sbt package
$ spark-submit --class "TextStreaming" --master local[2] target/scala-2.11/spark-scala-project_2.11-1.0.jar
$ cd ~/data/spark
$ rm streaming/*
$ cp input.txt streaming/input.txt  # to trigger TextStreaming
$ cp input.txt streaming/input2.txt # to trigger TextStreaming

#===============================
# Build and Run steps for WindowedTextStreaming, batch=15 seconds
# window duration = 30 seconds, sliding duration = 30 seconds
# output to directories ~/data/spark/output-timestamp 
#
$ sbt package
$ spark-submit --class "WindowedTextStreaming" --master local[2] target/scala-2.11/spark-scala-project_2.11-1.0.jar
$ cd ~/data/spark
$ rm streaming/*
$ cp input.txt streaming/input.txt  # to trigger WindowedTextStreaming
$ cp input.txt streaming/input2.txt # to trigger WindowedTextStreaming

output-1473359745000/part-00000
{empty file}

output-1473359775000/part-00000
(they,14)
(beautiful,4)

output-1473359805000/part-00000
(they,28)
(beautiful,8)

#===============================
# Build and Run steps for UpdateTextStreaming, batch=15 seconds
# track "infinitely growing" counts
# output to directories ~/data/spark/output-timestamp 
#
$ sbt package
$ spark-submit --class "UpdateTextStreaming" --master local[2] target/scala-2.11/spark-scala-project_2.11-1.0.jar
$ cd ~/data/spark
$ rm streaming/*
$ cp input.txt streaming/input.txt  # to trigger UpdateTextStreaming
$ cp input.txt streaming/input2.txt # to trigger UpdateTextStreaming

#===============================
# Build and Run steps for EmployeeHive
#
$ sbt package
$ spark-submit --class "EmployeeHive" --master local[2] target/scala-2.11/spark-scala-project_2.11-1.0.jar

#===============================
# Build and Run steps for ParquetHive
#
$ sbt package
$ spark-submit --class "ParquetHive" --master local[2] target/scala-2.11/spark-scala-project_2.11-1.0.jar

#===============================
# Build and Run steps for StructuredStreaming
# nc -lk 9999
# Anything typed in the "nc" terminal will appear in Spark console outpu
#
$ sbt package
$ spark-submit --class "StructuredStreaming" --master local[2] target/scala-2.11/spark-scala-project_2.11-1.0.jar
