import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.log4j.{Level, Logger}

object TextStreaming { 
   def main(args: Array[String]) { 

       println("Start of TextStreaming")
       // Create a StreamingContext with a 1-second batch size from a SparkConf
       val conf = new SparkConf().setMaster("local").setAppName("Text Streaming")
       val ssc = new StreamingContext(conf,Seconds(15))

       // Set the Logging level to WARN
       val rootLogger = Logger.getRootLogger()
       rootLogger.setLevel(Level.WARN)

       // Create a DStream using data received when a new file appears in the monitored directory
       val lines = ssc.textFileStream("/Users/skuo/data/spark/streaming")
       println("=============== lines.print() =============")
       lines.print()
       val words = lines.flatMap(_.split(" "))
       val wc = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)

       // Interesting the code below writes data to output-timestamp, e.g. /Users/skuo/dta/spark/output-1473297090000/
       //wc.saveAsTextFiles("/Users/skuo/data/spark/output")
       //wc.print

       // Start our streaming context and wait for it to "finish"
       println("TextStreaming: sscStart")
       ssc.start()
       // Wait for the job to finish
       println("TextStreaming: awaitTermination")
       ssc.awaitTermination()
       println("TextStreaming: done!")
   } 
} 
