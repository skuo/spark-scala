package tutorials

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.log4j.{Level, Logger}

object UpdateTextStreaming { 
   def updateRunningSum(values: Seq[Long], state: Option[Long]) = {
       //print("----- updateRunningSum -----")
       //println(values.mkString(","))
       Some(state.getOrElse(0L) + values.size)
   }

   def main(args: Array[String]) { 

       println("Start of UpdateTextStreaming")
       // Create a StreamingContext with a 1-second batch size from a SparkConf
       val conf = new SparkConf().setMaster("local").setAppName("Text Streaming")
       val ssc = new StreamingContext(conf,Seconds(15))

       // Set the Logging level to WARN
       val rootLogger = Logger.getRootLogger()
       rootLogger.setLevel(Level.WARN)

       // set checkpointing
       ssc.checkpoint("/tmp");

       // Create a DStream using data received when a new file appears in the monitored directory
       val linesDStream = ssc.textFileStream("/Users/skuo/data/spark/streaming")
       println("=============== linesDStream.print() =============")
       //linesDStream.print()
       val wordsCountDStream = linesDStream.flatMap(_.split(" ")).map(x => (x, 1L))
       val successfulStream = wordsCountDStream.updateStateByKey(updateRunningSum _)
       successfulStream.print()

       // Interesting the code below writes data to output-timestamp, e.g. /Users/skuo/dta/spark/output-1473297090000/
       successfulStream.saveAsTextFiles("/Users/skuo/data/spark/output")

       // Start our streaming context and wait for it to "finish"
       println("UpdateTextStreaming: sscStart")
       ssc.start()
       // Wait for the job to finish
       println("UpdateTextStreaming: awaitTermination")
       ssc.awaitTermination()
       println("UpdateTextStreaming: done!")
   } 
} 
