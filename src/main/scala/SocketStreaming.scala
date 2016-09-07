import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

object SocketStreaming { 
   def main(args: Array[String]) { 

       println("Start of SocketStreaming")
       // Create a StreamingContext with a 1-second batch size from a SparkConf
       val conf = new SparkConf().setMaster("local").setAppName("Socket Streaming")
       val ssc = new StreamingContext(conf,Seconds(2))

       // Create a DStream using data received after connection to port 7777 on local machine
       val lines = ssc.socketTextStream("localhost", 7777)
       println("=============== lines.print() =============")
       lines.print()
       // Filter out DStream for lines with "error"
       val errorLines = lines.filter(_.contains("erorr"))
       // print out the lines with errors
       println("=============== errorLines.print() =============")
       errorLines.print()

       // Start our streaming context and wait for it to "finish"
       println("SocketStreaming: sscStart")
       ssc.start()
       // Wait for the job to finish
       println("SocketStreaming: awaitTermination")
       ssc.awaitTermination()
       println("SocketStreaming: done!")
   } 
} 
