package tutorials

import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark._  

object SparkWordCount { 
   def main(args: Array[String]) { 

      val conf = new SparkConf().setAppName("Spark Word Count")
      val sc = new SparkContext(conf)
		
      /* local = master URL; Word Count = application name; */  
      /* /usr/local/spark = Spark Home; Nil = jars; Map = environment */ 
      /* Map = variables to work nodes */ 
      // val sc = new SparkContext( "local", "Word Count", "/usr/local/spark", Nil, Map(), Map()) 

      /*creating an inputRDD to read text file (in.txt) through Spark context*/ 
      val input = sc.textFile("src/main/data/README.md") 

      /* Transform the inputRDD into countRDD */ 
      val valcount = input.flatMap(line ⇒ line.split(" ")) 
            .map(word ⇒ (word, 1)) 
      	    .reduceByKey(_ + _) 
       
      /* saveAsTextFile method is an action that effects on the RDD */  
      valcount.saveAsTextFile("outfile") 
      System.out.println("OK"); 
   } 
} 
