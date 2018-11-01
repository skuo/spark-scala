package tutorials

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object SparkDataFrame {
  def main(args: Array[String]) { 
    // --------------------
    // spark session
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Set the Logging level to WARN
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // ---------------------
    // DataFrame
    val df = spark.read.json("src/main/data/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    // Print the schema in a tree format
    df.printSchema()
    // Select only the "name" column
    df.select("name").show()
    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()
    // Select people older than 21
    df.filter($"age" > 21).show()
    // Count people by age
    df.groupBy("age").count().show()

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    // Register the DataFrame as a global temporary view
    // df.createGlobalTempView("people")  // not working
    // Global temporary view is tied to a system preserved database 'global_temp'
    // spark.sql("SELECT * from global_temp.people").show()
  }
}
