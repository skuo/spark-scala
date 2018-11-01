import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}
import java.io.File

// Full example is at examples/src/main/scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala" in spark repo
object DataSources {
  def deleteDir(file: File) {
    if (file.isDirectory) 
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(deleteDir(_))
    file.delete
  }

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

    // -----------------
    // Load Data Sources.  
    // parquet to parquet
    val usersDF = spark.read.load("src/main/data/users.parquet")
    var dirName = "outfile/namesAndFavColors.parquet"
    var dir = new File(dirName)
    deleteDir(dir)
    usersDF.select("name", "favorite_color").write.save(dirName)
    // json to parquet
    val peopleDF = spark.read.format("json").load("src/main/data/people.json")
    // Use SaveMode instead of deleting dir explicitly
    peopleDF.select("name", "age").write.format("parquet").mode(SaveMode.Overwrite).save(dirName)
  }
}