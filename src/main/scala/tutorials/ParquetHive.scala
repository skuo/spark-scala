package tutorials

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import java.io.File

/* ERROR metastore.RetryingHMSHandler: AlreadyExistsException(message:Database default already exists)
 * does not mean anything bad happened.  It checked to see if the default existed.  These error
 * logs should not be displayed if the default database exist.
*/
object ParquetHive { 
  // case class Person
  case class Person(firstName: String, lastName: String, gender: String)

  def deleteDir(file: File) {
    if (file.isDirectory) 
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(deleteDir(_))
    file.delete
  }

  def main(args: Array[String]) { 

    println("Start of ParquetHive")
    val conf = new SparkConf().setMaster("local").setAppName("ParquetHive")
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)
    import hiveCtx.implicits._

    // Set the Logging level to WARN
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)

    /* ====================================
     * Method 1: use case class and ScalaReflection
    val persons = sc.textFile("/Users/skuo/data/spark/person.csv").map(_.split(",")).map(p => Person(p(0),p(1),p(2)))
    //persons.collect().foreach(println)

    // Turn RDD to RowRDD
    val personsRowRDD = persons.map(p => Row(p.firstName,p.lastName,p.gender))

    val schema = ScalaReflection.schemaFor[Person].dataType.asInstanceOf[StructType]
       =================================== */

    /* =================================== 
     * Method 2:
       =================================== */
    val schemaString = "firstName,lastName,gender"
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val personsRowRDD = sc.textFile("/Users/skuo/data/spark/person.csv").map(_.split(",")).map(e => Row(e(0),e(1),e(2)))

    val personsDF = hiveCtx.createDataFrame(personsRowRDD,schema)
    personsDF.registerTempTable("person")
    val males = hiveCtx.sql("SELECT * FROM person where gender='M'")
    println("----- males -----")
    males.collect().foreach(println)
    // save to Parquet format
    val dir = new File("/Users/skuo/data/spark/person.parquet")
    deleteDir(dir)
    personsDF.write.parquet("/Users/skuo/data/spark/person.parquet")

    // Read in Parquest file
    val readPersonsDF = hiveCtx.read.parquet("/Users/skuo/data/spark/person.snappy.parquet")
    println("----- print readPersonsDF -----")
    readPersonsDF.collect().foreach(println)
  } 
} 
