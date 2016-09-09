import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

/* ERROR metastore.RetryingHMSHandler: AlreadyExistsException(message:Database default already exists)
 * does not mean anything bad happened.  It checked to see if the default existed.  These error
 * logs should not be displayed if the default database exist.
*/
object EmployeeHive { 
  def main(args: Array[String]) { 

    println("Start of EmployeeHive")
    val conf = new SparkConf().setMaster("local").setAppName("EmployeeHive")
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)
    import hiveCtx.implicits._

    // Set the Logging level to WARN
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)

    val employeeDF = hiveCtx.jsonFile("/Users/skuo/data/spark/employee.json")
    employeeDF.registerTempTable("employee")

    // operations on DataFrame    
    employeeDF.show()
    employeeDF.printSchema()

    // Hive queries
    val olderEmployees = hiveCtx.sql("SELECT age, id, name FROM employee WHERE age > 23")
    olderEmployees.show()

    val employeesCountByAge = hiveCtx.sql("SELECT age, count(*) FROM employee group by age")
    employeesCountByAge.show()
  } 
} 
