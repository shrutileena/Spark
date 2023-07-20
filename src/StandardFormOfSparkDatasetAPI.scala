import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

object StandardFormOfSparkDatasetAPI extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "StandardFormOfSparkDatasetAPI")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //csv file format
  val ordersDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", "E:/Big Data By Sumit Mittal/Week 11/Datasets/orders-201019-002101.csv")
  .load
  
  //json file format
  val playersDf = spark.read
  .format("json")
  .option("path", "E:/Big Data By Sumit Mittal/Week 11/Datasets/players-201019-002101.json")
//  .option("mode", "PERMISSIVE")
//  .option("mode", "DROPMALFORMED")
//  .option("mode", "FAILFAST")
  .load
  
  val usersDf = spark.read
//  .format("parquet")
  .option("path", "E:/Big Data By Sumit Mittal/Week 11/Datasets/users-201019-002101.parquet")
  .load
  
  ordersDf.printSchema()
  ordersDf.show()
  
  playersDf.printSchema()
  playersDf.show(false)
  
  usersDf.printSchema()
  usersDf.show(false)
  
  scala.io.StdIn.readLine()
  spark.stop()
  
}