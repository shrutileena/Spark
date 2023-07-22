import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SaveMode

object DBCreation extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  
  sparkConf.set("spark.app.name", "dataframe and dataset")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  val ordersDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", "E:/Big Data By Sumit Mittal/Week 12/Datasets/orders-201025-223502.csv")
  .load()
  
  // creating new db
  spark.sql("create database if not exists retail")
  
  // writing result in a table
  ordersDf.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .saveAsTable("retail.orders")
  
  // listing tables in db
  spark.catalog.listTables("retail").show()
  
  scala.io.StdIn.readLine()
  spark.stop()
  
}