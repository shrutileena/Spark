import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

object OrdersDataFrame extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf= new SparkConf()
  sparkConf.set("spark.app.name","orders app")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  // loading csv file
  val ordersDF = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("E:/Big Data By Sumit Mittal/Week 11/Datasets/orders-201019-002101.csv")
  
  // conditioning
  val groupedOrdersDF = ordersDF
  .repartition(4)
  .where("order_customer_id > 10000")
  .select("order_id", "order_customer_id")
  .groupBy("order_customer_id")
  .count()
  
  // to print - 20 lines by default
  groupedOrdersDF.show()
  
  // print schema
  ordersDF.printSchema()
  
  Logger.getLogger(getClass.getName).info("my application is completed successfully")
  
  scala.io.StdIn.readLine()
  
  spark.stop()
}