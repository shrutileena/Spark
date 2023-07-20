import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import scala.collection.immutable.List
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType

object ExplicitSchema extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "StandardFormOfSparkDatasetAPI")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val ordersSchema = StructType(List(
  StructField("orderid", IntegerType),
  StructField("orderdate", TimestampType),
  StructField("customerid", IntegerType),
  StructField("status", StringType)
  ))
  
  val ordersSchemaDDL = "orderid int, orderdate String, custid int, orderstatus String"
  
  val ordersDf = spark.read
  .format("csv")
  .option("header", true)
  .schema(ordersSchema)
  .option("path", "E:/Big Data By Sumit Mittal/Week 11/Datasets/orders-201019-002101.csv")
  .load
  
  val ordersDf1 = spark.read
  .format("csv")
  .option("header", true)
  .schema(ordersSchemaDDL)
  .option("path", "E:/Big Data By Sumit Mittal/Week 11/Datasets/orders-201019-002101.csv")
  .load
  
  ordersDf.printSchema()
  ordersDf.show()
  
  ordersDf1.printSchema()
  ordersDf1.show()
  
  scala.io.StdIn.readLine()
  spark.stop()
  
}