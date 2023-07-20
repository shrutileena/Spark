import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp

case class OrdersData (order_id: Int, 
    order_date: Timestamp, 
    order_customer_id: Int, 
    order_status: String)

object DataFrameAndDataSet extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  
  sparkConf.set("spark.app.name", "dataframe and dataset")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  // DataFrame
  val orderDf: Dataset[Row] = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("E:/Big Data By Sumit Mittal/Week 11/Datasets/orders-201019-002101.csv")
  
  // order_ids < 10 give error at run time instead of at compile time
  orderDf.filter("order_ids < 10").show()
  
  import spark.implicits._
  
  // DataSet
  val orderDs = orderDf.as[OrdersData]
  
  // this will give compile time error
  //orderDs.filter(x => x.order_ids < 10).show()
  
  // correct
  orderDs.filter(x => x.order_id < 10).show()
  
  // condition expression doesn't give error at compile time
  // So, use condition instead with datasets
  orderDs.filter("order_ids < 10").show()
  
  scala.io.StdIn.readLine()
  spark.stop()
  
}