import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object DataFramesExample extends App {
  
  
  // creating session
  // boiler plate code
  val spark = SparkSession.builder()
  .appName("My Application One")
  .master("local[2]")
  .getOrCreate()
  
    // spark conf object instead of configuring in other way
//  val sparkConf = new SparkConf()
//  sparkConf.set("spark.app.name", "my first application")
//  sparkConf.set("spark.master", "local[2]")
//  
//  val spark = SparkSession.builder()
//  .config(sparkConf).getOrCreate()
  
  // processing
  
  spark.stop()
  
}