import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SaveMode

object TransformationsInSpark extends App {
 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  case class Orders(order_id: Int, customer_id: Int, order_status: String)
  
  val myregex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r
  
  def parser(line: String) = {
    line match {
      case myregex(order_id, date, customer_id, order_status) => 
        Orders(order_id.toInt, customer_id.toInt, order_status)
    }
  }
  
  val sparkConf = new SparkConf()
  
  sparkConf.set("spark.app.name", "dataframe and dataset")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  // loading file using sc
  val lines = spark.sparkContext.textFile("E:/Big Data By Sumit Mittal/Week 11/Datasets/orders_new-201019-002101.csv")
  
  import spark.implicits._
  
  // creating rdd of Orders type
  // val rdd = lines.map(parser)
  
  // Creating Dataset using Orders type case class
  // Lower level transformations
  val ordersDS = lines.map(parser).toDS().cache()
  
  // Higher level transformations
  ordersDS.printSchema()
  ordersDS.select("order_id").show()
  ordersDS.groupBy("order_status").count().show()
  ordersDS.filter(x => x.customer_id > 1000).show()
  
  scala.io.StdIn.readLine()
  spark.stop()
}