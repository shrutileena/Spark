import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID

object session15 extends App {
 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "session15")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //1.
  //created list
  val myList = List(
      (1,"2013-07-25",11599,"CLOSED"),
      (2,"2014-07-25",256,"PENDING_PAYMENT"),
      (3,"2013-07-25",11599,"COMPLETED"),
      (4,"2019-07-25",8827,"CLOSED"))
      
  //2.
  //how to quickly create a dataframe from a list
  import spark.implicits._
//  
//  //first create RDD and then to DF
//  val df = spark.sparkContext.parallelize(myList).toDF()
  
  //OR directly convert to DF using createDataFrame
  val ordersDf = spark.createDataFrame(myList)
  .toDF("orderid","orderdate","customerid","status")
  
  //3.
  val df1 = ordersDf.withColumn("orderdate", unix_timestamp(col("orderdate").cast(DateType)))
  
  //4.
  val df2 = df1.withColumn("newid", monotonically_increasing_id)
  
  //5.
  val df3 = df2.dropDuplicates("orderdate", "customerid")
  
  //6.
  val df4 = df3.drop("orderid")
  
  //7.
  val df5 = df4.sort("orderdate")
  
  df5.printSchema()
  
  df5.show
  
  scala.io.StdIn.readLine()
}