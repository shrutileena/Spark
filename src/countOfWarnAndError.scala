import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object countOfWarnAndError extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "avgConnections")
  
  val myList = List("WARN: Tuesday 4 September 0405", 
      "ERROR: Tuesday 4 September 0408", 
      "ERROR: Tuesday 4 September 0408", 
      "ERROR: Tuesday 4 September 0408", 
      "ERROR: Tuesday 4 September 0408", 
      "ERROR: Tuesday 4 September 0408")
  
  val input = sc.parallelize(myList)
  
  val splittedData = input.map(x => (x.split(":")(0), 1))
  
  val results = splittedData.reduceByKey((x, y) => x + y)
  
  results.collect().foreach(println)
  
//  sc.parallelize(myList).map(x => (x.split(":")(0), 1)).reduceByKey(_ + _).collect().foreach(println)
}