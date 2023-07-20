import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TotalSpent16 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "totalSpent")
  
  val input = sc.textFile("E://Big Data By Sumit Mittal//Week 9//Datasets//customerorders-201008-180523.csv")
  
  val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))
  
  val totalByCust = mappedInput.reduceByKey((x,y) => x+y)
  
  val premiumCustomers = totalByCust.filter(x => x._2 > 5000)
  
  val doubleAmount = premiumCustomers.map(x => (x._1, x._2 * 2)).cache()
  
  val results = doubleAmount.collect()
  
  results.foreach(println)
  
  println(doubleAmount.count)
  
  scala.io.StdIn.readLine()
}