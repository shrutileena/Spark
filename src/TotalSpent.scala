import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TotalSpent extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "totalSpent")
  
  val input = sc.textFile("E://Big Data By Sumit Mittal//Week 9//Datasets//customerorders-201008-180523.csv")
  
  val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))
  
  val totalByCust = mappedInput.reduceByKey((x,y) => x+y)
  
  val sortedTotal = totalByCust.sortBy(x => x._2,false)
  
  val results = sortedTotal.collect()
  
  results.foreach(println)
  
//  for(result <- results){
//    val id = result._1
//    val count = result._2
//    println(s"id: $count")
//  }
  
  scala.io.StdIn.readLine()
  
}