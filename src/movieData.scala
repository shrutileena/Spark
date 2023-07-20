import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object movieData extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "movieData")
  
  val input = sc.textFile("E://Big Data By Sumit Mittal//Week 9//Datasets//moviedata-201008-180523.data")
  
  val splittedInput = input.map(x => (x.split("\t")(2), 1))
//  val splittedInput = input.map(x => x.split("\t")(2))
  
  val ratingTotalCount = splittedInput.reduceByKey((x,y) => x+y).sortBy(x=>x._1, false)
//  val ratingTotalCount = splittedInput.countByValue()
  ratingTotalCount.collect.foreach(println)
  
  scala.io.StdIn.readLine()
  
}