import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object AvgConnections extends App {
  
  def parseLine(line: String)={
    val fields = line.split("::")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age,numFriends)  // last statement is always a return statement
  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "avgConnections")
  
  val input = sc.textFile("E:/Big Data By Sumit Mittal/Week 9/Datasets/friendsdata-201008-180523.csv")
  
  val mappedInput = input.map(parseLine)
  
//  val mappedFinal = mappedInput.map(x => (x._1,(x._2,1)))
  
  val mappedFinal = mappedInput.mapValues(x => (x,1)) // mapValues treat the value part only; key is not considered
  
  val totalByAge = mappedFinal.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
  
//  val avgByAge = totalByAge.map(x => (x._1, x._2._1/x._2._2)).sortBy(x=>x._2)
  val avgByAge = totalByAge.mapValues(x => (x._1/x._2)).sortBy(x=>x._2)
  
  avgByAge.collect().foreach(println)
  
  scala.io.StdIn.readLine()
}