import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.math.min

object minTemp extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "avgConnections")
  
  val input = sc.textFile("E:/Big Data By Sumit Mittal/Week 9/Datasets/tempdata-201125-161348.csv")
  
  val tempData = input.map(x => (x.split(",")(0), x.split(",")(2), x.split(",")(3)))
  
  val minData = tempData.filter(x => x._2 == "TMIN")
  
  val data = minData.map(x => (x._1, x._3.toFloat))
  
  val minByStations = data.reduceByKey((x, y) => min(x, y))
  
  val results = minByStations.collect()
  
  for(result <- results){
    val station = result._1
    val temp = result._2
    val formatedTemp = f"$temp%.2f F"
    println(s"$station minimum temperature: $formatedTemp")
  }
  
}