import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object ageAboveEighteen extends App {
  
  def yesOrNo(x:Int) : String ={
    
    if(x>18)
      return "Y"
    else
      return "N"
  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "avgConnections")
  
  val input = sc.textFile("E:/Big Data By Sumit Mittal/Week 9/Datasets/-201125-161348.dataset1")
  
  val finalData = input.map(x => (x,yesOrNo(x.split(",")(1).toInt)))
  
  finalData.collect().foreach(println);
  
  
}