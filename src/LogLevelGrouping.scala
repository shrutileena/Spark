import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object LogLevelGrouping extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "logLevelGrouping")
  
  val baseRdd = sc.textFile("E:/Big Data By Sumit Mittal/Week 10/Datasets/bigLogtxt-201014-183159/bigLog.txt")
  
  baseRdd.map(x => {
    (x.split(":")(0), x.split(":")(1))
  }).groupByKey.map(x => (x._1, x._2.size)).collect().foreach(println)
  
}