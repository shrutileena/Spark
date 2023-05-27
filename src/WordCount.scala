import org.apache.spark.SparkContext

object WordCount extends App {
  
  val sc = new SparkContext("local[*]", "wordcount")

}