import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object WordCount extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "wordcount")
  
  val input = sc.textFile("E://Big Data By Sumit Mittal//Week 9//Datasets//search_data-201008-180523.txt")
  
//  val words = input.flatMap(x => x.split(" "))
  val words = input.flatMap(_.split(" "))
  
//  val wordsLowercase = words.map(x => x.toLowerCase())
  val wordsLowercase = words.map(_.toLowerCase())
  
//  val wordCount = wordsLowercase.map(x => (x,1))
  val wordCount = wordsLowercase.map((_,1))
  
//  val finalCount = wordCount.reduceByKey((x,y) => x+y)
  val finalCount = wordCount.reduceByKey(_+_)
  
  // value first, key second - swapping
  val reversedTuple = finalCount.map(x => (x._2,x._1))
  
  // sorting in descending order
  val SortedResults = reversedTuple.sortByKey(false)
  
  //swapping key and value again
  val finalSortedResults = SortedResults.map(x => (x._2,x._1))
  
  //val finalSortedResults = finalCount.sortBy(x => x._2) instead of
  //val reversedTuple = finalCount.map(x => (x._2,x._1))
  //val SortedResults = reversedTuple.sortByKey(false).map(x => (x._2,x._1))
  
//  finalCount.collect.foreach(println)
//  finalSortedResults.collect().foreach(println)
  val results = finalSortedResults.collect()
  
  for(result <- results){
    val word = result._1
    val count = result._2
    println(s"$word: $count")
  }
  
  scala.io.StdIn.readLine()
  
}