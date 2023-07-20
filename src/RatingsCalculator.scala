import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object RatingsCalculator extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","ratingsCalculator")
  
  val ratingsRdd = sc.textFile("E://Big Data By Sumit Mittal//Week 11//Datasets//ratings-201019-002101.dat")
  
  val mappedRdd = ratingsRdd.map(x => {
    val fields = x.split("::")
    (fields(1), fields(2))
  })
  
  //input
  //(1193, 5)
  //(1193, 3)
  //(1193, 4)
  
  //output
  //(1193, (5.0,1.0))
  //(1193, (3.0,1.0))
  //(1193, (4.0,1.0))
  
  val newMappedRdd = mappedRdd.mapValues(x => (x.toFloat,1.0))
  
  //output
  //(1193, (12.0, 3.0))
  val reducedRdd = newMappedRdd.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
  
  //input
  //(1193, (12.0, 3.0))
  
  val filteredRdd = reducedRdd.filter(x => x._2._2 >= 1000)
  
  //input
  //(1193, (12000.0, 3000.0))
  
  //output
  //(1192, 4)
  val ratingsProcessed = filteredRdd.mapValues(x => x._1/x._2).filter(x => x._2 > 4.5)
  
  
  //we want movie names and not IDs
  val moviesRdd = sc.textFile("E:/Big Data By Sumit Mittal/Week 11/Datasets/movies-201019-002101.dat")
  
  val moviesMappedRdd = moviesRdd.map(x => {
    val fields = x.split("::")
    (fields(0), fields(1))
  })
  
  val joinedRdd = moviesMappedRdd.join(ratingsProcessed)
  
  val topMovies = joinedRdd.map(x => (x._2._1, x._2._2))
  
  topMovies.collect.foreach(println)
  
  scala.io.StdIn.readLine()
  
}