import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

case class Person (name: String, age:Int, location:String)

object UDFInSpark extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def ageCheck(age: Int):String = {
    if(age>18) "Y" else "N"
  }
  
  val sparkConf = new SparkConf()
  
  sparkConf.set("spark.app.name", "dataframe and dataset")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val df = spark.read
  .format("csv")
  .option("inferSchema", true)
  .option("path", "E:/Big Data By Sumit Mittal/Week 12/Datasets/dataset1.csv")
  .load()
  
  import spark.implicits._
  
  //defining column names
  //creating Dataframe
  val dfWithCols: Dataset[Row] = df.toDF("name", "age", "location")
  
  //creating dataset using case class Person andd dataframe
  val dsWithCols = dfWithCols.as[Person]
  
  //dataset to dataframe then dataframe to dataset then to dataframe
  val df2 = dsWithCols.toDF().as[Person].toDF()
  
  //register function ageCheck() - column object expression udf
  val parseAgeFunction = udf(ageCheck(_:Int):String)
  
  //column object udf
  val finalDf = dfWithCols.withColumn("adult", parseAgeFunction(col("age")))
  
  //register function in sql/string expression udf
  spark.udf.register("parseAgeStringFunction", ageCheck(_:Int):String)
  //spark.udf.register("parseAgeStringFunction", (x:Int) => {if (x>18) "Y" else "N"})
  
  //sql/string udf
  val finalStringDf = dfWithCols.withColumn("adult", expr("parseAgeStringFunction(age)"))
  
  spark.catalog.listFunctions().filter(x => x.name == "parseAgeFunction").show()
  spark.catalog.listFunctions().filter(x => x.name == "parseAgeStringFunction").show()
  
//  dfWithCols.printSchema()
//  dsWithCols.printSchema()

//  dfWithCols.show()
//  dsWithCols.show()
  finalDf.show()
  finalStringDf.show()
  
  scala.io.StdIn.readLine()
  spark.stop()
  
}