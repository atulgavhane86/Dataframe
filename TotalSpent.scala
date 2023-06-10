import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object TotalSpent extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("C:/Users/atulg/Downloads/Spark_datasets/customerorders-201008-180523.csv")
  
  val mappedInput = input.map(x=> (x.split(",")(0),x.split(",")(2).toFloat))
  
  val totalByCustomer = mappedInput.reduceByKey((x,y)=> x+y)
  
  val sortedTotal = totalByCustomer.sortBy(x=>x._2)
  
  val result = sortedTotal.collect()
  
  result.foreach(println)
  
  
}