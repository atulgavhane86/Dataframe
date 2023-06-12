import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Assw10 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("C:/Users/atulg/Downloads/Spark_Assque/chapters-201108-004545.csv")

  val rdd1 = input.map(x=>(x.split(",")(1),1))
  val rdd2 = rdd1.reduceByKey((x,y)=>x+y)
  
  val rdd3 = rdd2.sortBy(x => x._1)
  
  rdd3.collect().foreach(println)
}