import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Practise7 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("C:/Users/atulg/Downloads/Spark_datasets_w10/bigdatacampaigndata-201014-183159.csv")
  
  val mappedInput = input.map(x=>(x.split(",")(10).toFloat,x.split(",")(0)))
  
  val words = mappedInput.flatMapValues(x => x.split(" "))

  
  val finalmapped = words.map(x =>(x._2.toLowerCase(),x._1))
 
  val total = finalmapped.reduceByKey((x,y)=>x+y)
 
  val sorted = total.sortBy(x => x._2,false)
 
  sorted.take(20).foreach(println)

 
}