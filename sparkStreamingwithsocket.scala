import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext



object sparkStreaming extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  
  val ssc = new StreamingContext(sc,Seconds(5))
  
  val lines = ssc.socketTextStream("localhost",9998)
  
  val words = lines.flatMap(x=>x.split(" "))
  
  val pairs = words.map(x=>(x,1))
  
  val wordsCount = pairs.reduceByKey((x,y)=>x+y)
  
  wordsCount.print()
  
  ssc.start()
  
  ssc.awaitTermination()
}