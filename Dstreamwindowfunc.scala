import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object sparkStreaming3 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  
  val ssc = new StreamingContext(sc,Seconds(2))
  
  val lines = ssc.socketTextStream("localhost",9997)
  
  ssc.checkpoint(".")
  
  val words = lines.flatMap(x=>x.split(" "))
  
  val pairs = words.map(x=>(x,1))
  
  val wordsCount = pairs.reduceByKeyAndWindow((x,y)=>x+y,(x,y)=>x-y,Seconds(10),Seconds(2)).filter(x=>x._2>0)
  
  wordsCount.print()
  
  ssc.start()
  
  ssc.awaitTermination()
  
}