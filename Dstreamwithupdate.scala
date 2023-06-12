import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object sparkStreaming2 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  
  val ssc = new StreamingContext(sc,Seconds(5))
  
  val lines = ssc.socketTextStream("localhost",9998)
  ssc.checkpoint(".")
  
  def updateFunc(newValues:Seq[Int],previousState:Option[Int]):Option[Int]={
    val newCount = previousState.getOrElse(0)+newValues.sum
    Some(newCount)
  }
  
  val words = lines.flatMap(x=>x.split(" "))
  
  val pairs = words.map(x=>(x,1))
  
  val wordsCount = pairs.updateStateByKey(updateFunc)
  
  wordsCount.print()
  
  ssc.start()
  
  ssc.awaitTermination()
}