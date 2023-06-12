import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object sparkStreaming4 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  
  val ssc = new StreamingContext(sc,Seconds(2))
  
  val lines = ssc.socketTextStream("localhost",9997)
  
  ssc.checkpoint(".")
  
  def sumFunc(x:String,y:String)={
    (x.toInt + y.toInt).toString
  }
  def inverseFunc(x:String,y:String)={
    (x.toInt - y.toInt).toString
  }
  
  //val wordCount = lines.reduceByWindow(sumFunc,inverseFunc,Seconds(10),Seconds(2))
  val wordCount = lines.countByWindow(Seconds(10),Seconds(2))
  
  
  wordCount.print()
  
  ssc.start()
  
  ssc.awaitTermination()
  
  
}