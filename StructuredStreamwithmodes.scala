import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object structstream1 extends App{
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val Spark= SparkSession.builder()
             .master("local[2]")
             .appName("my appliaction")
             .config("spark.sql.shuffle.partitions",3)
             .config("spark.streaming.stopGracefullyOnShutdown",true)
             .config("spark.sql.streaming.schemaInference","true")
             .getOrCreate()
  
  //1.reading
  val ordersDF = Spark.readStream
  .format("json")
  .option("path","myInputFolder")
  .load()
  
  ordersDF.createOrReplaceTempView("orders")
  val statusDF = Spark.sql("select * from orders where order_status= 'COMPLETE'")
  
  
  //3 writing
  
  val wordcount = statusDF.writeStream
  .format("json")
  .outputMode("append")
  .option("path","myoutputfolder")
  .option("checkpointLocation","checkoint-location10")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .start()
  
  
  wordcount.awaitTermination()
  
}