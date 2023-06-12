import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions._

object structstream3 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val Spark= SparkSession.builder()
             .master("local[2]")
             .appName("my appliaction")
             .config("spark.sql.shuffle.partitions",3)
             .config("spark.streaming.stopGracefullyOnShutdown","true")
             .getOrCreate()
             
  val orderSchema = StructType(List(
      StructField("order_id",IntegerType),
      StructField("order_date",TimestampType),
      StructField("order_customer_id",IntegerType),
      StructField("order_status",StringType),
      StructField("amount",IntegerType)
  ))
  
             
             
val ordersDF = Spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port","9998")
  .load()
 

val valueDF = ordersDF.select(from_json(col("value"),orderSchema).alias("value"))


  val refinedDF = valueDF.select("value.*")



val windowAggDF = refinedDF
                 .withWatermark("order_date","30 minute")
                 .groupBy(window(col("order_date"),"15 minute"))
                 .agg(sum("amount")
                 .alias("totalInvoice"))
                 
                 
                 
                 
val outputDF = windowAggDF.select("window.start","window.end","totalInvoice")
   
   
   
   val wordcount = outputDF.writeStream
                  .format("console")
                  .outputMode("update")
                  .option("checkpointLocation","checkoint-location2")
                  .trigger(Trigger.ProcessingTime("15 seconds"))
                  .start()


wordcount.awaitTermination()




}