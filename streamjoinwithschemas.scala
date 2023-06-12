import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object streamjoin2 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val Spark= SparkSession.builder()
             .master("local[2]")
             .appName("my appliaction")
             .config("spark.sql.shuffle.partitions",3)
             .config("spark.streaming.stopGracefullyOnShutdown","true")
             .getOrCreate()
             
             
 val impressionSchema = StructType(List(
      StructField("impressionID",StringType),
      StructField("ImpressionTime",TimestampType),
      StructField("CampaignName",StringType)
  ))
  
  
  val clickSchema = StructType(List(
      StructField("clickID",StringType),
      StructField("ClickTime",TimestampType)
  ))
  
  val impressionDf = Spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port","9998")
  .load()
  
  val clickDF = Spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port","9997")
  .load()
  
  
  val valueDF1 = impressionDf.select(from_json(col("value"),impressionSchema).alias("value"))
  
 val refinedDF1= valueDF1.select("value.*").withWatermark("ImpressionTime","30 minute")
 
 val valueDF2 = clickDF.select(from_json(col("value"),clickSchema).alias("value"))
  
 val refinedDF2= valueDF2.select("value.*").withWatermark("ClickTime","30 minute")
 
 
 val joinExpr = refinedDF1.col("impressionID")===refinedDF2.col("clickID")
  val joinType="inner"
  
  val enrichDF = refinedDF1.join(refinedDF2,joinExpr,joinType).drop(refinedDF2.col("clickID"))

  
  
  val outputDf = enrichDF.writeStream
                  .format("console")
                  .outputMode("append")
                  .option("checkpointLocation","checkoint-location223")
                  .trigger(Trigger.ProcessingTime("15 seconds"))
                  .start()


outputDf.awaitTermination()
  
}