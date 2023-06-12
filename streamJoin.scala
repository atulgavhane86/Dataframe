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



object streamJoin extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val Spark= SparkSession.builder()
             .master("local[2]")
             .appName("my appliaction")
             .config("spark.sql.shuffle.partitions",3)
             .config("spark.streaming.stopGracefullyOnShutdown","true")
             .getOrCreate()
             
             
 val transactionSchema = StructType(List(
      StructField("card_id",LongType),
      StructField("amount",TimestampType),
      StructField("postcode",IntegerType),
      StructField("pos_id",LongType),
      StructField("transaction_dt",TimestampType)
  ))
  
  
  
  val transDF = Spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port","9998")
  .load()
  
  //transDF.printSchema()
  
  val valueDF = transDF.select(from_json(col("value"),transactionSchema).alias("value"))
  
  //valueDF.printSchema()
  
  val refinedDF = valueDF.select("value.*")
  
  
  //refinedDF.printSchema()
  
  
  //load static DF 
  
  val memberDF = Spark.read
  .format("csv")
  .option("header",true)
  .option("inferSchema",true)
  .option("path","C:/Users/atulg/Downloads/Week16/Member_details.csv")
  .load()
  
  val joinExpr = refinedDF.col("card_id")===memberDF.col("card_id")
  val joinType="inner"
  
  val enrichDF = refinedDF.join(memberDF,joinExpr,joinType).drop(memberDF.col("card_id"))
  
 
  
val outputDf = enrichDF.writeStream
                  .format("console")
                  .outputMode("update")
                  .option("checkpointLocation","checkoint-location123")
                  .trigger(Trigger.ProcessingTime("15 seconds"))
                  .start()


outputDf.awaitTermination()
  
  
}


