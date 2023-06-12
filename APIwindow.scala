import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum





object sapi18 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  
  sparkConf.set("spark.app.name","My 1st application")
  sparkConf.set("spark.master","local[2]")
 
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate
  
  
  val orderDf = spark.read
  .format("csv")
  .option("header","true")
  .option("inferschema","true")
  .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/windowdata-201025-223502.csv")
  .load()
  
  
  
  
  val mywindow = Window.partitionBy("Country")
                        .orderBy("weeknum")
                        .rowsBetween(Window.unboundedPreceding,Window.currentRow)
                        
  
  val myDf = orderDf.withColumn("RunningTotal", sum("invoicevalue").over(mywindow))
  
  
  
  
  myDf.show()
  
  spark.stop()
}