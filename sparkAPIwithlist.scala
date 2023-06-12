import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType



object sapi15 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
 val SparkConf = new SparkConf()
  
  SparkConf.set("spark.app.name","my 1st application")
  SparkConf.set("spark.master","local[2]")
  
  //creating sparkSession//
  val spark = SparkSession.builder()
  .config(SparkConf)
  .getOrCreate()
  
  val mylist = List((1,"2013-07-25",11599,"CLOSED"),
(2,"2013-07-25",256,"PENDING_PAYMENT"),
(3,"2013-07-25",12111,"COMPLETE"),
(4,"2013-07-25",8827,"CLOSED"),
(5,"2013-07-25",11318,"COMPLETE"),
(6,"2013-07-25",7130,"COMPLETE"),
(7,"2013-07-25",4530,"COMPLETE"))


import spark.implicits._

val ordersDF = spark.createDataFrame(mylist).toDF("order_id","order_date","customer_id","status")

val newDF = ordersDF
  .withColumn("order_date", unix_timestamp(col("order_date")
  .cast(DateType)))
  .withColumn("newId", monotonically_increasing_id)
  .dropDuplicates("order_date","customer_id")
  .drop("order_id")
  .sort("order_date")

newDF.printSchema()
  
  newDF.show()
  
  
  
  
  
  
  
  
  
  
  
  spark.stop()
}