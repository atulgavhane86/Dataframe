import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum




object sapi16 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  
  sparkConf.set("spark.app.master","my 1st application")
  sparkConf.set("spark.master","local[2]")
  
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val orderDF = spark.read
  .format("csv")
  .option("header",true)
  .option("inferschema",true)
  .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12")
  .load()
  
  
  orderDF.select(
      count("*").as("row_count"),
      sum("Quantity").as("TotalQuantity"),
      avg("UnitPrice").as("AvgPrice"),
      countDistinct("InvoiceNo").as("CountDistinct")
  ).show()
  
  
  orderDF.selectExpr(
      "count(*) as rowcount",
      "sum(Quantity) as TotalQuantity",
      "avg(UnitPrice) as AvgPrice",
      "count(Distinct(InvoiceNo)) as CountDistinct" 
  ).show()
  
  orderDF.createTempView("sales")
  
  spark.sql("select count(*),sum(Quantity),avg(UnitPrice),count(distinct(InvoiceNo)) from sales").show()
  
  
  spark.stop()
}