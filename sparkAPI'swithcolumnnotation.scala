import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.expr

object sapi17 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spak.app.name","My 1st application")
  sparkConf.set("spark.master","local[2]")
  
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val orederDF = spark.read
  .format("csv")
  .option("header",true)
  .option("inferschema",true)
  .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/order_data-201025-223502.csv")
  .load()
  
  
  //column object 
  val summaryDf = orederDF.groupBy("Country","InvoiceNo")
                          .agg(sum("Quantity").as ("TOtalQuantity"),
                              sum(expr("Quantity*UnitPrice")).as("InvoicValue")
                           )
  
  
  summaryDf.show()
  
  //column string
  
  val summaryDF1 = orederDF.groupBy("Country","InvoiceNo")
                           .agg(expr("sum(Quantity) as TOtalQuantity"),
                                expr("sum(Quantity * UnitPrice) as InvoicValue")
      )
  
  summaryDF1.show()
  
  
  //spark sql 
  orederDF.createTempView("sales")
  
  spark.sql("""select Country,InvoiceNo,
    sum(Quantity) as TOtalQuantity,
    sum(Quantity * UnitPrice) as InvoicValue 
    from sales group by Country,InvoiceNo """).show()
  
  spark.stop()
 
}