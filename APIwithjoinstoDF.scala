import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sapi20 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my 1st application")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  
 val orderDf = spark.read
  .format("csv")
  .option("header","true")
  .option("inferschema","true")
  .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/orders-201025-223502.csv")
  .load()
  
  //orderDf.show
  
  val neworderDF = orderDf.withColumnRenamed("customer_id", "cust_id")
  
  val customersDF = spark.read
  .format("csv")
  .option("header","true")
  .option("inferschema","true")
  .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/customers-201025-223502.csv")
  .load()
  
  //customersDF.show
  
  
  val joinCondition = neworderDF.col("cust_id") === customersDF.col("customer_id")
  
  val joinType = "outer"
  
  val joinedDF = neworderDF.join(customersDF,joinCondition,joinType)
  
  //for null use coalesce
  //val joinedDF = neworderDF.join(customersDF,joinCondition,joinType)
  .withColumn("order_id",expr("coalesce(order_id,-1)"))
  
  joinedDF.show()
  
  
  spark.stop()

}