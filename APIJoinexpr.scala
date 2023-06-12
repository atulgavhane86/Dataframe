import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



object sapi19 extends App{
  
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
  
  
  val customersDF = spark.read
  .format("csv")
  .option("header","true")
  .option("inferschema","true")
  .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/customers-201025-223502.csv")
  .load()
  
  //customersDF.show
  
  
  val joinCondition = orderDf.col("order_customer_id") === customersDF.col("customer_id")
  
  val joinType = "outer"
  
  val joinedDF = orderDf.join(customersDF,joinCondition,joinType)
  
  joinedDF.show()
  
  
  spark.stop()
}