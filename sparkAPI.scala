import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType



object assqw11 extends App{
  
  val sparkconf = new SparkConf()
  sparkconf.set("spark.master","local[*]")
  
  val spark = SparkSession.builder()
  .config(sparkconf)
  .getOrCreate()
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val windowdataSchema = StructType(List(
      StructField("Country",StringType),
      StructField("weeknum",IntegerType),
      StructField("numinvoices",IntegerType),
      StructField("totalquantity",IntegerType),
      StructField("invoicevalue",DoubleType)
      
      ))
      
      val windowdataDF = spark.read
      .format("csv")
      .schema(windowdataSchema)
      .option("path","C:/Users/atulg/Downloads/Spark_datasets_w11/windowdata-201021-002706.csv")
      .load()
      
      
      windowdataDF.write
      .partitionBy("Country")
      .mode(SaveMode.Overwrite)
      .option("path","C:/Users/atulg/Downloads/Spark_datasets_w11/windows_dataoutput")
      .save()

//windowdataDF.show()

}