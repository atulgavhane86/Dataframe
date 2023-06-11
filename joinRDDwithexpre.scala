import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object assqw12 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my 1st appliaction")
  sparkConf.set("spark.master","local[2]")
  
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
 val  employeeDF = spark.read
 .format("json")
 .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/employee.json")
 .load()
 
 //employeeDF.show()
 
 val  deptDF = spark.read
 .format("json")
 .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/dept.json")
 .load()
 
 //deptDF.show()
 
 val joinCondition = employeeDF.col("deptid") === deptDF.col("deptid")
 
 val joinType = "left"
 
 val joinedDF = employeeDF.join(deptDF,joinCondition,joinType)
 
 val joined1DF = joinedDF.drop(deptDF.col("deptid"))
 
 joined1DF.groupBy("deptid").agg(count("empname").as("empcount"),first("deptName").as("deptName")).dropDuplicates("deptName").show()
 
 
 //spark.sql("select deptid,count(id) as employeecount from info group by deptid").show()
 
}