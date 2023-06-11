import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object assq3w12 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my 1st appliaction")
  sparkConf.set("spark.master","local[2]")
  
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  case class batsman(MatchNumber:Int,Batsman:String,Team:String,RunsScored:Int,StrikeRate:Double)
  
  val batsmenRdd = spark.sparkContext.textFile("C:/Users/atulg/Downloads/Spark_datasets_w12/fileA.txt")
  
  
  val batsmanschemaRDD = batsmenRdd.map(line => line.split("\t")).map(fields => batsman(fields(0).toInt,fields(1),fields(2),fields(3).toInt,fields(4).toDouble))
  
  import spark.implicits._
  
  val batsmenDF = batsmanschemaRDD.toDF()
  
  
  batsmenDF.printSchema()
  batsmenDF.show()
  
  
  
  spark.stop()
  
}