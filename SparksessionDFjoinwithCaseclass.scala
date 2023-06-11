import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object assq2w12 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my 1st appliaction")
  sparkConf.set("spark.master","local[2]")
  
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  case class ratings(userId:Int,movieID:Int,rating:Int,timestamp:String)
  
  val ratingdRDD = spark.sparkContext.textFile("C:/Users/atulg/Downloads/Spark_datasets_w11/ratings-201019-002101.dat")
  
  val caseClassScemaRDD = ratingdRDD.map(x=> x.split("::")).map(x=>ratings(x(0).toInt,x(1).toInt,x(2).toInt,x(3)))
  
  import spark.implicits._
  
 val ratingsDF = caseClassScemaRDD.toDF()
 
 //ratingsDF.show()
 
 
 val movieRDD = spark.sparkContext.textFile("C:/Users/atulg/Downloads/Spark_datasets_w11/movies-201019-002101.dat")

  case class movies(movieID:Int,moviename:String,genre:String)
  
val movieClassScemaRDD = movieRDD.map(x=> x.split("::")).map(x=>movies(x(0).toInt,x(1),x(2)))



val movienewDF = movieClassScemaRDD.toDF().select("movieID","moviename")


//movienewDF.printSchema()
//movienewDF.show()

val transformedDF = ratingsDF.groupBy("movieID").agg(count("rating").as("movieviewcount"),avg("rating").as("avgMovieRating")).orderBy(desc("movieviewcount"))
//transformedDF.show()

val popularMovieDF = transformedDF.filter("movieviewcount>1000 AND avgMovieRating>4.5")

//popularMovieDF.show()

spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

 val joinCondition = popularMovieDF.col("movieID") === movienewDF.col("movieID")
 
 val joinType = "inner"
 
  val joinedDF = popularMovieDF.join(broadcast(movienewDF),joinCondition,joinType).drop(popularMovieDF.col("movieID")).sort(desc("avgMovieRating"))
  
  joinedDF.drop("movieviewcount","avgMovieRating","movieID").show(false)
  
spark.stop()
}