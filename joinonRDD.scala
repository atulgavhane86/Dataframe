import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object practise10 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("C:/Users/atulg/Downloads/Spark_Datasets_w11/ratings-201019-002101.dat")

  val mappedinput = input.map(x=>{
    
    val fields = x.split("::")
    (fields(1),fields(2))
  })
  
  val newmappedinput = mappedinput.mapValues(x =>(x.toFloat,1.0))
  
  val reducedinput = newmappedinput.reduceByKey((x,y) =>( x._1 + y._1,x._2 + y._2))
  
  val filterinput = reducedinput.filter(x => x._2._2 > 10)
  
  val raitingprocessed = filterinput.mapValues(x=>x._1/x._2).filter(x=>x._2>4.0)
 
  
  val moviesrdd = sc.textFile("C:/Users/atulg/Downloads/Spark_Datasets_w11/movies-201019-002101.dat")
 
 
  val moviesmapped = moviesrdd.map(x=>{
   
   val fields = x.split("::")
   (fields(0),fields(1))
 })
 
 val joinedrdd = moviesmapped.join(raitingprocessed)
 
 val topmovies = joinedrdd.map(x=>x._2._1)
 
 
 topmovies.collect().foreach(println)
 
 ///raitingprocessed.collect().foreach(println)
}