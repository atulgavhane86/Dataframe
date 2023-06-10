import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object AssW10q2 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("C:/Users/atulg/Downloads/Spark_Assque/views*.csv")
  
  val chapterRdd = sc.textFile("C:/Users/atulg/Downloads/Spark_Assque/chapters-201108-004545.csv").map(x=>(x.split(",")(0),x.split(",")(1)))
  
  val rdd1 = input.map(x=>(x.split(",")(0),x.split(",")(1)))
  
  val rdd2 = rdd1.distinct()
  val flippedData = rdd2.map(x=>(x._2,x._1))
  
  val joinedRdd = flippedData.join(chapterRdd)
  
  val pairRdd = joinedRdd.map(x=>((x._2._1,x._2._2),1))
  
  val userPerCourseViewsrdd = pairRdd.reduceByKey(_+_)
  
  val courseperViews = userPerCourseViewsrdd.map(x=>(x._1._2,x._2))
  

  //val newjoinedrdd = courseperViews.join()
  //chaptercountrdd is op of previous question

  
}