import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object SparkAssignment extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("C:/Users/atulg/Downloads/Spark_datasets/dataset1.csv")
  
  val rdd1 = input.map(x =>{val fields = x.split(",")
    if(fields(1).toInt>18)
      (fields(0),fields(1),fields(2),"y")
      else
        (fields(0),fields(1),fields(2),"N")
  })

  
 rdd1.collect()foreach(println)
  
}  