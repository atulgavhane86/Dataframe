import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Practise9 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val myList = List("ERROR: Thu Jun 04 10:37:51 BST 2015",
                    "WARN: Sun Nov 06 10:37:51 GMT 2016",
                    "ERROR: Thu Jun 04 10:37:51 BST 2015",
                    "WARN: Sun Nov 06 10:37:51 GMT 2016",
                    "ERROR: Thu Jun 04 10:37:51 BST 2015",
                     "WARN: Sun Nov 06 10:37:51 GMT 2016")
                     
    val originalLogsRdd = sc.parallelize(myList)
    
    val mappedRdd = originalLogsRdd.map(x=>{
      val columns = x.split(":")
      val logLevel = columns(0)
      (logLevel,1)
    })
    
    val reducedRdd = mappedRdd.reduceByKey((x,y) => x+y)
    
    
    reducedRdd.collect().foreach(println)
    
  
}