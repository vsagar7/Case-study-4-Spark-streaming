import org.apache.spark.{SparkConf, SparkContext} 
import org.apache.spark.streaming.{Seconds, StreamingContext} 
import org.apache.log4j.{Level,Logger} 
  
object SparkFileStreamingWordCount  
 { 
 
		def main(args: Array[String]): Unit = {          
		println("hey Spark Streaming") 
 
      val conf = new SparkConf().setMaster("local[2]").setAppName("SparkSteamingExample")             
	  val sc = new SparkContext(conf)            
	  val rootLogger =Logger.getRootLogger()           
	  rootLogger.setLevel(Level.ERROR) 
 
   // Create Streaming context  to set batch duration 10 seconds       
   val ssc = new StreamingContext(sc, Seconds(10)) 
 
//Create RDD for text file streaming by
	val lines = ssc.textFileStream("file:///home/acadgild/Desktop/Spark_Streaming")	
  //val lines = ssc.textFileStream("/home/acadgild/Desktop/Spark_Streaming/match")      
//Split each line into words       
val words = lines.flatMap(_.split(" "))      
//Count each word in each batch       
val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)            
wordCounts.print()        
//Start the computation          
ssc.start()      
//wait for the computation to terminate      
ssc.awaitTermination() 
 
     } 
 
} 