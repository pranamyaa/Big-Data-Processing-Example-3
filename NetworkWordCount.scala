package streaming.count

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.matching.Regex
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime


object NetworkWordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: Assignment2 <directory>")
      System.exit(1)
    }
 
    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("Assignment2").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(10))
        
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    
    
    val words = lines.flatMap(_.split(" "))
    
    val trimmedWords = words.map(x => (x.replaceAll("[^0-9a-zA-Z]",""), 1))
    
    
    // SubTask 1 : Word Count at real time file upload 
    val wordCounts = trimmedWords.reduceByKey(_ + _)
    wordCounts.print()
    wordCounts.foreachRDD(t => {
      if(!t.isEmpty()){
        t.saveAsTextFile(args(1)+DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now()).toString())
      }
    })
    
    // SubTask 2 : Filtering out words which are of length less than 5
    val MedLongWords = trimmedWords.filter(_._1.length() >= 5).reduceByKey(_ + _)
    MedLongWords.print()
    MedLongWords.foreachRDD(t => {
      if(!t.isEmpty()){
        t.saveAsTextFile(args(2)+DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now()).toString())
      }
    })   
    
    
    //SubTask 3 : Map occurrences of words for each RDD. 
    val Lwords = lines.map(_.split(" ")
                            .map(x =>(x.replaceAll("[^0-9a-zA-Z]", "")))
                            .toList)
    val cooc = Lwords.flatMap(_.combinations(2)).map((_, 1)).reduceByKey(_ + _)
    cooc.print()
    cooc.foreachRDD(c => {
      if(!c.isEmpty()){
       c.saveAsTextFile(args(3)+DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now()).toString()) 
      }  
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
}