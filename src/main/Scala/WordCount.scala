package src.main.Scala
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object WordCount {
   def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")   
    
    // Read each line of the book into an RDD
    val input = sc.textFile("C:///Users/arnabmuk/Desktop/Dummy.txt")
    // hello how are you hello
    // Split into words separated by a space character
    val words = input.flatMap(x => x.split(" "))
    
    val wordCounts = words.countByValue()

    wordCounts.foreach(println)
    scala.io.StdIn.readLine()
  }

}