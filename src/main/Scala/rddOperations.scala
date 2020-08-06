import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
object rddOperations {
  def main(args: Array[String]){
    
     val spark = SparkSession.builder().appName("Dataframe Operation").master("local").getOrCreate()
     val sc = spark.sparkContext
     
     val rdd= sc.textFile("C:\\Users\\arnabmuk\\test.txt")
     rdd.flatMap(line=>line.split(" ")).map(word=>(word,1)).foreach(println)
     
     rdd.map(line=>line.split(" ")).map(word=>(word,1)).foreach(println)
     
    
     // array
  val ab = ArrayBuffer[String]()
  ab += "hello"
  ab += "world"
  println(ab.toArray.size)
  
  for (x <-ab.toArray){
    print(x)
  }
     
     
     
     
  }
}
