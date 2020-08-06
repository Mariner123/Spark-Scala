package src.main.Scala
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object onewordcount {
  def main(args: Array[String]) {
    var text= new SparkContext("local[*]", "onewordcount");
    val file = text.textFile("C:\\Users\\arnabmuk\\Documents\\wordcount.txt");
    val word = file.flatMap(line=>line.split(" ")).map(ln=>(ln,1)).reduceByKey(_+_).
    filter(word=>word._1.contains("arnab")).map(wrd=>wrd._2);
    word.foreach(println)
  }
}