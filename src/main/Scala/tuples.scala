package src.main.Scala

import org.apache.spark.SparkContext

object tuples {
  def parseline(line : String)={
    val fields=line.split(",")
    val firstname=fields(1)
    val lastname=fields(2)
    val occupation=fields(3)
    
    ((firstname,lastname),occupation)
  }
  
   def main(args: Array[String]){
     val sc = new SparkContext("local[*]","tuplecreate")
     
     val loadfile = sc.textFile("C:\\Users\\arnabmuk\\Desktop\\dataset.txt")
     
     val rdd = loadfile.map(parseline)
     
     val name =rdd.filter(x=>x._2.contains("agent")).map(y=>y._1)
     
     name.foreach(println)
   }
}