
import org.apache.spark.sql.SparkSession
object VertexDegree {
  def main(args: Array[String]){

    val spark = SparkSession.builder().appName("Create dataframe").master("local").getOrCreate()
    
    val file = spark.sparkContext.textFile("C:\\Users\\arnabmuk\\extra\\edges.csv")

    
    val header = file.first()
    val data = file.filter(row=> row!=header)
   
    val rddfinal = data.flatMap(line=>line.split(",")).map(x=>(x,1)).reduceByKey(_+_).sortByKey(true)
    val totalAvg = (rddfinal.map(x=>x._2).sum())./(rddfinal.map(x=>x._1).count())
    
    rddfinal.filter(wrd =>wrd._2.>=(totalAvg)).foreach(println)
  }
}