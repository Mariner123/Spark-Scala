import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_date,to_timestamp,date_format,unix_timestamp}
import org.apache.spark.sql.catalyst.expressions.Cast
import javassist.runtime.Desc
import  org.apache.spark.sql.functions.col;
import org.apache.spark.sql.{DataFrame,Row}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SQLContext


object dataframeOperation {
    def main(args: Array[String]){
    
     val spark = SparkSession.builder().appName("Dataframe Operation").master("local").getOrCreate()
     val sc = spark.sparkContext
    
    

     val df_employee = spark.read.option("header", "true").csv("C:\\Users\\arnabmuk\\Employee_Details.csv")
     val df_lookup = spark.read.option("header", "true").csv("C:\\Users\\arnabmuk\\lookup_table.csv")
     val df = spark.read.option("header", "true").csv("C:\\Users\\arnabmuk\\data1.csv")
     df_employee.createOrReplaceTempView("emp_details")
     df_lookup.createOrReplaceTempView("look_table")
   /* val partsize= df.rdd.partitions.size
    println(partsize)
     val df2= df.repartition(2)
     val partsize_new= df2.rdd.partitions.size
    println(partsize_new)
    val innerjoin =df_employee.join(df_lookup,"Emp_Id")
    innerjoin.show()*/
     
     
    val higestName = df.orderBy(col("sal").desc).select("ename").first()
    val highestSal = df.orderBy(col("sal").desc).select("sal").first()
    val lowestName = df.orderBy(col("sal")).select("ename").first()
    val lowestSal = df.orderBy(col("sal")).select("sal").first()
    
     val schematype = StructType(
                                 Array(StructField("HighestName", StringType, true),
                                      StructField("HighestSal", StringType, true),
                                      StructField("LowestName", StringType, true),
                                      StructField("LowestSal", StringType, true)))
                                  

     import spark.sqlContext.implicits._
    
sc.parallelize(Array(higestName,highestSal,lowestName,lowestSal))
   
   
   
  // spark.createDataFrame(rdd,schematype).show()
   
   
    }
}