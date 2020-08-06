import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import javassist.runtime.Desc
import javassist.expr.Cast
import org.apache.spark.sql.catalyst.expressions.Cast
import java.util.function.ToIntFunction


object SapientProgram  extends Serializable{
  
  def main(args: Array[String]){
    
     val spark = SparkSession.builder().appName("Sapient Operation").master("local").getOrCreate()
     val sc = spark.sparkContext
//     import spark.implicits._
      import spark.sqlContext.implicits._
       val internal_product_data = spark.read.option("header","true").options(Map("delimiter"->"|"))
                                       .csv("C:\\Users\\arnabmuk\\Big Data new coding excercise for SAL1,SAl2 dec 2017\\test_pack\\data_for_probs\\ecom\\internal_product_data.txt")
 
       val ecom_competitor_data = spark.read.option("header","true").options(Map("delimiter"->"|"))
                                       .csv("C:\\Users\\arnabmuk\\Big Data new coding excercise for SAL1,SAl2 dec 2017\\test_pack\\data_for_probs\\ecom\\ecom_competitor_data.txt")
       
       val seller_data = spark.read.option("header","true").options(Map("delimiter"->"|"))
                                       .csv("C:\\Users\\arnabmuk\\Big Data new coding excercise for SAL1,SAl2 dec 2017\\test_pack\\data_for_probs\\ecom\\seller_data.txt")
   
     
       
       val windowSpec = Window.partitionBy("productId").orderBy(col("Price"))
       val df_ecom_minPrice =ecom_competitor_data.withColumn("row_num", row_number().over(windowSpec)).select("productId","price","rivalName","saleEvent").filter("row_num=1")
      
       
       val internal_ecom_join_data = internal_product_data.join(df_ecom_minPrice,"productId").join(seller_data, "SellerId")
       
       internal_ecom_join_data.createOrReplaceTempView("internal_ecom_data_join")

       
       
       
      
       val sum = (s1:String , s2:String , s3:String,s4:String,con:String,netVal:String)=>{
         if (s1.toFloat + s2.toFloat < s4.toFloat)
           (s1.toFloat + s2.toFloat)
         else if (s1.toFloat+s3.toFloat < s4.toFloat)
           s4.toFloat
         else if ((s1.toFloat < s4.toFloat) && (con == "Special")) 
           s3.toFloat
         else if ((s3.toFloat < s1.toFloat) && (netVal == "VeryHigh"))
           0.9*s1.toFloat
         else
           s1.toFloat
         
       }
       
        spark.udf.register("Total_price",sum)
        val output=spark.sql("select productId,total_price(procuredValue,maxMargin,minMargin,price,saleEvent,netValue) as final,lastModified as TimeStamp,price as CheapestAmongAllRival,rivalName from internal_ecom_data_join")
       output.write.option("header", "true").csv("C:\\Users\\arnabmuk\\Big Data new coding excercise for SAL1,SAl2 dec 2017\\test_pack\\data_for_probs\\ecom\\output.csv")
       
  }
}