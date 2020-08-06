import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import java.text.Format
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{to_date,to_timestamp,date_format,unix_timestamp}
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.ArrayType

object dataframe {
  def main(args: Array[String]){
  
    val conf = new SparkConf().setAppName("dataframe").setMaster("local")
    val sc= new SparkContext(conf)
    
    val spark = SparkSession.builder().appName("Create Dataframe").master("local").getOrCreate()
    val sc1 = spark.sparkContext
    
   /* val jdbcdf = spark.read.format("jdbc")
                      .option("url", "jdbc:oracle:thin:GHCFDB/GHCFDB@//whf00iuz.in.oracle.com:1521/ora11g34")
                      .option("user","GHCFDB")
                      .option("password", "GHCFDB")
                      .option("dbtable", "GHCFDB.emp")
                      .option("query","select * emp")
                      .option("driver", "oracle.jdbc.driver.OracleDriver")
                      .load().show()
                      
      
   */ 
  /* 
    val schematype = StructType(
                                 Array(StructField("empno",IntegerType,true),
                                      StructField("enane",StringType,true),
                                      StructField("job",StringType,true),
                                      StructField("mgr",IntegerType,true),
                                      StructField("hiredate",DateType,true),
                                      StructField("sal",IntegerType,true),
                                      StructField("address",StringType,true)))
    
    
   val df_textfile = spark.read
                         //   .format("text")
                            .option("header","true")
                            .option("inferSchema", "true")
                            .options(Map("delimiter"->","))
                            .option("nullValue", "null")
                            .option("timestampFormat", "MM-dd-yyyy hh mm ss")
                           // .option("quotechar", ",")
                           // .schema(schematype)
                            .csv("C:\\Users\\arnabmuk\\data1.txt")
                        //    .textFile("C:\\Users\\arnabmuk\\data1.txt")
    
    
    df_textfile.withColumn("hiredate",df_textfile.col("hiredate").cast(DateType))
    
   df_textfile.select(to_date(df_textfile.col("HIREDATE"),"MM/DD/YYYY")).alias("dd/mm/yyyy").show()
    df_textfile.withColumn("HIREDATE", to_date(df_textfile.col("HIREDATE"),"MM/DD/YYYY")).show()
     df_textfile.withColumn("HIREDATE",date_format(df_textfile.col("HIREDATE"), "dd-mm-yyyy").as("dd/mm/yyyy")).show()
  
  
    df_textfile.printSchema()
    df_textfile.show()
  */  
/*     val rdd = sc.textFile("C:\\Users\\arnabmuk\\data2.txt")
     val word = rdd.map(line =>line.split(",")).filter(wrd => wrd(2).contains("HR")).map(ln=>(ln(0),ln(1),ln(2)))
     word.foreach(println)*/
    
    //data.collect().foreach(println)
       
       val data = 1 to 12
       val rdd = sc.parallelize(data,3)
       val data_part_pos =rdd.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) =>it.toList.map(x => if (index ==2) {println(x)}).iterator).collect()
       val zeroth = rdd
  // If partition number is not zero ignore data
       val p =rdd.mapPartitionsWithIndex((idx, x) => if (idx == 0) x else Iterator())
       val part_rdd= rdd.glom().collect()(0)
       p.foreach(println)
    
     
/*       val df = spark.read
       //            .format("csv")
                     .option("header", "true")
                   .option("inferSchema", "true")
                   //  .schema(schematype)
                     .option("nullValue", "null")
                    .options(Map("delimiter"->","))
                    .csv("C:\\Users\\arnabmuk\\data.csv")
      
       df.show()
       
       df.na.fill("bal", Seq("MGR","COMM")).show()
                  */
  /*     val df_new = spark.read
       //            .format("csv")
                     .option("header", "true")
     //              .option("inferSchema", "true")
                    .options(Map("delimiter"->","))
                    .csv("C:\\Users\\arnabmuk\\data1.csv")
    
      // df_new.show()
             
       val updated_empno =df_new.except(df).select("empno").collectAsList().get(0)
       //println(updated_empno)
       
      // df_new.withColumnRenamed("empno", "eno").show()
     //  df.distinct().show()
        df.createOrReplaceTempView("Employee")
       // val query = spark.sql("SELECT * from Employee")
        //query.collect().foreach(println)
        //val query1 = spark.sql("DELETE * from Employee where empno=9999")
        //query1.collect().foreach(println)
        //df.filter("deptno=20").show() instead of using spark.sql we can fetch data based on particuler condition in this way

        val df1=df.drop("name == ABC")
        //df1.show()  
        //df.withColumn("NewCol",df("sal")).show()
*/        
        
      
  }
}