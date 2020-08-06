import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DataTypes
import javassist.runtime.Desc
import org.apache.spark.sql.DataFrame




object footballDataManipulation {
  def main(args: Array[String]){
    val spark = SparkSession.builder().appName("Dataframe Operation").master("local").getOrCreate()
     val sc = spark.sparkContext
     
     val df =spark.read.option("header", "true").option("inferschema", "true").csv("C:\\Users\\arnabmuk\\LaLiga_data.csv")
     
/*       //fetch 1st 5 records
     df.show(5) 
     
     //replace all - with 0
     val df_col = df.columns
     df.na.replace(df_col,Map("-"->"0")).show()
   
     //top five teams based on point
     df.withColumn("Points",col("Points").cast(IntegerType)).sort(col("Points").desc).select("team").show(5)
     df.withColumn("Points", col("Points").cast(IntegerType)).orderBy(col("Points").desc).select("team").show(5)
     
     //list of teams whose debut year between 1930-1980
     df.createOrReplaceTempView("LaLiga")
      val DebutYear = (s: String) => {
             if(s.indexOf("-")!= -1)
              {
                  s.substring(0 , s.indexOf("-")).toInt
              }
            else
              {
                  s.toInt
              }
       }
    spark.udf.register("Debut_Year", DebutYear)
  
    spark.sql("select team,Debut from LaLiga where Debut_Year(Debut) between 1930 and 1980").show()*/
 
     //list of team names with their goal difference & teams which have maximum and minimum goal difference
    /*val GoalDiff = (s1: String,s2:String)=>{
      if (s1.contains("-") || s2.contains("-")){
        val new_s1 = s1.replace("-", "0").toInt
        val new_s2= s2.replace("-", "0").toInt
        new_s1 -new_s2
      }
      else{
        s1.toInt-s2.toInt
      }
    }
    spark.udf.register("Goal_Diff", GoalDiff)
    
    def goal_difference(df:DataFrame):DataFrame={
        spark.sql("select Team,Goal_Diff(GoalsFor,GoalsAgainst) as diff from LaLiga")          
      }
    
     println(goal_difference(df).orderBy(col("diff").desc).first())
     println(goal_difference(df).orderBy(col("diff")).first())*/
     
   //adding a column with wining percentage  
   /*df.withColumn("Winning Percentage",(df.col("GamesWon")/df.col("GamesPlayed"))*100).show()*/
   
   // Group teams based on their “Best position” and print the sum of their points for all positions
  df.withColumn("Points", col("Points").cast(IntegerType)).groupBy("BestPosition").sum("Points").show()
  }
  
}