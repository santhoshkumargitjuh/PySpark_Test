package sparkpack
import org.apache.spark.{SparkConf,SparkContext,_}
import org.apache.spark.sql.{ SparkSession, Row, functions, types, DataFrame, _ }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast,column, _}
import org.apache.spark.sql.types._
object incremental_scen10 {
  
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("incremental_scen10")
                              .setMaster("local[*]")
    

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._
    

    
    val df = spark
            .read
            .format("json")
            .load("file:///D:/Cloud_Journey/DE/data/realtime_scenarios/10/2023-25-10")
    
//    df.show()
//    
//    
//    df.printSchema()
    
    val flattendf = df.withColumn("products",explode(col("products")))
                      .select(
                          "address.*",
                          "custid",
                          "products"                          
                            )
                      .withColumn("delete_indicator",lit(0))
                      .withColumn("incID",monotonically_increasing_id())
                      .withColumn("batch_id",lit(0))
    
//    flattendf.show()
//    flattendf.printSchema()
//    downloaded jar from here - 
//                      https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.21/
        
    flattendf.write.format("jdbc")
    .option("url","jdbc:mysql://localhost/Sakila")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .option("dbtable","custtab")
    .option("user","root")
    .option("password","Tiger@123")
    .mode("append")
    .save()
    
    print("table created and data written")
  
  }
}