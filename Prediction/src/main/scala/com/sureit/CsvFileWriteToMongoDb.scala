package com.sureit
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.io.File
import com.mongodb.spark.MongoSpark
object CsvFileWriteToMongoDb {
  
   def main(args:Array[String])
    {
      val conf = new SparkConf().setAppName("SQLtoJSON").setMaster("local[*]")
      .set("spark.executor.memory", "150g")
      .set("spark.driver.memory", "10g")
      .set("spark.executor.cores", "4")
      .set("spark.driver.cores", "4")
  //  .set("spark.testing.memory", "2147480000")
	    .set("spark.sql.shuffle.partitions", "2000")
	    .set("spark.memory.offHeap.enabled", "true")
	    .set("spark.memory.offHeap.size", "100g")
	    .set("spark.memory.fraction", "0.75")
	    .set("spark.mongodb.output.uri", "mongodb://192.168.70.13/ACQUIRER")
      .set("spark.mongodb.output.collection", "vehicl_list_qualified_trips_info") //to Get
	    //.set("spark.sql.warehouse.dir", "/home/sagar/spark_sql_warehouse")
	    //.set("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      
      //@transient
      val sparkContext = new SparkContext(conf)
      
       val sqlContext = new SQLContext(sparkContext)
      
      
      //sqlContext.read.format("csv").option("header", "true").load("../Downloads/*.csv")
      //sqlContext.read.option("header", "true").csv("D:\\spark\\csvs\\")
      
     val files=new File("D:\\spark\\csvs\\new").list().toList
     files.foreach(fileName=>{
       println("before")
       //val fileData=sqlContext.read.option("header", "true").csv("D:\\spark\\csvs\\"+fileName)
       val fileData=sqlContext.read.option("header", "true").csv("D:\\spark\\csvs\\new\\"+fileName)
       MongoSpark.save(fileData)
       println("after")
       //println("file name "+fileName)
     })
     
    }

}