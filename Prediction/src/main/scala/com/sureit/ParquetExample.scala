package com.sureit

import org.apache.spark.sql._
import org.apache.spark.sql.functions
object ParquetExample {
  def main(args:Array[String]){
    val sparkSession=SparkSession.builder().appName("ReadFileAndAddSchema").master("local").getOrCreate()
    
    import sparkSession.implicits._
    val fileData=sparkSession.read.json("Employee.json")
    
    fileData.show()
    fileData.write.parquet("employee.parquet")
    val readParquet=sparkSession.read.parquet("employee.parquet")
    readParquet.show()
    
  }
}