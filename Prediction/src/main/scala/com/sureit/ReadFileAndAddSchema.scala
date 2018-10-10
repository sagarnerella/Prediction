package com.sureit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
object ReadFileAndAddSchema {
  
  
  def main(args:Array[String]){
    val sparkSession=SparkSession.builder().appName("ReadFileAndAddSchema").master("local").getOrCreate()
    
    val fileData=sparkSession.sparkContext.textFile("Employee.txt")
    val mapData=fileData.map(_.split(","))
    val rowData=mapData.map(att=>Row(att(0),att(1)))
    
    val fileds="name age"
    val fields=fileds.split(" ").map(field=>StructField(field,StringType,nullable=true))
    
    val schema=StructType(fields)
    
    val resultDF=sparkSession.createDataFrame(rowData, schema)
    resultDF.show()
    
    resultDF.createOrReplaceTempView("Employee")
    
    val result=sparkSession.sqlContext.sql("select name,age from Employee")
    implicit val mapEncoder=org.apache.spark.sql.Encoders.kryo[Map[String,Any]]
    
    result.map(att=>att.getValuesMap[Any](List("name","age"))).collect()
    
    
    
  }

}