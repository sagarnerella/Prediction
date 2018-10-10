package com.sureit
import org.apache.spark.sql._
case class ConvertTxtToEmployee(name:String,age:Int)
object AddingSchemaToRDD {
  
   def main(args:Array[String]){
    
    val sparkSession=SparkSession.builder().appName("AddingSchemaToRDD").master("local").getOrCreate()
    val textDF= sparkSession.sparkContext.textFile("Employee.txt")
    val rowData=textDF.map(_.split(","))
     import sparkSession.implicits._
      val rowDataDF=rowData.map(att=>ConvertTxtToEmployee(att(0).toString(),att(1).toInt)).toDF()
    /*val rowDataPlanDF=rowData.map(att=>Seq((att(0),att(1)))).toDF("Name","Age")
    rowDataPlanDF.show()*/
      
      rowDataDF.map(NameAndAgeRow=>"Age "+NameAndAgeRow.getAs[Int]("age")).show()
      rowDataDF.map(NameAndAgeRow=>"Name "+NameAndAgeRow(1)).show()
      
      rowDataDF.createOrReplaceTempView("Employee")
      val resultNameAndAge=sparkSession.sqlContext.sql("select age,name,age from Employee")
      
      resultNameAndAge.map(NameAndAgeRow=>"Name "+NameAndAgeRow(1)).show()
      resultNameAndAge.map(NameAndAgeRow=>"Age "+NameAndAgeRow.getAs[Int]("age")).show()
      resultNameAndAge.select("name", "age").show()
    
   }

}