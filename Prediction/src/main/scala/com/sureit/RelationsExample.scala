package com.sureit

import org.apache.spark.sql._
object RelationsExample {
   def main(args:Array[String]){
    val sparkSession=SparkSession.builder().appName("RelationsExample").master("local").getOrCreate()
    
    val empDataFrame=sparkSession.read.json("Emp.json")
    //empDataFrame.show()
    
    empDataFrame.createOrReplaceTempView("Emp")
    val addressDataFrame=sparkSession.read.json("Address.json")
    addressDataFrame.createOrReplaceTempView("Address")
    //sparkSession.sqlContext.sql("select * from Emp").show()
    //sparkSession.sqlContext.sql("select * from Address").show()
    val resultDF= sparkSession.sqlContext.sql("select firstName as Name,age as Age,Area,City,Mandal from Emp e,Address a where a.empId=e.id")
    resultDF.repartition(1).write.json("D:\\spark\\Practiese")
    
   }
}