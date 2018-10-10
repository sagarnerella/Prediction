package com.sureit
import org.apache.spark.sql._
object ReadJsonAndDataFrameSqlOperations {
  
  
  def main(args:Array[String]){
    
    val sparkSession=SparkSession.builder().appName("ReadJson").master("local").getOrCreate()
    
    val itemsDF=sparkSession.read.json("items.json")
    itemsDF.show()
    
    import sparkSession.implicits._
    //itemsDF.printSchema()
    //itemsDF.select("firstName", "age").show()
    /*itemsDF.select($"firstName", $"age"+2).show()
    itemsDF.filter($"age">20).show()*/
    
    println("order = By")
    itemsDF.select("firstName", "age").orderBy("firstName").show()
    
    /*println("Alias")
    itemsDF.select(itemsDF("firstName").alias("Names")).show()*/
    
    println("group by")
    itemsDF.groupBy("age").count().show()
    
    println("where condition")
    itemsDF.where("age > 10 and firstName='First'" ).show()
    
    
    itemsDF.createOrReplaceTempView("Employee")
    sparkSession.sqlContext.sql("select firstName,age from Employee ").show()
    sparkSession.sqlContext.sql("select firstName,age from Employee  where age > 10").show()
    sparkSession.sqlContext.sql("select age,sum(age) from Employee  group by  age").show()
    sparkSession.sqlContext.sql("select age,count(age) from Employee  group by  age").show()
    
    //below are applying string functions on name
    import org.apache.spark.sql.functions.udf
    val upperCase=udf(upper)
    //itemsDF.withColumn("NameInUpper", upperCase('firstName)).show
    
    
    
  }
  
   val upper: String => String =_.toUpperCase
  val lower: String => String=_.toLowerCase

  
}