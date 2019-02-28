package com.sureit.prediction
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql
import org.apache.spark.sql._
import scala.math.pow
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.MongoSpark
import java.util.ArrayList
import java.lang._
case class VehiclePredictionForToday(tagId:String,plazaId:String,predictionPercntagForToday:Double)
object FinalPredictionOfTag {
  def main(args:Array[String]){
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
	    .set("spark.mongodb.input.uri", "mongodb://192.168.70.13/ACQUIRER")
      .set("spark.mongodb.input.collection", "test1") //to Get vehicl_list_qualified_trips_info
      .set("spark.mongodb.output.uri", "mongodb://192.168.70.13/ACQUIRER")
      .set("spark.mongodb.output.collection", "TestTodaysPrediction") //to Get
	    //.set("spark.sql.warehouse.dir", "/home/sagar/spark_sql_warehouse")
	    //.set("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      
      //@transient
      val sparkContext = new SparkContext(conf)
      
       val sqlContext = new SQLContext(sparkContext)
      
      import sqlContext.implicits._
      import sqlContext.implicits._
      val db = "ACQUIRER"
      val jdbcSqlConnStr = s"jdbc:sqlserver://192.168.70.15;databaseName=$db;user=vivek;password=welcome123;"  
       val inboundProcdTxn = "[TOLLPLUS].[INBOUND_PROCESSEDTXNS]"
      val resultInboundProcdTxn = sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr,"dbtable" -> inboundProcdTxn,"driver"-> "com.microsoft.sqlserver.jdbc.SQLServerDriver")).load()
      //resultInboundProcdTxn.createOrReplaceTempView("INBOUND_PROCESSEDTXNS")
     val plazas = "[TOLLPLUS].[PLAZAS]"
     val resultPlazas = sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr,"dbtable" -> plazas,"driver"-> "com.microsoft.sqlserver.jdbc.SQLServerDriver")).load()
      resultPlazas.createOrReplaceTempView("PLAZAS")
      
      
      //sparkContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://example.com/database.collection")))
      //val readConfigVal = ReadConfig(Map("uri" -> uriName,"database" -> dbName, "collection" -> collName, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparksession)))
 
val readConfig = ReadConfig(Map("uri" -> "mongodb://192.168.70.13","database" -> "ACQUIRER","collection" -> "vehiclePrediction", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparkContext)))
val vehiclePrediction = MongoSpark.load(sqlContext, readConfig)  
vehiclePrediction.createOrReplaceTempView("vehiclePrediction")
vehiclePrediction.show()
val readConf = ReadConfig(Map("uri" -> "mongodb://192.168.70.13","database" -> "ACQUIRER","collection" -> "maxminanduniquedays", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparkContext)))
val maxminanduniquedays = MongoSpark.load(sqlContext, readConf)
maxminanduniquedays.createOrReplaceTempView("maxminanduniquedays")

val readCon = ReadConfig(Map("uri" -> "mongodb://192.168.70.13","database" -> "ACQUIRER","collection" -> "noOfTripAndDaysByPlazaLevel", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparkContext)))
val discountResult = MongoSpark.load(sqlContext, readCon)
discountResult.createOrReplaceTempView("noOfTripAndDaysByPlazaLevel")

val distinctTagIds=sqlContext.sql("select distinct(tagId) from vehiclePrediction")
val uniqueTagids=distinctTagIds.select("tagid").rdd.collect()

val plazaIds=sqlContext.sql("select distinct(plazacode) from PLAZAS").select("plazacode").rdd.collect()
uniqueTagids.foreach(tagids=>{
  val tagid=tagids(0).toString()
  var list=new ArrayList[VehiclePredictionForToday];
  plazaIds.foreach(plazacodes=>{
    val plazacode=plazacodes(0)
val daysSum=sqlContext.sql(s"select -((((mmu.uniquedays/mmu.maxdays)*100)*0.044259)+(CASE WHEN mmu.mindays > 15 THEN 1 else 0  END  *-1.962858)+(-0.352331*vp.onDayOneisTravelled)+(vp.onDayTwoisTravelled*0.20997)+(0.194127*vp.onDayThreeisTravelled)+(0.186123*vp.onDayFourisTravelled)+(0.105154*vp.onDayFiveisTravelled)+(0.19696*vp.onDaySixisTravelled)+(vp.onDaySevenisTravelled*0.274139)+(vp.dailyPass*0.280865)+(discount.isDiscountApplied*0.15187))  as  total from vehiclePrediction vp,maxminanduniquedays mmu,noOfTripAndDaysByPlazaLevel discount  where vp.tagId=mmu.tagId and vp.tagId=discount.tagId and vp.plazaId=discount.plazaCode and discount.plazaCode=vp.plazaId and vp.plazaId='$plazacode' and   vp.tagId='$tagid' ")
import org.apache.spark.sql.types.IntegerType
//val daysValu=daysSum.withColumn("total",daysSum("total").cast(IntegerType)).rdd.collect()
val daysValu=daysSum.select("total").rdd.collect()
println("daysValu "+daysValu)
var test=0;
    if(daysValu!=null)
if(daysValu.length>0){
val num=daysValu(0)(0)
println("daysValu "+Math.exp(num.toString().toDouble))
val predictionPercent=1/(1+Math.exp(num.toString().toDouble))
val obj=new VehiclePredictionForToday(tagid,plazacode.toString(),predictionPercent)
    list.add(obj)
    println("predictionPercent "+predictionPercent)
    import collection.JavaConversions._
    MongoSpark.save(list.toSeq.toDS())
    list.remove(obj)
}
})
//import collection.JavaConversions._
//MongoSpark.save(list.toSeq.toDS())
//list=null;
})
      
    }
}