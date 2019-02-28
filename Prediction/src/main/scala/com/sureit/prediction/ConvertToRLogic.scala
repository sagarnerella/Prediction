package com.sureit.prediction
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import java.util.HashMap
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Locale
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import java.sql.Time

object ConvertToRLogic {
  var hashMap:HashMap[String,HashMap[String,Int]]=new HashMap[String,HashMap[String,Int]]
  def main(args:Array[String])
    {
    val conf = new SparkConf().setAppName("SQLtoJSON").setMaster("local[*]")
      .set("spark.executor.memory", "150g")
      .set("spark.driver.memory", "100g")
      .set("spark.executor.cores", "4")
      .set("spark.driver.cores", "4")
	    .set("spark.sql.shuffle.partitions", "2000")
	    .set("spark.driver.maxResultSize", "50g")	    
	    .set("spark.memory.offHeap.enabled", "true")
	    .set("spark.memory.offHeap.size", "100g")
	    .set("spark.memory.fraction", "0.75")
	    //.set("spark.mongodb.output.uri", "mongodb://192.168.70.13/ACQUIRER")
	    .set("spark.debug.maxToStringFields", "1000000") 
      .set("spark.mongodb.output.collection", "VariablesForNewModel_new") //to Get
   val db = "SUREIT"
 //@transient
      val sparkContext = new SparkContext(conf)
      //@transient
      val sqlContext = new SQLContext(sparkContext)
      import sqlContext.implicits._

      val jdbcSqlConnStr = s"jdbc:sqlserver://192.168.70.15;databaseName=$db;user=vivek;password=welcome123;"      

      
      
    val dbAcuirer="SUREIT"
        val jdbcAcquirerSqlConnStr = s"jdbc:sqlserver://192.168.70.15;databaseName=$dbAcuirer;user=vivek;password=welcome123;"      
        val tableProcessedTxns="[CTP].[ORION]"
        val jdbcDF7 = sqlContext.read.format("jdbc").options(Map("url" -> jdbcAcquirerSqlConnStr,"dbtable" -> tableProcessedTxns,"driver"-> "com.microsoft.sqlserver.jdbc.SQLServerDriver","fetchsize"->"100000")).load()
        jdbcDF7.createOrReplaceTempView("TEST7")
        val performanceWindowRes=sqlContext.sql("select  *, case when recentDt between '2017-03-24' and '2017-03-30' then recentDt else '2017-03-24' end as Mindays1 from ( select a.Tagid, a.recentDt, case when a.recentDt between '2017-03-24' and '2017-03-30' then 1 else 0 end as Event from  (  (SELECT TAGID,max(cast(EXITTXNDATE as date)) as recentDt     from TEST7 where ENTRYPLAZAID='002001' and cast(EXITTXNDATE as date) between '2017-03-24' and  '2017-03-30'   group by TAGID )  union (Select  tagid, max(cast(EXITTXNDATE as date)) as recentDt  from TEST7 where tagid not in (SELECT TAGID     from TEST7 where ENTRYPLAZAID='002001' and cast(EXITTXNDATE as date) between '2017-03-24' and  '2017-03-30') and ENTRYPLAZAID='002001' and  responsecode='A'  group by TAGID ) )a)b  ")
        performanceWindowRes.createOrReplaceTempView("PerformanceWindowRes")
        
        val distinctDatesOfPerformance=performanceWindowRes.select("Mindays1").orderBy(desc("Mindays1")).distinct().rdd.collect().toList
        val schema = StructType(
            StructField("INBOUNDPROCESSEDTXNID", IntegerType, true) ::
            	StructField("INBOUNDTOLLTXNID", IntegerType, false) ::
            		StructField("RESPONSECODE", IntegerType, false) ::
            			StructField("TXNSTATUSID", IntegerType, false) ::
            				StructField("TRIPSTAGEID", IntegerType, false) ::
      StructField("TAGID", StringType, false) ::
    	  StructField("REASONCODE", StringType, false) ::
    		  StructField("REASONDESC", StringType, false) ::
    			  StructField("CHARGEDVEHICLECLASS", StringType, false) ::
    				  StructField("EXITTXNDATE", DateType, false) ::
    					  StructField("ENTRYTXNDATE", DateType, false) ::
    						  StructField("VEHICLENUMBER", StringType, false) ::
    							  StructField("event", StringType, false) ::
    			  
    			  Nil)
      
      var dataRDD = sparkContext.emptyRDD[Row]  
      var totalDf=sqlContext.createDataFrame(dataRDD, schema)
        //performanceWindowRes.filter(performanceWindowRes.Mindays1=="").collect()
        var count:Int=1
        distinctDatesOfPerformance.foreach(row=>{
          try{
            if(count>=3){
          var performanceDate=row(0).toString().trim()
          print("performanceDate "+performanceDate)
          var obsWindow=requiredDate(performanceDate,1)
          //var listtagidsTemp=sqlContext.sql(s"select * from performanceWindowRes where Mindays1='$performanceDate'").select($"Tagid").rdd.map(r => r(0)).collect.toList
           var TXNTemp=sqlContext.sql(s"select * from TEST7 where EXITPLAZAID='002001'and responsecode='A' and EXITTXNDATE<='$performanceDate' and TAGID in (select Tagid from performanceWindowRes where Mindays1='$performanceDate')")
            TXNTemp.createOrReplaceTempView("PerformanceDayResult")
            var OW=sqlContext.sql(s"select * from PerformanceDayResult where EXITTXNDATE <='$obsWindow'")
          var TAG=sqlContext.sql(s"select * from PerformanceDayResult where EXITTXNDATE='$performanceDate'")
          OW.createOrReplaceTempView("OWTABLE")
          TAG.createOrReplaceTempView("TAGTABLE")
          import sqlContext.implicits._
          //TAG.toDF().write.csv("D:\\spark\\temp")
          //OW.rdd.partitions.size//=>4
          //TAG.rdd.partitions.size//=>4
          //OW.show()
          
          import org.apache.spark.sql.functions._
          //var TAGID=OW.select("TAGID").distinct().orderBy(desc("EXITTXNDATE")).groupBy("TAGID")
          //TAGID
          var TAGID=sqlContext.sql("select * from (select *,ROW_NUMBER() over (partition by TAGID order by  EXITTXNDATE desc) as row from OWTABLE ) as data where row = 1")
          import org.apache.spark.sql.functions.lit
         var TAGtagids=sqlContext.sql("select TAGID from TAGTABLE")
        var listtagidsOfTAG=TAGtagids.rdd.map(r => r(0)).collect.toList
          var tagidsOfTAG:String=listtagidsOfTAG.mkString("", "','", "").toString()
          val checkTagIsInList = udf((tagId: String) =>
          if (tagidsOfTAG.contains(tagId)){ 
          print("if tagId "+tagId+" tagidsOfTAG "+tagidsOfTAG)
          "1"
          }
          else{ 
            print("else tagId "+tagId+" tagidsOfTAG "+tagidsOfTAG)
            "0"
            }
        )
          
        
        TAGID=TAGID.withColumn("event",checkTagIsInList(col("TAGID")))
        TAGID.createOrReplaceTempView("TAGIDTABLE")
        
        TAGID=sqlContext.sql("select INBOUNDPROCESSEDTXNID,INBOUNDTOLLTXNID,RESPONSECODE,TXNSTATUSID,TRIPSTAGEID,TAGID,REASONCODE,REASONDESC,CHARGEDVEHICLECLASS,ENTRYTXNDATE,EXITTXNDATE,VEHICLENUMBER from TAGIDTABLE")
        
        var pre1=requiredDate(performanceDate,1)
        var pre2=requiredDate(performanceDate,2)
        var pre3=requiredDate(performanceDate,3)
        var pre4=requiredDate(performanceDate,4)
        var pre5=requiredDate(performanceDate,5)
        var pre6=requiredDate(performanceDate,6)
        var pre7=requiredDate(performanceDate,7)
        var pre1Res=sqlContext.sql(s"select * from PerformanceDayResult where EXITTXNDATE='$pre1'")
        var pre2Res=sqlContext.sql(s"select * from PerformanceDayResult where EXITTXNDATE='$pre2'")
        var pre3Res=sqlContext.sql(s"select * from PerformanceDayResult where EXITTXNDATE='$pre3'")
        var pre4Res=sqlContext.sql(s"select * from PerformanceDayResult where EXITTXNDATE='$pre4'")
        var pre5Res=sqlContext.sql(s"select * from PerformanceDayResult where EXITTXNDATE='$pre5'")
        var pre6Res=sqlContext.sql(s"select * from PerformanceDayResult where EXITTXNDATE='$pre6'")
        var pre7Res=sqlContext.sql(s"select * from PerformanceDayResult where EXITTXNDATE='$pre7'")
        
        var pre1OfTAG=pre1Res.select("TAGID").rdd.map(r => r(0)).collect.toList
          var pre1tagidsOfTAG:String=pre1OfTAG.mkString("", "','", "").toString()
          val pre1OfTAGTagIsInList = udf((tagId: String) =>
          if (tagidsOfTAG.contains(tagId)){ 
          print("if tagId "+tagId+" tagidsOfTAG "+tagidsOfTAG)
          "1"
          }
          else{ 
            print("else tagId "+tagId+" tagidsOfTAG "+tagidsOfTAG)
            "0"
            }
        )
        
        var pre2OfTAG=pre1Res.select("TAGID").rdd.map(r => r(0)).collect.toList
          var pre2tagidsOfTAG:String=pre2OfTAG.mkString("", "','", "").toString()
          val pre2OfTAGTagIsInList = udf((tagId: String) =>
          if (tagidsOfTAG.contains(tagId)){ 
          print("if tagId "+tagId+" tagidsOfTAG "+tagidsOfTAG)
          "1"
          }
          else{ 
            print("else tagId "+tagId+" tagidsOfTAG "+tagidsOfTAG)
            "0"
            }
        )
        TAGID=TAGID.withColumn("event",pre1OfTAGTagIsInList(col("TAGID")))
        TAGID=TAGID.withColumn("event",pre2OfTAGTagIsInList(col("TAGID")))
        
        totalDf.union(TAGID)
        //TAGID.show()
        //TAGtagids.show()
        //print("tagidsOfTAG "+tagidsOfTAG)
            }
            count=count+1
          }catch {
            case exe:Exception=>{
              exe.printStackTrace()
            }
          }
        })
        totalDf.write.format("com.databricks.spark.csv").save("D:\\spark\\temp")
        totalDf.show()
        
    }
  
  def requiredDate(date:String,dayCount:Int):String={
    val dateArr=date.split("-")
    val calaendar=Calendar.getInstance()
     val year= dateArr.takeRight(4)(0).toString().toInt;
    val month=dateArr.takeRight(4)(1).toInt
    val day=dateArr.takeRight(4)(2).split("T").takeRight(4)(0).toInt
    calaendar.set(year, month-1, day)
   calaendar.add(Calendar.DATE, -dayCount)
    val sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
     val wantedDate = sdf.format(calaendar.getTime());
    return wantedDate
  }
}