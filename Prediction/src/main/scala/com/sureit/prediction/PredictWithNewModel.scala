package com.sureit.prediction
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import java.util.HashMap
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Locale
import java.io.NotSerializableException
import org.apache.spark.sql.catalyst.parser.ParseException
import com.mongodb.spark.MongoSpark
import org.apache.log4j.Logger
 import org.apache.log4j.PropertyConfigurator;
case class PredictionForTodayWithNewModel(tagId:String,plazaId:String,predictionPercntagForToday:Double)
case class PrdictionForLatestModel(tagId:String,plazaId:String,dayOne:String,onDayOneisTravelled:Int,
    dayTwo:String,onDayTwoisTravelled:Int,dayThree:String,onDayThreeisTravelled:Int,dayFour:String,onDayFourisTravelled:Int,
    dayFive:String,onDayFiveisTravelled:Int,daySix:String,onDaySixisTravelled:Int,daySeven:String,onDaySevenisTravelled:Int,discount:Double,maxMinDaysDiff:Int,minDays:Int,maxDays:Int,uniquedays:Int,noOfTrips:Int,dailyPass:Int)
object PredictWithNewModel {
  PropertyConfigurator.configure("log4j.properties");
  val logger=Logger.getRootLogger()
  logger.info("program started")
  var hashMap:HashMap[String,HashMap[String,Int]]=new HashMap[String,HashMap[String,Int]]
  def main(args:Array[String])
    {
    val conf = new SparkConf().setAppName("SQLtoJSON").setMaster("local[*]")
      .set("spark.executor.memory", "150g")
      .set("spark.driver.memory", "10g")
      .set("spark.executor.cores", "4")
      .set("spark.driver.cores", "4")
	    .set("spark.sql.shuffle.partitions", "2000")
	    .set("spark.driver.maxResultSize", "50g")	    
	    .set("spark.memory.offHeap.enabled", "true")
	    .set("spark.memory.offHeap.size", "100g")
	    .set("spark.memory.fraction", "0.75")
	    .set("spark.mongodb.output.uri", "mongodb://192.168.70.7/ACQUIRER")
      .set("spark.mongodb.output.collection", "VariablesInfoForEachTag") //to Get
val db = "ACQUIRER"
      //@transient
      val sparkContext = new SparkContext(conf)
      //@transient
      val sqlContext = new SQLContext(sparkContext)
      import sqlContext.implicits._

      val jdbcSqlConnStr = s"jdbc:sqlserver://192.168.70.15;databaseName=$db;user=vivek;password=welcome123;"      

    val dbAcuirer="ACQUIRER" 
        val jdbcAcquirerSqlConnStr = s"jdbc:sqlserver://192.168.70.15;databaseName=$dbAcuirer;user=vivek;password=welcome123;"      
        val tableProcessedTxns="[TOLLPLUS].[TEST7]"
        val jdbcDF7 = sqlContext.read.format("jdbc").options(Map("url" -> jdbcAcquirerSqlConnStr,"dbtable" -> tableProcessedTxns,"driver"-> "com.microsoft.sqlserver.jdbc.SQLServerDriver","fetchsize"->"100000")).load()
        //"select *,TAGID, case when recentDt between '2017-03-24' and '2017-03-30' then recentDt else '2017-03-24' end as Mindays1 from ( select a.Tagid, a.recentDt, case when a.recentDt between '2017-03-24' and '2017-03-30' then 1 else 0 end as Event from  (  (SELECT TAGID,max(cast(EXITTXNDATE as date)) as recentDt     from TEST7 where ENTRYPLAZAID='002001' and cast(EXITTXNDATE as date) between '2017-03-24' and  '2017-03-30'   group by TAGID )  union (Select  tagid, max(cast(EXITTXNDATE as date)) as recentDt  from TEST7 where tagid not in (SELECT TAGID     from TEST7 where ENTRYPLAZAID='002001' and cast(EXITTXNDATE as date) between '2017-03-24' and  '2017-03-30') and ENTRYPLAZAID='002001'  group by TAGID ) )a)b"
        jdbcDF7.createOrReplaceTempView("TEST7")
        
              
             
            
        
        val performanceWindowRes=sqlContext.sql("select *, case when recentDt between '2017-03-24' and '2017-03-30' then recentDt else '2017-03-24' end as Mindays1 from ( select a.Tagid, a.recentDt, case when a.recentDt between '2017-03-24' and '2017-03-30' then 1 else 0 end as Event from  (  (SELECT TAGID,max(cast(EXITTXNDATE as date)) as recentDt     from TEST7 where ENTRYPLAZAID='002001' and cast(EXITTXNDATE as date) between '2017-03-24' and  '2017-03-30'   group by TAGID )  union (Select  tagid, max(cast(EXITTXNDATE as date)) as recentDt  from TEST7 where tagid not in (SELECT TAGID     from TEST7 where ENTRYPLAZAID='002001' and cast(EXITTXNDATE as date) between '2017-03-24' and  '2017-03-30') and ENTRYPLAZAID='002001'  group by TAGID ) )a)b  ")
        println("before loading result")
        val tempPerformanceWindowRes= performanceWindowRes
        performanceWindowRes.createOrReplaceTempView("PerformanceWindowRes")
        val tagidList=performanceWindowRes.select("Tagid","Mindays1").rdd.collect().toList
        tagidList.foreach(
            r=>{
              try{
              val tagid=r(0).toString().trim()
              val performanceDate=r(1).toString()
              val startDate=requiredDate(r(1).toString(),7)
            		  val endDate=requiredDate(r(1).toString(),1)
              //val endDate=dateRange(r(1).toString())
             val result= sqlContext.sql(s"select tagid,date_format(b.EXITTXNDATE,'yyyy-MM-dd') as travelDt,sum(DISCOUNTAMOUNT) DISCOUNTAMOUNT from TEST7 b where  b.EXITTXNDATE between '$startDate' and '$endDate' and tagid='$tagid' and EXITPLAZAID='002001'  group by tagid,date_format(b.EXITTXNDATE,'yyyy-MM-dd') LIMIT 1000 ")
             
             result.rdd.foreach(r=>pushToMap(tagid,r(1).toString()))
             
             var trackedHashMapOfTag=hashMap.get(tagid)
             val obsrvation:HashMap[String,String]=dateList(r(1).toString())
             import collection.JavaConversions._
             for((date,sameDate)<-obsrvation){
               if(trackedHashMapOfTag==null)
                 trackedHashMapOfTag=new HashMap[String,Int]
               if(!trackedHashMapOfTag.containsKey(date.toString())){
                 trackedHashMapOfTag.put(date.toString(), 0)
               }
             }
              var discount=""
              val test=result.rdd.isEmpty()
              if(result!=null && !test)
                discount=result.rdd.first()(2).toString()
                else
                  discount="0.0"
              val maxMinDaysDiff=sqlContext.sql(s"select count(distinct date_format(b.EXITTXNDATE, 'MM/dd/yyyy')) as uniquedays,count( date_format(b.EXITTXNDATE, 'MM/dd/yyyy')) as noOfTrips,datediff(max(b.EXITTXNDATE),min(b.EXITTXNDATE)) daysDiff,DATEDIFF('$performanceDate',date_format(max(b.EXITTXNDATE), 'yyyy-MM-dd'))  as minDays,DATEDIFF('$performanceDate',date_format(min(b.EXITTXNDATE), 'yyyy-MM-dd'))  as maxDays,sum(CASE WHEN b.REASONDESC LIKE '%Daily%' THEN 1 else 0  END ) dailyPass  from TEST7 b where tagid='$tagid' and b.EXITTXNDATE between '2016-01-01' and '$endDate'  group by tagid")
               var minDays=""
              var maxDays=""
              var daysDiff=""
              var uniquedays:Int=0
              var noOfTrips:Int=0
              var dailyPass:Int=0
              if(maxMinDaysDiff!=null && !maxMinDaysDiff.rdd.isEmpty()){
              val maxMinDaysDiffResult=maxMinDaysDiff.rdd.first()
              uniquedays=maxMinDaysDiffResult(0).toString().toInt
              noOfTrips=maxMinDaysDiffResult(1).toString().toInt
              daysDiff=maxMinDaysDiffResult(2).toString()
              minDays=maxMinDaysDiffResult(3).toString()
              maxDays=maxMinDaysDiffResult(4).toString()
              if((maxMinDaysDiffResult(5).toString().toInt>0))
              dailyPass=1
              }else{
                minDays="0"
                maxDays="0"
                daysDiff="0"
                
              }
              val predictionObject=PrdictionForLatestModel(r(0).toString(),"002001",requiredDate(r(1).toString(),1),
                  trackedHashMapOfTag.get(requiredDate(r(1).toString(),1)),requiredDate(r(1).toString(),2),trackedHashMapOfTag.get(requiredDate(r(1).toString(),2)),
                  requiredDate(r(1).toString(),3),trackedHashMapOfTag.get(requiredDate(r(1).toString(),3)),requiredDate(r(1).toString(),4),trackedHashMapOfTag.get(requiredDate(r(1).toString(),4)),
                  requiredDate(r(1).toString(),5),trackedHashMapOfTag.get(requiredDate(r(1).toString(),5)),requiredDate(r(1).toString(),6),trackedHashMapOfTag.get(requiredDate(r(1).toString(),6)),
                  requiredDate(r(1).toString(),7),trackedHashMapOfTag.get(requiredDate(r(1).toString(),7)),discount.toString().toDouble,daysDiff.toString().toInt,minDays.toString().toInt,
                  maxDays.toString().toInt,uniquedays,noOfTrips,dailyPass)
                import sqlContext.implicits._
                MongoSpark.save(Seq(predictionObject).toDS())
              
              var daysPropotion:Double=0
              var tempMaxDays:Int=predictionObject.maxDays
                 if(uniquedays!=0){
                		   daysPropotion=(-((predictionObject.uniquedays/tempMaxDays)*100)*0.044259)
                 }
                 var minD:Double=0
                 if(predictionObject.minDays>15)
                   minD=(-0.352331*1)
                   var dayOneTr:Double=(predictionObject.onDayOneisTravelled*(-0.352331))
                   var dayTwoTr:Double=predictionObject.onDayTwoisTravelled*0.20997
                   var dayThreeTr:Double=predictionObject.onDayThreeisTravelled*0.194127
                   var dayFourTr:Double=predictionObject.onDayOneisTravelled*0.186123
                   var dayFiveTr:Double=predictionObject.onDayOneisTravelled*0.105154
                   var daySixTr:Double=predictionObject.onDayOneisTravelled*0.19696
                   var daySevenTr:Double=predictionObject.onDayOneisTravelled*0.274139
                   var dailyPassTr:Double=predictionObject.dailyPass*0.280865
                   var discountTr:Double=predictionObject.discount*0.15187
                   
                   val res:Double=daysPropotion+minD+dayOneTr+dayTwoTr+dayThreeTr+dayFourTr+dayFiveTr+daySixTr+daySevenTr+dailyPassTr+discountTr
                   val predictionPercent=1/(1+Math.exp(res))
                   
                   val prediction=new PredictionForTodayWithNewModel(predictionObject.tagId,"002001",predictionPercent)
                   
              import sqlContext.implicits._
              //MongoSpark.save(Seq(prediction).toDS())
              import com.mongodb.spark.config._
         val writeConfig = WriteConfig(Map("collection" -> "dailyPrediction", "writeConcern.w" -> "majority"), Some(WriteConfig(sparkContext)))
          MongoSpark.save(Seq(prediction).toDS(),writeConfig)
              
             }catch
     {
       
       case oom:OutOfMemoryError=>{
         oom.printStackTrace()
         logger.error(" "+oom.printStackTrace())
       }
       case anyException:Exception=>{
          logger.error(" "+anyException.printStackTrace())
         anyException.printStackTrace()
       }
     }finally {
       println("end of the program")
     }
            }
        )
        
        
        
        
        
        //val rangeDate:HashMap[String,String]=dateRange("2017-03-24")
        //val outOfRangeDate:HashMap[String,String]=dateRange("2017-03-24")
      
    }
  def pushToMap(tagId:String,travelDt:String){
    var newMap=hashMap.get(tagId)
     if(newMap==null)
       newMap=new HashMap[String,Int]
    
    newMap.put(travelDt, 1)
    hashMap.put(tagId, newMap)
    
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
  def dateList(date:String): HashMap[String,String]={
    val dateArr=date.split("-")
    val dateSet: HashMap[String, String] = new HashMap()
    for(i:Int <-1 to 7)
    {
    val calaendar=Calendar.getInstance()
     val year= dateArr.takeRight(4)(0).toString().toInt;
    val month=dateArr.takeRight(4)(1).toInt
    val day=dateArr.takeRight(4)(2).split("T").takeRight(4)(0).toInt
    calaendar.set(year, month-1, day)
   calaendar.add(Calendar.DATE, -i)
    val sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
     val wantedDate = sdf.format(calaendar.getTime());
     dateSet.put(wantedDate, wantedDate) 
    }
    return dateSet;
  }
}