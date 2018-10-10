package com.sureit.prediction
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql
import org.apache.spark.sql._
import java.io.NotSerializableException
import org.apache.spark.sql.catalyst.parser.ParseException
import java.io.File
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Row
import scala.collection.mutable.WrappedArray
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.HashMap
import java.util.ArrayList
import org.bson.Document
import java.lang.Exception

case class PredictVehicle(tagId:String,plazaId:String,dayOne:String,onDayOneisTravelled:Int,
    dayTwo:String,onDayTwoisTravelled:Int,dayThree:String,onDayThreeisTravelled:Int,dayFour:String,onDayFourisTravelled:Int,
    dayFive:String,onDayFiveisTravelled:Int,daySix:String,onDaySixisTravelled:Int,daySeven:String,onDaySevenisTravelled:Int)
object VehiclePrediction {
  var hashMap:HashMap[String,HashMap[String,Int]]=new HashMap[String,HashMap[String,Int]]
  def main(args:Array[String]){
    
    try{
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
      .set("spark.mongodb.input.collection", "vehicl_list_qualified_trips_info") //to Get vehicl_list_qualified_trips_info
      .set("spark.mongodb.output.uri", "mongodb://192.168.70.13/ACQUIRER")
      .set("spark.mongodb.output.collection", "latest_first_new_test_lastSevenDays_Details") //to Get
	    //.set("spark.sql.warehouse.dir", "/home/sagar/spark_sql_warehouse")
	    //.set("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      
      //@transient
      val sparkContext = new SparkContext(conf)
      
       val sqlContext = new SQLContext(sparkContext)
      
      val df = MongoSpark.load(sqlContext)
  df.createOrReplaceTempView("PredictVehicle")
  println(" args.length "+args.length)
  if(args.length==0){
    throw new Exception("Prediction Date must be mandatoy in format(2017-05-02T11:20:07.447+05:30) ")
  }
      val endDt=args(0) // sample input 2017-05-02T11:20:07.447+05:30
  val result=getSubtractedDate(endDt)
  val startDt=result._1
  println("endDt "+endDt)
  
  //result._2.forEach(key)
  
  //below is for one tag
   val dayOne=result._3.get(7)
  val dayTwo=result._3.get(6)
  val dayThree=result._3.get(5)
  val dayFour=result._3.get(4)
  val dayFive=result._3.get(3)
  val daySix=result._3.get(2)
  val daySeven=result._3.get(1)
  println("dayOne "+dayOne+" daySeven "+daySeven)
  //scenario-1  //TAGID='91890704804000001F91' and 
  val res=sqlContext.sql("select INBOUNDPROCESSEDTXNID,TAGID, date_format(TRIPPOSTEDDATE, 'MM/dd/yyyy') as travelDate,EXITPLAZAID from PredictVehicle   where  TRIPPOSTEDDATE between '"+startDt+"' and '"+endDt+"'")    
  val distinctPlazas=res.select("EXITPLAZAID").distinct().rdd.collect().toList
  
  res.createOrReplaceTempView("PlazaBasedInformation")
  distinctPlazas.foreach(plazaId=>{ 
    println("plazaId "+plazaId(0))
    
  val plazaBaseResult=sqlContext.sql("select TAGID, travelDate,EXITPLAZAID from PlazaBasedInformation where EXITPLAZAID="+plazaId(0))
  
  plazaBaseResult.select("TAGID","travelDate").rdd.foreach(r => pushToHashMap(result._2,r(0),r(1)))
  import collection.JavaConversions._
  
  val finalResult=hashMap.map(result=>{
    new PredictVehicle(result._1,plazaId(0).toString(),dayOne,result._2.get(dayOne),
        dayTwo,result._2.get(dayTwo),dayThree,result._2.get(dayThree),dayFour,result._2.get(dayFour),
        dayFive,result._2.get(dayFive),daySix,result._2.get(daySix),daySeven,result._2.get(daySeven))
    
  })
  import sqlContext.implicits._
  val outPut=finalResult.toSeq.toDS()
  MongoSpark.save(outPut)
  hashMap.empty
  })
      }catch
     {
       case syntaxError:ParseException=>{
         println("Error in query "+syntaxError.getMessage())
         syntaxError.printStackTrace()
       }
       case oom:OutOfMemoryError=>{
         oom.printStackTrace()
       }
       case notSerEx:NotSerializableException=>{
         notSerEx.printStackTrace()
       }
       case anyException:Exception=>{
         anyException.printStackTrace()
       }case throwable: Throwable=>{
         throwable.getMessage()
       }
     }finally {
       println("end of the program")
     }
  }
  
   def getSubtractedDate(date:String) : (String,HashMap[String,String],HashMap[Int,String]) ={
    val dateArr=date.split("-")
    
    val mapKeyasDtValueasDt: HashMap[String, String] = new HashMap()
    val mapKeyasDayValueasDt: HashMap[Int, String] = new HashMap()
    
    
    val year= dateArr.takeRight(4)(0).toString().toInt;
    val month=dateArr.takeRight(4)(1).toInt
    val day=dateArr.takeRight(4)(2).split("T").takeRight(4)(0).toInt
    
    val calaendar=Calendar.getInstance()
    val length= 7
    for(i:Int <-1  to length){
  calaendar.set(year, month-1, day)
  println("i => "+i)
  calaendar.add(Calendar.DATE, -i)
  val sdf = new SimpleDateFormat("MM/dd/yyyy", Locale.ENGLISH);
     val wantedDate = sdf.format(calaendar.getTime());
     mapKeyasDtValueasDt.put(wantedDate,wantedDate)
     mapKeyasDayValueasDt.put(i, wantedDate)
    }
    calaendar.set(year, month-1, day)
  calaendar.add(Calendar.DATE, -7)
  val sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
     val wantedDate = sdf.format(calaendar.getTime());
     mapKeyasDtValueasDt.put(wantedDate,wantedDate)
     return (wantedDate,mapKeyasDtValueasDt,mapKeyasDayValueasDt);
  }
  
   def predictHashMap(list:List[Any],hashMap:HashMap[String,String]): HashMap[String,Int]={
    import collection.JavaConversions._
    //for ((k,v) <- hashMap) println(s"key: $k, value: $v")
    
    val newMap=new HashMap[String,Int]
    for((st:String,et:String)<-hashMap){
      
      if(list.contains(st.trim())){
    	  println("OnDay "+st+" ishetravelled 1")
        newMap.put(st, 1)
      }else{
    	  println("OnDay "+st+" ishetravelled 0")
        newMap.put(st, 0)
      }
    }
    return newMap;

  }
   
   
   def  pushToHashMap(hashMapObj:HashMap[String,String],tagId:Any,travelledDt:Any){
     import collection.JavaConversions._
     var newMap=hashMap.get(tagId)
     if(newMap==null)
       newMap=new HashMap[String,Int]
     //}else{
     //for((travelledDt:String,et:Int)<-newMap){
      if(!newMap.containsKey(travelledDt)){
      if(hashMapObj.containsKey(travelledDt.toString().trim())){
    	  println(tagId+" OnDay "+travelledDt+" ishetravelled 1")
        newMap.put(travelledDt.toString(), 1)
      }else{
    	  println(tagId+" OnDay "+travelledDt+" ishetravelled 0")
        newMap.put(travelledDt.toString(), 0)
      }
      //}
     //}
        hashMap.put(tagId.toString(), newMap)
    }
   }
}