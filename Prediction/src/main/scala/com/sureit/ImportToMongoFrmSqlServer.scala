package com.sureit
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel._
import com.mongodb.spark.MongoSpark
import org.bson.Document
import java.io.NotSerializableException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.log4j.Logger
object ImportToMongoFrmSqlServer {
  
  
  
  def main(args:Array[String])
    {
    val logger=Logger.getRootLogger()
    //System.setProperty("hadoop.home.dir",  "C:\\SparkSetup\\winutil")
      
      val db = "ISSUER"
      val tableCustPrePdAccounts = "[HISTORY].[TP_CUSTOMER_PREPAIDACCOUNTS]"
      val tableCustAddr = "[tollplus].TP_CUSTOMER_ADDRESSES"
      val tableCustBusiness = "[tollplus].TP_CUSTOMER_BUSINESS"
      val tableCustEmail = "[tollplus].TP_CUSTOMER_EMAILS"
      val tableCustLogins = "[tollplus].TP_CUSTOMER_LOGINS"
      val tableCustPhones = "[tollplus].TP_CUSTOMER_PHONES"
      val tableCustTipes = "[tollplus].TP_CUSTOMERTRIPS"
      
      
      
      val conf = new SparkConf().setAppName("SQLtoJSON").setMaster("local[*]")
      .set("spark.executor.memory", "150g")
      .set("spark.driver.memory", "10g")
      .set("spark.executor.cores", "4")
      .set("spark.driver.cores", "4")
  //  .set("spark.testing.memory", "2147480000")
	    .set("spark.sql.shuffle.partitions", "2000")
	    .set("spark.driver.maxResultSize", "50g")	    
	    .set("spark.memory.offHeap.enabled", "true")
	    .set("spark.memory.offHeap.size", "100g")
	    .set("spark.memory.fraction", "0.75")
	    //.set("spark.mongodb.input.uri", "mongodb://192.168.70.13/mydb.issuer")
	    .set("spark.mongodb.output.uri", "mongodb://192.168.70.13/issuer")
      .set("spark.mongodb.output.collection", "customer_info") //to Get
	    //.set("spark.sql.warehouse.dir", "/home/sagar/spark_sql_warehouse")
	    //.set("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      
      //@transient
      val sparkContext = new SparkContext(conf)
      //@transient
      val sqlContext = new SQLContext(sparkContext)
      import sqlContext.implicits._

      val jdbcSqlConnStr = s"jdbc:sqlserver://192.168.70.15;databaseName=$db;user=vivek;password=welcome123;"      
     
      
      
      val resultCustPrePdAccounts = sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr,"dbtable" -> tableCustPrePdAccounts,"driver"-> "com.microsoft.sqlserver.jdbc.SQLServerDriver")).load()
      val resultCustAddr = sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr,"dbtable" -> tableCustAddr,"driver"-> "com.microsoft.sqlserver.jdbc.SQLServerDriver")).load()
      val resultCustBusiness = sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr,"dbtable" -> tableCustBusiness,"driver"-> "com.microsoft.sqlserver.jdbc.SQLServerDriver")).load()
      val resultCustEmail = sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr,"dbtable" -> tableCustEmail,"driver"-> "com.microsoft.sqlserver.jdbc.SQLServerDriver")).load()
      val resultCustLogins = sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr,"dbtable" -> tableCustLogins,"driver"-> "com.microsoft.sqlserver.jdbc.SQLServerDriver")).load()
      val resultCustPhones = sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr,"dbtable" -> tableCustPhones,"driver"-> "com.microsoft.sqlserver.jdbc.SQLServerDriver")).load()
      
      
  
     resultCustPrePdAccounts.registerTempTable("customer_account")
     resultCustAddr.registerTempTable("customer_address")
     resultCustBusiness.registerTempTable("customer_business")
     resultCustEmail.registerTempTable("customer_email")
     resultCustLogins.registerTempTable("customer_login")
     resultCustPhones.registerTempTable("customer_phones")
     
     try{
     val qryToJnPrPdandAdd = "WITH RowNumberedAccounts AS(select  O.CUSTADDRESSID ,O.ADDRESSTYPE ,O.ADDRESSLINE1 ,O.ADDRESSLINE2 ,O.ADDRESSLINE3 ,O.CITY,O.STATE  ,O.COUNTRY  ,O.ZIP1  ,O.ISACTIVE  ,O.ISCOMMUNICATION  ,O.CREATEDDATE  ,O.CREATEDUSER  ,O.UPDATEDDATE  ,O.UPDATEDUSER  ,O.REASONCODE ,O.ZIP2	,C.ACCOUNTNO		 as C_ACCNO        ,C.CUSTOMERID        ,C.ACCOUNTGROUPID        ,C.PREPAIDACCOUNTSTATUSID        ,C.PREPAIDACCOUNTSTATUSDATE        ,C.SOURCEOFENTRY        ,C.REVENUECATEGORYID        ,C.VEHICLENUMBER        ,C.VEHICLECLASS        ,C.SERIALNO        ,C.HEXTAGID        ,C.TAGSTATUS        ,C.TAGSTARTEFFDATE        ,C.TAGENDEFFDATE        ,C.ISTAGBLACKLISTED        ,C.ISBLACKLISTHOLD        ,C.RCVERIFICATIONSTATUS        ,C.EMAILADDRESS        ,C.PHONENUMBER        ,C.CREATEDDATE AS CCreatedDate  ,C.CREATEDUSER AS CCreatedUser        ,C.UPDATEDDATE AS CUpdatedDate        ,C.UPDATEDUSER AS CUpdatedUser        ,C.HISTID        ,C.ACTION        ,C.ISFEEWAIVER        ,C.FEEWAIVERPASSTYPE        ,C.VEHICLEIMGVERIFICATIONSTATUS        ,C.TAGTID        ,C.ISREVENUERECHARGE        , ROW_NUMBER() OVER (            PARTITION BY C.VEHICLENUMBER            ORDER BY C.TAGSTARTEFFDATE DESC) AS rn    from        customer_account c        LEFT join customer_address o on c.ACCOUNTNO = o.ACCOUNTNO) SELECT    R.* FROM    RowNumberedAccounts AS R WHERE    rn = 1 order by C_ACCNO"
     val resultJoinedPrePdandAddr = sqlContext.sql(qryToJnPrPdandAdd.toString) 
     resultJoinedPrePdandAddr.registerTempTable("joined_acc_add")
     resultJoinedPrePdandAddr.printSchema
     resultJoinedPrePdandAddr.show(10);
     
     
     
    
     val qryToFetchAllCustInfo = "SELECT S.CUSTPHONEID,	S.PHONETYPE,	S.PHONENUMBER AS PHONENUMBER_PHONES,	S.EXTENTION,	S.ISACTIVE AS ISACTIVE_PHONES,	S.ISCOMMUNICATION AS ISCOMMUNICATION_PHONES,R.LOGINID,	R.USERNAME,	R.PASSWORD,	R.LAST_LOGINDATE,	R.LAST_PWD_MODIFIEDDATE,	R.CURRENT_PWD_EXPIRYDATE,	R.PWD_ATTEMPTS_COUNT,	R.PINNUMBER,	R.ISLOCKED,		R.THEMES,	R.LANGUAGES,	R.STATUSID,	R.USERTYPEID,	R.ROLENAME,	R.SQ_ATTEMPTCOUNT,	R.SQ_LOCKOUTTIME,Q.CUSTMAILID,	Q.EMAILTYPE,	Q.EMAILADDRESS AS EMAIL,	Q.ISACTIVE AS ISACTIVE_EMAIL,	Q.ISCOMMUNICATION AS ISCOMMUNICATION_EMAIL,P.ORGANISATIONNAME,	P.DATEOFINCORPORATION,	P.PANCARDNUMBER,	P.ORGANIZATIONTYPEID, C.CUSTADDRESSID ,C.ADDRESSTYPE ,C.ADDRESSLINE1 ,C.ADDRESSLINE2 ,C.ADDRESSLINE3 ,C.CITY,C.STATE  ,C.COUNTRY  ,C.ZIP1  ,C.ISACTIVE  ,C.ISCOMMUNICATION  ,C.CREATEDDATE  ,C.CREATEDUSER  ,C.UPDATEDDATE  ,C.UPDATEDUSER  ,C.REASONCODE ,C.ZIP2	,C.C_ACCNO        ,C.CUSTOMERID        ,C.ACCOUNTGROUPID        ,C.PREPAIDACCOUNTSTATUSID        ,C.PREPAIDACCOUNTSTATUSDATE        ,C.SOURCEOFENTRY        ,C.REVENUECATEGORYID        ,C.VEHICLENUMBER        ,C.VEHICLECLASS        ,C.SERIALNO        ,C.HEXTAGID        ,C.TAGSTATUS        ,C.TAGSTARTEFFDATE        ,C.TAGENDEFFDATE        ,C.ISTAGBLACKLISTED        ,C.ISBLACKLISTHOLD        ,C.RCVERIFICATIONSTATUS        ,C.EMAILADDRESS        ,C.PHONENUMBER        ,C.CREATEDDATE AS CCreatedDate  ,C.CREATEDUSER AS CCreatedUser        ,C.UPDATEDDATE AS CUpdatedDate        ,C.UPDATEDUSER AS CUpdatedUser        ,C.HISTID        ,C.ACTION        ,C.ISFEEWAIVER        ,C.FEEWAIVERPASSTYPE        ,C.VEHICLEIMGVERIFICATIONSTATUS        ,C.TAGTID        ,C.ISREVENUERECHARGE  from        joined_acc_add C    LEFT join  customer_business P on C.C_ACCNO=P.ACCOUNTNO LEFT JOIN customer_email Q  on C.C_ACCNO=Q.ACCOUNTNO LEFT JOIN customer_login R  on C.C_ACCNO=R.ACCOUNTNO LEFT JOIN customer_phones S  on C.C_ACCNO=S.ACCOUNTNO"
     val resultAllCustInfo = sqlContext.sql(qryToFetchAllCustInfo.toString) 
     resultAllCustInfo.registerTempTable("joined_acc_custinfo_trips") //FINAL TABLE OF CUSTOMER TRIPS
     resultAllCustInfo.printSchema
     resultAllCustInfo.show(1)
     
     
     sqlContext.dropTempTable("customer_account")
     sqlContext.dropTempTable("customer_address")
     sqlContext.dropTempTable("customer_business")
     sqlContext.dropTempTable("customer_email")
     sqlContext.dropTempTable("customer_login")
     sqlContext.dropTempTable("customer_phones")

     val qryToCollctAllCustInfo = "SELECT  C_ACCNO AS ACCOUNTNO, VEHICLENUMBER, collect_set(struct(CUSTOMERID,ACCOUNTGROUPID,PREPAIDACCOUNTSTATUSID,PREPAIDACCOUNTSTATUSDATE,SOURCEOFENTRY,REVENUECATEGORYID,VEHICLECLASS,SERIALNO,HEXTAGID,TAGSTATUS,TAGSTARTEFFDATE,TAGENDEFFDATE,ISTAGBLACKLISTED,ISBLACKLISTHOLD,RCVERIFICATIONSTATUS,EMAILADDRESS,PHONENUMBER,ISFEEWAIVER,FEEWAIVERPASSTYPE,VEHICLEIMGVERIFICATIONSTATUS,TAGTID,ISREVENUERECHARGE,CCreatedDate,CCreatedUser,CUpdatedDate,CUpdatedUser)) as VEHICLEINFO,  collect_set(struct(CUSTADDRESSID ,ADDRESSTYPE ,ADDRESSLINE1 ,ADDRESSLINE2 ,ADDRESSLINE3 ,CITY,STATE  ,COUNTRY  ,ZIP1  ,ISACTIVE  ,ISCOMMUNICATION  ,CREATEDDATE  ,CREATEDUSER  ,UPDATEDDATE  ,UPDATEDUSER  ,REASONCODE ,ZIP2)) as ADDRESS, collect_set(struct(ORGANISATIONNAME,DATEOFINCORPORATION,PANCARDNUMBER,ORGANIZATIONTYPEID)) as BUSINESS, collect_set(struct(CUSTMAILID,EMAILTYPE,EMAIL,ISACTIVE_EMAIL,ISCOMMUNICATION_EMAIL)) as EMAIL, collect_set(struct(LOGINID,	USERNAME,	PASSWORD,	LAST_LOGINDATE,	LAST_PWD_MODIFIEDDATE,	CURRENT_PWD_EXPIRYDATE,	PWD_ATTEMPTS_COUNT,	PINNUMBER,	ISLOCKED,THEMES,LANGUAGES,	STATUSID,	USERTYPEID,	ROLENAME,	SQ_ATTEMPTCOUNT,	SQ_LOCKOUTTIME)) as LOGIN, collect_set(struct(CUSTPHONEID,	PHONETYPE, PHONENUMBER_PHONES,	EXTENTION, ISACTIVE_PHONES,	ISCOMMUNICATION_PHONES)) as PHONES FROM joined_acc_custinfo_trips  GROUP BY ACCOUNTNO, VEHICLENUMBER"

     val resultOfCollctAllCustInfo = sqlContext.sql(qryToCollctAllCustInfo.toString) 
     resultOfCollctAllCustInfo.registerTempTable("joined_acc_custinfo_trips_actual") 
     sqlContext.dropTempTable("joined_acc_custinfo_trips")
     
     val qryToCollctActualCustInfo = "SELECT ACCOUNTNO, COLLECT_SET(STRUCT(VEHICLENUMBER,VEHICLEINFO)) AS VEHICLE, COLLECT_SET(STRUCT(ADDRESS, BUSINESS, EMAIL, LOGIN, PHONES)) AS CUSTOMER FROM joined_acc_custinfo_trips_actual GROUP BY ACCOUNTNO ORDER BY ACCOUNTNO"
     
     val resultOfCollctActualCustInfo = sqlContext.sql(qryToCollctActualCustInfo.toString) 
        resultOfCollctActualCustInfo.printSchema
        MongoSpark.save(resultOfCollctActualCustInfo)
        sqlContext.dropTempTable("joined_acc_custinfo_trips_actual")
        
        
        val jdbcDF7 = sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr,"dbtable" -> tableCustTipes,"driver"-> "com.microsoft.sqlserver.jdbc.SQLServerDriver")).load()
        jdbcDF7.registerTempTable("customer_trips") 
        
          // @transient     
        val distincDF=jdbcDF7.select(jdbcDF7.col("VEHICLESTATE")).distinct.rdd.map(r => r(0)).collect.toList
        distincDF.foreach(value=>
          {
            val plazaName=value
        val tripsQuery=s"select * from customer_trips where VEHICLESTATE='$plazaName'"
         val tripResult = sqlContext.sql(tripsQuery.toString)
         import com.mongodb.spark.config._
         val writeConfig = WriteConfig(Map("collection" -> "cust_trips", "writeConcern.w" -> "majority"), Some(WriteConfig(sparkContext)))
          MongoSpark.save(tripResult,writeConfig)
          })
  
     }catch
     {
       case syntaxError:ParseException=>{
         println("Error in query "+syntaxError.getMessage())
         logger.error("Error in query error is "+syntaxError.getMessage())
         syntaxError.printStackTrace()
       }
       case oom:OutOfMemoryError=>{
         logger.error("OutOfMemory")
         oom.printStackTrace()
       }
       case notSerEx:NotSerializableException=>{
         logger.error("NotSerializable")
         notSerEx.printStackTrace()
       }
       case anyException:Exception=>{
         logger.error("Exception "+anyException)
         anyException.printStackTrace()
       }
     }finally {
       println("end of the program")
     }
    }
  
  
  
}