
// Same as FakeFrndsSQL  with hive enable
//fakefriends_clean.txt as input for HIVE as it snot loading the original csv some format issue
package com.sparkbricks
import org.apache.spark.sql.execution._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.internal.SQLConf._
import org.json4s.jackson.Json

object FakeFndsHive {
  def main(args: Array[String]): Unit = {
 def toDebug( spark:org.apache.spark.sql.SparkSession):Unit ={
  if(args(3)==true) {
    println("get all defaults " + spark.conf.getAll)

    println("Default preferSortMergeJoin " + spark.sessionState.conf.preferSortMergeJoin)
    println("autoBroadcastJoinThreshold " + spark.sessionState.conf.autoBroadcastJoinThreshold)
    println("cboEnabled " + spark.sessionState.conf.cboEnabled)
    println("sessionLocalTimeZone " + spark.sessionState.conf.sessionLocalTimeZone)
    println("Adjusted shuffle partitions " + spark.sessionState.conf.numShufflePartitions)

  }
}
    // define schema ...
    def buildSchema() : StructType = {
      val schema = StructType(
        Array(
          StructField("ID", IntegerType, true),
          StructField("name", StringType, true),
          StructField("age", IntegerType, true),
          StructField("numFriends", IntegerType, true)
        )
      )
      schema
    }

    // create sprksqlcontext - set conf --not required after 2.0 session == context
    // val sparkconf= new SparkConf().setAppName("SQLCTX")
    //                                 .setMaster(args(0))
    // set spark context --not required after 2.0 session == context
    // val sparkctx= SparkContext.getOrCreate(sparkconf)
    // set spark sql session
    // to instantiate SparkSession with Hive support add sbt libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.3"
    //without setting shuffle partitions ...
    val spark  = SparkSession.builder.
                        appName("fakeFriendsSTDSQL")
                         .enableHiveSupport()                     // *** Prep for HIVE
                          .master(args(0)).getOrCreate()
    spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, 2)
       // give env info
    toDebug(spark)
    // We cna run hive queries local path   works for non s3 for now ... s3 with Load needs s3 to be set up as FS
    // file format has issue runs in ec2 after dos2unix conversion and cleanup
    spark.sql("CREATE TABLE IF NOT EXISTS cleanfakes(ID INT, name STRING, x INT, y INT) ROW FORMAT DELIMITED FIELDS TERMINATED  BY ',' LINES TERMINATED BY '\n'      ")
    spark.sql("LOAD DATA LOCAL INPATH '"+args(1)+"'  INTO TABLE cleanfakes")
    spark.sql(" ANALYZE TABLE cleanfakes COMPUTE STATISTICS FOR COLUMNS  ID")

    //spark.sql( "select count(*) from cleanfakes").show()
    val morethen100 = spark.sql( "select * from cleanfakes ")
    val morethen100grpd = spark.sql( "select x,y ,count(1) from cleanfakes group by x,y")
    val dataDir=args(2)
   // morethen100.write.parquet(dataDir)
    //morethen100.write.partitionBy("x").parquet("C:\\sparkscala\\SparkSBT\\ParthPaqr_sample")
    spark.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS hive_records_cln(ID INT, name STRING, x INT, y INT) STORED AS PARQUET  LOCATION '$dataDir' " )
    // spark.sql(" select * from hive_records_cln").show()
    // get nth rec
   //  println(spark.conf.get("spark.default.parallelism"))  exception no key
    // println(spark.conf.get("spark.sql.shuffle.partitions"))
    println("get default config ")
    spark.sparkContext.getConf.getAll.map(x=>println(x))
    println(" config Details ...")
    println(spark.sessionState.conf.getAllConfs)
    println("get RDD info ")
    println(morethen100grpd.rdd.toDebugString)    // 2 partitions from 17 before shuffle partns are set  ? (the number in brackets ..so when query is executed so many tasks get created and destroyed - wow based on spark.sql.shuffle.partitions
    println(" Plan details ..")
    println(morethen100grpd.explain(true))


    //org.apache.spark.internal.config.
    val r2= spark.sql( "select * from cleanfakes ")
    val r5= spark.sql( "select * from cleanfakes limit 5")
    r2.createOrReplaceTempView("r2")
    r5.createOrReplaceTempView("r5")
    // spark.sql( "select * from r2 cross join r5 on r5.x=r2.x ").show()
    // println(r5.rdd.toDebugString)

    spark.sql(" truncate table fakes")


  }
}
