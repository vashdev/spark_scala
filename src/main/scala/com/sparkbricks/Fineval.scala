package com.sparkbricks
import org.apache.spark.sql.execution._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.internal.SQLConf._

object Fineval {
  def main(args: Array[String]): Unit = {


    def toDebug(spark: org.apache.spark.sql.SparkSession): Unit = {

        println("get all defaults " + spark.conf.getAll)
        println("Default preferSortMergeJoin " + spark.sessionState.conf.preferSortMergeJoin)
        println("autoBroadcastJoinThreshold " + spark.sessionState.conf.autoBroadcastJoinThreshold)
        println("cboEnabled " + spark.sessionState.conf.cboEnabled)
        println("sessionLocalTimeZone " + spark.sessionState.conf.sessionLocalTimeZone)
        println("Adjusted shuffle partitions " + spark.sessionState.conf.numShufflePartitions)


          }
    val spark  = SparkSession.builder.
      appName("finraeval")
      .enableHiveSupport()                     //  Prep for HIVE
      .master("local[2]").getOrCreate()
    spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, 2)
    // give env info
    toDebug(spark)
    import spark.implicits._
    val dtDF =Seq("20190825","20190826","20190827","20190828","20190829","20190830","20190831","20190901").toDF("original_date")
    // Create Hive equivalant table  create tbl if not exists .....
    dtDF.createOrReplaceTempView("dtview")

     spark.sql("  select original_date_hive, CASE WHEN weekday !=1 THEN date_add(original_date_hive,8-weekday) ELSE  original_date_hive END  end_of_week , last_day(original_date_hive)  end_of_month  from ( select   original_date_hive , extract(dayofweek from original_date_hive  ) weekday from  ( select original_date,  from_unixtime(unix_timestamp(original_date, 'yyyyMMdd'), 'yyyy-MM-dd') original_date_hive  from  dtview) ht ) X  ").show()

    val actDF =Seq( ("100","2019-10-03 10:00:00","2019-10-03 12:00:00"),
      ("100" ,"2019-10-03 10:15:00" ,"2019-10-03 12:30:00"),
      ("100", "2019-10-03 12:15:00" ,"2019-10-03 12:45:00"),
      ("100", "2019-10-03 13:00:00" ,"2019-10-03 14:00:00"),
      ("200", "2019-10-03 10:15:00", "2019-10-03 10:30:00")).toDF("id","start_ts","end_ts")
    actDF.createOrReplaceTempView("activity")

    // spark.sql(" select id, start_ts, unix_timestamp(start_ts) unixstart_ts, end_ts, unix_timestamp(end_ts) unixstart_ts,, LEAD() OVER( PARTITION BY id , ORDER BY start_ts from activity ").show()
     spark.sql(" select id , start_ts, end_ts ,dense_rank( (unix_timestamp(start_ts) <= unix_timestamp(lead_endts)) and (unix_timestamp(lead_startts) <= unix_timestamp(end_ts))  ) OVER( PARTITION BY id ORDER BY unix_timestamp(start_ts) ) GROUP_ID  from ( select id, start_ts, end_ts,  LEAD(start_ts,1)  OVER( PARTITION BY id  ORDER BY unix_timestamp(start_ts)  ) lead_startts  , LEAD(end_ts,1)  OVER( PARTITION BY id ORDER BY unix_timestamp(start_ts) ) lead_endts from activity " +
      " ) subactvty   ").show()

    val movieDF =Seq( ("Leonardo DiCaprio","The Revenant"), ("x","The Revenant1"),("x","The Revenant2"),("x","The Revenant3"),("Leonardo DiCaprio","The Great Gatsby"),  ("Tom Cruise","Mission Impossible"), ("Samuel L. Jackson","Pulp Fictiont")).toDF("actor","movie")
    movieDF.createOrReplaceTempView("film")

    spark.sql( "   select actor, movie,total ,rnk from ( select actor,movie,total,  DENSE_RANK() OVER( PARTITION BY ACTOR  order by total DESC)  rnk from (   select  actor, movie , sum(1)  OVER ( PARTITION BY actor  ORDER BY movie ) total  from film  ) x  ) y where rnk=1  limit 2").show()
  spark.stop()
  }
}
