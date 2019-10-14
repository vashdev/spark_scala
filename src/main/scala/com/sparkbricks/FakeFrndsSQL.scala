
// Example of SQL read Write   ...
package com.sparkbricks

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
// import org.apache.spark.sql.types.{DataType,IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types._
object FakeFrndsSQL {
  def main(args: Array[String]): Unit = {
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
    val spark  = SparkSession.builder.appName("fakeFriendsSTDSQL").master(args(0)).getOrCreate()
    val lines = spark.read.format("csv").option("header", "false").schema(buildSchema).load(args(1))
    // lines.sqlContext.sql("select * from lines").show()  --> tbl or view not found error
    lines.createOrReplaceTempView("people")
    spark.sql("select * from people").show()
    lines.write.csv(args(2))
    lines.write.parquet(args(2)+"_parquet")
  }
}
