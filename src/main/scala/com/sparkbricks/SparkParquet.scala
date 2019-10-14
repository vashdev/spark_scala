/*
example of using parquet files
args(0) -> execution mode
args(1) -> input path
args(2) -> output path
args(3) -> spark.sql.warehouse.dir   value
 */

package com.sparkbricks

import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.types.{DataType,IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types._
object SparkParquet {
 def main(args: Array[String]): Unit = {

    val spark  = SparkSession.builder.appName("SparkParquet").master(args(0)).config("spark.sql.warehouse.dir", args(3)).getOrCreate()
    import spark.implicits._

    /**
      * Build and return a schema to use for the sample data.
      * the StructType is the schema class, and it contains a StructField for each column of data. Each StructField provides the column name, preferred data type, and whether null values are allowed.
      */
    //case class Person(ID:Int, name:String, age:Int, numFriends:Int)
   def buildSchema() : StructType = {
     val schema = StructType(
       Array(

         StructField("name", StringType, true),
         StructField("age", IntegerType, true)

       )
     )
     schema
   }

   // Read Json with custom Schema
   val lines = spark.read.schema(buildSchema).json(args(1))
       lines.write.parquet(args(2))
   // read json with inferred schema
   val peopleDF = spark.read.json(args(1))


   // read the data
   lines.createOrReplaceTempView("people")
   spark.sqlContext.sql(" select count(*) as explicitcount from people").show()

   peopleDF.createOrReplaceTempView("peopleDF")
   spark.sqlContext.sql(" select count(*) as implicitCount from peopleDF").show()

   //write the parquet files
   lines.write.parquet(args(2)+"withexplicitSchema")
   peopleDF.write.parquet(args(2)+"withimplicitSchema")

   println(" ***** read the parquet files back ****************")

   val parqexplicitDF=spark.read.parquet(args(2)+"withexplicitSchema")
   parqexplicitDF.createOrReplaceTempView("parqexplicitDFTbl")
   spark.sqlContext.sql(" select count(*) as inferredCount from parqexplicitDFTbl").show()

   val parqimplicitDF=spark.read.parquet(args(2)+"withimplicitSchema")
   peopleDF.createOrReplaceTempView("peopleDFimplicit")
   spark.sqlContext.sql(" select count(*) as implicitCount from peopleDFimplicit").show()

    spark.stop()
  }
}
