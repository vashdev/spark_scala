/*
example of defining schema for - ROW RDD have to parse all rows thru schema
args(0) -> execution mode
args(1) -> input path
args(2) -> output path
args(3) -> spark.sql.warehouse.dir   value
 */

package com.sparkbricks
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types._
object FakeFrndSchemBindSQL {
  def main(args: Array[String]): Unit = {

    val spark  = SparkSession.builder.appName("fakeFriendsSQLSchemaBind").master(args(0)).config("spark.sql.warehouse.dir", args(3)).getOrCreate()
    import spark.implicits._
    /**
      * Build and return a schema to use for the sample data.
      * the StructType is the schema class, and it contains a StructField for each column of data. Each StructField provides the column name, preferred data type, and whether null values are allowed.
      */
    case class Person(ID:Int, name:String, age:Int, numFriends:Int)
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
      // now dataframes can take this schema for ROW BASED Rdds
      val lines = spark.read.format("csv").option("header", "false").schema(buildSchema).load(args(1))
    lines.createOrReplaceTempView("people")
    println("Here is our inferred schema:")
    spark.sql("describe people").show()

    println("Let's select the name column:")
    spark.sql("select name from people").show(10)

    println("Filter out anyone over 21:")
    spark.sql(" select * from people where age < 21").show(10)

    println("Group by age:")
    spark.sql(" select age,count(1) from people group by age")

    println("Make everyone 10 years older:")
    val finalrdd=spark.sql(" select  name, age+10 from people")

    finalrdd.toJavaRDD.saveAsTextFile(args(2))

    spark.stop()
  }
}
