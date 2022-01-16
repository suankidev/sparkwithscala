package com.suanki.init

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.log4j.{Appender, ConsoleAppender, Layout, Level, LogManager, SimpleLayout}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.suanki.scalaBasics.ForAwayCode
import com.suanki.rdds.{AdvanceRdds, Rdds}
import com.suanki.dataframes.{AggregationLearn, DataFrames, WorkingWithComplextType, WorkingWithDifferentTypeChapter6}


object Main {

  def main(args: Array[String]) = {

    println(s"arguments are ${args(0)}")

    val spark: SparkSession = {
      if (args(0) == "local")
        local
      else
        prod
    }

    spark.conf.set("spark.sql.shuffle.partitions", 5)

    val flightSchema = StructType(
      Array(
        StructField("destination", StringType, true),
        StructField("origin", StringType, true),
        StructField("total", IntegerType, true)
      )
    )

    val mydataFrame = mydf(spark)

    val path = "C:\\Users\\sujee\\OneDrive\\Documents\\bigdata_and_hadoop\\databricks_udemy\\data\\fligt_data\\"
    spark.conf.set("spark.warehouse.dir", "C:\\Users\\sujee\\IdeaProjects\\flightsdata\\src\\main\\resources\\")

    val flightDf = spark.read.options(Map("inferSchema" -> "false", "header" -> "false"))
      .schema(flightSchema)
      .format("csv")
      .load(path = path)
      .coalesce(2)
      .sortWithinPartitions(col("destination")).na.drop()

    val retailDf = spark.read.options(Map("inferSchema" -> "true", "header" -> "true"))
      .format("csv")
      .load(path = "C:\\Users\\sujee\\Desktop\\spark_input\\retail-data\\")
      .coalesce(2)

    //testing the codes in scala
    //new ForAwayCode().display

    //learning dataframes basics
    // DataFrames.dfBasic(spark:SparkSession)


    //chapter 6 Working with different types
    //WorkingWithDifferentTypeChapter6.lesson(spark, flightDf, retailDf, mydataFrame)


    //Working with complex data type like list. collect list and map ...etc
    //WorkingWithComplextType.workingWithComplextType(spark = spark, df=mydf(spark), retailDf = retailDf)


    //working with aggregation in dataFrame
   // AggregationLearn.aggregationLearn(spark, retailDf,mydf(spark))

    //joing in spark
   // import dataframes.Joines
   // Joines.joins(spark)


    //Data sources
    //import dataframes.DataSources
   // DataSources.code(spark)

    //working with rdds
     //Rdds.rdds(spark = spark,mydf(spark))
     AdvanceRdds.code(spark=spark,mydf(spark))
  }

  def prod: SparkSession = {
    val spark = SparkSession.builder().master("yarn").appName("prod App").enableHiveSupport()
      .getOrCreate()
    spark
  }

  def local: SparkSession = {
    val spark = SparkSession.builder().master("local[2]").appName("local App").getOrCreate()
    spark
  }

  def mydf(spark: SparkSession) = {


    val data = Seq(
      Row("Surendra", "9860867225", "Rajasthan", 9000),
      Row("Sujeet", "7355609142", "Gorakhpur", null),
      Row("Aaditya", "3736833333", null, 3648),
      Row("trakashur", null, null, null),
      Row(null, null, null, null)
    )

    val schemaForData = new StructType(
      Array(
        StructField("name", StringType, true),
        StructField("mobile", StringType, true),
        StructField("district", StringType, true),
        StructField("roll", IntegerType, true)

      )
    )


    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df2 = simpleData.toDF("employee_name", "department", "salary")


    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data), schemaForData)


    (df1, df2)
  }


}