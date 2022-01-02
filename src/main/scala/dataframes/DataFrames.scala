package com.suanki
package dataframes

import org.apache.spark.sql.types.{IntegerType, Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions._

object DataFrames {

  def dfBasic(spark:SparkSession): Unit ={
    val path="C:\\Users\\sujee\\OneDrive\\Documents\\bigdata_and_hadoop\\databricks_udemy\\data\\fligt_data\\"
    spark.conf.set("spark.warehouse.dir","C:\\Users\\sujee\\IdeaProjects\\flightsdata\\src\\main\\resources\\")

    spark.conf.set("spark.sql.shuffle.partitions",5)
    val myschema=StructType(Array(
      StructField("country",StringType,true),
      StructField("count",StringType,true)
    ))

    val test=Seq(Row("India","1234"),Row("France",""))
    def replaceEmptyCols(columns:Array[String]):Array[Column]={
      columns.map(c=>{
        when(col(c)==="" ,null).otherwise(col(c)).alias(c)
      })
    }
    val t=spark.sparkContext.parallelize(test)
    val mydf=spark.createDataFrame(t, myschema)


    val flightSchema=StructType(
      Array(
        StructField("destination",StringType,true),
        StructField("origin",StringType,true),
        StructField("total",IntegerType,true)
      )
    )

    val flightDf=spark.read.options(Map("inferSchema"->"false", "header"->"false"))
      .schema(flightSchema)
      .format("csv")
      .load(path = path)
      .coalesce(2)
      .sortWithinPartitions(col("destination"))
        //filtering the column


    val filterdf=flightDf.where(col("total")>2).where(col("destination")==="United States")



    val distinctRow=filterdf.select(col("destination"), col("origin"), col("total")).distinct()

    //println(s"distinct Rows: ${distinctRow.count()},  totalRow: ${flightDf.count()} , filter row:${filterdf.count()}")



    val sampleDf=flightDf.sample(true,0.2,5).toJSON


    /**
     * This just concatenates the two DataFramess
     * To union two DataFrames, you must be sure that they have the same schema and
     *   number of columns; otherwise, the union will fail
     */

    val dfSchema=StructType(Array(StructField("name",StringType,true), StructField("id",StringType,true)))

    val data1=Seq(
      Row("Rohan","n0173829"),
      Row("sujeet","n017502"),
      Row("rohit","oes189278")
    )

    val data2=Seq(
      Row("amit","t173829"),
      Row("pratik","t17502"),
      Row("nikhil","t189278")
    )

    val sc=spark.sparkContext
    val df1=spark.createDataFrame(sc.parallelize(data1),dfSchema)

    val df2=spark.createDataFrame(sc.parallelize(data2),dfSchema)

    //df1.union(df2).show(10,false)


    /**
     * sort
     * orderBy
     */

//     flightDf.sort(col("total")).show(5,false)

     //flightDf.orderBy(col("total").desc_nulls_first).show(false)

    val one=col("destination").isNull
    val two=col("origin").isNull
    val three=col("total").isNull

    //finding null column

    val totalNullRows=flightDf.where(one || two || three)

    //println(s"totalNullRows:${totalNullRows.count()}")


    //println(s"number of partition:${flightDf.rdd.getNumPartitions}")


    //val flightPart5=flightDf.repartition(5)

    //println(s"number of partition:${flightPart5.rdd.getNumPartitions}")

    //val flightPart2=flightDf.repartition(2)

    //println(s"number of partition:${flightPart2.rdd.getNumPartitions}")


    val flightPart3=flightDf.repartition(5,col("destination"))

    //flightPart3.where(col("destination")==="India").show(false)

    println(flightDf.take(5))

        //    mydf.select(replaceEmptyCols(mydf.columns):_*).toJSON.rdd.saveAsTextFile("\"C:\\\\Users\\\\sujee\\\\Desktop\\\\spark_output\\\\myjson5")

     //or

//      mydf.na.replace("count",Map(""->null)).coalesce(1).write.format("json")
//        .save("C:\\\\Users\\\\sujee\\\\Desktop\\\\spark_output\\\\myjson6")






  }
}
