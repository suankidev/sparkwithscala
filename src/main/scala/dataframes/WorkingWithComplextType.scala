package com.suanki
package dataframes

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{array_contains, col, collect_list, collect_set, explode, expr, lit, map, map_keys, map_values, regexp_replace, size, split, struct, udf}

object WorkingWithComplextType {


  def workingWithComplextType(spark: SparkSession, df: Dataset[Row], retailDf: Dataset[Row]) = {



    // df.show(false)
    retailDf.show(false)

    /**
     *
     * You can think of structs as DataFrames within DataFrames.
     *
     *
     * Demo data
     * +---------+---------+-----------------------------------+--------+-------------------+---------+----------+--------------+
     * |InvoiceNo|StockCode|Description                        |Quantity|InvoiceDate        |UnitPrice|CustomerID|Country       |
     * +---------+---------+-----------------------------------+--------+-------------------+---------+----------+--------------+
     * |537226   |22811    |SET OF 6 T-LIGHTS CACTI            |6       |2010-12-06 08:34:00|2.95     |15987.0   |United Kingdom|
     * |537226   |21713    |CITRONELLA CANDLE FLOWERPOT        |8       |2010-12-06 08:34:00|2.1      |15987.0   |United Kingdom|
     * |537226   |22927    |GREEN GIANT GARDEN THERMOMETER     |2       |2010-12-06 08:34:00|5.95     |15987.0   |United Kingdom|
     * |537226   |20802    |SMALL GLASS SUNDAE DISH CLEAR      |6       |2010-12-06 08:34:00|1.65     |15987.0   |United Kingdom|
     */


    val myUdf = (a: Boolean, b: String) => {
      if (a) {
        "confidential"
      }
      else {
        b
      }

    }


    val d = udf(myUdf)


    spark.udf.register("sqlFun", d)

    val mulCol = (col("invoiceno"), col("customerid"))

    retailDf.select(
      struct(col("invoiceno"), col("customerid"), col("description")).alias("structType"),
      col("Country")
    ).withColumn("test",
      (col("structType").getField("invoiceno") === "537226")
    ).withColumn("replaced", d(col("test"), col("country")))

    //  .show(false)


    /**
     * To define arrays, let’s work through a use case. With our current data, our objective is to take
     * every single word in our Description column and convert that into a row in our DataFrame
     * This is quite powerful because Spark allows us to manipulate this complex type as another
     * column
     *
     */


    retailDf.select(col("Description"))
      .withColumn("splitted", split(col("Description"), " "))
      .withColumn("arraysize", size(col("splitted")))
      .withColumn("contains", array_contains(col("splitted"), "RED"))
      .withColumn("t1", col("splitted")(0))
      .show(false)


    //explode

    retailDf.select(col("Description")).withColumn("splitted", split(col("description"), " "))
      .withColumn("exploded", explode(col("splitted")))
    //.show(false)


    //udf
     def convertToPower(n:Int,n1:Int):Int=n*n1

     val pw=udf(convertToPower(_:Int,_:Int):Int)
     spark.udf.register("pw",pw)

    val t = udf(
      (b: Int,c:Int) => {
        b * c
      }
    )
    spark.range(1, 20).toDF("id")
      .withColumn("test", pw(col("id"),lit("10")))
      .withColumn("map1",map(col("id"),col("test")))
      .withColumn("extractedfrommap", map_values(col("map1")))
      .withColumn("test1", map_keys(col("map1")))
      .show(false)


    print("\n")

  }


}
