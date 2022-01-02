package com.suanki
package dataframes

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, collect_list, collect_set, count, countDistinct, dense_rank, desc, expr, max, min, rank, regexp_extract, row_number, sum, udf, window}

import java.awt.Window

object AggregationLearn {
  /**
   *
   * In addition to working with any type of values, Spark also allows us to create the following
   * groupings types:
   * The simplest grouping is to just summarize a complete DataFrame by performing an
   * aggregation in a select statement.
   * A “group by” allows you to specify one or more keys as well as one or more
   * aggregation functions to transform the value columns.
   * A “window” gives you the ability to specify one or more keys as well as one or more
   * aggregation functions to transform the value columns. However, the rows input to the
   * function are somehow related to the current row.
   * A “grouping set,” which you can use to aggregate at multiple different levels. Grouping
   * sets are available as a primitive in SQL and via rollups and cubes in DataFrames.
   * A “rollup” makes it possible for you to specify one or more keys as well as one or more
   * aggregation functions to transform the value columns, which will be summarized
   * hierarchically.
   * A “cube” allows you to specify one or more keys as well as one or more aggregation
   * functions to transform the value columns, which will be summarized across all
   * combinations of columns.
   * Each grouping returns a RelationalGroupedDataset on which we specify our aggregations.
   *
   * @param spark
   * @param retailDf
   */

  def aggregationLearn(spark: SparkSession, retailDf: Dataset[Row], myDf: (Dataset[Row], Dataset[Row])) = {


    println("=" * 10 + "Start of the Aggregation" + "=" * 10 + "\n")

    //   retailDf.show(5)

    /** -----Sample DATA--------
     * +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
     * |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
     * +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
     * |   537226|    22811|SET OF 6 T-LIGHTS...|       6|2010-12-06 08:34:00|     2.95|   15987.0|United Kingdom|
     * |   537226|    21713|CITRONELLA CANDLE...|       8|2010-12-06 08:34:00|      2.1|   15987.0|United Kingdom|
     * |   537226|    22927|GREEN GIANT GARDE...|       2|2010-12-06 08:34:00|     5.95|   15987.0|United Kingdom|
     * |   537226|    20802|SMALL GLASS SUNDA...|       6|2010-12-06 08:34:00|     1.65|   15987.0|United Kingdom|
     * |   537226|    22052|VINTAGE CARAVAN G...|      25|2010-12-06 08:34:00|     0.42|   15987.0|United Kingdom|
     * +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
     */


    val df1 = myDf._1
    val df2 = myDf._2


    //df1.show()

    //df2.show()

    val finalDf = retailDf.coalesce(2).cache()

    finalDf.createTempView("retaildata")

    //eagerly executed
    //print(finalDf.count())


    //lazy count
    finalDf.select(count("*")) //.show()
    spark.sql("select count(*) from retaildata") //.show()

    finalDf.select(countDistinct("stockcode")) //.show()
    finalDf.select(approx_count_distinct("stockcode", 0.1)) //.show()


    /**
     * first(col("stockcode")
     * last(col("stockcode")
     * min(col("stockcode")
     * max(col("stockcode")
     * sum(col("quantity")
     * sumDistinct(col("qunatity")
     * avg(col("qunatitiy")
     * df.describe()        --to see min max stddeviation,
     * mean(col("quantity")
     * */

    /**
     * Aggregating to Complex Types
     */


    //  val d=udf(
    //      (a:List[String])=>{
    //        //println(a.mkString("(","|",")"))
    //
    //        val rslt=a.foreach(
    //          (a:String)=>{
    //            println(s"checking for ${a}")
    //            if (a=="United States"){
    //              a
    //            }
    //            else {
    //              "India"
    //            }
    //          }
    //        )
    //        rslt
    //      }
    //    )


    //retailDf.select(col("country")).distinct().select(collect_list("country")).show(false)
    // retailDf.select("country").distinct().select(d(collect_list("country")))
    /**
     * Grouping:
     * which we group our data on one column and perform some calculations on the other columns
     * that end up in that group.
     * First we specify the column(s) on which we would like to
     * group, and then we specify the aggregation(s). The first step returns a
     * RelationalGroupedDataset, and the second step returns a DataFrame
     */


    finalDf.groupBy("InvoiceNo", "CustomerId")
    //.count().show()

    finalDf.groupBy("invoiceno", "customerid")
    //.count().orderBy(col("invoiceno"),col("customerid")).show()




    finalDf.groupBy("InvoiceNo").agg(
      count("Quantity").alias("quan"),
      expr("count(Quantity)"))
    //.show()

    finalDf.groupBy("InvoiceNo").agg("Quantity" -> "avg", "Quantity" -> "stddev_pop")
    //.show()


    //Window function

    import org.apache.spark.sql.expressions.Window

    val t = Window.partitionBy(col("department")).orderBy(col("salary"))

    val testUdf = udf((a: String, b: String) => {
      if (a == "2") {
        b
      } else {
        ""
      }
    })

    df2.withColumn("rowNum", row_number.over(t))
      .withColumn("rank", rank.over(t))
      .withColumn("densRank", dense_rank.over(t))
      .withColumn("second_highesh_sal", testUdf(col("densRank"), col("salary")))
    //.show(false)


    //partion aggregation

    val win = Window.partitionBy(col("department"))

    df2.withColumn("rownumber", row_number().over(t)) //because it requires order by clause
      .withColumn("avgSal", avg(col("salary")).over(win))
      .withColumn("max", max(col("salary")).over(win))
      .withColumn("min", min(col("salary")).over(win))
      .where("rownumber == 1")
      //.show(false)


    df2.groupBy("department").agg(
      avg("salary"),
      min("salary"),
      max("salary")
    )//.show()

    //name the employee whose having min salary in each department

    df2.createTempView("t")


    spark.sql(
      """
        |select j.employee_name,j.department,j.salary from
        |(select employee_name,department,salary,
        |row_number() over( partition by department order by salary) rnk from t) j where j.rnk=1
        |""".stripMargin)
      //.show(false)



//Q. we would like to get the  total quantity of all stock codes and customers


    finalDf.na.drop("any").groupBy(col("CustomerID"),col("StockCode"))
      .agg(
        sum(col("quantity"))
      ).show(50,false)

    println("=" * 10 + "End of the Aggregation" + "=" * 10)


  }

}
