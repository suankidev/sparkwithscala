package com.suanki
package dataframes

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{array_contains, coalesce, col, current_date, current_timestamp, date_sub, datediff, initcap, instr, lit, lower, lpad, ltrim, monotonically_increasing_id, not, pow, rpad, rtrim, substring, to_date, to_timestamp, trim, upper}


object WorkingWithDifferentTypeChapter6 {


  def lesson(spark: SparkSession, df: Dataset[Row], retailDf: Dataset[Row], mydataFrame: Dataset[Row]): Unit = {

    /**
     * Dataset submodules like DataFrameStatFunctions and DataFrameNaFunctions have more
     * methods that solve specific sets of problems.
     */

    val c = df.withColumn("test", col("origin").contains("India"))

    //c.where("test")show(50,false)

    /**
     * the lit function. This function converts a
     * type in another language to its correspnding Spark representation.
     */

    val d = df.withColumn("one", lit(5)).withColumn("two", lit("two"))

    //d.show(5, false)


    df.createTempView("flighttab")

    //spark.sql("""select *, 1 as one from flighttab""".stripMargin).show(10,false)


    /**
     * Working with Boolean
     */

    /**
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


    val a = retailDf.where(col("InvoiceNo") === 536365)
    //  a.show(5,false)

    val listOfCustomer = List(63864, 463846, 346386, 15987)


    //retailDf.na.drop("any").where(col("customerid").isin(list =listOfCustomer))

    retailDf.where((col("customerid") === 237937).alias("customer"))

    val pricefilt = col("UnitPrice") > 300
    val filt2 = col("description").contains("GLASS")
    val filt3 = col("quantity") > 6
    //val filt4=col("quntity").isin(List(2,6,8,10))

    val filt5 = not(col("customerid") === 23263)
    retailDf.where(pricefilt and filt2).where(filt3)

    //can also specify column

    val DOTCodeFilter = col("StockCode") === "DOT"
    val priceFilter = col("UnitPrice") > 600
    val descripFilter = col("Description").contains("POSTAGE")
    val descripFilter1 = instr(col("Description"), "POSTAGE") >= 1


    retailDf.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive")

    retailDf.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter))

    val l = List("25372", "36826", "23826", "23826")

    val t1 = retailDf.withColumn("checkList", (col("stockCode").isin("25372", "36826", "23826", "23826")))

    val t2 = retailDf.withColumn("checkList", (col("stockCode").isin(l: _*)))

    //t1.show(10,false)


    /**
     * working with numbers
     */


    val retaildf1 = retailDf.withColumn("totalPrice", pow(col("quantity") * col("unitprice"), 2))

    /**
     * working with strings
     *
     * initcap
     * ltrim
     * rtrim
     * rpad
     * lpad
     *
     */


    retailDf.select(col("Description"),
      lower(col("Description")),
      upper(lower(col("Description"))))


    retailDf.select(
      ltrim(lit(" HELLO ")).as("ltrim"),
      rtrim(lit(" HELLO ")).as("rtrim"),
      trim(lit(" HELLO ")).as("trim"),
      lpad(lit("HELLO"), 3, " ").as("lp"),
      rpad(lit("HELLO"), 10, " ").as("rp"))


    /**
     * regexp_extract and regexp_replace. These
     * functions extract values and replace values, respectively.
     */

    import org.apache.spark.sql.functions.{regexp_extract, regexp_replace, translate}

    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val regexString = simpleColors.map(word => word.toUpperCase).mkString("|")

    val regexString2 = simpleColors.map(word => word.toUpperCase).mkString("(", "|", ")")

    val test = retailDf.withColumn("regexp_replace", regexp_replace(col("Description"), regexString, "COLOR"))
      .withColumn("translate", translate(col("description"), "GREEN", "REDHAT"))

    // test.show(30,false)

    val test2 = retailDf.select(
      regexp_extract(col("Description"), regexString2, 1).alias("color_clean"), col("Description"))
    // test2.show(50, false)

    /**
     * Building this as a
     * regular expression could be tedious, so Spark also provides the translate function to replace these
     * values.
     */


    /** *
     * Working with Dates and Timestamps
     */

    val datedf = spark.range(10).toDF("serial")

    val datedf1 = datedf.withColumn("timestamp", lit(current_timestamp()))
      .withColumn("date", lit(current_date())).withColumn("date-7", date_sub(col("date"), 7))
      .withColumn("date-diff", datediff(col("date-7"), col("date")))
      .withColumn("date-format", to_date(col("date"), "DD-MM-YYYY"))

    //datedf1.show(10,truncate = false)


    val dateFormat = "yyyy-dd-MM"

    val cleanDateDF = spark.range(5).select(
      to_date(lit("2017-12-11"), dateFormat).alias("date"),
      to_date(lit("2017-20-12"), dateFormat).alias("date2")
    )

    cleanDateDF.createOrReplaceTempView("dateTable2")


    //cleanDateDF.show()

    //to_timestamp, which always requires a format to be specified:

    cleanDateDF.select(to_timestamp(col("date"), dateFormat)) //.show()


    //compare date

    val cleanDateDF1 = cleanDateDF.select(col("date") > lit("2020-11-20")) //.show()



    //Working with null data


    //retailDf.show(5, false)


    //ifnull, nullIf, nvl, and nvl2


    val retailDfDealingWithNulls = retailDf.select(
      coalesce(col("CustomerID"), col("country")).alias("Coalesce")
    )


    retailDf.createTempView("retailtable")

    /**
     * you could
     * nullif, which returns null if the two values are equal or else returns the second if they are not.
     * nvl returns the second value if the first is null, but defaults to the first.
     * nvl2 returns the second value if the first is not null; otherwise, it will return the last specified value
     * (else_value in the following example):
     */

    val reatildfwithsql = spark.sql(
      """ select nvl(customerid,country) as a ,
        |  ifnull(null,country) as b,
        |  nullif(customerid,country) as c,
        | nvl2(null,customerid,country) as d
        |  from retailtable limit 10""".stripMargin)

    //reatildfwithsql.show(false)
    mydataFrame.show(false)

    /**
     * +---------+----------+---------+----+
     * |name     |mobile    |district |roll|
     * +---------+----------+---------+----+
     * |Surendra |9860867225|Rajasthan|9000|
     * |Sujeet   |7355609142|Gorakhpur|null|
     * |Aaditya  |3736833333|null     |3648|
     * |trakashur|null      |null     |null|
     * +---------+----------+---------+----+
     */
import org.apache.log4j.Logger
   // val mylog=Logger.getLogger("stdout")

    //drop a row if all column having null value
    mydataFrame.na.drop("all")//.show(false)

    //drop a row if any column value is null
    mydataFrame.na.drop("any")//.show(false)


    //drop only if mention column is null
    mydataFrame.na.drop("all", Seq("district","roll"))//.show()


    /**
     * you can fill one or more columns with a set of values
     */


    //fill all value
//it will replace only string column not interger column
    mydataFrame.na.fill("this was a null").na.fill(0)
      .show()

    //mydataFrame.na.fill(5)

val fillcalvalue=Map("name" -> "Uknown","mobile" -> "Uknown","district" -> "Uknown", "roll" -> 0)

    mydataFrame.na.fill(fillcalvalue).show(false)


    //better to fill the value
   // mydataFrame.na.replace("roll", Map("0" -> "1" )).show()



  }


}
