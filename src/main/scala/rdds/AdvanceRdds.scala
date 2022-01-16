package com.suanki
package rdds

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Row

object AdvanceRdds {

  def code(spark: SparkSession, df: (Dataset[Row], Dataset[Row])) = {

    /**
     * this file is newly added in rdds branch
     */
    //df declarations
    val df1 = df._1 //market data
    val df2 = df._2 //sample data
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Spark Simple".split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2) //Array(String)
    val textRdd = spark.sparkContext.textFile("C:\\Users\\sujee\\OneDrive\\Documents\\bigdata_and_hadoop\\databricks_udemy\\data\\fligt_data\\2010-summary.csv")

    val pairWord = words.map(f => (f, 1))
    pairWord.sortByKey()

    val keyword = words.keyBy(f => f(0))
    val mapword = keyword.mapValues(f => f.toUpperCase())

    //or
    //pairWord.mapValues(f=>f).foreach(println)

    mapword.foreach(println)
    keyword.flatMapValues(f => f.toUpperCase())


    val chaars = words.flatMap(f => f)
    val kvChar = chaars.map(f => (f, 1))

    def addFun(x: Int, y: Int) = x + y

    def maxFun(x: Int, y: Int): Int = math.max(x, y)

    //println(kvChar.countByKey())
    //groupbykey require more exevutor memory b/c it has to bring all the value in memory related to key to apply the logic
    //where as reducebykeey will calculate only value for the each partition and finally reduce it

    kvChar.groupByKey().map(f => (f._1, f._2.reduce(addFun)))
    //.foreach(println)
    kvChar.reduceByKey(addFun)
    //.foreach(println)


    //cogroup









    //Q1. find no of times India comes as in destination in textRdd

    val rslt1 = textRdd.map(f => f.split(",")).filter(f => f(0) != "DEST_COUNTRY_NAME")
      .map(f => (f(0), 1)).reduceByKey(_ + _).filter(f => f._1.toLowerCase() == "india")

    //Q1. find where source is united state and destination is outside united state and frequency is more than 10


    val rslt2 = textRdd.map(f => f.split(",")).filter(f => f(0) != "DEST_COUNTRY_NAME")
      .map(f => (f(1), f(0))).filter(f => f._1 == "United States" && (if (f._2 != "United States") true else false))

    //Understating aggregations


  }
}
